# atoa.call.action.resolver
import json
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, List, Callable, Sequence, Optional
from rich.text import Text

from arch.xor.parser.action import ActionSchemaCompiler, DEFAULT
from eco.call.disc.action import Observation
from eco.call.disc.tool import Tool
from eco.call.disc.action import Action

from atoa.call.action.factory import MessageIntent, TopologicalIntent, CoreAction, ActionProxy, build_action

if TYPE_CHECKING:
    from atoa.agent.disc.base.conv import ProtoConv
    from atoa.gov.context.protocol import ConvStateProtocol
    from atoa.call.action.definition import ActionDefinition

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

ResolverCallable = Callable[[dict[str, Any], "ConvStateProtocol"], Sequence['ActionDefinition']]

class ActionResolver:
    _LOCK = RLock()
    
    # Schema-driven Registry (JSON Configs & Proxies)
    _ROUTES: Dict[str, Dict[str, Any]] = {}
    _INSTANCES: Dict[str, ActionProxy] = {}
    
    # Custom Callable Registry
    _RESOLVERS: Dict[str, ResolverCallable] = {}

    # ---------------------------------------------------
    # [A] Schema-driven Pipeline (Dynamic Actions)
    # ---------------------------------------------------
    @classmethod
    def load_from_json(cls, file_path: str | Path) -> None:
        """JSON 설정 파일을 읽어 런타임 파이썬 객체로 컴파일 후 레지스트리에 확장(Extend)/적재합니다."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                raw_json = json.load(f)
                
            compiled_routes = ActionSchemaCompiler.compile_routes(raw_json)
            for route, config in compiled_routes.items():
                cls.register_route(route, config, override=True)
            log.info(f"Loaded and registered {len(compiled_routes)} actions from {file_path}")
        except Exception as e:
            log.error(f"Failed to load actions from {file_path}: {e}")

    @classmethod
    def register_route(cls, route: str, config: Dict[str, Any], override: bool = False) -> None:
        """JSON 스키마 기반의 동적 라우트를 등록합니다."""
        with cls._LOCK:
            if route in cls._ROUTES and not override:
                raise ValueError(f"Action route '{route}' already exists. Set override=True to replace.")
            cls._ROUTES[route] = config
            if route in cls._INSTANCES:
                del cls._INSTANCES[route]

    @classmethod
    def get_proxy(cls, route: str) -> ActionProxy:
        """스키마 기반 라우트의 ActionProxy를 지연(Lazy) 생성하여 반환합니다."""
        with cls._LOCK:
            if route in cls._INSTANCES:
                return cls._INSTANCES[route]
                
            config = cls._ROUTES.get(route)
            if not config:
                raise KeyError(f"[ActionResolver] Unrecognized action route: '{route}'. Ensure schema is loaded.")
                
            proxy = build_action(
                name=route,
                description=config["description"],
                fields=config["fields"],
                handler=lambda act, cv, obs: cls.handle(route, act, cv, obs),
                action_visualizer=VISUALIZERS.get(route),
                obs_fields=config.get("obs_fields"),
                hide_observation=config.get("hide_observation", False),
                annotations=config.get("annotations")
            )
            cls._INSTANCES[route] = proxy
            return proxy

    @classmethod
    def handle(cls, route: str, action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
        """동적 도구의 실행(Execution)을 HANDLERS 딕셔너리로 디스패치합니다."""
        handler_fn = HANDLERS.get(route)
        if not handler_fn:
            return ObsClass.from_text(text=f"Execution Error: No handler defined for route '{route}'.", is_error=True)
        return handler_fn(action, conv, ObsClass)

    @classmethod
    def list_routes(cls) -> List[str]:
        with cls._LOCK:
            return list(cls._ROUTES.keys())

    # ---------------------------------------------------
    # [B] Custom Callable Pipeline & Unified Resolution
    # ---------------------------------------------------
    @classmethod
    def register(cls, name: str, resolver: ResolverCallable) -> None:
        """터미널이나 MCP 도구 등 커스텀 람다/함수를 직접 등록합니다."""
        with cls._LOCK:
            cls._RESOLVERS[name] = resolver

    @classmethod
    def resolve(cls, tool_spec: Tool, conv_state: "ConvStateProtocol") -> Sequence['ActionDefinition']:
        """
        엔진에서 액션/툴을 요청할 때 단일 진입점으로 작동합니다.
        1. 커스텀 _RESOLVERS (terminal, mcp 등) 우선 확인
        2. 동적 스키마 _ROUTES 확인
        """
        with cls._LOCK:
            # 1. Custom Callable (Physical Tools)
            if tool_spec.name in cls._RESOLVERS:
                return cls._RESOLVERS[tool_spec.name](tool_spec.params, conv_state)
            
            # 2. Dynamic Schema Proxy (Cognitive Actions)
            if tool_spec.name in cls._ROUTES:
                proxy = cls.get_proxy(tool_spec.name)
                return proxy.create(conv_state=conv_state, **(tool_spec.params or {}))
                
        raise KeyError(f"ActionDefinition '{tool_spec.name}' is not registered in the system.")


def _bootstrap_core_actions():
    compiled_defaults = ActionSchemaCompiler.compile_routes(DEFAULT)
    for route, config in compiled_defaults.items():
        ActionResolver.register_route(route, config, override=True)
    log.debug(f"[ActionResolver] Bootstrapped {len(compiled_defaults)} core actions.")

_bootstrap_core_actions()

def _handle_finish(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    from atoa.agent.disc.status import ConverStatus
    from atoa.gov.context.command import TransitionStatus
    
    state = getattr(conv, "state", None) if conv else None
    msg = f"Task marked as finished: {action.summary}" if getattr(action, "summary", None) else "Task marked as finished."
    
    if state: 
        state.apply(TransitionStatus(new_status=ConverStatus.FINISHED, reason="Action: FINISH executed"))
        
    return ObsClass.from_text(text=msg)


def _handle_think(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    return ObsClass.from_text(text="Your thought has been logged.")


def _handle_lang(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    from atoa.agent.disc.status import ConverStatus
    from atoa.gov.context.command import TransitionStatus
    
    state = getattr(conv, "state", None) if conv else None
    msg = "Message successfully delivered to the user."
    
    if state:
        if action.intent == MessageIntent.CLARIFY:
            msg += " System paused. Waiting for user input..."
            state.apply(TransitionStatus(new_status=ConverStatus.WAITING_FOR_USER, reason="Action: LANG (CLARIFY)"))
        elif action.intent == MessageIntent.SUMMARY:
            msg += " Task marked as complete. Terminating execution loop."
            state.apply(TransitionStatus(new_status=ConverStatus.FINISHED, reason="Action: LANG (SUMMARY)"))
            
    return ObsClass.from_text(text=msg)


def _handle_bridge(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    from atoa.agent.disc.status import ConverStatus
    from atoa.agent.disc.event.llm.observation import ObservationEvent
    from arch.contract.event.next import next_id
    from atoa.gov.context.command import TransitionStatus, UpdateAgentState

    state = getattr(conv, "state", None) if conv else None
    msg = "Bridge initiated."
    kwargs = {"routing_intent": action.intent, "requires_halt": True}
    
    if state:
        is_critical = action.tension_level and action.tension_level >= 4
        if is_critical or action.intent != TopologicalIntent.REPLAN:
            msg = f"Execution Halted. System taking control for routing: {action.intent.value.upper()}"
            
            # 1. 상태 전이 Command 발행
            state.apply(TransitionStatus(new_status=ConverStatus.NEEDS_REPLAN, reason=f"Action: BRIDGE ({action.intent.value})"))
            
            # 2. 에이전트 런타임 상태 업데이트 Command 발행 (직접 병합 방지)
            route_data = {
                "intent": action.intent.value,
                "tension": action.tension_level,
                "target_aspects": action.target_aspects,
                "original_thought": action.thought
            }
            state.apply(UpdateAgentState(key="pending_route", value=route_data))
            
        else:
            msg = "System Note: Intent logged. Continuing current loop."
            kwargs["requires_halt"] = False
            if hasattr(state, "inject_virtual_event"):
                bridge_id = f"bridge-{next_id()}"
                overlay_msg = f"## @topos.intent: {action.intent.value}"
                if action.target_aspects: overlay_msg += f" | @target.aspects: {', '.join(action.target_aspects)}"
                if action.tension_level: overlay_msg += f" | @cognitive.tension: {action.tension_level}/5"
                
                state.inject_virtual_event(ObservationEvent(
                    id=bridge_id, action_id="system-orchestrator", tool_name="bridge",
                    tool_call_id=f"virtual-call-{next_id()}",
                    observation=ObsClass.from_text(text=overlay_msg, routing_intent=action.intent, requires_halt=False)
                ))
                msg += "\n(Phase hints have been securely overlaid on your context.)"
                
    return ObsClass.from_text(text=msg, **kwargs)


def _handle_signal(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    from atoa.agent.disc.status import ConverStatus
    from atoa.gov.context.command import TransitionStatus
    
    state = getattr(conv, "state", None) if conv else None
    msg = (
        f"[Semantic Telemetry 📡] Broadcasted to '{action.channel}'.\n"
        f"Target Audience: {action.audience.upper()}\n"
        f"Translation: {action.semantic_translation}"
    )
    if action.requires_consensus and state:
        msg += "\n[Status] System paused. Waiting for human consensus (Merge/Approval)."
        state.apply(TransitionStatus(new_status=ConverStatus.WAITING_FOR_USER, reason="Action: SIGNAL (Requires Consensus)"))
        
    return ObsClass.from_text(text=msg)


HANDLERS: Dict[str, Callable] = {
    CoreAction.FINISH.value: _handle_finish,
    CoreAction.THINK.value: _handle_think,
    CoreAction.LANG.value: _handle_lang,
    CoreAction.BRIDGE.value: _handle_bridge,
    CoreAction.SIGNAL.value: _handle_signal,
}


VISUALIZERS: Dict[str, Callable[[Any], Text]] = {
    CoreAction.FINISH.value: lambda act: Text(f"🏁 Finish: {getattr(act, 'summary', 'Task Complete')}", style="bold green"),
    CoreAction.THINK.value: lambda act: Text(f"🤔 Thinking: \n{act.thought}", style="italic white"),
    CoreAction.LANG.value: lambda act: Text(f"💬 Lang [{act.intent.value.upper()}]: \n{act.message}", style="cyan"),
    CoreAction.BRIDGE.value: lambda act: Text(f"🌉 Bridge [{act.intent.value.upper()}]\n{act.thought}", style="cyan"),
    CoreAction.SIGNAL.value: lambda act: Text(f"📡 Signal [{act.channel}]: {act.semantic_translation}", style="bold magenta"),
}