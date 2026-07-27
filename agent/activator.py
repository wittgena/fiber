# agent.activator
from __future__ import annotations
from abc import ABC, abstractmethod
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Any, Optional
from pathlib import Path
from pydantic import Field, ValidationError, model_validator

from atoa.schema.action import Action, Observation
from eco.tenant.conv.event import Event
from eco.tenant.conv.message import Message, MessageToolCall, ReasoningItemModel, RedactedThinkingBlock, TextContent, ThinkingBlock

from bound.parser.conv.action import format_context_exceeded_message, ActionParser

from agent.disc.ator import Ator
from atoa.event.llm.action import ActionEvent
from atoa.event.llm.message import MessageEvent
from atoa.event.llm.system import SystemPromptEvent, TokenEvent
from atoa.event.llm.observation import ObservationEvent, UserRejectObservation, AgentErrorEvent
from agent.disc.status import ConverStatus
from agent.driver.llm.response import LLMResponse

from agent.handler.step import StepHandler, StepContext
from agent.handler.graph.loop import LLMInvocationHandler, ToolCallHandler, TextResponseHandler
from agent.handler.tension import TensionHandler
from agent.handler.graph.eval import EvalReflector
from agent.disc.reflect.evaluator import ActionEvaluator 

from agent.handler.graph.organizer import DagOrganizer
from arch.gov.state.compiler import StateCompiler
from arch.gov.state.projector import StateProjector
from arch.gov.state.schema import FragmentSig

from arch.topos.bound.tunnel import UniversalFacade
from watcher.plane.observer.span import observe, should_enable_observability
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)
INIT_STATE_PREFIX_SCAN_WINDOW = 3

class AgentStateSnapshot:
    def __init__(self, task_payload: dict):
        self.conversation_id = task_payload.get("conversation_id")
        self.iteration = task_payload.get("iteration", 0)
        raw_events = task_payload.get("events", [])
        self.events = []
        for e in raw_events:
            parsed = self._parse_event(e)
            if parsed:
                self.events.append(parsed)

    def _parse_event(self, data: dict) -> Event | None:
        """Robust event reconstruction parser"""
        try:
            if "system_prompt" in data:
                return SystemPromptEvent.model_validate(data)
            elif "action" in data and "tool_name" in data and "parameters" in data.get("action", {}):
                return ActionEvent.model_validate(data)
            elif "observation" in data and "tool_name" in data:
                return ObservationEvent.model_validate(data)
            elif "llm_message" in data:
                return MessageEvent.model_validate(data)
            elif "rejection_reason" in data:
                return UserRejectObservation.model_validate(data)
            elif "error" in data and "tool_name" in data:
                return AgentErrorEvent.model_validate(data)
            return Event.model_validate(data)
        except Exception as e:
            log.warning(f"Failed to parse event dict into Pydantic model: {e}")
            return None

class Activator(Ator):
    """
    @desc: DAG and dynamic topology-based cognitive orchestrator.
    """
    step_handlers: list[StepHandler] = Field(default_factory=list, exclude=True)
    is_graph_mode: bool = Field(default=False, exclude=True)
    dag_materials: Dict[str, Any] = Field(default_factory=dict, exclude=True)
    evaluator: ActionEvaluator | None = Field(default=None, exclude=True)

    def model_post_init(self, __context: object) -> None:
        super().model_post_init(__context)
        self.step_handlers = self._build_default_handlers()
        if self.reflector:
            self.evaluator = ActionEvaluator(self.reflector)

    def _build_default_handlers(self) -> list[StepHandler]:
        return [
            TensionHandler(),
            EvalReflector(),
            LLMInvocationHandler(),
            ToolCallHandler(),
            TextResponseHandler(),
        ]
    
    def set_step_handlers(self, handlers: list[StepHandler]) -> None:
        self.step_handlers = handlers
        self.is_graph_mode = False
    
    def mount_graph_topology(self, runtime_specs: dict, entry_point: str, telemetry_path: Optional[Path] = None) -> None:
        log.info(f"[EVOLUTION] Mounting Graph Topology. Anchor: {entry_point}")
        graph_handler = DagOrganizer(
            runtime_specs=runtime_specs,
            entry_point=entry_point,
            telemetry_path=telemetry_path
        )
        self.step_handlers = [graph_handler]
        self.is_graph_mode = True

    def is_running_in_graph_mode(self) -> bool:
        return self.is_graph_mode
    
    def harvest_and_evolve_graph(self, raw_schema: Dict[str, Any], telemetry_path: Optional[Path] = None) -> bool:
        log.info("[EVOLUTION] Synthesizing graph substrate...")
        try:
            compiler = StateCompiler()
            ir_sig: FragmentSig = compiler.compile_from_schema(raw_schema)
            projector = StateProjector()
            runtime_specs = projector.project(ir_sig)
            
            self.dag_materials.update({
                "ir_signature": ir_sig,
                "runtime_specs": runtime_specs,
                "evolution_status": "success"
            })
            self.mount_graph_topology(runtime_specs, ir_sig.entry_point, telemetry_path)
            return True
        except Exception as e:
            log.error(f"[EVOLUTION:RUPTURE] Topos collapse: {e}")
            self.dag_materials.update({
                "evolution_status": "failed",
                "failure_fragments": str(e),
                "raw_schema_dump": raw_schema
            })
            return False

    @model_validator(mode="before")
    @classmethod
    def _enforce_security_prompt(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        ctx = data.setdefault("agent_context", {})
        if isinstance(ctx, dict):
            kwargs = ctx.setdefault("system_prompt_kwargs", {})
            kwargs.setdefault("llm_security_analyzer", True)
        elif hasattr(ctx, "system_prompt_kwargs"):
            ctx.system_prompt_kwargs.setdefault("llm_security_analyzer", True)
        return data

    def _generate_system_prompt_event(self, secret_infos: list) -> SystemPromptEvent:
        has_browser = any(t.name == "browser" for t in self.tools)
        canonical_name = getattr(self.llm, "model_canonical_name", None)

        static_msg = self.agent_context.get_static_system_message(
            llm_model=self.llm.model,
            llm_model_canonical=canonical_name,
            has_browser_tool=has_browser
        )
        dynamic_msg = self.agent_context.get_system_message_suffix(
            llm_model=self.llm.model,
            llm_model_canonical=canonical_name,
            additional_secret_infos=secret_infos,
        )

        return SystemPromptEvent(
            source="agent",
            system_prompt=TextContent(text=static_msg),
            tools=list(self.tools_map.values()),
            dynamic_context=TextContent(text=dynamic_msg) if dynamic_msg else None,
        )

    @observe(name="activator.process_task")
    async def process_task(self, task_payload: dict, tunnel: UniversalFacade, response_topic: str) -> None:
        log.info(f"[{self.name}] Initiating async step sequence for task iteration {task_payload.get('iteration')}")
        
        snapshot = AgentStateSnapshot(task_payload)
        context = StepContext()

        if snapshot.iteration == 0:
            sys_event = self._generate_system_prompt_event(secret_infos=task_payload.get("secret_infos", []))
            snapshot.events.insert(0, sys_event)
            await self._emit_event_to_gov(sys_event, tunnel, response_topic, snapshot)

        async def async_on_event(event: Any):
            # [개선 1] Event 모델을 상속받은 객체만 events 리스트에 추가 (TransitionStatus 방어)
            if isinstance(event, Event):
                snapshot.events.append(event)
            await self._emit_event_to_gov(event, tunnel, response_topic, snapshot)

        for handler in self.step_handlers:
            if hasattr(handler, "handle_async"):
                handled = await handler.handle_async(self, snapshot, async_on_event, context)
            else:
                handled = handler.handle(self, snapshot, lambda e: asyncio.create_task(async_on_event(e)), None, context)
                
            if handled:
                break

    async def _emit_event_to_gov(self, event: Any, tunnel: UniversalFacade, response_topic: str, snapshot: Optional[AgentStateSnapshot] = None) -> None:
        from arch.topos.bound.payload import StreamPayloadAdapter 
        
        payload_raw = None
        current_topo = len(snapshot.events) if snapshot else 0
        current_tick = snapshot.iteration if snapshot else 0
        
        if isinstance(event, ActionEvent):
            current_press = getattr(event, "completion_tokens", 50) 
            payload_raw = {
                "type": "action",
                "event_payload": event.model_dump(mode="json"),
                "_telemetry": {"topo": current_topo, "press": current_press, "rupture": False, "tick": current_tick}
            }
            log.debug(f"[{self.name}] Emitted Action: {event.tool_name}")
        elif isinstance(event, MessageEvent):
            current_press = getattr(event, "completion_tokens", 20)
            payload_raw = {
                "type": "message",
                "event_payload": event.model_dump(mode="json"),
                "_telemetry": {"topo": current_topo, "press": current_press, "rupture": False, "tick": current_tick}
            }
            log.debug(f"[{self.name}] Emitted Message Event")
        elif isinstance(event, SystemPromptEvent):
            payload_raw = {
                "type": "system_prompt",
                "event_payload": event.model_dump(mode="json"),
                "_telemetry": {"topo": current_topo, "press": 0, "rupture": False, "tick": current_tick}
            }
        elif isinstance(event, AgentErrorEvent):
            payload_raw = {
                "type": "error",
                "event_payload": event.model_dump(mode="json"),
                "_telemetry": {"topo": current_topo, "press": 0, "rupture": True, "tick": current_tick}
            }
            log.debug(f"[{self.name}] Emitted AgentErrorEvent")

        if getattr(event, "is_finish_signal", False) or type(event).__name__ == "TransitionStatus":
            payload_data = event.model_dump(mode="json") if hasattr(event, "model_dump") else str(event)
            payload_raw = {
                "type": "finish", 
                "event_payload": payload_data,
                "_telemetry": {
                    "topo": current_topo, 
                    "press": 0, 
                    "rupture": True,
                    "tick": current_tick
                }
            }
            log.debug(f"[{self.name}] Emitted Finish/Transition Signal (Loop Breaker)")

        if payload_raw:
            await tunnel.stream_produce(
                response_topic, 
                payload=StreamPayloadAdapter.encode(payload_raw)
            )

    def _get_action_event(
        self,
        tool_call: MessageToolCall,
        llm_response_id: str,
        snapshot: AgentStateSnapshot,
        security_analyzer: Any = None,
        thought: list[TextContent] | None = None,
        reasoning_content: str | None = None,
        thinking_blocks: list[ThinkingBlock | RedactedThinkingBlock] | None = None,
        responses_reasoning_item: ReasoningItemModel | None = None,
    ) -> tuple[ActionEvent | None, Event | None]:
        
        action_event, error_event = ActionParser.parse_tool_call(
            tool_call=tool_call,
            tools_map=self.tools_map,
            llm_response_id=llm_response_id,
            security_analyzer=security_analyzer,
            thought=thought,
            reasoning_content=reasoning_content,
            thinking_blocks=thinking_blocks,
            responses_reasoning_item=responses_reasoning_item,
        )

        if error_event:
            return None, error_event

        if self.evaluator and self.evaluator.should_evaluate(action_event.tool_name):
            reflector_result = self.evaluator.evaluate(snapshot, action_event) 
            if reflector_result:
                action_event = action_event.model_copy(update={"reflector_result": reflector_result})

        return action_event, None

    def _maybe_emit_vllm_tokens(self, llm_response: LLMResponse) -> TokenEvent | None:
        if self.llm.brane_extra_body.get("return_token_ids"):
            return TokenEvent(
                source="agent",
                prompt_token_ids=llm_response.raw_response["prompt_token_ids"],
                response_token_ids=llm_response.raw_response["choices"][0]["provider_specific_fields"]["token_ids"],
            )
        return None

    def _log_context_window_exceeded_warning(self) -> None:
        warning_msg = format_context_exceeded_message(self.llm.model)
        log.warning(f"[BOUND] Topos volume exceeded: {warning_msg}")