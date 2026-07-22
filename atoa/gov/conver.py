# atoa.gov.conver
import json
import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from eco.agent.action.message import Message, TextContent
from eco.agent.disc.action import Action, Observation

from watcher.plane.observer.span import observe
from watcher.plane.emitter import get_emitter

from atoa.agent.parser import render_template
from atoa.agent.disc.event.llm.message import MessageEvent
from atoa.agent.disc.event.llm.action import ActionEvent
from atoa.agent.disc.event.llm.observation import ObservationEvent, UserRejectObservation, AgentErrorEvent
from atoa.agent.disc.event.llm.system import SystemPromptEvent

from atoa.agent.disc.event.conv.error import ConversationErrorEvent
from atoa.agent.disc.event.conv.pause import PauseEvent
from atoa.agent.disc.status import ConverStatus
from atoa.gov.exception.conv.connection import ConversationRunError

from atoa.agent.driver.tensor import Driver
from atoa.agent.action.factory import CoreAction

from atoa.conv.context.command import TransitionStatus
from atoa.conv.state import ConversationState
from atoa.conv.parser.title import generate_conversation_title
from atoa.conv.parser.builder import MessageBuilder, LLMFacade

from arch.xor.store.file import LocalFileStore
from bound.xor.store.log import LogStore
from arch.topos.bound.payload import StreamPayloadAdapter
from arch.topos.bound.tunnel import TunnelFactory, UniversalFacade

if TYPE_CHECKING:
    from atoa.conv.wrapper import ConvContext
    convType = ConvContext | Any
else:
    convType = Any

log = get_emitter("executor.conver")
MAX_ABSOLUTE_ITERATIONS = 7


class AgentSessionManager:
    """
    @desc: 대화 상태와 연관된 메타데이터를 관리합니다. 
           Agent 객체를 직접 제어하지 않고, 터널(Redis)을 통해 제어 명령을 브로드캐스트합니다.
    """
    def __init__(self, conv: convType):
        self.conv = conv

    async def switch_profile(self, profile_name: str) -> None:
        tunnel = await TunnelFactory.get_default()
        conv_id_str = str(self.conv.id)
        control_channel = f"agent_control:{conv_id_str}"
        command_payload = {
            "command": "switch_profile",
            "profile_name": profile_name,
            "conversation_id": conv_id_str
        }
        
        await tunnel.publish(control_channel, json.dumps(command_payload))
        log.info(f"[SessionManager] Requested Agent profile switch to: {profile_name} via {control_channel}")


class AgentSidecar:
    """
    @desc: 대화의 메인 루프(Conver) 밖에서 일어나는 부가 기능(타이틀 생성, 메타 질문 등)을 담당합니다.
           무상태 Agent(Activator)를 깨우지 않고 Gov에 등록된 LLMFacade만을 사용해 가볍게 동작합니다.
    """
    def __init__(self, conv: convType, session_manager: AgentSessionManager):
        self.conv = conv
        self.session = session_manager

    def _get_fallback_llm(self) -> Driver:
        registry = getattr(self.conv, "llm_registry", None)
        if registry:
            return registry.get_default()
        
        config_llm = self.conv.state.agent_config.get("llm") if hasattr(self.conv.state, "agent_config") else None
        if config_llm and isinstance(config_llm, dict):
            return Driver(**config_llm)
        return Driver(model="gpt-4o") 

    def ask(self, question: str) -> str:
        template_dir = Path(__file__).parent.parent.parent / "context" / "prompts" / "templates"
        question_text = render_template(str(template_dir), "ask_agent_template.j2", question=question)

        user_message = Message(role="user", content=[TextContent(text=question_text)])
        messages = MessageBuilder.prepare_llm_messages(
            self.conv.state.events, additional_messages=[user_message]
        )

        llm_to_use = self._get_fallback_llm().model_copy(update={"usage_id": "ask-agent-llm"}, deep=True)
        available_tools = list(getattr(self.conv, "tools", {}).values())

        response = LLMFacade.make_completion(
            llm=llm_to_use, 
            messages=messages, 
            tools=available_tools
        )
        
        message = response.message
        if message.content and len(message.content) > 0:
            for content in message.content:
                if isinstance(content, TextContent):
                    return content.text

        raise Exception("Failed to generate answer via Sidecar")

    @observe(name="sidecar.generate_title", ignore_inputs=["llm"])
    def generate_title(self, llm: Driver | None = None, max_length: int = 50) -> str:
        llm_to_use = llm or self._get_fallback_llm()
        if llm_to_use.model == "acp-managed":
            llm_to_use = None
        return generate_conversation_title(events=self.conv.state.events, llm=llm_to_use, max_length=max_length)


class Conver:
    """
    @desc: 대화(Flow)의 신체이자 환경(Environment)을 관장하는 핵심 오케스트레이터입니다.
           - Agent(무상태 두뇌)에게 현재 상태를 전송(Produce)
           - Agent의 결정을 수신(Consume)하여 물리적 도구를 실행(Execute)
           - 모든 결과를 Context(상태)에 기록
    """
    def __init__(self, conversation: convType):
        self.conv = conversation
        self.session = AgentSessionManager(self.conv)
        self.sidecar = AgentSidecar(self.conv, self.session)

    @observe(name="conver.run")
    async def run(self) -> None:
        """이벤트 기반 비동기 제어 루프"""
        if self.conv.state.execution_status in [
            ConverStatus.IDLE, ConverStatus.PAUSED,
            ConverStatus.ERROR, ConverStatus.STUCK,
        ]:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.RUNNING, reason="Engine loop started"))

        tunnel = await TunnelFactory.get_default()
        conv_id_str = str(self.conv.id)
        agent_task_topic = f"agent:tasks:{conv_id_str}"
        agent_response_topic = f"agent:responses:{conv_id_str}"
        consumer_group = "gov_orchestrator"

        iteration = 0
        try:
            while True:
                log.debug(f"[ConvRunner] Execution iteration: {iteration}")
                if self._check_halt_conditions(iteration):
                    break

                # [Phase 1] 상태 스냅샷 구성 및 발송 (Encode)
                step_payload = {
                    "conversation_id": conv_id_str,
                    "iteration": iteration,
                    "events": [e.model_dump() for e in self.conv.state.events]
                }
                await tunnel.stream_produce(
                    topic=agent_task_topic, 
                    payload=StreamPayloadAdapter.encode(step_payload)
                )
                log.debug(f"[ConvRunner] Step request produced to {agent_task_topic}")

                # [Phase 2] Agent 응답 대기
                stream_results = await tunnel.stream_consume(
                    topic=agent_response_topic, 
                    group=consumer_group, 
                    consumer="conver_worker_1", 
                    count=1, 
                    block=30000 
                )

                if not stream_results:
                    log.warning("[ConvRunner] Agent response timeout. Retrying loop...")
                    continue

                # [Phase 3] 결정 수신 및 물리적 반영 (Decode & Process)
                for stream_name, messages in stream_results:
                    for message_id, message_data in messages:
                        try:
                            parsed_decision = StreamPayloadAdapter.decode(message_data)
                            await self._process_agent_decision(parsed_decision)
                        finally:
                            await tunnel.stream_ack(agent_response_topic, consumer_group, message_id)

                iteration += 1
                
                # 상태가 RUNNING이 아니면 루프 종료
                if self.conv.state.execution_status in [
                    ConverStatus.WAITING_FOR_USER, ConverStatus.FINISHED,
                    ConverStatus.STUCK, ConverStatus.ERROR, ConverStatus.NEEDS_REPLAN
                ]:
                    break
                    
        except Exception as e:
            if not isinstance(e, ConversationRunError):
                self.conv.state.apply(TransitionStatus(new_status=ConverStatus.ERROR, reason=f"Unhandled exception: {e}"))
                self.conv._on_event(ConversationErrorEvent(source="environment", code=e.__class__.__name__, detail=str(e)))
                raise ConversationRunError(self.conv.state.id, e, persistence_dir=self.conv.state.persistence_dir) from e
            raise

    async def _process_agent_decision(self, data: dict) -> None:
        """Agent가 보내온 의도(Intent)를 파싱하여 상태에 기록하고 도구를 실행합니다."""
        decision_type = data.get("type")
        
        if decision_type == "action":
            # 1. 완벽하게 덤프된 객체를 Pydantic을 이용해 그대로 수화(Hydration)
            action_event = ActionEvent.model_validate(data.get("event_payload"))
            self.conv._on_event(action_event)
            
            tool_name = action_event.tool_name
            action_obj = action_event.action
            
            try:
                # 2. 물리적 도구 실행 시도
                obs = self.execute_tool(tool_name, action_obj)
                
                # 3. 순수 인지 도구(think 등)는 obs가 None으로 반환되며 Observation을 생성하지 않음
                if obs is not None:
                    obs_event = ObservationEvent(observation=obs, tool_name=tool_name, action_id=action_event.id)
                    self.conv._on_event(obs_event)
                    
            except Exception as e:
                log.error(f"[ConvRunner] Tool execution failed: {e}")
                # 4. 강제 조립으로 인한 검증 에러 방지를 위해 전용 AgentErrorEvent 방출
                error_event = AgentErrorEvent(
                    source="environment",
                    error=str(e),
                    tool_name=tool_name,
                    tool_call_id=action_event.tool_call_id
                )
                self.conv._on_event(error_event)
                
        elif decision_type == "message":
            msg_event = MessageEvent.model_validate(data.get("event_payload"))
            self.conv._on_event(msg_event)
            
        elif decision_type == "system_prompt":
            sys_event = SystemPromptEvent.model_validate(data.get("event_payload"))
            self.conv._on_event(sys_event)
            
        elif decision_type == "finish":
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.WAITING_FOR_USER, reason="Agent finalized turn"))

    def execute_tool(self, tool_name: str, action: Action) -> Observation | None:
        """
        도구를 실행합니다. 
        단, 물리적 실행기(Executor)가 없는 인지 도구(think, finish 등)는 예외를 발생시키지 않고 안전하게 통과시킵니다.
        """
        local_tools_map = getattr(self.conv, "tools", {})
        tool = local_tools_map.get(tool_name)
        
        if tool is None:
            available_tools = list(local_tools_map.keys())
            raise KeyError(f"Tool '{tool_name}' not found in Gov Environment. Available tools: {available_tools}")

        if not getattr(tool, "executor", None):
            # 인지 도구(Cognitive Tool) 면책 특권: 물리적 실행 없이 None 반환
            if CoreAction.is_safe_cognitive(tool_name) or tool_name in ["finish", "bridge", "signal", "lang"]:
                log.debug(f"[ConvRunner] Cognitive action '{tool_name}' processed without physical execution.")
                return None
            else:
                raise NotImplementedError(f"Physical Tool '{tool_name}' has no configured executor in Gov Env.")
        
        return tool(action, self.conv)

    def _check_halt_conditions(self, iteration: int) -> bool:
        if iteration >= MAX_ABSOLUTE_ITERATIONS:
            self._halt_execution(
                "AbsoluteMaxIterationsReached",
                f"Topological Rupture: Absolute system iterations limit ({MAX_ABSOLUTE_ITERATIONS}) reached."
            )

        if self.conv.state.execution_status in [
            ConverStatus.PAUSED, ConverStatus.STUCK, 
            ConverStatus.FINISHED, ConverStatus.ERROR, ConverStatus.NEEDS_REPLAN
        ]:
            return True

        if getattr(self.conv, "_stuck_detector", None) and self.conv._stuck_detector.is_stuck():
            self._halt_execution("AgentStuck", "Cognitive Livelock (Stuck pattern) detected.")
        
        if getattr(self.conv, "_drift_detector", None) and self.conv._drift_detector.is_drifting():
            self._halt_execution("AgentDrift", "Topological Drift detected: Agent is wandering without progress.")

        if self.conv.state.execution_status == ConverStatus.WAITING_FOR_USER:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.RUNNING, reason="Resuming from user wait"))
            
        max_iter = getattr(self.conv, "max_iteration_per_run", MAX_ABSOLUTE_ITERATIONS)
        if iteration >= max_iter:
            self._halt_execution("MaxIterationsReached", "User-defined iterations limit reached.")
            
        return False

    def _halt_execution(self, code: str, detail: str) -> None:
        log.error(detail)
        new_status = ConverStatus.STUCK if "Stuck" in code or "Drift" in code else ConverStatus.ERROR
        self.conv.state.apply(TransitionStatus(new_status=new_status, reason=detail))
        
        self.conv._on_event(ConversationErrorEvent(source="environment", code=code, detail=detail))
        raise ConversationRunError(self.conv.state.id, Exception(detail), persistence_dir=self.conv.state.persistence_dir)

    def pause(self) -> None:
        if self.conv.state.execution_status == ConverStatus.PAUSED:
            return
            
        if self.conv.state.execution_status in [ConverStatus.IDLE, ConverStatus.RUNNING]:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.PAUSED, reason="Agent execution pause requested"))
            self.conv._on_event(PauseEvent())
            log.info("Agent execution pause requested")

    def reject_pending_actions(self, reason: str = "User rejected the action") -> None:
        pending_actions = ConversationState.get_unmatched_actions(self.conv.state.events)
        
        if self.conv.state.execution_status == ConverStatus.WAITING_FOR_USER:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.IDLE, reason="User rejected pending action(s)"))

        if not pending_actions:
            log.warning("No pending actions to reject")
            return

        for action_event in pending_actions:
            rejection_event = UserRejectObservation(
                action_id=action_event.id,
                tool_name=action_event.tool_name,
                tool_call_id=action_event.tool_call_id,
                rejection_reason=reason,
            )
            self.conv._on_event(rejection_event)
            log.info(f"Rejected pending action: {action_event.tool_name} - {reason}")

    def rerun_actions(self, rerun_log_path: str | Path | None = None) -> bool:
        rerun_log: LogStore | None = None
        if rerun_log_path is not None:
            log_dir = Path(rerun_log_path)
            log_dir.mkdir(parents=True, exist_ok=True)
            file_store = LocalFileStore(str(log_dir))
            rerun_log = LogStore(file_store, dir_path="events")

        local_tools_map = getattr(self.conv, "tools", {})
        action_count = 0
        
        for event in self.conv.state.events:
            if not isinstance(event, ActionEvent) or event.action is None:
                continue

            action_count += 1
            tool_name = event.tool_name
            tool = local_tools_map.get(tool_name)
            
            if tool is None:
                raise KeyError(f"Tool '{tool_name}' not found during rerun.")
            if not getattr(tool, "executor", None):
                log.warning(f"Skipping action {action_count}: tool '{tool_name}' has no physical executor")
                continue

            try:
                log.info(f"Rerunning action {action_count}: {tool_name}")
                observation = tool(event.action, self.conv)

                if rerun_log is not None and observation is not None:
                    rerun_log.append(event)
                    obs_event = ObservationEvent(
                        source="environment",
                        tool_name=tool_name,
                        tool_call_id=event.tool_call_id,
                        observation=observation,
                        action_id=event.id,
                    )
                    rerun_log.append(obs_event)
            except Exception as e:
                log.error(f"Action {action_count} ({tool_name}) failed during rerun: {e}")
                return False

        log.info(f"Rerun complete: {action_count} actions processed successfully")
        return True

    def close_executors(self) -> None:
        try:
            local_tools_map = getattr(self.conv, "tools", {})
        except (AttributeError, RuntimeError):
            return
            
        for tool in local_tools_map.values():
            try:
                if hasattr(tool, "as_executable"):
                    executable_tool = tool.as_executable()
                    executable_tool.executor.close()
            except NotImplementedError:
                continue
            except Exception as e:
                log.warning(f"Error closing executor for tool '{tool.name}': {e}")