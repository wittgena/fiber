# atoa.gov.conver
import json
import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from eco.call.action.message import Message, TextContent
from eco.call.disc.action import Action, Observation

from watcher.plane.observer.span import observe
from watcher.plane.emitter import get_emitter

from atoa.agent.parser import render_template
from atoa.agent.disc.event.llm.message import MessageEvent
from atoa.agent.disc.event.llm.action import ActionEvent
from atoa.agent.disc.event.llm.observation import ObservationEvent, UserRejectObservation
from atoa.agent.disc.event.llm.system import SystemPromptEvent

from atoa.agent.disc.event.conv.error import ConversationErrorEvent
from atoa.agent.disc.event.conv.pause import PauseEvent
from atoa.agent.disc.status import ConverStatus
from atoa.exception.conv.connection import ConversationRunError

from atoa.call.driver.tensor import Driver

from atoa.gov.context.command import TransitionStatus
from atoa.gov.context.state import ConversationState

from arch.xor.store.file import LocalFileStore
from xor.store.log import LogStore
from atoa.gov.context.message.parser.title import generate_conversation_title
from atoa.gov.context.message.parser.builder import MessageBuilder, LLMFacade

from arch.topos.bound.payload import StreamPayloadAdapter
from arch.topos.bound.tunnel import TunnelFactory, UniversalFacade

if TYPE_CHECKING:
    from atoa.gov.context.context import ConvContext
    convType = ConvContext | Any
else:
    convType = Any

log = get_emitter("executor.conver")
MAX_ABSOLUTE_ITERATIONS = 7


class AgentSessionManager:
    """
    @desc: 대화 상태와 연관된 LLM 프로필 및 환경 메타데이터를 관리합니다.
           (Agent 객체를 직접 다루지 않고, 통신 채널을 통해 제어 메시지를 발행합니다.)
    """
    def __init__(self, conv: convType):
        self.conv = conv

    async def switch_profile(self, profile_name: str) -> None:
        """
        @desc: 메모리 객체 교체 대신, Agent 노드에게 프로필 변경 제어 메시지를 브로드캐스트합니다.
        """
        tunnel = await TunnelFactory.get_default()
        
        # [핵심 변경] .hex 제거 및 안전한 문자열 변환
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
    @desc: OOB(Out-Of-Band) 부가 기능(타이틀 생성, 메타 질문 등).
           Ator 의존성을 완전히 제거하고 Gov에 등록된 LLMFacade와 Context만으로 독립 실행합니다.
    """
    def __init__(self, conv: convType, session_manager: AgentSessionManager):
        self.conv = conv
        self.session = session_manager

    def _get_fallback_llm(self) -> Driver:
        """llm_registry가 없을 경우를 대비한 안전한 Fallback"""
        registry = getattr(self.conv, "llm_registry", None)
        if registry:
            return registry.get_default()
        
        # Registry가 없으면 state에 저장된 agent_config 등을 활용해 복원 시도, 없으면 기본값
        config_llm = self.conv.state.agent_config.get("llm") if hasattr(self.conv.state, "agent_config") else None
        if config_llm and isinstance(config_llm, dict):
            return Driver(**config_llm)
        return Driver(model="gpt-4o") # 시스템 기본값

    def ask(self, question: str) -> str:
        template_dir = Path(__file__).parent.parent.parent / "context" / "prompts" / "templates"
        question_text = render_template(str(template_dir), "ask_agent_template.j2", question=question)

        user_message = Message(role="user", content=[TextContent(text=question_text)])
        messages = MessageBuilder.prepare_llm_messages(
            self.conv.state.events, additional_messages=[user_message]
        )

        registry = getattr(self.conv, "llm_registry", None)
        if registry:
            try:
                question_llm = registry.get("ask-agent-llm")
            except KeyError:
                question_llm = registry.get_default().model_copy(update={"usage_id": "ask-agent-llm"}, deep=True)
                registry.add(question_llm)
        else:
            question_llm = self._get_fallback_llm().model_copy(update={"usage_id": "ask-agent-llm"}, deep=True)

        available_tools = list(getattr(self.conv, "tools", {}).values())

        response = LLMFacade.make_completion(
            llm=question_llm, 
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
    @desc: 이벤트 기반 비동기 Orchestrator. 
           Agent(두뇌)에게 상태를 던지고(produce), 결정(Action/Message)을 받아(consume) 
           환경(도구 실행/보안)을 통제합니다.
    """
    def __init__(self, conversation: convType):
        self.conv = conversation
        self.session = AgentSessionManager(self.conv)
        self.sidecar = AgentSidecar(self.conv, self.session)

    @observe(name="conver.run")
    async def run(self) -> None:
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

                ## 상태 스냅샷을 구성하고 어댑터를 통해 안전하게 직렬화(Encode)
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

                ## Agent의 응답 대기
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

                ## 응답 수신 후 어댑터를 통해 역직렬화(Decode)
                for stream_name, messages in stream_results:
                    for message_id, message_data in messages:
                        try:
                            parsed_decision = StreamPayloadAdapter.decode(message_data)
                            await self._process_agent_decision(parsed_decision)
                        finally:
                            await tunnel.stream_ack(agent_response_topic, consumer_group, message_id)

                iteration += 1
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
        decision_type = data.get("type")
        
        if decision_type == "action":
            tool_name = data.get("tool_name")
            action_kwargs = data.get("action_args", {})
            action_obj = Action(name=tool_name, parameters=action_kwargs)
            
            action_event = ActionEvent(action=action_obj, tool_name=tool_name, tool_call_id=data.get("call_id"))
            self.conv._on_event(action_event)
            
            try:
                obs = self.execute_tool(tool_name, action_obj)
                obs_event = ObservationEvent(observation=obs, tool_name=tool_name, action_id=action_event.id)
                self.conv._on_event(obs_event)
            except Exception as e:
                log.error(f"[ConvRunner] Tool execution failed: {e}")
                error_obs = Observation(error=str(e))
                self.conv._on_event(ObservationEvent(observation=error_obs, tool_name=tool_name, action_id=action_event.id))
                
        elif decision_type == "message":
            msg_event = MessageEvent.model_validate(data.get("event_payload"))
            self.conv._on_event(msg_event)
            
        elif decision_type == "system_prompt":
            sys_event = SystemPromptEvent.model_validate(data.get("event_payload"))
            self.conv._on_event(sys_event)
            
        elif decision_type == "finish":
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.WAITING_FOR_USER, reason="Agent finalized turn"))

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
            log.info(f"Rejected pending action: {action_event} - {reason}")

    def execute_tool(self, tool_name: str, action: Action) -> Observation:
        local_tools_map = getattr(self.conv, "tools", {})
        tool = local_tools_map.get(tool_name)
        
        if tool is None:
            available_tools = list(local_tools_map.keys())
            raise KeyError(f"Tool '{tool_name}' not found in Gov Environment. Available tools: {available_tools}")

        if not getattr(tool, "executor", None):
            raise NotImplementedError(f"Tool '{tool_name}' has no configured executor in Gov Env.")
        
        return tool(action, self.conv)

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
                log.warning(f"Skipping action {action_count}: tool '{tool_name}' has no executor")
                continue

            try:
                log.info(f"Rerunning action {action_count}: {tool_name}")
                observation = tool(event.action, self.conv)

                if rerun_log is not None:
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