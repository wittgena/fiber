# atoa.gov.conver
## @lineage: gov.conver
## @lineage: gov.engine.conver
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from eco.call.action.message import Message, TextContent
from eco.call.disc.action import Action, Observation

from watcher.plane.observer.span import observe

from atoa.agent.parser import render_template
from atoa.agent.disc.event.llm.message import MessageEvent
from atoa.agent.disc.event.llm.action import ActionEvent
from atoa.agent.disc.event.llm.observation import ObservationEvent, UserRejectObservation
from atoa.agent.disc.event.conv.error import ConversationErrorEvent
from atoa.agent.disc.event.conv.pause import PauseEvent
from atoa.agent.disc.status import ConverStatus
from atoa.agent.disc.memory.profile import LLMProfileStore
from atoa.exception.conv.connection import ConversationRunError

from atoa.call.driver.tensor import Driver

from atoa.gov.context.command import TransitionStatus, UpdateAgentState
from atoa.gov.context.state import ConversationState

from arch.xor.store.file import LocalFileStore
from xor.store.log import LogStore
from atoa.gov.context.message.parser.title import generate_conversation_title
from atoa.gov.context.message.parser.builder import MessageBuilder, LLMFacade

from watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from atoa.gov.context.context import ConvContext
    convType = ConvContext
else:
    convType = Any

log = get_emitter("executor.conver")
MAX_ABSOLUTE_ITERATIONS = 7


class AgentSessionManager:
    """
    @desc: 에이전트의 라이프사이클(초기화, LLM 프로필 스위칭, 레지스트리 관리)을 전담하는 클래스.
    """
    def __init__(self, conv: convType):
        self.conv = conv
        self._profile_store = LLMProfileStore()

    def register_file_based_agents(self) -> None:
        # Note: AtorLoader 와 _GLOBAL_REGISTRY 가 import 되어야 정상 동작합니다.
        try:
            from meta.agent.loader import AtorLoader, _GLOBAL_REGISTRY
            AtorLoader.register_files(self.conv.workspace.working_dir, _GLOBAL_REGISTRY)
        except ImportError:
            log.warning("AtorLoader could not be imported; skipping file-based agent registration.")

    def ensure_agent_ready(self) -> None:
        # [개선점] Public 속성(is_agent_ready) 사용
        if getattr(self.conv, "is_agent_ready", False):
            return

        # [개선점] with 락(Lock) 제거
        self.register_file_based_agents()
        self.conv.ator.init_state(self.conv.state, on_event=self.conv._on_event)
        self.conv.llm_registry.subscribe(self.conv.state.stats.register_llm)
        
        registered = set(self.conv.llm_registry.list_usage_ids())
        for llm in list(self.conv.ator.get_all_llms()):
            if llm.usage_id not in registered:
                self.conv.llm_registry.add(llm)

        self.conv.is_agent_ready = True

    def switch_profile(self, profile_name: str) -> None:
        usage_id = f"profile:{profile_name}"
        try:
            new_llm = self.conv.llm_registry.get(usage_id)
        except KeyError:
            new_llm = self._profile_store.load(profile_name)
            new_llm = new_llm.model_copy(update={"usage_id": usage_id})
            self.conv.llm_registry.add(new_llm)
            
        # [개선점] with 락 제거 및 Command 패턴 활용 (agent 모델 통째 교체 방지 또는 우회)
        new_ator = self.conv.ator.model_copy(update={"llm": new_llm})
        self.conv.ator = new_ator
        # 상태에 에이전트를 업데이트할 때도 직접 할당 대신 적용 (ConversationState 내부에서 처리 가능)
        self.conv.state.agent = new_ator 
        log.info(f"Agent profile switched to: {profile_name}")


class AgentSidecar:
    """@desc: 대화 컨텍스트를 활용한 OOB(Out-Of-Band) 부가 기능 및 메타 질의를 전담하는 클래스"""
    def __init__(self, conv: convType, session_manager: AgentSessionManager):
        self.conv = conv
        self.session = session_manager

    def ask(self, question: str) -> str:
        self.session.ensure_agent_ready()
        agent_response = self.conv.ator.ask(question)
        if agent_response is not None:
            return agent_response

        template_dir = Path(__file__).parent.parent.parent / "context" / "prompts" / "templates"
        question_text = render_template(str(template_dir), "ask_agent_template.j2", question=question)

        user_message = Message(role="user", content=[TextContent(text=question_text)])
        
        messages = MessageBuilder.prepare_llm_messages(
            self.conv.state.events, additional_messages=[user_message]
        )

        try:
            question_llm = self.conv.llm_registry.get("ask-agent-llm")
        except KeyError:
            question_llm = self.conv.ator.llm.model_copy(
                update={"usage_id": "ask-agent-llm"},
                deep=True,
            )
            self.conv.llm_registry.add(question_llm)

        response = LLMFacade.make_completion(
            llm=question_llm, 
            messages=messages, 
            tools=list(self.conv.ator.tools_map.values())
        )
        
        message = response.message
        if message.content and len(message.content) > 0:
            for content in message.content:
                if isinstance(content, TextContent):
                    return content.text

        raise Exception("Failed to generate answer via Sidecar")

    @observe(name="sidecar.generate_title", ignore_inputs=["llm"])
    def generate_title(self, llm: Driver | None = None, max_length: int = 50) -> str:
        llm_to_use = llm or self.conv.ator.llm
        if llm_to_use.model == "acp-managed":
            llm_to_use = None
        return generate_conversation_title(events=self.conv.state.events, llm=llm_to_use, max_length=max_length)


class Conver:
    """@desc: 대화의 실행 루프, 도구 호출, 액션 거절, 일시정지 등 '실행 및 제어 흐름' 전반을 통제"""
    def __init__(self, conversation: convType):
        self.conv = conversation
        self.session = AgentSessionManager(self.conv)
        self.sidecar = AgentSidecar(self.conv, self.session)

    @observe(name="conver.run")
    def run(self) -> None:
        """@desc: Core iterative loop for the Agent's cognitive process"""
        self.session.ensure_agent_ready()
        
        # [개선점] 락 제거 및 Command 패턴
        if self.conv.state.execution_status in [
            ConverStatus.IDLE, ConverStatus.PAUSED,
            ConverStatus.ERROR, ConverStatus.STUCK,
        ]:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.RUNNING, reason="Engine loop started"))

        iteration = 0
        try:
            while True:
                log.debug(f"[ConvRunner] Execution iteration: {iteration}")
                
                if iteration >= MAX_ABSOLUTE_ITERATIONS:
                    self._halt_execution(
                        "AbsoluteMaxIterationsReached",
                        f"Topological Rupture: Absolute system iterations limit ({MAX_ABSOLUTE_ITERATIONS}) reached."
                    )

                # [개선점] 내부 루프 검사에서도 락 제거
                if self.conv.state.execution_status in [
                    ConverStatus.PAUSED, ConverStatus.STUCK, 
                    ConverStatus.FINISHED, ConverStatus.ERROR, ConverStatus.NEEDS_REPLAN
                ]:
                    break

                # Cognitive Livelock (Stuck & Drift) Detection
                if getattr(self.conv, "_stuck_detector", None) and self.conv._stuck_detector.is_stuck():
                    self._halt_execution("AgentStuck", "Cognitive Livelock (Stuck pattern) detected.")
                
                if getattr(self.conv, "_drift_detector", None) and self.conv._drift_detector.is_drifting():
                    self._halt_execution("AgentDrift", "Topological Drift detected: Agent is wandering without progress.")

                if self.conv.state.execution_status == ConverStatus.WAITING_FOR_USER:
                    self.conv.state.apply(TransitionStatus(new_status=ConverStatus.RUNNING, reason="Resuming from user wait"))

                # Actuate Agent Step
                self.conv.ator.step(self.conv, on_event=self.conv._on_event, on_token=self.conv._on_token)
                iteration += 1

                if self.conv.state.execution_status in [
                    ConverStatus.WAITING_FOR_USER, ConverStatus.FINISHED,
                    ConverStatus.STUCK, ConverStatus.ERROR, ConverStatus.NEEDS_REPLAN
                ]:
                    break

                if iteration >= getattr(self.conv, "max_iteration_per_run", MAX_ABSOLUTE_ITERATIONS):
                    self._halt_execution(
                        "MaxIterationsReached",
                        f"User-defined iterations limit reached."
                    )

        except Exception as e:
            if not isinstance(e, ConversationRunError):
                self.conv.state.apply(TransitionStatus(new_status=ConverStatus.ERROR, reason=f"Unhandled exception: {e}"))
                self.conv._on_event(ConversationErrorEvent(source="environment", code=e.__class__.__name__, detail=str(e)))
                raise ConversationRunError(self.conv.state.id, e, persistence_dir=self.conv.state.persistence_dir) from e
            raise

    def _halt_execution(self, code: str, detail: str) -> None:
        """내부 헬퍼: 중단 이벤트를 발생시키고 스택을 끊습니다."""
        log.error(detail)
        new_status = ConverStatus.STUCK if "Stuck" in code or "Drift" in code else ConverStatus.ERROR
        self.conv.state.apply(TransitionStatus(new_status=new_status, reason=detail))
        
        self.conv._on_event(ConversationErrorEvent(source="environment", code=code, detail=detail))
        raise ConversationRunError(self.conv.state.id, Exception(detail), persistence_dir=self.conv.state.persistence_dir)

    def pause(self) -> None:
        if self.conv.state.execution_status == ConverStatus.PAUSED:
            return
            
        # [개선점] 락 제거 및 커맨드 패턴 적용
        if self.conv.state.execution_status in [ConverStatus.IDLE, ConverStatus.RUNNING]:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.PAUSED, reason="Agent execution pause requested"))
            self.conv._on_event(PauseEvent())
            log.info("Agent execution pause requested")

    def reject_pending_actions(self, reason: str = "User rejected the action") -> None:
        pending_actions = ConversationState.get_unmatched_actions(self.conv.state.events)
        
        # [개선점] 락 제거 및 커맨드 패턴 적용
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
        self.session.ensure_agent_ready()
        tool = self.conv.ator.tools_map.get(tool_name)
        if tool is None:
            available_tools = list(self.conv.ator.tools_map.keys())
            raise KeyError(f"Tool '{tool_name}' not found. Available tools: {available_tools}")

        if not tool.executor:
            raise NotImplementedError(f"Tool '{tool_name}' has no executor")
        return tool(action, self.conv)

    def rerun_actions(self, rerun_log_path: str | Path | None = None) -> bool:
        self.session.ensure_agent_ready()
        rerun_log: LogStore | None = None
        if rerun_log_path is not None:
            log_dir = Path(rerun_log_path)
            log_dir.mkdir(parents=True, exist_ok=True)
            file_store = LocalFileStore(str(log_dir))
            rerun_log = LogStore(file_store, dir_path="events")

        action_count = 0
        for event in self.conv.state.events:
            if not isinstance(event, ActionEvent) or event.action is None:
                continue

            action_count += 1
            tool_name = event.tool_name
            tool = self.conv.ator.tools_map.get(tool_name)
            
            if tool is None:
                raise KeyError(f"Tool '{tool_name}' not found during rerun.")
            if not tool.executor:
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
        """@desc: 도구 실행기들의 리소스를 깔끔하게 정리합니다."""
        try:
            tools_map = self.conv.ator.tools_map
        except (AttributeError, RuntimeError):
            return
            
        for tool in tools_map.values():
            try:
                executable_tool = tool.as_executable()
                executable_tool.executor.close()
            except NotImplementedError:
                continue
            except Exception as e:
                log.warning(f"Error closing executor for tool '{tool.name}': {e}")