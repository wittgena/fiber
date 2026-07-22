# atoa.gov.context.adapter
## @lineage: atoa.context.gov.adapter
## @lineage: gov.conv.adapter
from pathlib import Path
from collections.abc import Mapping
from typing import Any

from eco.call.disc.action import Action, Observation
from atoa.agent.disc.base.conv import (
    AgentCommunicationProtocol, 
    ExecutionControlProtocol, 
    SecurityControlProtocol, 
    EngineContextProtocol,
    ProtoConv
)
from atoa.agent.disc.event.llm.message import MessageEvent
from atoa.agent.disc.event.llm.observation import UserRejectObservation
from atoa.call.types import ConversationID
from atoa.call.driver.tensor import Driver

from eco.call.action.message import Message, TextContent
from xor.watcher.stats import ConversationStats
from atoa.agent.disc.status import ConverStatus
from atoa.gov.context.protocol import ConvStateProtocol

from atoa.gov.context.command import TransitionStatus, UpdateSecurityPolicy
from atoa.gov.security.analyzer import SecurityAnalyzerBase
from atoa.gov.security.confirm import ConfirmationPolicyBase
from atoa.gov.security.confirm import NeverConfirm

from atoa.gov.conver import Conver
from atoa.gov.conver import AgentSessionManager, AgentSidecar

from atoa.agent.disc.ator import Ator
from bound.resolver.secret import SecretValue

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class AgentCommunicator:
    """
    @implements: AgentCommunicationProtocol
    @desc: 외부 API(웹 소켓, REST)나 UI 컴포넌트가 에이전트와 대화하기 위한 입출력 전용 어댑터.
           기존 Conv의 send_message 및 ask 로직을 완전히 흡수하여 독자적으로 수행합니다.
    """
    def __init__(self, context: ProtoConv):
        self._context = context
        self._sidecar = AgentSidecar(context, AgentSessionManager(context))

    @property
    def id(self) -> ConversationID:
        return self._context.id

    def _dispatch(self, event: Any) -> None:
        """내부 이벤트 라우팅 헬퍼 (Conv의 _on_event 우회 호출)"""
        if hasattr(self._context, "_on_event"):
            self._context._on_event(event)
        else:
            # IOManager 기반이므로 .events.append() 역시 백그라운드 큐를 타거나 즉시 수행됩니다.
            self._context.state.events.append(event)

    def send_message(self, message: str | Message, sender: str | None = None) -> None:
        if isinstance(message, str):
            message = Message(role="user", content=[TextContent(text=message)])

        state = self._context.state
        ator = self._context.ator
        
        # [개선됨] 락(with state:) 블록 제거 및 Command 패턴 적용
        if state.execution_status in [ConverStatus.FINISHED, ConverStatus.STUCK]:
            state.apply(TransitionStatus(new_status=ConverStatus.IDLE, reason="User sent a new message"))

        activated_skill_names: list[str] = []
        extended_content: list[TextContent] = []

        if ator.agent_context:
            ctx = ator.agent_context.get_user_message_suffix(
                user_message=message, 
                skip_skill_names=state.activated_knowledge_skills
            )
            if ctx:
                content, activated_skill_names = ctx
                extended_content.append(content)
                # skills 업데이트는 헬퍼 메서드이므로 (Command 적용이 복잡하다면) Tracker에서 무시하거나 UpdateAgentState로 처리
                state.activated_knowledge_skills.extend(activated_skill_names)

        user_msg_event = MessageEvent(
            source="user",
            llm_message=message,
            activated_skills=activated_skill_names,
            extended_content=extended_content,
            sender=sender,
        )
        self._dispatch(user_msg_event)

    def ask(self, question: str) -> str:
        return self._sidecar.ask(question)


class ExecutionController:
    """
    @implements: ExecutionControlProtocol
    @desc: 스케줄러, 백그라운드 워커, 메인 루프 제어기가 대화를 실행하고 중지하기 위한 어댑터.
           실제 루프 실행은 이전에 분리했던 Conver 클래스에 위임합니다.
    """
    def __init__(self, context: ProtoConv):
        self._context = context
        self._runner = Conver(context)

    def run(self) -> None:
        self._runner.run()

    def pause(self) -> None:
        self._runner.pause()

    def close(self) -> None:
        if getattr(self._context, "_cleanup_initiated", False):
            return
        self._context._cleanup_initiated = True
        
        # Obsevability span 종료 시도
        try:
            self._context._end_observability_span()
        except AttributeError:
            pass
            
        try:
            self._context.ator.close()
        except Exception as e:
            log.warning(f"Error closing agent: {e}")
            
        # Executor 정리 로직
        if getattr(self._context, "delete_on_close", True):
            self._runner.close_executors()

    def rerun_actions(self, rerun_log_path: str | Path | None = None) -> bool:
        return self._runner.rerun_actions(rerun_log_path)

    def execute_tool(self, tool_name: str, action: Action) -> Observation:
        return self._runner.execute_tool(tool_name, action)


class SecurityManager:
    """
    @implements: SecurityControlProtocol
    @desc: 보안 미들웨어, 유저 컨펌 시스템, 시크릿 볼트(Vault)가 사용하는 가드레일 제어 어댑터.
           Conv를 거치지 않고 직접 state를 안전하게 조작합니다.
    """
    def __init__(self, context: ProtoConv):
        self._context = context

    @property
    def confirmation_policy_active(self) -> bool:
        return not isinstance(self._context.state.confirmation_policy, NeverConfirm)

    @property
    def is_confirmation_mode_active(self) -> bool:
        return (self._context.state.security_analyzer is not None and self.confirmation_policy_active)

    def set_confirmation_policy(self, policy: ConfirmationPolicyBase) -> None:
        self._context.state.apply(UpdateSecurityPolicy(confirmation_policy=policy))
        log.info(f"Confirmation policy set to: {policy}")

    def set_security_analyzer(self, analyzer: SecurityAnalyzerBase | None) -> None:
        self._context.state.apply(UpdateSecurityPolicy(security_analyzer=analyzer))
        log.info("Security analyzer updated.")

    def reject_pending_actions(self, reason: str = "User rejected the action") -> None:
        from atoa.gov.context.state import ConversationState
        state = self._context.state
        pending_actions = ConversationState.get_unmatched_actions(state.events)
        if state.execution_status == ConverStatus.WAITING_FOR_USER:
            state.apply(TransitionStatus(new_status=ConverStatus.IDLE, reason="User rejected pending action"))

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
            if hasattr(self._context, "_on_event"):
                self._context._on_event(rejection_event)
            else:
                state.events.append(rejection_event)
            log.info(f"Rejected pending action: {action_event} - {reason}")

    def update_secrets(self, secrets: Mapping[str, SecretValue]) -> None:
        secret_registry = self._context.state.secret_registry
        secret_registry.update_secrets(secrets)
        log.info(f"Added {len(secrets)} secrets to conversation")


class EngineContextAdapter:
    """
    @implements: EngineContextProtocol
    @desc: Activator 등 내부 엔진 코어 레이어가 상태를 깊게 조작하기 위한 브릿지.
    """
    def __init__(self, context: ProtoConv):
        self._context = context
        self._session_manager = AgentSessionManager(context)
        self._sidecar = AgentSidecar(context, self._session_manager)

    @property
    def state(self) -> ConvStateProtocol:
        # [개선됨] 구체 클래스 대신 프로토콜 타입을 반환하여 외부의 무분별한 조작 방지
        return self._context.state

    @property
    def ator(self) -> Ator:
        return self._context.ator

    @property
    def conversation_stats(self) -> ConversationStats:
        return self._context.conversation_stats

    def switch_profile(self, profile_name: str) -> None:
        self._session_manager.switch_profile(profile_name)

    def generate_title(self, llm: Driver | None = None, max_length: int = 50) -> str:
        return self._sidecar.generate_title(llm, max_length)
    
    def ensure_agent_ready(self) -> None:
        self._session_manager.ensure_agent_ready()