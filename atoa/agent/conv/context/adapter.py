# atoa.agent.conv.context.adapter
## @lineage: agent.conv.context.adapter
## @lineage: atoa.conv.context.adapter
## @lineage: atoa.gov.context.adapter
import json
from pathlib import Path
from collections.abc import Mapping
from typing import Any

from atoa.gov.disc.schema.action import Action, Observation
from atoa.gov.disc.conv import (
    AgentCommunicationProtocol, 
    ExecutionControlProtocol, 
    SecurityControlProtocol, 
    EngineContextProtocol,
    ProtoConv
)
from atoa.gov.disc.event.llm.message import MessageEvent
from atoa.gov.disc.event.llm.observation import UserRejectObservation
from atoa.types import ConversationID
from atoa.driver.tensor import Driver

from eco.fiber.action.message import Message, TextContent
from eco.watcher.stats import ConversationStats
from atoa.gov.disc.status import ConverStatus
from atoa.agent.conv.context.protocol import ConvStateProtocol

from atoa.agent.conv.context.command import TransitionStatus, UpdateSecurityPolicy
from atoa.gov.security.analyzer import SecurityAnalyzerBase
from atoa.gov.security.confirm import ConfirmationPolicyBase
from atoa.gov.security.confirm import NeverConfirm

from atoa.gov.conver import Conver, AgentSessionManager, AgentSidecar
from bound.resolver.secret import SecretValue
from watcher.plane.emitter import get_emitter

from arch.topos.bound.tunnel import TunnelFactory

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
            self._context.state.events.append(event)

    def send_message(self, message: str | Message, sender: str | None = None) -> None:
        if isinstance(message, str):
            message = Message(role="user", content=[TextContent(text=message)])

        state = self._context.state
        
        if state.execution_status in [ConverStatus.FINISHED, ConverStatus.STUCK]:
            state.apply(TransitionStatus(new_status=ConverStatus.IDLE, reason="User sent a new message"))

        activated_skill_names: list[str] = []
        extended_content: list[TextContent] = []

        user_msg_event = MessageEvent(
            source="user",
            llm_message=message,
            activated_skills=activated_skill_names,
            extended_content=extended_content,
            sender=sender,
        )
        self._dispatch(user_msg_event)

    def ask(self, question: str) -> str:
        """Sidecar는 Gov 로컬에서 독립 실행되므로 동기 호출을 유지합니다."""
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

    async def run(self) -> None:
        await self._runner.run()

    def pause(self) -> None:
        self._runner.pause()

    async def close(self) -> None:
        if getattr(self._context, "_cleanup_initiated", False):
            return
        self._context._cleanup_initiated = True
        
        # Obsevability span 종료 시도
        try:
            self._context._end_observability_span()
        except AttributeError:
            pass
            
        # [핵심 변경] self._context.id.hex -> str(self._context.id) 로 안전하게 변환
        conv_id_str = str(self._context.id)
        
        try:
            tunnel = await TunnelFactory.get_default()
            control_channel = f"agent_control:{conv_id_str}"
            payload = {"command": "shutdown", "conversation_id": conv_id_str}
            await tunnel.publish(control_channel, json.dumps(payload))
            log.info(f"Published shutdown signal to remote Agent via {control_channel}")
        except Exception as e:
            log.warning(f"Error publishing close signal to agent: {e}")
            
        # Executor 정리 로직 (Gov 로컬 툴 정리)
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
        from atoa.agent.conv.state import ConversationState
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
        return self._context.state

    @property
    def conversation_stats(self) -> ConversationStats:
        return self._context.conversation_stats

    async def switch_profile(self, profile_name: str) -> None:
        await self._session_manager.switch_profile(profile_name)

    def generate_title(self, llm: Driver | None = None, max_length: int = 50) -> str:
        return self._sidecar.generate_title(llm, max_length)
    
    def ensure_agent_ready(self) -> None:
        pass