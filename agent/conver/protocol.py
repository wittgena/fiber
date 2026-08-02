# agent.conver.protocol
## @lineage: actor.conver.protocol
## @lineage: topos.agent.conver.protocol
from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, Any

from engine.driver.security.confirm import ConfirmationPolicyBase
from engine.protocol.atoa.schema.disc.action import Action, Observation
from engine.driver.security.analyzer import SecurityAnalyzerBase

from arch.topos.resolver.secret import SecretValue

from engine.protocol.atoa.conv.types import ConversationID
from engine.protocol.atoa.conv.message import Message

from engine.driver.llm.model import LLMModel
from agent.state.protocol import ConvStateProtocol
from engine.protocol.atoa.context.stats import ConversationStats

if TYPE_CHECKING:
    from engine.protocol.atoa.schema.disc.ator import Ator 
    from engine.protocol.atoa.schema.disc.workspace import BaseWorkspace

from watcher.plane.observer.span import end_active_span, should_enable_observability, start_active_span

class ProtoConv(ABC):
    def __init__(self) -> None:
        self._span_ended = False

    def _start_observability_span(self, session_id: str) -> None:
        if should_enable_observability():
            start_active_span("conversation", session_id=session_id)

    def _end_observability_span(self) -> None:
        if not self._span_ended and should_enable_observability():
            end_active_span()
            self._span_ended = True
    
    @property
    @abstractmethod
    def id(self) -> ConversationID: ...

    @property
    @abstractmethod
    def state(self) -> ConvStateProtocol: ...

    @property
    @abstractmethod
    def workspace(self) -> "BaseWorkspace": ...

    @property
    @abstractmethod
    def ator(self) -> "Ator": ...

    @property
    @abstractmethod
    def conversation_stats(self) -> ConversationStats: ...

class SecretProviderProtocol(Protocol):
    """터미널 등이 환경변수 및 마스킹을 위해 사용하는 시크릿 레지스트리 명세"""
    def get_secrets_as_env_vars(self, command: str) -> dict[str, str]: ...
    def mask_secrets_in_output(self, text: str) -> str: ...

class ToolStateProtocol(Protocol):
    @property
    def secret_registry(self) -> SecretProviderProtocol: ...

class ToolExecutionContextProtocol(Protocol):
    """ActionExecutor 등 도구가 사용하는 최소한의 컨텍스트"""
    @property
    def state(self) -> ToolStateProtocol: ...
    @property
    def workspace(self) -> Any: ...

class AgentCommunicationProtocol(Protocol):
    """외부에서 에이전트와 메시지를 주고받는 입출력 전용 명세"""
    @property
    def id(self) -> ConversationID: ...
    def send_message(self, message: str | Message, sender: str | None = None) -> None: ...
    def ask(self, question: str) -> str: ...

class ExecutionControlProtocol(Protocol):
    """대화의 생명주기와 실행 루프를 제어하는 권한 명세"""
    def run(self) -> None: ...
    def pause(self) -> None: ...
    def close(self) -> None: ...
    def rerun_actions(self, rerun_log_path: str | Path | None = None) -> bool: ...
    def execute_tool(self, tool_name: str, action: Action) -> Observation: ...

class SecurityControlProtocol(Protocol):
    """가드레일, 정책 설정, 사용자 컨펌 및 비밀 데이터 관리 명세"""
    @property
    def is_confirmation_mode_active(self) -> bool: ...
    @property
    def confirmation_policy_active(self) -> bool: ...
    def set_confirmation_policy(self, policy: ConfirmationPolicyBase) -> None: ...
    def set_security_analyzer(self, analyzer: "SecurityAnalyzerBase | None") -> None: ...
    def reject_pending_actions(self, reason: str = "User rejected the action") -> None: ...
    def update_secrets(self, secrets: Mapping[str, SecretValue]) -> None: ...

class EngineContextProtocol(Protocol):
    """Activator, Profiler 등이 대화 상태 깊숙이 개입하기 위한 명세"""
    @property
    def state(self) -> ConvStateProtocol: ...
    @property
    def ator(self) -> "Ator": ...
    @property
    def conversation_stats(self) -> ConversationStats: ...
    def switch_profile(self, profile_name: str) -> None: ...
    def condense(self) -> None: ...
    def generate_title(self, llm: LLMModel | None = None, max_length: int = 50) -> str: ...