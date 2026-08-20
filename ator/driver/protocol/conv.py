# ator.driver.protocol.conv
## @lineage: agent.protocol.conv
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, Any

from ator.driver.schema.types import ConversationID, ConversationTags, ConversationCallbackType
from ator.driver.llm.model import LLMModel
from ator.driver.stats import ConversationStats
from ator.driver.security.confirm import ConfirmationPolicyBase
from ator.driver.security.analyzer import SecurityAnalyzerBase

from ator.runtime.conv.command import StateCommand
# from agent.loop.ator import Ator

from arch.topos.context.space import BaseWorkspace
from arch.topos.context.status import ConverStatus
from arch.model.event import Event, EventID
from arch.xor.stream.conv import LogStore
from arch.contract.resolver.secret import SecretRegistry
from watcher.plane.observer.span import end_active_span, should_enable_observability, start_active_span

class ConvStateProtocol(Protocol):
    @property
    def id(self) -> ConversationID: ...
    # @property
    # def agent(self) -> "Ator": ...
    @property
    def workspace(self) -> BaseWorkspace: ...
    @property
    def persistence_dir(self) -> str | None: ...
    @property
    def env_observation_dir(self) -> str | None: ...
    
    @property
    def max_iterations(self) -> int: ...
    @property
    def stuck_detection(self) -> bool: ...
    
    @property
    def execution_status(self) -> "ConverStatus": ...
    @property
    def confirmation_policy(self) -> ConfirmationPolicyBase: ...
    @property
    def security_analyzer(self) -> SecurityAnalyzerBase: ...
    
    @property
    def activated_knowledge_skills(self) -> list[str]: ...
    
    @property
    def blocked_actions(self) -> dict[str, str]: ...
    @property
    def blocked_messages(self) -> dict[str, str]: ...
    @property
    def last_user_message_id(self) -> EventID | None: ...
    
    @property
    def stats(self) -> ConversationStats: ...
    @property
    def secret_registry(self) -> SecretRegistry: ...
    @property
    def tags(self) -> ConversationTags: ...
    @property
    def agent_state(self) -> dict[str, Any]: ...
    @property
    def events(self) -> LogStore | None: ...

    def apply(self, command: StateCommand) -> None: ...
    def get_effective_events(self) -> Sequence[Event]: ...
    def inject_virtual_event(self, event: Event) -> None: ...
    def set_on_state_change(self, callback: ConversationCallbackType | None) -> None: ...
    def pop_blocked_action(self, action_id: str) -> str | None: ...
    def pop_blocked_message(self, message_id: str) -> str | None: ...


# ==========================================
# 2. Conversation Protocol (대화 세션 명세)
# ==========================================
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
    def workspace(self) -> BaseWorkspace: ...

    @property
    @abstractmethod
    def ator(self) -> "Ator": ...

    @property
    @abstractmethod
    def conversation_stats(self) -> ConversationStats: ...

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