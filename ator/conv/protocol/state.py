# ator.conv.protocol.state
## @lineage: ator.conv.protocol
from collections.abc import Sequence
from typing import Any, Protocol, TYPE_CHECKING

from ator.conv.schema.types import ConversationID, ConversationTags, ConversationCallbackType
from ator.agent.space.base import BaseWorkspace
from ator.conv.schema.event import Event
from ator.conv.schema.event import EventID

from eco.bound.xor.bridge.security.confirm import ConfirmationPolicyBase
from ator.conv.context.stats import ConversationStats
from eco.bound.xor.store.log import LogStore

from ator.conv.command import StateCommand

if TYPE_CHECKING:
    from ator.conv.context.state.status import ConverStatus
    from ator.agent.action.schema.ator import Ator
    from arch.contract.resolver.secret import SecretRegistry
    from eco.bound.xor.bridge.security.analyzer import SecurityAnalyzerBase
    SecurityType = SecurityAnalyzerBase | Any
else:
    SecurityType = Any

class ConvStateProtocol(Protocol):
    @property
    def id(self) -> ConversationID: ...
    @property
    def agent(self) -> "Ator": ...
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
    def security_analyzer(self) -> SecurityType: ...
    
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
    def secret_registry(self) -> "SecretRegistry": ...
    @property
    def tags(self) -> ConversationTags: ...
    @property
    def agent_state(self) -> dict[str, Any]: ...
    @property
    def events(self) -> LogStore | None: ...

    def apply(self, command: StateCommand) -> None:
        ...

    def get_effective_events(self) -> Sequence[Event]:
        ...

    def inject_virtual_event(self, event: Event) -> None:
        ...

    def set_on_state_change(self, callback: ConversationCallbackType | None) -> None:
        ...

    def pop_blocked_action(self, action_id: str) -> str | None:
        ...

    def pop_blocked_message(self, message_id: str) -> str | None:
        ...