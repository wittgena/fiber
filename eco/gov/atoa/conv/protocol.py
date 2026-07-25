# eco.gov.atoa.conv.protocol
## @lineage: atoa.agent.conv.context.protocol
## @lineage: agent.conv.context.protocol
## @lineage: atoa.conv.context.protocol
## @lineage: atoa.gov.context.protocol
## @lineage: atoa.context.gov.protocol
## @lineage: gov.conv.protocol
from collections.abc import Sequence
from typing import Any, Protocol, TYPE_CHECKING

from atoa.types import ConversationID, ConversationTags, ConversationCallbackType
from atoa.disc.workspace import BaseWorkspace
from eco.fiber.event.base import Event
from eco.fiber.event.types import EventID

from eco.gov.atoa.security.confirm import ConfirmationPolicyBase
from eco.watcher.stats import ConversationStats
from eco.gov.store.log import LogStore

from eco.gov.atoa.conv.command import StateCommand

if TYPE_CHECKING:
    from atoa.disc.status import ConverStatus
    from atoa.disc.ator import Ator
    from bound.resolver.secret import SecretRegistry
    from eco.gov.atoa.security.analyzer import SecurityAnalyzerBase
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