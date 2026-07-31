# phi.conver.state.protocol
## @lineage: swarm.mesh.protocol
## @lineage: swarm.mesh.conv.protocol
from collections.abc import Sequence
from typing import Any, Protocol, TYPE_CHECKING

from atoa.conv.types import ConversationID, ConversationTags, ConversationCallbackType
from atoa.agent.disc.workspace import BaseWorkspace
from atoa.conv.event import Event
from atoa.conv.event import EventID

from atoa.event.security.confirm import ConfirmationPolicyBase
from phi.conver.conv.stats import ConversationStats
from phi.conver.state.store.log import LogStore

from phi.conver.state.command import StateCommand

if TYPE_CHECKING:
    from phi.conver.status import ConverStatus
    from atoa.agent.disc.ator import Ator
    from arch.topos.resolver.secret import SecretRegistry
    from atoa.event.security.analyzer import SecurityAnalyzerBase
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