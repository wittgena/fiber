# bound.xor.store.base
## @lineage: xor.store.base
## @lineage: ops.xor.store.base
from abc import ABC, abstractmethod
from collections.abc import Sequence
from eco.agent.event.base import Event

class EventsListBase(Sequence[Event], ABC):
    @abstractmethod
    def append(self, event: Event) -> None:
        """Add a new event to the list."""
        ...
