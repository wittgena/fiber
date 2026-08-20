# bound.xor.store.base
## @lineage: bound.eco.xor.store.base
## @lineage: eco.bound.xor.store.base
## @lineage: engine.xor.store.base
## @lineage: xor.store.base
from abc import ABC, abstractmethod
from collections.abc import Sequence
from ator.driver.schema.event import Event

class EventsListBase(Sequence[Event], ABC):
    @abstractmethod
    def append(self, event: Event) -> None:
        """Add a new event to the list."""
        ...
