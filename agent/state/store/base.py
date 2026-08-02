# agent.state.store.base
from abc import ABC, abstractmethod
from collections.abc import Sequence
from engine.atoa.conv.event import Event

class EventsListBase(Sequence[Event], ABC):
    @abstractmethod
    def append(self, event: Event) -> None:
        """Add a new event to the list."""
        ...
