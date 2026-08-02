# agent.state.store.base
## @lineage: agent.topos.store.base
## @lineage: agent.topos.state.store.base
## @lineage: actor.topos.state.store.base
## @lineage: topos.state.store.base
## @lineage: agent.conver.state.store.base
## @lineage: phi.conver.state.store.base
## @lineage: swarm.mesh.store.base
## @lineage: swarm.mesh.conv.store.base
## @lineage: mesh.store.base
## @lineage: gov.store.base
## @lineage: eco.gov.store.base
## @lineage: atoa.gov.store.base
## @lineage: agent.gov.store.base
## @lineage: bound.xor.store.base
## @lineage: xor.store.base
## @lineage: ops.xor.store.base
from abc import ABC, abstractmethod
from collections.abc import Sequence
from engine.protocol.atoa.conv.event import Event

class EventsListBase(Sequence[Event], ABC):
    @abstractmethod
    def append(self, event: Event) -> None:
        """Add a new event to the list."""
        ...
