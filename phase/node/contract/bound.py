# phase.node.contract.bound
## @lineage: phase.node.bound
from __future__ import annotations
import asyncio
import math
import random
from typing import List, Dict, Optional, Any, Type, Callable

from arch.contract.registry.unified import registry, contract 
from arch.contract.event.bus import AsyncEventBus
from arch.contract.event.psi import PsiCarrier, PsiEvent
from arch.contract.interface import IPhaseAtor, IPhaseField, ICriticalDetector, ISystemRegime
from watcher.plane.emitter import get_emitter

@contract.field("node.global")
class GlobalNode(IPhaseField):
    """Φ: global phase manifold (state container)"""
    def __init__(self, size: int, init_phase_range: tuple, omega_range: tuple, kernel: Any, rng: random.Random):
        self.kernel = kernel
        self.rng = rng
        self.nodes_state = {
            str(i): {
                "phase": self.rng.uniform(*init_phase_range),
                "tension": 0.0,
                "omega": self.rng.uniform(*omega_range),
                "state": "NORMAL" 
            } for i in range(size)
        }

    def get_state(self) -> Dict[str, Any]:
        return self.nodes_state
    
    def compute_gradient(self) -> Dict[str, float]:
        return {node_id: data["tension"] for node_id, data in self.nodes_state.items()}

    def evolve(self, dt: float) -> None:
        deltas = self.kernel.compute_step(self.nodes_state, dt)
        for node_id, delta in deltas.items():
            self.nodes_state[node_id]["phase"] = (self.nodes_state[node_id]["phase"] + delta["d_phase"]) % (2 * math.pi)
            if "target_tension" in delta:
                self.nodes_state[node_id]["tension"] = delta["target_tension"]

    def update_node_state(self, node_id: str, new_state: str) -> None:
        if node_id in self.nodes_state: self.nodes_state[node_id]["state"] = new_state

    def set_tension(self, node_id: str, tension: float) -> None:
        if node_id in self.nodes_state: self.nodes_state[node_id]["tension"] = tension

@contract.ator("topos.ator")
class ToposAtor(IPhaseAtor):
    """ψ: local ator mediating Φ interaction"""
    def __init__(self, ator_id: str, reflector_boost: float = 0.5, attractor_gain: float = 1.2, state: str = "NORMAL"):
        self._id = ator_id
        self._state = state
        self.reflector_boost = reflector_boost
        self.attractor_gain = attractor_gain
        self.log = get_emitter(name=f"node.{ator_id}", phase="STABLE")

    @property
    def ator_id(self) -> str: return self._id
    @property
    def state(self) -> str: return self._state
    def set_state(self, new_state: str) -> None: self._state = new_state

    async def react(self, event: PsiEvent, field: IPhaseField, bus: AsyncEventBus) -> None:
        my_data = field.get_state()[self._id]
        if self._state == "REFLECTOR":
            my_data["phase"] = (my_data["phase"] + self.reflector_boost) % (2 * math.pi) 
            my_data["tension"] = 0.0 
            
            inject_carrier = PsiCarrier(kind="INJECT", tag="NETWORK", payload={"tension": 1.0})
            inject_event = PsiEvent(
                event_id=f"inject-{self._id}-{event.tick}", parent_id=event.event_id, 
                source_id=self._id, scope="NETWORK", tick=event.tick,
                carrier=inject_carrier, context={"phase": "loop", "domain": "watcher"}
            )
            await bus.publish(inject_event)
            
        elif self._state == "ATTRACTOR":
            my_data["omega"] *= self.attractor_gain 

@contract.watcher("bound.observer")
class BoundObserver(ICriticalDetector):
    """∂Φ: boundary layer (continuous + rupture)"""
    def __init__(self, rupture_limit: float = 0.9):
        self.rupture_limit = rupture_limit
        self.log = get_emitter(name="watcher.∂Φ", phase="DETECTION")

    def extract(self, field: IPhaseField) -> Dict[str, float]:
        return {node_id: data["tension"] for node_id, data in field.get_state().items()}

    def evaluate(self, field: IPhaseField, history: List, current_tick: int, parent_event: PsiEvent) -> Optional[PsiEvent]:
        for node_id, data in field.get_state().items():
            if data["tension"] >= self.rupture_limit:
                rup_carrier = PsiCarrier(kind="RUPTURE", tag="SYSTEMIC", payload={"target_node": node_id})
                return PsiEvent(
                    event_id=f"rup-{current_tick}-{node_id}", parent_id=parent_event.event_id, 
                    source_id="watcher.∂Φ", scope="SYSTEMIC", tick=current_tick,
                    carrier=rup_carrier, context={"phase": "loop", "domain": "watcher"}
                )
        return None


@contract.regime("rupture.regime")
class RuptureRegime(ISystemRegime):
    def __init__(self, target_state: str, reset_tension: bool):
        self.target_state = target_state
        self.reset_tension = reset_tension

    def modify_field(self, field: IPhaseField, target_id: str) -> None:
        field.update_node_state(target_id, self.target_state)
        if self.reset_tension: field.set_tension(target_id, 0.0)

    def constrain_ator(self, ator: IPhaseAtor) -> None:
        ator.set_state(self.target_state)

    def filter_event(self, event: PsiEvent) -> Optional[PsiEvent]:
        return event
