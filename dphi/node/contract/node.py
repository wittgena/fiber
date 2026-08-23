# dphi.node.contract.node
## @lineage: phase.contract.node
## @lineage: phase.anchor.contract.node
## @lineage: phase.dphi.contract.node
## @lineage: dphi.contract.node
from __future__ import annotations
import json
import asyncio
import math
import random
from typing import Optional, List, Dict, Any

from xphi.arch.contract.registry.unified import contract 
from xphi.arch.contract.discovery import discover_modules
from xphi.arch.contract.event.bus import AsyncEventBus
from xphi.arch.contract.event.psi import PsiCarrier, PsiEvent
from xphi.arch.contract.interface import IPhaseField, ICriticalDetector, ISystemRegime, IPhaseAtor

from xphi.kernel.phase.runtime.executor.dynamics import DynamicsExecutor
from xphi.kernel.phase.runtime.node import NodeRuntime
from xphi.kernel.phase.runtime.flow.cont import LoopCarrier
from xphi.kernel.bind.resolver import find_current_self
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("ator.node")

@contract.ator("node.global", role="field")
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
        if node_id in self.nodes_state: 
            self.nodes_state[node_id]["state"] = new_state

    def set_tension(self, node_id: str, tension: float) -> None:
        if node_id in self.nodes_state: 
            self.nodes_state[node_id]["tension"] = tension

@contract.ator("topos.ator", role="ator")
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

@contract.ator("topos.watcher", role="watcher")
class ToposWatcher(ICriticalDetector):
    """@role: ∂Φ 임계 감시자 (스케일링 돌파 감지)"""
    def __init__(self, upper_limit: float = 80.0, lower_limit: float = 20.0):
        self.upper_limit = upper_limit
        self.lower_limit = lower_limit

    def evaluate(self, field: IPhaseField, history: list, current_tick: int, parent: PsiEvent) -> Optional[PsiEvent]:
        for node_id, data in field.get_state().items():
            tension = data.get("tension", 0.0)
            state = data.get("state", "NORMAL")
            
            if state in ["SCALED_OUT", "SCALED_IN"]:
                continue

            carrier = None
            if tension >= self.upper_limit:
                carrier = PsiCarrier(kind="AWS_SCALE_REQUEST", tag=node_id, payload="Φ0")
            elif tension <= self.lower_limit:
                carrier = PsiCarrier(kind="AWS_SCALE_REQUEST", tag=node_id, payload="∂Φ")

            if carrier:
                log.warning(f"Threshold breached for {node_id} (Tension: {tension:.1f}). Emitting Phase Transition.")
                return PsiEvent(
                    event_id=f"scale-{current_tick}-{node_id}", parent_id=parent.event_id,
                    source_id="receptor.watcher", scope="SYSTEMIC", tick=current_tick, carrier=carrier
                )
        return None


@contract.ator("bound.observer", role="watcher")
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

@contract.ator("node.regime", role="regime")
class NodeRegime(ISystemRegime):
    """@role: Γ_rupture actuator | System reset & topological realignment"""
    def __init__(self, **kwargs):
        self.params = kwargs

    def modify_field(self, field: IPhaseField) -> None:
        states = field.get_state()
        for node_id, data in states.items():
            data["tension"] = 0.0
            if data["state"] == "NORMAL":
                data["phase"] = random.uniform(0, 2 * math.pi)
            elif data["state"] == "REFLECTOR":
                data["phase"] = 0.0  

        if hasattr(field, 'pressure'):
            field.pressure = 0.0
            
        log.info("[Regime] Field collapsed and reformed. Tension reset to 0.0")

    def constrain_ator(self, ator: IPhaseAtor) -> None:
        pass

    def filter_event(self, event: PsiEvent) -> Optional[PsiEvent]:
        return event if event.context.get("epoch") == "new" else event


@contract.ator("scale.regime", role="regime")
class ScaleRegime(ISystemRegime):
    """@role: 위상 전이 체제 (스케일링 쿨다운 처리)"""
    def modify_field(self, field: IPhaseField, target_id: str) -> None:
        target_data = field.get_state().get(target_id)
        if target_data:
            tension = target_data.get("tension", 0.0)
            target_data["state"] = "SCALED_OUT" if tension >= 50.0 else "SCALED_IN"
            target_data["tension"] = 50.0  # 안정 상태 초기화

    def constrain_ator(self, ator: IPhaseAtor) -> None:
        pass


@contract.ator("rupture.regime", role="regime")
class RuptureRegime(ISystemRegime):
    def __init__(self, target_state: str, reset_tension: bool):
        self.target_state = target_state
        self.reset_tension = reset_tension

    def modify_field(self, field: IPhaseField, target_id: str) -> None:
        field.update_node_state(target_id, self.target_state)
        if self.reset_tension: 
            field.set_tension(target_id, 0.0)

    def constrain_ator(self, ator: IPhaseAtor) -> None:
        ator.set_state(self.target_state)

    def filter_event(self, event: PsiEvent) -> Optional[PsiEvent]:
        return event