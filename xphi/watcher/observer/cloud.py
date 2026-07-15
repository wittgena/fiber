# xphi.watcher.observer.cloud
## @lineage: bound.watcher.observer.cloud
## @lineage: bound.watcher.sphere.observer
## @lineage: xphi.watcher.sphere.observer
"""
@desc: Topos-aligned Phase Observation Loop
@flow:
-> @sense:  Ψ_raw
-> @map:    Ψ_raw ↦ Ψ (PhaseEvent/Snapshot)
-> @prop:   Ψ ↦ EventBus ↦ Σ (Ators)
-> @eval:   Σ_eval(Ψ) ↦ Φ′ ↦ { Φ⁺ (Coherent) | RA_req (Drift) }
-> @field:  Σ_field(Ψ) ↦ Φ_local ↦ ∂Φ (Tension)
-> @evolve: Φ(t) ↦ Φ(t+dt) (Time evolution & Dissipation)
"""
import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Protocol, Tuple
import redis.asyncio as redis_async

from xphi.watcher.snapshot.sphere import UniversalPhaseSnapshot
from arch.contract.event.bus import AsyncEventBus
from arch.contract.interface import (
    IPhaseAtor,
    IPhaseField,
    IEventBus,
    PsiEvent
)
from watcher.plane.emitter import get_emitter

@dataclass
class EvaluationResult:
    is_coherent: bool
    drift_score: float
    recommended_actions: Dict[str, Any] = field(default_factory=dict)
    residues: List[Tuple[str, Any]] = field(default_factory=list)

@dataclass
class RawPhaseSignal:
    """@type: Ψ_raw (Unbounded external state vector)"""
    id: str
    resource_id: str
    payload: Dict[str, Any]

class IPhaseProjector(Protocol):
    """@role: Φ′ evaluator kernel"""
    def evaluate(self, snapshot: UniversalPhaseSnapshot) -> EvaluationResult:
        ...

class PhaseEventAdapter:
    """@role: Isomorphism [ Ψ_raw ↔ Ψ_event ↔ Ψ_snapshot ]"""
    
    def to_event(self, raw: RawPhaseSignal) -> PsiEvent:
        """@map: Ψ_raw ↦ Ψ_event"""
        return PsiEvent(
            event_id=raw.id,
            parent_id=None,
            event_type="PHASE_METRIC",
            source_id=raw.resource_id,
            scope="LOCAL",
            payload=raw.payload,
            tick=int(time.time())
        )

    def to_snapshot(self, event: PsiEvent) -> UniversalPhaseSnapshot:
        """@map: Ψ_event ↦ Ψ_snapshot"""
        payload = event.payload
        return UniversalPhaseSnapshot(
            timestamp=event.tick,
            resource_id=event.source_id,
            metadata=payload.get("metadata", {}),
            target_scale=payload.get("target_scale", 0),
            actual_scale=payload.get("actual_scale", 0),
            error_weight=payload.get("error_weight", 0.0),
            is_locked=payload.get("is_locked", False)
        )

class SystemPhaseField(IPhaseField):
    """@role: Φ (Global phase field manifold)"""
    def __init__(self):
        self.nodes_state: Dict[str, Dict[str, Any]] = {}

    def get_state(self) -> Dict[str, Any]:
        """@return: Φ(t)"""
        return self.nodes_state

    def compute_gradient(self) -> Dict[str, float]:
        """@return: ∇Φ (Spatial tension gradient)"""
        return {
            rid: data.get("tension", 0.0)
            for rid, data in self.nodes_state.items()
        }

    def evolve(self, dt: float) -> None:
        """@map: Φ(t) ↦ Φ(t+dt) | Dissipation operator (γ = 0.98)"""
        for node in self.nodes_state.values():
            node["tension"] *= 0.98

class FieldProjector(IPhaseAtor):
    """@role: Σ_field (Local phase tension mapper: Ψ ↦ ∂Φ)"""
    def __init__(self, ator_id: str = "field.projector"):
        self._id = ator_id
        self._state: Dict[str, Any] = {}
        self.adapter = PhaseEventAdapter()

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> Dict[str, Any]:
        return self._state

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus):
        """@flow: Ψ ↦ field.Φ(rid) update"""
        if event.event_type != "PHASE_METRIC":
            return

        snapshot = self.adapter.to_snapshot(event)
        rid = snapshot.resource_id

        field.get_state()[rid] = {
            "tension": snapshot.error_weight / (snapshot.target_scale + 1),
            "state": "LOCKED" if snapshot.is_locked else "STABLE",
            "updated_at": event.tick
        }

class PhaseEvaluator(IPhaseAtor):
    """@role: Σ_eval (Observer evaluating Φ′ threshold)"""
    def __init__(self, ator_id: str, projector: IPhaseProjector):
        self._id = ator_id
        self._state = {"mode": "observe"}
        self.projector = projector
        self.adapter = PhaseEventAdapter()
        self.log = get_emitter(f"ator.{ator_id}", phase="THEORIA")

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> Dict[str, Any]:
        return self._state

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus):
        """@flow: Ψ ↦ Φ′ ↦ { RA_req | Φ⁺ }"""
        if event.event_type != "PHASE_METRIC":
            return

        snapshot = self.adapter.to_snapshot(event)
        
        ## @eval: Φ′ projection
        result = self.projector.evaluate(snapshot)
        
        if not result.is_coherent:
            ## @emit: ∂Φ > threshold ↦ RA_req
            self.log.warn(f"[∂Φ Drift] {snapshot.resource_id} | Score: {result.drift_score}")
            
            await bus.publish(PsiEvent(
                event_id=f"reanchor-{event.event_id}",
                parent_id=event.event_id,
                event_type="REANCHOR_REQUEST",
                source_id=self._id,
                scope="LOCAL",
                payload={
                    "target": snapshot.resource_id,
                    "actions": result.recommended_actions
                },
                tick=event.tick
            ))
        else:
            ## @emit: Φ⁺ stability
            self.log.info(f"[Φ⁺ Stable] {snapshot.resource_id}")

class SystemObserver:
    """@role: Ω (Global topos coordinator)"""

    def __init__(self, projector: IPhaseProjector):
        self.redis: Optional[redis_async.Redis] = None
        self.adapter = PhaseEventAdapter()
        self.field = SystemPhaseField()
        self.bus = AsyncEventBus()
        
        ## @bind: Ators
        self.evaluator = PhaseEvaluator("system.theoria", projector)
        self.field_projector = FieldProjector() 
        self.log = get_emitter("system.observer", phase="OBSERVER")

    async def setup(self):
        """@flow: Topology binding"""
        self.redis = await redis_async.from_url("redis://localhost:6379")
        self.bus.bind_field(self.field)

        self.bus.subscribe(self.field_projector)
        self.bus.subscribe(self.evaluator)

        self.log.info("Topos Observer bound: Φ field active")

    async def ingest(self, raw_psi: RawPhaseSignal):
        """@flow: Ψ_raw ↦ Ψ_event ↦ EventBus"""
        event = self.adapter.to_event(raw_psi)
        await self.bus.publish(event)

    async def run(self):
        """@flow: Continuous temporal evolution loop (t → ∞)"""
        while True:
            try:
                ## @sense: Ψ_raw
                signals: List[RawPhaseSignal] = await self._sense()
                
                ## @ingest: Ψ
                for psi in signals:
                    await self.ingest(psi)

                ## @evolve: Φ
                self.field.evolve(1.0)
                await asyncio.sleep(1.0)

            except Exception as e:
                self.log.error(f"[Collapse] Loop integrity fault: {e}")
                await asyncio.sleep(2)

    async def _sense(self) -> List[RawPhaseSignal]:
        """@map: Interface ↦ Ψ_raw stream"""
        return []