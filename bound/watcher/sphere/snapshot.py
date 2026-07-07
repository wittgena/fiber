# bound.watcher.sphere.snapshot
## @lineage: xphi.watcher.sphere.snapshot
"""
@desc: Phase Transition Model
@flow: Adapter ↦ Ψ (UniversalPhaseSnapshot) ↦ ∂Φ (Tension) ↦ Diffusion ↦ Attractor
"""
import asyncio
import math
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Protocol, Any
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

@dataclass
class UniversalPhaseSnapshot:
    """@type: Ψ (Vendor-agnostic phase projection)"""
    timestamp: float
    resource_id: str
    metadata: Dict[str, str] = field(default_factory=dict)
    target_scale: int = 0
    actual_scale: int = 0
    error_weight: float = 0.0
    is_locked: bool = False

class IMetricsAdapter(Protocol):
    """@port: Ingress (Ψ acquisition)"""
    async def fetch_snapshot(self) -> UniversalPhaseSnapshot:
        ...

class IInterventionAdapter(Protocol):
    """@port: Egress (Phase re-anchor / Intervention)"""
    async def apply_correction(self, resource_id: str, adjustments: Dict[str, Any]) -> bool:
        ...

@dataclass
class Clock:
    """@type: t (Global asynchronous clock reference)"""
    tick: int = 0

class Node:
    """
    @role: Distributed phase carrier (Local Φ node)
    @desc: Pure mathematical model of complex system dynamics
    """
    def __init__(self, node_id: str, phase_val: float = 0.0):
        self.id = node_id
        self.state = "NORMAL"  # NORMAL, REFLECTOR, CANDIDATE, ATTRACTOR
        self.phase = phase_val
        self.tension = 0.0
        self.neighbors: List["Node"] = []
        
        self.candidate_threshold = 10.0
        self.rupture_limit = 25.0

    def connect(self, other_node: "Node"):
        """@topos: Establish phase coupling edge (E)"""
        if other_node not in self.neighbors:
            self.neighbors.append(other_node)
            other_node.neighbors.append(self)

    def ingest(self, snapshot: UniversalPhaseSnapshot):
        """@map: Ψ ↦ ∂Φ (Universal metric to local tension mapping)"""
        if self.state in ["REFLECTOR", "ATTRACTOR"]:
            return

        ## @detect: Semantic drift (M-Drift)
        if snapshot.metadata.get("Env") != "production":
            self.tension += 1.5

        ## @calc: Universal error weight mapping
        if snapshot.error_weight > snapshot.target_scale:
            self.tension += snapshot.target_scale * 0.3

    async def exist(self, clock: Clock):
        """
        @loop: Local phase evolution (t → ∞)
        @flow: Φ ↔ ∂Φ resonance ↦ phase transition
        """
        while True:
            current_tick = clock.tick
            tension_diff = 0.0
            phase_pull = 0.0
            
            ## @diffusion: ∂Φ resonance & Φ synchronization (Kuramoto model)
            for n in self.neighbors:
                tension_diff += (n.tension - self.tension) * 0.1
                phase_pull += math.sin(n.phase - self.phase) * 0.2

            if self.state == "NORMAL":
                self.tension = max(0.0, self.tension + tension_diff + random.uniform(0, 0.2))
                self.phase = (self.phase + phase_pull) % (2 * math.pi)
                
                ## @transition: Emergence (NORMAL ↦ CANDIDATE)
                if self.tension > self.candidate_threshold:
                    self.state = "CANDIDATE"
                    log.info(f"[t={current_tick}] [{self.id}] EMERGENCE: ∂Φ={self.tension:.1f} ↦ CANDIDATE")
                    
            elif self.state == "REFLECTOR":
                ## @inject: External phase vector
                self.phase = math.pi
                for n in self.neighbors:
                    n.tension += 1.0
                if current_tick > 0 and current_tick % 5 == 0:
                    log.info(f"[t={current_tick}] [{self.id}] REFLECT: Tension propagation")

            elif self.state == "CANDIDATE":
                ## @absorb: Drain ∂Φ from local neighborhood
                absorbed = 0.0
                for n in self.neighbors:
                    drain = min(n.tension, 1.5)
                    n.tension -= drain
                    absorbed += drain
                self.tension += absorbed
                
                ## @rupture: ∂Φ ↦ Φ⁺ (Attractor formation)
                if self.tension >= self.rupture_limit:
                    self.state = "ATTRACTOR"
                    self.tension = 0.0
                    self.phase = random.choice([math.pi/2, math.pi, 3*math.pi/2])
                    log.info(f"[t={current_tick}] [{self.id}] RUPTURE: Attractor inversion (Φ={self.phase:.2f})")

            elif self.state == "ATTRACTOR":
                ## @lock: Phase synchronization & tension dissipation
                for n in self.neighbors:
                    n.tension = max(0.0, n.tension - 2.0)
                    n.phase = self.phase

            await asyncio.sleep(0.5)

class SimulatedSystemAdapter(IMetricsAdapter, IInterventionAdapter):
    """
    @role: Virtual system environment (Entropy generator)
    @desc: Simulates internal entropy progression for testing.
    """
    def __init__(self, resource_id: str = "System-Core"):
        self.state = {
            "resource_id": resource_id,
            "metadata": {"Env": "prodction"},  # Intentional drift
            "target": 1,
            "actual": 1,
            "errors": 0.0,
            "locked": False
        }

    async def fetch_snapshot(self) -> UniversalPhaseSnapshot:
        ## 1. @evolve: Internal entropy progression
        self.state["errors"] += self.state["actual"] * 0.5
        if not self.state["locked"]:
            self.state["target"] += 1
            self.state["actual"] += 1

        ## 2. @map: Raw state ↦ Ψ
        return UniversalPhaseSnapshot(
            timestamp=time.time(),
            resource_id=self.state["resource_id"],
            metadata=self.state["metadata"].copy(),
            target_scale=self.state["target"],
            actual_scale=self.state["actual"],
            error_weight=self.state["errors"],
            is_locked=self.state["locked"]
        )

    async def apply_correction(self, resource_id: str, adjustments: Dict[str, Any]) -> bool:
        """@map: RA_req ↦ System state mutation"""
        if "metadata" in adjustments:
            self.state["metadata"].update(adjustments["metadata"])
            log.info(f"[RA] System metadata re-anchored: {self.state['metadata']}")
        return True

class EnvSimCoordinator:
    """
    @role: System orchestrator (Ω)
    @desc: Bridges external domain (Adapter) and core manifold (Node Field)
    """
    def __init__(self, size: int, adapter: IMetricsAdapter):
        self.adapter = adapter
        self.clock = Clock()
        self.nodes = [Node(f"N{i}", random.uniform(0, 0.5)) for i in range(size)]
        self._build_network()

    def _build_network(self):
        """@topos: Small-world / scale-free hybrid graph construction"""
        for i in range(len(self.nodes)):
            self.nodes[i].connect(self.nodes[(i+1) % len(self.nodes)])
            if random.random() < 0.3:
                target = random.choice(self.nodes)
                self.nodes[i].connect(target)
                
        self.nodes[0].state = "REFLECTOR"
        self.nodes[0].id = "R0-REFLECTOR"

    async def _poll_metrics(self):
        """@bridge: Ψ ↦ Node field injection loop"""
        while self.clock.tick < 20:
            await asyncio.sleep(1.0)
            self.clock.tick += 1

            ## @fetch: Acquire Ψ via adapter
            snapshot = await self.adapter.fetch_snapshot()

            ## @propagate: Inject Ψ into Φ manifold
            for node in self.nodes:
                node.ingest(snapshot)

            log.info(f"\n--- t={self.clock.tick} | Target: {snapshot.target_scale}, ErrWt: {snapshot.error_weight:.1f} ---")

    async def run(self):
        """@exec: Launch asynchronous evolution loop"""
        log.info("[START] Phase Topos Simulator (Vendor-Agnostic)")
        tasks = [asyncio.create_task(node.exist(self.clock)) for node in self.nodes]
        metrics_task = asyncio.create_task(self._poll_metrics())
        
        await metrics_task
        
        for t in tasks:
            t.cancel()
            
        log.info("\n[END] Phase Transition Terminated.")


if __name__ == "__main__":
    adapter = SimulatedSystemAdapter(resource_id="Cluster-01")
    sim = EnvSimCoordinator(size=10, adapter=adapter)
    
    try:
        asyncio.run(sim.run())
    except KeyboardInterrupt:
        log.info("[HALT] Interrupted by user.")