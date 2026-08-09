# phase.contract.node.network
## @lineage: phase.node.contract.network
import math
import random
from typing import Dict, Any, List, Optional
from arch.contract.registry.unified import contract
from arch.contract.interface import IPhaseField

@contract.field("node.network")
class NodeNetwork(IPhaseField):
    """
    @role: Φ_canvas | Physical Bound wrapped by XeCont
    @flow: Ψ_input → Kernel(Δ) → Φ_evolution → Ω_watcher → Γ_regime
    """
    def __init__(self, **kwargs):
        self.size = kwargs.get("size", 10)
        self.init_phase_range = kwargs.get("init_phase_range", [0.0, 1.0])
        self.omega_range = kwargs.get("omega_range", [0.8, 1.2])

        self.kernel = None
        self.watcher = None
        self.regime = None
        self.ators = []

        self._states: Dict[str, Dict[str, Any]] = {}
        self.pressure: float = 0.0
        self.topology: int = 1

    ## SystemBuilder Binding Methods
    def bind_kernel(self, kernel): self.kernel = kernel
    def bind_watcher(self, watcher): self.watcher = watcher
    def bind_regime(self, regime): self.regime = regime
    def bind_ators(self, ators):
        self.ators = ators
        for a in ators:
            self._states[a.ator_id] = {
                "phase": random.uniform(*self.init_phase_range) * math.pi * 2,
                "omega": random.uniform(*self.omega_range),
                "state": getattr(a, "initial_state", "NORMAL"),
                "tension": 0.0
            }

    ## IPhaseField Interface
    def get_state(self) -> Dict[str, Any]:
        return self._states

    def compute_gradient(self) -> Dict[str, float]:
        return {node_id: data["tension"] for node_id, data in self._states.items()}

    def evolve(self, dt: float) -> None:
        """
        @phase.execution: evolve(dt)
        - @step.1: Kernel(Φ) → Δ(d_phase, target_tension)
        - @step.2: φ(t+dt) = (φ(t) + Δφ) mod 2π
        - @step.3: τ(t+dt) = τ(t) + (Δτ * dt)
        - @step.4: P(Φ) = Σ(τ) / N
        """
        if not self.kernel: return
        
        # Kernel(예: KuramotoSensor)에 상태를 넘겨 미분값(Delta) 계산
        deltas = self.kernel.compute_step(self._states, dt)
        
        total_tension = 0.0
        for node_id, delta in deltas.items():
            ## @trace.phase: orbital progression
            self._states[node_id]["phase"] = (self._states[node_id]["phase"] + delta["d_phase"]) % (2 * math.pi)
            
            ## @trace.tension: cumulative stress buildup
            self._states[node_id]["tension"] += (delta["target_tension"] * dt)
            
            total_tension += self._states[node_id]["tension"]
            
        self.pressure = total_tension / max(1, len(self._states))
        if hasattr(self.kernel, 'render_state'):
            visual = self.kernel.render_state(self._states)
            print(f"\r[Phase Field] {visual} | Pressure: {self.pressure:.2f}/17.0 ", end="", flush=True)

    def absorb(self, batch_payload: List[Dict[str, Any]]):
        """
        @step: XeCont.execute.1 (Absorption)
        @flow: Ψ_batch → Φ.evolve(dt=0.1)
        """
        self.evolve(dt=0.1)

    def evaluate(self) -> str:
        """
        @step: XeCont.execute.2 (Threshold Check)
        @flow: Ω.evaluate(Φ) → RUPTURE? → DEPOSIT : SATURATE
        """
        if self.watcher:
            trigger = self.watcher.evaluate(self, history=[], current_tick=0)
            if trigger and getattr(trigger.carrier, 'kind', '') == "RUPTURE":
                return "DEPOSIT"
        return "SATURATE"

    def commit(self):
        """
        @step: XeCont.execute.2-1 (Phase Transition)
        @flow: Γ.modify_field(Φ) → epoch(topology)++
        """
        if self.regime:
            self.regime.modify_field(self)
        self.topology += 1