# meta.phase.contract.sensor
## @lineage: phase.contract.sensor
## @lineage: phase.node.sensor
import math
import random
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field, asdict

from arch.contract.interface import IDynamicsKernel
from arch.contract.registry.unified import contract
from phase.node.contract.config import KernelConfig

@contract.kernel("sensor.ator")
class SensorAtor(IDynamicsKernel):
    """Φ-evolution kernel: Multi-Ator Cognitive Consensus & Clustering"""
    def __init__(self, **kwargs):
        if "config" in kwargs and isinstance(kwargs["config"], KernelConfig):
            self.config = kwargs["config"]
        else:
            self.config = KernelConfig(**kwargs)
            
        self.trust_radius = kwargs.get("trust_radius", 1.0)
        self.repulsion_factor = kwargs.get("repulsion_factor", 0.2)

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)

        for i_id, i_data in states.items():
            if i_data.get("state") in ["ATTRACTOR", "REFLECTOR"]:
                deltas[i_id] = {"d_phase": (i_data["omega"] * 0.1) * dt, "target_tension": 0.0}
                continue

            consensus_force = 0.0
            cognitive_dissonance = 0.0

            for j_id, j_data in states.items():
                if i_id == j_id: continue
                
                diff = (j_data["phase"] - i_data["phase"] + math.pi) % (2 * math.pi) - math.pi
                distance = abs(diff)

                if distance < self.trust_radius:
                    consensus_force += math.sin(diff) * self.config.global_coupling
                else:
                    consensus_force -= math.sin(diff) * self.repulsion_factor
                    cognitive_dissonance += distance # 이해할 수 없는 의견이 많을수록 긴장도 급증

            d_phase = (i_data["omega"] + (consensus_force / total_nodes)) * dt
            new_tension = min(cognitive_dissonance / total_nodes, 10.0)

            deltas[i_id] = {"d_phase": d_phase, "target_tension": new_tension}

        return deltas

    def render_state(self, states: Dict[str, Dict[str, Any]]) -> str:
        hypotheses = ['🟦', '🟩', '🟨', '🟥']
        visual = [hypotheses[int((s['phase'] / (2 * math.pi)) * 4) % 4] for s in states.values()]
        avg_tension = sum(s['tension'] for s in states.values()) / len(states)
        status_bar = "".join(visual)
        return f"Dissonance: {avg_tension:.2f} | {status_bar}"

@contract.kernel("sensor.kuramoto")
class SensorKuramoto(IDynamicsKernel):
    """Φ-evolution kernel: global phase coupling operator"""
    def __init__(self, **kwargs):
        if "config" in kwargs and isinstance(kwargs["config"], KernelConfig):
            self.config = kwargs["config"]
        else:
            self.config = KernelConfig(**kwargs)

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        """dΦ/dt: distributed phase update"""
        deltas = {}
        total_nodes = len(states)

        for i_id, i_data in states.items():
            coupling_force = 0.0
            total_incoherence = 0.0

            if i_data.get("state") not in ["ATTRACTOR", "REFLECTOR"]:
                for j_id, j_data in states.items():
                    if i_id == j_id: continue
                    phase_diff = j_data["phase"] - i_data["phase"]
                    coupling_force += math.sin(phase_diff)
                    total_incoherence += abs(phase_diff)

                d_phase = (i_data["omega"] + (self.config.global_coupling / total_nodes) * coupling_force) * dt
                new_tension = total_incoherence / total_nodes
                deltas[i_id] = {"d_phase": d_phase, "target_tension": new_tension}
            else:
                deltas[i_id] = {"d_phase": (i_data["omega"] * 1.5) * dt, "target_tension": 0.0}
                
        return deltas

    def render_state(self, states: Dict[str, Dict[str, Any]]) -> str:
        chars = ['🌑', '🌘', '🌗', '🌖', '🌕', '🌔', '🌓', '🌒']
        visual = [chars[int((s['phase'] / (2 * math.pi)) * 8) % 8] for s in states.values()]
        avg_tension = sum(s['tension'] for s in states.values()) / len(states)
        return f"Tension: {avg_tension:.2f} | " + "".join(visual)

@contract.kernel("sensor.exchange")
class SensorExchange(IDynamicsKernel):
    """
    @evo.sensor: Macroscopic Mean-Field Operator
    - Models the mechanism of 'Cognitive Surrender' and critical tension eruptions.
    - Tracks phase transitions triggered when local node topologies collide with  the macroscopic Mean-Field.
    """
    def __init__(self, **kwargs):
        if "config" in kwargs and isinstance(kwargs["config"], KernelConfig):
            self.config = kwargs["config"]
        else:
            self.config = KernelConfig(**kwargs)
            
        ## Phase Surrender Threshold: The critical boundary of topological stability for individual cognitive autonomy.
        self.surrender_threshold = kwargs.get("sync_threshold", 0.3)

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)
        
        ## Macroscopic Mean-Field Synthesis: Calculating the collective phase attractor
        avg_phase = sum(d["phase"] for d in states.values()) / total_nodes

        for i_id, i_data in states.items():
            if i_data.get("state") == "ATTRACTOR": 
                ## @anchor: Invariant manifolds that resist the global field to maintain absolute reference points
                deltas[i_id] = {"d_phase": i_data["omega"] * dt, "target_tension": 0.0}
                continue

            ## @topos.drift: Deviation between local node identity and the macroscopic field
            field_drift = avg_phase - i_data["phase"]
            
            ## @phase.collapse
            if abs(field_drift) > self.surrender_threshold:
                d_phase = (field_drift * self.config.global_coupling) * dt
                ## Ontological surrender results in the dissipation of cognitive dissonance (Tension)
                new_tension = 0.0 
            else:
                ## @phase.resistance
                d_phase = i_data["omega"] * dt
                new_tension = min(i_data["tension"] + abs(field_drift) * 0.1, 10.0)

            deltas[i_id] = {"d_phase": d_phase, "target_tension": new_tension}

        return deltas
    
    def render_state(self, states: Dict[str, Dict[str, Any]]) -> str:
        """@desc: Projects the continuous phase space onto discrete cognitive resonance metrics"""
        resonance = 0
        divergence = 0 
        visual = []

        for s in states.values():
            ## Map circular phase space to a linear projection axis [-1, 1]
            position = math.sin(s["phase"]) 
            
            if position > 0.5:
                visual.append('🟢') ## Positive Resonance
                resonance += 1
            elif position > 0:
                visual.append('↗️')
                resonance += 1
            elif position > -0.5:
                visual.append('↘️')
                divergence += 1
            else:
                visual.append('🔴') ## Negative Resonance
                divergence += 1
                
        avg_tension = sum(s['tension'] for s in states.values()) / len(states)
        field_regime = "RESONANT" if resonance > divergence else "DIVERGENT"
        status_bar = "".join(visual)
        return f"Flux(Tension): {avg_tension:.2f} | Sync({resonance:02d}:{divergence:02d}) [{field_regime}] | {status_bar}"