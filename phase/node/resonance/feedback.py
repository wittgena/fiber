# phase.node.resonance.feedback
## @lineage: watcher.ator.resonance.feedback
## @lineage: arch.topos.ator.resonance.feedback
## @lineage: phase.ator.resonance.feedback
## @lineage: phase.bind.resonance.feedback
## @lineage: phase.resonance.feedback
## @lineage: swarm.resonance.feedback
## @lineage: hub.residue.resonance.feedback
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict
from arch.model.phase.flow import PhaseFlow, FlowState, Transduction, Resonance, Judgment, Align
from arch.contract.registry.unified import contract
from watcher.plane.emitter import get_logger

log = get_logger("ator.resonator")

@contract.ator("resonance.feedback")
class ResonanceFeedback(Resonance):
    def interfere(self, gen, crit):
        """Ψ_gen ⊕ Ψ_crit → Δ → decision"""
        if "fail" in str(crit).lower():
            return {
                "status": "retry",
                "payload": gen
            }
        return {
            "status": "ok",
            "payload": f"[RESOLVED]\nGEN={gen}\nCRIT={crit}"
        }

@contract.ator("security.resonance")
class SecurityResonance(Resonance):
    def interfere(self, gen_payload, crit_payload):
        score = crit_payload.get("security_score", 0)
        vulnerabilities = crit_payload.get("vulnerabilities", [])
        if score < 80:
            return {"status": "retry", "payload": {"feedback": f"Score {score}. Fix {vulnerabilities}"}}
        return {"status": "stable", "payload": {"final_artifact": gen_payload.get("code")}}
