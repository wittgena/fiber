# xphi.xor.tester.flow.judgment
## @lineage: anchor.tester.flow.judgment
## @lineage: anchor.tester.judgment
## @lineage: meta.ops.judgment
import time
import random
import asyncio
import argparse
from typing import Dict, Any, AsyncGenerator, Callable
from pathlib import Path

from arch.contract.registry.unified import contract
from phase.gov.node.anchor import EpochManager
from arch.xor.judger import PureJudger, Signal, Residue
from arch.contract.protocol import proto, BASE_LOOP
from arch.proto.event.network import MultiEventFlow
from arch.proto.event.psi import PsiEvent, PsiCarrier, CarrierType, PhaseField

from phase.bind.resolver import find_current_self, get_invoker
from watcher.plane.flow.executor import dispatch_flow_cli
from watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

class PseudoField:
    def __init__(self, name: str):
        self.name = name

class FlowEvent:
    def __init__(self, phase: str, psi_name: str, phi_name: str, boundary_name: str):
        self.phase = phase
        self.psi = PseudoField(psi_name)
        self.phi = PseudoField(phi_name)
        self.boundary = PseudoField(boundary_name)

class CollapseEvent:
    def __init__(self, surface_name: str):
        self.surface = PseudoField(surface_name)


class JudgmentFlow(MultiEventFlow[PsiEvent, PureJudger]):
    def __init__(self, node_name: str, workspace_path: str, initial_flow: PsiEvent):
        super().__init__()
        self.judger = PureJudger()
        self.initial_flow = initial_flow
        self.anchor = EpochManager(
            name=node_name,
            path=workspace_path,
            runner=lambda p, msg, apply: f"state_{int(time.time() * 1000)}" 
        )

    async def execute(self) -> AsyncGenerator[Any, None]:
        """@flow: Ψ → Interference(Signal vs Residue) → Φ' → Lineage Commit → Attractor"""
        log.info(f"[*] Initiating Pure judgment Flow (Source: {self.initial_flow.source_id})")
        
        payload = self.initial_flow.carrier.payload
        pressure_factor = payload.get("recognition_strength", 0.8) * payload.get("replication_rate", 1.5)
        resistance = payload.get("methylation_level", 0.2)
        max_steps = payload.get("steps", 50)

        cycle = 0
        is_unstable = True
        reentry_msg = "Initial topological injection"
        era_id = f"era_{self.initial_flow.event_id}"

        while is_unstable and cycle < max_steps:
            cycle += 1
            
            current_signal = Signal(
                source=self.initial_flow.symbol,
                pressure=pressure_factor,
                frequency="high" if pressure_factor > 1.0 else "low",
                payload=reentry_msg
            )
            
            residues = []
            effective_cleavage = pressure_factor - resistance
            if random.random() < effective_cleavage:
                residues.append(Residue(
                    topos_path=f"field.cycle.{cycle}",
                    dissonance_type="cleavage_rupture",
                    content="Phase topology disrupted by external pressure."
                ))
                pressure_factor *= 0.5 
            else:
                pressure_factor *= 1.1 

            is_unstable, reentry_msg = self.judger.integrate(current_signal, residues, cycle)
            
            commit_msg = (
                f"[Cycle {cycle}] Energy: {self.judger.phi_prime.potential_energy:.2f} | "
                f"Residues: {len(residues)} | State: {'Evolving' if is_unstable else 'Collapsed'}"
            )
            self.anchor.inscribe(
                anchor_id=era_id,
                parent_anchor_id=None,
                parent_commit_id=getattr(self, "_last_commit", "0000000"),
                message=commit_msg,
                apply=True
            )
            self._last_commit = f"cycle_{cycle}"
            
            yield FlowEvent(
                phase="INTERFERENCE",
                psi_name=f"Cycle-{cycle}",
                phi_name=f"Energy-{self.judger.phi_prime.potential_energy:.2f}",
                boundary_name=f"Residues-{len(residues)}"
            )
            await asyncio.sleep(0.1)

        log.info(f"[*] Flow Collapsed. Final projection generated.")
        yield CollapseEvent(surface_name=f"Converged_in_{cycle}_cycles")

@contract.flow(name=MODULE_NAMESPACE, entry="judgment_entry")
def judgment_entry(cli_args: list = None, **payload) -> JudgmentFlow:
    carrier = PsiCarrier(
        kind="judgment_injection",
        tag="judgment",
        payload=payload,
        carrier_type=CarrierType.RECURSIVE,
        target_field=PhaseField.INTERFERENCE
    )
    initial_event = PsiEvent(
        event_id=f"psi_{int(time.time())}",
        parent_id=None,
        source_id="external_cli",
        scope="global",
        tick=0,
        carrier=carrier
    )
    return JudgmentFlow(node_name="judgment_core", workspace_path="./workspace", initial_flow=initial_event)

if __name__ == "__main__":
    dispatch_flow_cli(
        command_name=MODULE_NAMESPACE, 
        entry_func=judgment_entry, 
        file_path=__file__
    )