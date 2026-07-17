# xphi.kernel.contract.channel
from __future__ import annotations
import asyncio
import json
import sys
import argparse
from pathlib import Path
from typing import Dict, Any, Union

from xphi.kernel.contract.proposer import execute_proposer

from arch.contract.registry.path import path_registry
from arch.contract.discovery import discover_modules
from arch.contract.event.psi import PsiCarrier, PsiEvent
from arch.topos.edge.treg import FrameRegistry, TregEdge, PhaseState
from arch.topos.edge.trajectory import SignatureBound, TrajectoryXor

from phase.bind.resolver import find_current_self
from phase.executor.dynamics import DynamicsExecutor
from phase.runtime.node import NodeRuntime

from arch.contract.validator import ContractValidator
from watcher.xe.cont import LoopCarrier
from watcher.plane.emitter import get_emitter

log = get_emitter("contract.channel")

DEFAULT_REDIS_PAYLOAD: Dict[str, Any] = {
    "system_type": "CHANNEL_EXCHANGE", "ators": [],
    "runtime": {"seed": 77, "max_ticks": 1000, "sleep_interval": 0.05, "dt": 0.1},
    "kernel": {"type": "sensor.exchange", "params": {"global_coupling": 1.5, "herd_threshold": 0.35}},
    "field": {"type": "node.network", "params": {"size": 30, "init_phase_range": [0, 6.28], "omega_range": [0.1, 0.8]}},
    "watcher": {"type": "singularity.watcher", "params": {"candidate_limit": 10.0, "rupture_limit": 25.0}},
    "regime": {"type": "node.regime", "params": {}}
}

class ContractChannel:
    """@topos: Orchestrator for Macroscopic Herd Dynamics"""
    def __init__(self, config_payload: Union[str, Dict[str, Any]]):
        discover_modules(find_current_self())
        self.log = get_emitter("exchange.system", phase="BOOT")
        self.config = config_payload if isinstance(config_payload, dict) else json.loads(config_payload)
        self.executor: DynamicsExecutor = None
        self.node: NodeRuntime = None
        self.treg_gate: TregEdge = None
        self.signature: SignatureBound = None
        self._initialize_topology()

    def _perform_static_validation(self):
        """flow.json 로드 및 정합성 체크"""
        try:
            flow_path = path_registry.resolve("contract") / "flow.json"
            if not flow_path.exists():
                self.log.warning("!!! flow.json not found. Skipping static validation. !!!")
                return

            validator = ContractValidator(flow_path)
            validator.validate(self.config)
            self.log.info(">>> Static Contract Validation Passed. (Phase: Cryst) <<<")
        except Exception as e:
            self.log.critical(f"🚨 [STATIC VALIDATION FAILED] {e}")
            sys.exit(1)

    def _initialize_topology(self):
        """Prepares the manifold and seeds the initial population of agents."""
        field_size = self.config["field"]["params"]["size"]
        self.config["ators"] = [
            {
                "type": "node.ator", 
                "id": f"trader_{i}", 
                "initial_state": "ATTRACTOR" if i % 10 == 0 else "NORMAL",
                "params": {"tolerance_threshold": 8.0}
            }
            for i in range(field_size)
        ]
        self.executor = DynamicsExecutor(config_dict=self.config)
        loop_xe = LoopCarrier(
            xe=self.executor, 
            max_ticks=self.config["runtime"]["max_ticks"], 
            interval=self.config["runtime"]["sleep_interval"]
        )
        self.node = NodeRuntime(executor=loop_xe)
        self._setup_immune_checkpoint()

    def _setup_immune_checkpoint(self):
        """Initializes the Treg-inspired homeostatic regulation layer."""
        registry = FrameRegistry()
        xor_engine = TrajectoryXor(tension_threshold=0.5)
        self.signature = SignatureBound(
            module_id="meta.self.exchange.macro",
            base_instructions="Exchange Herd Dynamics Circuit Breaker",
            input_fields=[], output_fields=[]
        )
        self.treg_gate = TregEdge(
            registry=registry, 
            signature=self.signature, 
            xor_engine=xor_engine
        )

    async def _circuit_breaker_monitor(self):
        """Systemic monitor for macroscopic tension (VIX) and manifold rupture."""
        await asyncio.sleep(3.0) # Warm-up period for field stabilization
        self.log.info(">>> Treg Circuit Breaker Armed. Monitoring Macro Tension... <<<")
        
        while True:
            await asyncio.sleep(0.5)
            states = self.executor.states
            if not states:
                continue
                
            ## Compute Macroscopic Fatigue (Systemic IX)
            total_tension = sum(s.get("tension", 0.0) for s in states.values())
            ix = total_tension / max(len(states), 1)
            
            ## Manifold integrity check via Treg traverse
            market_state = PhaseState(
                membrane_bound=True, axp_ratio=ix, 
                ctla_4_expression=0.1, cd28_expression=0.8,
                lineage_path="cognitive.exchange.mean_field.collapse"
            )
            res = self.treg_gate.traverse(market_state)
            if res["status"] in ["closed", "rejected_by_memory"]:
                await self._handle_rupture(ix, res.get('message', 'Macro Herd Collapse'))
                break

    async def _handle_rupture(self, ix: float, reason: str):
        """Handles emergency phase suspension when the system exceeds homeostatic limits."""
        self.log.critical("=" * 60)
        self.log.critical(f"🚨 [CIRCUIT BREAKER ACTIVATED] {reason}")
        self.log.critical(f"🚨 Systemic Mean Field Rupture (IX: {ix:.2f}). Enforcing Suspension.")
        self.log.critical("=" * 60)
        
        self.log.info(f"\n>>> Final Imlog.infoed Immune Memory (Φ) <<<\n{self.signature.dump_state()}")
        
        if hasattr(self.node, 'stop'):
            await self.node.stop()
        else:
            sys.exit(1)

    async def _boot_clock(self):
        """Injects the initial kinetic pulse to catalyze the field's evolution."""
        await asyncio.sleep(2.0)
        self.log.info(">>> Injecting Field Mean-Field Boot Pulse (Bell Ringing)... <<<")
        seed_event = PsiEvent(
            event_id="boot-tick-exchange", parent_id=None,
            source_id="system.exchange", scope="LOCAL", tick=1,
            carrier=PsiCarrier(kind="TICK", tag="MARKET_OPEN", payload={}),
            phase_id=0, context={"phase": "loop", "domain": "watcher"}
        )
        await self.node.bus.publish(seed_event)

    async def run(self):
        """Launches the entire macroscopic exchange system."""
        self.log.info("Exchange Node launching Macroscopic Herd Dynamics (Exa-Hange)...")
        
        # Schedule concurrent monitoring and pulse tasks
        asyncio.create_task(self._circuit_breaker_monitor())
        asyncio.create_task(self._boot_clock())
        await self.node.start()