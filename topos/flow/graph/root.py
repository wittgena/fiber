# topos.flow.graph.root
import asyncio
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any

from topos.flow.transition import FlowTransition
from topos.flow.folding import TopologyController

from topos.flow.graph.executor import GraphExecutor
from topos.flow.graph.node import EngineNode
from arch.topos.space.organizer import SpaceNode
from arch.model.surge.blueprint import SurgeBlueprint
from arch.topos.node.gan import Message, GanNode
from arch.model.sealer import EpochSealer

from phase.executor.flow.event import AgentConfigured
from watcher.dphi.broker import WasmBroker
from watcher.dphi.adapter.exchange import ExchangeAdapter, TransactionReceipt
from watcher.dphi.cgroup import Tier
from watcher.plane.emitter import get_emitter

log = get_emitter("topos.root")

DEFAULT_TOPOLOGY_SPEC: Dict[str, Dict[str, Any]] = {
    "ConfigPolicyNode": {
        "location": "local",
        "edges": ["ConfigSettingsNode", "DockerWorkspaceNode"]
    },
    "DockerWorkspaceNode": {
        "location": "local",
        "edges": ["ConfigPolicyNode"]
    },
    "ConfigSettingsNode": {
        "location": "local",
        "edges": ["ConfigPolicyNode"]
    }
}

@dataclass
class TopologyResidue:
    edge_state: str
    trajectory_trace: str
    receipt: Optional[TransactionReceipt]

class AgentTopos(GanNode):
    def __init__(self, name: str, run_context: dict):
        super().__init__(name)
        self.run_context = run_context
        
        # [WASM Kernel & Exchange]
        self.broker = WasmBroker()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key="local_clearing_pub_key")
        self.last_receipt: Optional[TransactionReceipt] = None
        
        # [State Management]
        self.config_count = 0
        self.target_configs = 2  
        self._active_blueprint: Optional[SurgeBlueprint] = None
        self._initial_instruction: str = ""
        self._injected_settings = None  
        
        # [Refactored] 이중 래퍼였던 Tracker를 제거하고, 통합된 FlowTransition을 직접 구동합니다.
        self.transition = FlowTransition(origin=name)

    def setup_default_nodes(self):
        # [Refactored] TopologyAssembler 제거 -> TopologyController로 조립 위임
        TopologyController.assemble_and_mount(
            topos=self, 
            run_context=self.run_context, 
            broker=self.broker, 
            topology_spec=DEFAULT_TOPOLOGY_SPEC
        )
        return self

    async def _execute_pipeline(self, prompt: str = "", blueprint: Optional[SurgeBlueprint] = None) -> TopologyResidue:
        """@desc: Unified execution pipeline bounded by WASM Cgroup Policies."""
        self._initial_instruction = prompt
        self._active_blueprint = blueprint
        self.transition.unbind_and_reset()
        
        log.info(f"[{self.name}] 🛡️ Enforcing standard spatial density via WASM Cgroup (Tier: STANDARD).")
        await self.broker.update_policy(Tier.STANDARD.value)
        
        app_task = asyncio.create_task(self.run())
        self.post_message(Message("boot"))
        
        log.info(f"[{self.name}] Awaiting fluid topology convergence...")
        
        # [Refactored] Tracker의 이중 메서드를 거치지 않고, FlowTransition에서 직접 대기합니다.
        trajectory_result = await self.transition.await_convergence(timeout=600.0)
        
        log.info(f"[{self.name}] Initiating teardown sequence. Current Edge State: {self.transition.edge.value}")
        self.post_message(Message("shutdown"))
        
        if not app_task.done():
            await app_task
            
        residue = TopologyResidue(
            edge_state=self.transition.edge.value,
            trajectory_trace=trajectory_result,
            receipt=self.last_receipt
        )
        
        self._print_verification_report(residue)
        return residue

    def _print_verification_report(self, residue: TopologyResidue):
        log.info("\n" + "="*60)
        log.info(f"🌌 [TOPOLOGY FINALIZED] Dominium Edge State: {residue.edge_state}")
        log.info("-" * 60)
        log.info("[EXECUTION TRAJECTORY]")
        for line in residue.trajectory_trace.split('\n'):
            log.info(f"  {line}")
            
        log.info("-" * 60)
        log.info("[CRYPTOGRAPHIC SETTLEMENT RECEIPT]")
        if residue.receipt:
            log.info(f"  Receipt ID   (Topos): {residue.receipt.job_id}")
            log.info(f"  State Root   (Parity): {residue.receipt.unified_parity_hash}")
            log.info(f"  Signatures   (M-of-N): {len(residue.receipt.clearing_signatures)} validators")
            log.info(f"  Fuel Burned  (Compute): {residue.receipt.fuel_consumed}")
            log.info(f"  Status       : {residue.receipt.settlement_status}")
        else:
            log.warning("  ⚠️ No receipt generated (Epoch sealing fractured or bypassed).")
        log.info("="*60 + "\n")

    async def run_task(self, instruction: str) -> TopologyResidue:
        return await self._execute_pipeline(prompt=instruction)

    async def run_scheme(self, blueprint: SurgeBlueprint) -> TopologyResidue:
        log.info(f"[{self.name}] 🎯 Aligning topology context: {blueprint.topology_name} (Focus: {blueprint.focus})")
        return await self._execute_pipeline(blueprint=blueprint)

    async def on_boot(self, message: Message):
        log.info(f"[{self.name}] 🚀 Booting hybrid system layers")
        for child in self.children:
            if isinstance(child, (GraphExecutor, EngineNode)):
                child.post_message(Message("boot"))

    async def on_agent_configured(self, message: Message):
        self.config_count += 1
        if isinstance(message, AgentConfigured) and message.settings:
            self._injected_settings = message.settings
            runtime_type = "Proxy" if message.is_proxy else "Local"
            log.info(f"[{self.name}] 📥 Engine asset captured (Resolved Archetype: {runtime_type}).")

        log.info(f"[{self.name}] 📥 Configuration quorum updated ({self.config_count}/{self.target_configs})")
        if self.config_count == self.target_configs:
            log.info(f"[{self.name}] 🚦 Quorum reached. Signaling Workspace creation wave.")
            for child in self.children:
                if isinstance(child, SpaceNode):
                    child.post_message(Message("start_workspace"))

    async def on_workspace_ready(self, message: Message):
        space_type = "Remote Proxy" if getattr(message, 'is_proxy', False) else "Local Docker"
        log.info(f"[{self.name}] 🌐 Execution space aligned via {space_type}.")
        
        for child in self.children:
            if isinstance(child, GraphExecutor):
                log.info(f"[{self.name}] Deploying payload to PolicyNode.")
                # [Refactored] DeploymentDispatcher 제거 -> TopologyController로 메시지 배포 위임
                TopologyController.dispatch_payload(
                    policy_node=child, 
                    blueprint=self._active_blueprint, 
                    instruction=self._initial_instruction, 
                    settings=self._injected_settings
                )

    async def on_llm_event(self, message: Message):
        raw_content = getattr(message, 'llm_message', '')
        llm_text = str(raw_content)
        log.info(f"[{self.name}] 💬 [LLM Flux Path]: {llm_text[:100]}...")
        # [Refactored] Tracker._log 호출 중복 제거
        self.transition.record(f"LLM: {llm_text}")

    async def _fetch_final_parity_state(self) -> dict:
        return {
            "parity": {
                "topos_id": f"task_{self.transition.id}",
                "phase_id": 0, 
                "nexus_id": 0
            },
            "repos": {}
        }

    async def on_task_completed(self, message: Message):
        cost = getattr(message, 'cost', 0.0)
        fuel_consumed = getattr(message, 'fuel_consumed', 0)
        mode_tag = "Proxy Channel" if getattr(message, 'is_proxy', False) else "Local Resource"
        
        log.info(f"[{self.name}] ✅ Task converged (Cost: {cost}, Incurred via: {mode_tag})")
        self.transition.record(f"Task Completed. Cost: {cost} ({mode_tag})")
        
        actual_entangled_state = await self._fetch_final_parity_state()
        
        canonical_payload = EpochSealer.generate_seal_payload(
            entangled_state=actual_entangled_state,
            parent_commit_id="genesis"
        )
        
        res = await self.broker.invoke("seal_epoch", canonical_payload)
        signatures = []
        if res.success:
            try:
                seal_data = json.loads(res.output)
                signatures = seal_data.get("signatures", [])
            except json.JSONDecodeError:
                log.warning(f"[{self.name}] Unparseable response from seal_epoch.")
        else:
            log.warning(f"[{self.name}] Epoch sealing encountered friction: {res.error}")
        
        self.last_receipt = self.exchange_adapter.finalize_settlement(
            entangled_state=actual_entangled_state,
            signatures=signatures, 
            cost_metrics={"fuel_consumed": fuel_consumed, "accumulated_cost": cost},
            tier=Tier.STANDARD.value
        )
        
        log.info(f"[{self.name}] 🧾 Transaction Receipt Issued: {self.last_receipt.job_id}")
        
        # [Refactored] Transition 객체가 직접 Dominium에 도달하고 Future를 완료시킴
        self.transition.reach_dominium(resource_address=f"urn:surgent:resource:resolved_task_{self.transition.id}")

    async def on_node_error(self, message: Message):
        source = getattr(message, 'source_node', 'Unknown')
        error = getattr(message, 'error', 'Unknown Error')
        
        log.critical(f"[{self.name}] 🚨 Fatal topological rupture originating from [{source}].")
        log.critical(f"[{self.name}] ❌ Error Details: {error}")
        if "Connection refused" in str(error) and "DockerWorkspaceNode" in source:
            log.info(f"[{self.name}] 💡 HINT: Docker daemon is unreachable. Please ensure Docker Desktop/OrbStack is running, or use '--proxy' for remote execution.")
            
        self.transition.record(f"System Error: {source} failed -> {error}")
        self.transition.fracture_topology(lmbda=0.2, tau=1.0, force_collapse=True)

    async def on_shutdown(self, message: Message):
        log.info(f"[{self.name}] 💤 Purging system manifolds and reclaiming resources.")
        if self.transition.future and not self.transition.future.done():
            log.warning(f"[{self.name}] ⚠️ Premature shutdown detected before state convergence. Fracturing topology.")
            self.transition.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            
        for child in list(self.children):
            await self.unmount(child)
        self.stop()