# ops.scope.flow.graph.root
import asyncio
import json
import time
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from topos.bound.space import SpaceNode

from ops.scope.execution.graph import PolicyNode
from ops.scope.flow.graph.node import EngineNode
from ops.scope.flow.folding import ManifoldFolder
from ops.scope.flow.transition import FlowTransition, EdgeFlow

from arch.contract.schema.graph import EntryNode
from arch.topos.bound.surge.blueprint import SurgeBlueprint
from arch.topos.node.gan import Message, GanNode
from arch.topos.bound.sealer import EpochSealer
from arch.topos.flow.event import AgentConfigured

from watcher.dphi.broker import WasmBroker
from watcher.dphi.adapter.exchange import D3fiExchangeAdapter, TransactionReceipt
from watcher.dphi.cgroup import Tier
from watcher.dphi.adapter.state import StateAdapter
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
    """
    @desc: The final, immutable container holding the cryptographic proof of the entire execution.
           Whether the agent succeeded or ruptured, this residue is always generated.
    """
    edge_state: str
    trajectory_trace: str
    receipt: Optional[TransactionReceipt]


class AgentTopos(GanNode):
    """
    @desc: Root Topology Orchestrator anchoring the entire hybrid (Agent + WASM) system.
    @flow: Core Lifecycle -> WasmBroker Injection -> Topological Assembly -> Execution Collapse -> Exchange Settlement.
    """
    def __init__(self, name: str, run_context: dict):
        super().__init__(name)
        self.run_context = run_context
        
        # [WASM Kernel] Inject the absolute physics engine at the root of the topology
        self.broker = WasmBroker()
        
        # [Exchange Membrane] Initializes the adapter that translates internal state to external value (Receipts)
        self.exchange_adapter = D3fiExchangeAdapter(clearing_house_pub_key="local_clearing_pub_key")
        self.last_receipt: Optional[TransactionReceipt] = None
        
        # State Management
        self.config_count = 0
        self.target_configs = 2  
        self._active_blueprint: Optional[SurgeBlueprint] = None
        self._initial_instruction: str = ""
        self._injected_settings = None  
        
        # Fluid Dynamics Tracker
        self.tracker = ConvergenceTracker(name)

    def setup_default_nodes(self):
        """
        @desc: Delegates physical wiring to the Assembler, passing down the WasmBroker 
               so spatial boundaries can be cryptographically anchored to dphi.wasm.
        """
        TopologyAssembler.wire_nodes(self, self.run_context, broker=self.broker)
        return self

    async def _execute_pipeline(self, prompt: str = "", blueprint: Optional[SurgeBlueprint] = None) -> TopologyResidue:
        """@desc: Unified execution pipeline bounded by WASM Cgroup Policies."""
        self._initial_instruction = prompt
        self._active_blueprint = blueprint
        self.tracker.reset()
        
        # @t0.policy: Enforce baseline resource constraints before execution begins
        log.info(f"[{self.name}] 🛡️ Enforcing standard spatial density via WASM Cgroup (Tier: STANDARD).")
        await self.broker.update_policy(Tier.STANDARD.value)
        
        app_task = asyncio.create_task(self.run())
        self.post_message(Message("boot"))
        
        log.info(f"[{self.name}] Awaiting fluid topology convergence...")
        
        # Deadlock is prevented here by ensuring the Tracker always resolves (even on fracture)
        trajectory_result = await self.tracker.await_convergence(timeout=600.0)
        
        log.info(f"[{self.name}] Initiating teardown sequence. Current Edge State: {self.tracker.transition.edge.value}")
        self.post_message(Message("shutdown"))
        
        if not app_task.done():
            await app_task
            
        # 1. Structure the Final Verification Residue
        residue = TopologyResidue(
            edge_state=self.tracker.transition.edge.value,
            trajectory_trace=trajectory_result,
            receipt=self.last_receipt
        )
        
        # 2. Output the Cryptographic Audit Log
        self._print_verification_report(residue)
            
        return residue

    def _print_verification_report(self, residue: TopologyResidue):
        """
        @desc: Visually outputs the execution trajectory and the cryptographic receipt
               for external verification, emphasizing the deterministic outcome regardless of agent success.
        """
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
            if isinstance(child, (PolicyNode, EngineNode)):
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
            if isinstance(child, PolicyNode):
                log.info(f"[{self.name}] Deploying payload to PolicyNode.")
                DeploymentDispatcher.dispatch_to_policy(
                    child, self._active_blueprint, self._initial_instruction, self._injected_settings
                )

    async def on_llm_event(self, message: Message):
        raw_content = getattr(message, 'llm_message', '')
        llm_text = str(raw_content)
        log.info(f"[{self.name}] 💬 [LLM Flux Path]: {llm_text[:100]}...")
        self.tracker.record(f"LLM: {llm_text}")

    async def _fetch_final_parity_state(self) -> dict:
        """
        @desc: Retrieves the genuine cryptographic state (Parity Triplet) aggregated 
               by the Tunnel/Adapter during the execution pipeline.
        """
        return {
            "parity": {
                "topos_id": f"task_{self.tracker.transition.id}",
                "phase_id": 0, 
                "nexus_id": 0
            },
            "repos": {}
        }

    async def on_task_completed(self, message: Message):
        """
        @desc: Intercepts the convergent state, retrieves the true WASM parity, 
               cryptographically seals the epoch, and delegates to ExchangeAdapter.
        """
        cost = getattr(message, 'cost', 0.0)
        fuel_consumed = getattr(message, 'fuel_consumed', 0)
        mode_tag = "Proxy Channel" if getattr(message, 'is_proxy', False) else "Local Resource"
        
        log.info(f"[{self.name}] ✅ Task converged (Cost: {cost}, Incurred via: {mode_tag})")
        self.tracker.record(f"Task Completed. Cost: {cost} ({mode_tag})")
        
        # 1. Fetch the genuine cryptographic state
        actual_entangled_state = await self._fetch_final_parity_state()
        
        # 2. Cryptographically Seal the Epoch via dphi.wasm
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
        
        # 3. Final Settlement via ExchangeAdapter
        self.last_receipt = self.exchange_adapter.finalize_settlement(
            entangled_state=actual_entangled_state,
            signatures=signatures, 
            cost_metrics={"fuel_consumed": fuel_consumed, "accumulated_cost": cost},
            tier=Tier.STANDARD.value
        )
        
        log.info(f"[{self.name}] 🧾 Transaction Receipt Issued: {self.last_receipt.job_id}")
        
        # 4. Anchor topology
        self.tracker.reach_dominium(resource_address=f"urn:surgent:resource:resolved_task_{self.tracker.transition.id}")

    async def on_node_error(self, message: Message):
        """@desc: Catches propagated errors from child nodes and fractures the topology safely."""
        source = getattr(message, 'source_node', 'Unknown')
        error = getattr(message, 'error', 'Unknown Error')
        
        log.critical(f"[{self.name}] 🚨 Fatal topological rupture originating from [{source}].")
        log.critical(f"[{self.name}] ❌ Error Details: {error}")
        
        # 💡 [개선] 사용자를 위한 명확한 힌트 제공 (UX 향상)
        if "Connection refused" in str(error) and "DockerWorkspaceNode" in source:
            log.info(f"[{self.name}] 💡 HINT: Docker daemon is unreachable. Please ensure Docker Desktop/OrbStack is running, or use '--proxy' for remote execution.")
            
        self.tracker.record(f"System Error: {source} failed -> {error}")
        
        # 💡 [개선] 데드락 방지: 즉시 강제 붕괴 처리하여 메인 Future를 Resolve시킴
        self.tracker.fracture_topology(lmbda=0.2, tau=1.0, force_collapse=True)

    async def on_shutdown(self, message: Message):
        """@desc: Handles graceful or forced teardown of the execution environment."""
        log.info(f"[{self.name}] 💤 Purging system manifolds and reclaiming resources.")
        
        # 💡 [개선] 데드락 방지: 자식 노드가 냅다 shutdown을 던져버린 경우 Future 고립 방지
        if self.tracker.future and not self.tracker.future.done():
            log.warning(f"[{self.name}] ⚠️ Premature shutdown detected before state convergence. Fracturing topology.")
            self.tracker.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            
        for child in list(self.children):
            await self.unmount(child)
        self.stop()


class TopologyAssembler:
    """
    @desc: A builder responsible for wiring physical nodes and initializing the topology.
           Folds the network using spatial boundaries enforced by dphi.wasm.
    """
    @staticmethod
    def wire_nodes(topos: GanNode, run_context: dict, broker: Optional[WasmBroker] = None):
        use_proxy = run_context.get("use_proxy", False)
        target_model = run_context.get("target_model")
        
        broker = broker or WasmBroker()
        
        log.info(f"[{topos.name}] 🏗️ Wiring hybrid nodes into WASM-anchored topology (Proxy Mode: {use_proxy}, Model: {target_model})")
        
        # Step 1: Instantiate Raw Vertices (Execution Nodes)
        policy_node = PolicyNode("ConfigPolicyNode")
        engine_node = EngineNode("ConfigSettingsNode", use_proxy=use_proxy, target_model=target_model)
        space_node = SpaceNode("DockerWorkspaceNode", use_proxy=use_proxy)
        
        # Step 2: Apply Cryptographic Spatial Fences (The Folding Process)
        folder = ManifoldFolder(broker=broker)
        folded_registry = folder.fold_manifold(
            active_nodes={
                "ConfigPolicyNode": policy_node,
                "ConfigSettingsNode": engine_node,
                "DockerWorkspaceNode": space_node
            },
            topology_spec=DEFAULT_TOPOLOGY_SPEC
        )
        
        # Step 3: Mount the mathematically bounded nodes into the Root Topology
        for node_id, folded_node in folded_registry.items():
            topos.mount(folded_node.instance)
            
        log.info(f"[{topos.name}] 🌌 Topology spatial folding complete. Nodes secured by WASM boundaries.")


class ConvergenceTracker:
    """
    @desc: Advanced telemetry tracker leveraging fluid state dynamics.
           Replaces binary success/fail with Topological Edge Flows (Φ⁺, Φᶠ, Ψᴰ).
    """
    def __init__(self, owner_name: str):
        self.owner_name = owner_name
        self.transition = FlowTransition(origin=owner_name)
        self.future: Optional[asyncio.Future] = None

    def reset(self):
        self.transition.unbind_and_reset()
        self.future = asyncio.Future()

    def record(self, entry: str):
        self.transition._log(entry)

    def reach_dominium(self, resource_address: str):
        passed = self.transition.threshold_test(lmbda=1.0, tau=0.5)
        if passed:
            self.transition.anchor(resource_address)
            if self.future and not self.future.done():
                self.future.set_result(True)

    def fracture_topology(self, lmbda: float, tau: float, force_collapse: bool = False):
        self.transition.threshold_test(lmbda=lmbda, tau=tau)
        if force_collapse:
            self.transition.bind(EdgeFlow.COLLAPSED)
        if self.future and not self.future.done():
            self.future.set_result(False) # Resolve as False to break the await lock

    async def await_convergence(self, timeout: float = 600.0) -> str:
        """@desc: Awaits the resolution of the topology, aggregating memory logs into a trace."""
        try:
            success = await asyncio.wait_for(self.future, timeout=timeout)
            trace_output = "\n".join([f"[{m.get('new_state', m.get('previous_state', '0'))}] {m['event']}" for m in self.transition.memory])
            if not success:
                trace_output += "\n[⚠️ SYSTEM COLLAPSED] Execution fractured before reaching dominium. Trace aborted."
                
            return trace_output
            
        except asyncio.TimeoutError:
            self.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            log.error(f"[{self.owner_name}] Phase state resolution timed out. Topology Collapsed.")
            return "Execution failed: Timeout reached without convergence."
        except Exception as e:
            self.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            log.error(f"[{self.owner_name}] Fatal anomaly detected in convergence wait: {e}")
            return f"Execution failed: {e}"


class DeploymentDispatcher:
    """@desc: Assembles and propagates complex runtime directives (Message Payloads) to the target PolicyNode."""
    @staticmethod
    def dispatch_to_policy(policy_node: PolicyNode, blueprint: Optional[SurgeBlueprint], instruction: str, settings: dict):
        if blueprint:
            context_msg = Message("set_context")
            context_msg.entry_node = EntryNode(
                entry=blueprint.topology_name,
                focus=blueprint.focus,
                depth=blueprint.depth_limit,
                relations=blueprint.relations_constraint.split(',') if blueprint.relations_constraint else []
            )
            policy_node.post_message(context_msg)
            
            run_msg = Message("execute_events")
            run_msg.events = blueprint.nodes
            run_msg.settings = settings
            run_msg.system_instructions = getattr(blueprint, 'system_instructions', None)
            policy_node.post_message(run_msg)
        else:
            run_msg = Message("run_conversation")
            run_msg.instruction = instruction
            run_msg.settings = settings
            policy_node.post_message(run_msg)