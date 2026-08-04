# dphi.eco.flow
import asyncio
import json
import uuid
import time
import hashlib
from enum import Enum
from typing import Dict, Any, Optional, List, Tuple
from contextlib import asynccontextmanager, AsyncExitStack

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from ator.topos.loop.executor import LoopExecutor
from ator.topos.space.manager import SpaceNode, space_provider
from dphi.eco.node import RuntimeNode
from dphi.eco.surface.registry import get_surface_class, SurfaceConfig

from arch.xor.surge.blueprint import SurgeBlueprint
from arch.model.phase.flow import PhaseFlow, FlowState
from arch.contract.model.graph import EntryNode
from arch.topos.node.gan import Message, GanNode
from arch.contract.event.next import next_id
from arch.model.sealer import EpochSealer
from arch.contract.event.mesh.transport import MeshP2PTransport

from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.broker import WasmBroker, WasmMethod
from kernel.dphi.adapter.eco import ExchangeAdapter, TransactionReceipt
from kernel.dphi.cgroup import Tier
from watcher.plane.emitter import get_emitter
from watcher.tracer.scope import scope_trace, get_current_trace_path

log = get_emitter("topos.flow")

# ==========================================
# 1. Flow & State Definitions
# ==========================================

class EdgeFlow(Enum):
    ZERO = "0"           # 구조적 정체성의 공백 (Void)
    COLLAPSED = "Φ⁻"     # 붕괴됨: 재결속 전 성찰 필요
    COHERENT = "Φ⁺"      # 일관된 판단: Dominium 앵커링 가능
    FRAGMENTED = "Φᶠ"    # 파편화된 기억: 실패했으나 재시도를 위해 보존됨
    DOMINIUM = "Ψᴰ"      # 앵커링된 최종 상태

class FlowTransition:
    def __init__(self, origin: str = "0"):
        self.id: str = next_id()
        self.origin: str = origin
        self.edge: EdgeFlow = EdgeFlow.ZERO
        self.reflective: bool = True
        self.reversible: bool = True
        self.memory: List[Dict[str, Any]] = []
        self.anchored_target: Optional[str] = None
        self.future: Optional[asyncio.Future] = None
        self._reset_future()

    def _reset_future(self) -> None:
        if self.future and not self.future.done():
            self.future.cancel()
        self.future = asyncio.Future()

    def record(self, message: str, state_change: Optional[EdgeFlow] = None) -> None:
        log_entry = {"event": message, "previous_state": self.edge.value}
        if state_change:
            log_entry["new_state"] = state_change.value
            self.edge = state_change
        self.memory.append(log_entry)

    def bind(self, target_phase: EdgeFlow) -> None:
        if self.edge == EdgeFlow.COLLAPSED and not self.reflective:
            raise ValueError("Collapsed node requires reflection before rebinding.")
        self.record(f"Bound to phase {target_phase.value}", state_change=target_phase)

    def threshold_test(self, lmbda: float, tau: float) -> bool:
        if lmbda < tau:
            self.record(f"Threshold failed: λ({lmbda}) < τ({tau})", state_change=EdgeFlow.FRAGMENTED)
            return False
        self.record(f"Threshold passed: λ({lmbda}) >= τ({tau})", state_change=EdgeFlow.COHERENT)
        return True

    def reach_dominium(self, resource_address: str) -> None:
        if self.threshold_test(lmbda=1.0, tau=0.5):
            self.anchored_target = resource_address
            self.record(f"Anchored Dominium to {resource_address}", state_change=EdgeFlow.DOMINIUM)
            if self.future and not self.future.done():
                self.future.set_result(True)
        else:
            raise PermissionError(f"Cannot reach Dominium. Threshold test failed.")

    def fracture_topology(self, lmbda: float, tau: float, force_collapse: bool = False) -> None:
        self.threshold_test(lmbda=lmbda, tau=tau)
        if force_collapse:
            self.bind(EdgeFlow.COLLAPSED)
        if self.future and not self.future.done():
            self.future.set_result(False)

    def unbind_and_reset(self) -> None:
        self.edge = EdgeFlow.ZERO
        self.anchored_target = None
        self._reset_future()
        self.record("Reversible exit declared. Returned to 0.")

    async def await_convergence(self, timeout: float = 600.0) -> str:
        try:
            success = await asyncio.wait_for(self.future, timeout=timeout)
            trace = "\n".join([f"[{m.get('new_state', m.get('previous_state', '0'))}] {m['event']}" for m in self.memory])
            if not success:
                trace += "\n[⚠️ SYSTEM COLLAPSED] Execution fractured before reaching dominium."
            return trace
        except asyncio.TimeoutError:
            self.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            log.error(f"[{self.origin}] Phase state resolution timed out. Topology Collapsed.")
            return "Execution failed: Timeout reached without convergence."
        except Exception as e:
            self.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            log.error(f"[{self.origin}] Fatal anomaly detected: {e}")
            return f"Execution failed: {e}"

# ==========================================
# 2. Topology & Mesh Core
# ==========================================

class FlowMesh:
    def __init__(self, transport: MeshP2PTransport, broker: WasmBroker, topos_group: str = "global_nexus"):
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.peer_id = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()[:16]
        self.topos_group = topos_group
        self.transport = transport
        self.broker = broker
        self.local_tension = 0.0
        self.peer_tensions: Dict[str, float] = {}

    def _serialize(self, payload: dict) -> bytes:
        return json.dumps(payload).encode('utf-8')

    def _deserialize(self, raw_bytes: bytes) -> dict:
        return json.loads(raw_bytes.decode('utf-8'))

    def _sign_payload(self, payload_dict: dict) -> str:
        raw_json_bytes = json.dumps(payload_dict, sort_keys=True).encode('utf-8')
        commit_hash = hashlib.sha256(raw_json_bytes).digest()
        return self.private_key.sign(commit_hash).hex()

    async def start_listening(self):
        log.info(f"[{self.peer_id}] Binding to MeshP2PTransport on topic: {self.topos_group}")
        await self.transport.bind_and_start(ingress_callback=self._ingress_callback)
        await self.transport.join_topic(self.topos_group)

    async def _ingress_callback(self, sender_id: str, raw_bytes: bytes):
        try:
            payload = self._deserialize(raw_bytes)
            if payload.get("sender") == self.peer_id: return
            await self._process_message(payload)
        except Exception as e:
            log.error(f"[{self.peer_id}] Ingress error: {e}")

    async def _process_message(self, payload: dict):
        msg_type, sender = payload.get("type"), payload.get("sender")
        if msg_type == "TENSION_BEACON":
            self.peer_tensions[sender] = payload.get("tension_score", 0.0)
            await self._evaluate_swarm_health()
        elif msg_type == "PROPOSE_SPLIT":
            new_group = payload.get("new_group")
            val_ctx = {"action": "VOTE_SPLIT", "target_group": new_group, "peer_tensions": self.peer_tensions}
            res = await self.broker.invoke(WasmMethod.VALIDATE_INTENT, StateAdapter.to_canonical_bytes(val_ctx).decode('utf-8'))
            if res.success and json.loads(res.output).get("approved", False):
                sig = self._sign_payload({"action": "SPLIT", "new_group": new_group})
                await self.transport.broadcast(self.topos_group, self._serialize({"type": "SPLIT_VOTE", "sender": self.peer_id, "signature": sig, "new_group": new_group}))

    async def _evaluate_swarm_health(self):
        val_ctx = {"peer_tensions": self.peer_tensions, "current_group": self.topos_group}
        res = await self.broker.invoke(WasmMethod.EVALUATE_TENSION, StateAdapter.to_canonical_bytes(val_ctx).decode('utf-8'))
        if res.success:
            if json.loads(res.output).get("require_split", False) and self.topos_group == "global_nexus":
                new_sub_group = f"nexus_shard_{int(time.time())}"
                log.critical(f"[{self.peer_id}] TENSION SPIKE. Proposing Split to '{new_sub_group}'!")
                await self.transport.broadcast(self.topos_group, self._serialize({"type": "PROPOSE_SPLIT", "sender": self.peer_id, "new_group": new_sub_group}))
                self.local_tension, self.peer_tensions = 0.0, {}

    async def broadcast_telemetry_loop(self):
        for _ in range(3):
            await asyncio.sleep(1)
            self.local_tension += 8.5
            await self.transport.broadcast(self.topos_group, self._serialize({"type": "TENSION_BEACON", "sender": self.peer_id, "tension_score": self.local_tension}))
            
    async def shutdown(self):
        await self.transport.close()

# ==========================================
# 3. Execution Bounds & Manifolds
# ==========================================

class Bound:
    def __init__(self, broker: Any):
        self.broker = broker

    async def _execute_atomic_transition(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        payload = {
            "intent_action": f"trans_{flow.id[:8]}",
            "intent_payload": {"target": target_id, "manifold_keys": list(flow.payload.keys())},
            "evolution_ctx": {"phase_root": ctx.state.get("phase_root", {}), "external_rules": ctx.state.get("external_rules", [])}
        }
        try:
            res = json.loads(await self.broker.execute("execute_transition", payload))
            if not res.get("is_authorized"):
                return False
            if res.get("final_root"): ctx.state["phase_root"] = res["final_root"]
            if res.get("all_residues"): ctx.state.setdefault("residues", []).extend(res["all_residues"])
            return True
        except Exception:
            return False

class LocalBound(Bound):
    def __init__(self, local_registry: Dict[str, Any], broker: Any):
        super().__init__(broker)
        self.registry = local_registry

    async def emit(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        if not await self._execute_atomic_transition(target_id, flow, ctx): return False
        target_node = self.registry.get(target_id)
        if not target_node: return False

        if isinstance(target_node, GanNode):
            target_node.post_message(Message("flow_ingress", flow_id=flow.id, payload={"flow": flow, "ctx": ctx}))
        elif hasattr(target_node, "run"):
            asyncio.create_task(self._legacy_run(target_node, flow, ctx))
        return True

    async def _legacy_run(self, target_node: Any, flow: PhaseFlow, ctx: FlowState):
        for next_node_id, next_ctx in await target_node.run(flow, ctx.state.get("operator"), ctx):
            if next_node_id != "END": await self.emit(next_node_id, flow, next_ctx)

class RemoteBound(Bound):
    def __init__(self, pool: Any, broker: Any):
        super().__init__(broker)
        self.pool = pool

    async def emit(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        if not await self._execute_atomic_transition(target_id, flow, ctx): return False
        await self.pool.base_node.psi_queue.put((target_id, {"target_id": target_id, "flow_id": flow.id, "payload": flow.payload, "state_snapshot": ctx.state}))
        return True

class ManifoldFolder:
    def __init__(self, broker: Any, redis_pool: Optional[Any] = None):
        self.local_registry = {}
        self.broker = broker
        self.local_bound = LocalBound(self.local_registry, broker)
        self.remote_bound = RemoteBound(redis_pool, broker) if redis_pool else None

    def fold_manifold(self, active_nodes: Dict[str, Any], topology_spec: Dict[str, Any]) -> Dict[str, Any]:
        self.local_registry.update(active_nodes)
        for node_id, spec in topology_spec.items():
            if source_node := self.local_registry.get(node_id):
                if not hasattr(source_node, "boundaries"): source_node.boundaries = {}
                for target_id in spec.get("edges", []):
                    source_node.boundaries[target_id] = self.local_bound if topology_spec.get(target_id, {}).get("location", "local") == "local" else self.remote_bound
        return self.local_registry

# ==========================================
# 4. Orchestrator Controller
# ==========================================

class ToposController:
    """Topology Orchestration, Event Dispatching, and Cryptographic Sealing"""

    @staticmethod
    def assemble_and_mount(topos: GanNode, run_context: dict, broker: Any, topology_spec: dict) -> None:
        use_proxy, target_model = run_context.get("use_proxy", False), run_context.get("target_model")
        log.info(f"[ToposController] Wiring hybrid nodes (Proxy Mode: {use_proxy}, Model: {target_model})")
        
        active_nodes = {
            "ConfigPolicyNode": LoopExecutor("ConfigPolicyNode"),
            "ConfigSettingsNode": RuntimeNode("ConfigSettingsNode", use_proxy=use_proxy, target_model=target_model),
            # ✅ 이전 답변의 수정사항 적용: provider 주입
            "DockerWorkspaceNode": SpaceNode("DockerWorkspaceNode", provider=space_provider, use_proxy=use_proxy)
        }
        ManifoldFolder(broker=broker).fold_manifold(active_nodes=active_nodes, topology_spec=topology_spec)
        for node in active_nodes.values():
            topos.mount(node)
        log.info(f"[ToposController] Topology spatial folding complete.")

    @staticmethod
    def dispatch_payload(policy_node: Any, blueprint: Optional[SurgeBlueprint], instruction: str, settings: dict) -> None:
        if blueprint:
            context_msg = Message("set_context")
            context_msg.entry_node = EntryNode(
                entry=blueprint.topology_name, focus=blueprint.focus,
                depth=blueprint.depth_limit, relations=blueprint.relations_constraint.split(',') if blueprint.relations_constraint else []
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

    @staticmethod
    async def settle_dominium(
        origin_name: str,
        transition: FlowTransition,
        broker: WasmBroker,
        exchange_adapter: ExchangeAdapter,
        cost: float,
        fuel_consumed: int
    ) -> Optional[TransactionReceipt]:
        """@desc: 에포크를 봉인하고 영지(Dominium)에 도달하기 위한 합의 및 정산 과정을 총괄합니다."""
        entangled_state = {
            "parity": {"topos_id": f"task_{transition.id}", "phase_id": 0, "nexus_id": 0},
            "repos": {}
        }
        canonical_payload = EpochSealer.generate_seal_payload(entangled_state, parent_commit_id="genesis")
        res = await broker.invoke("seal_epoch", canonical_payload)
        
        signatures = []
        if res.success:
            try:
                signatures = json.loads(res.output).get("signatures", [])
            except json.JSONDecodeError:
                log.warning(f"[{origin_name}] Unparseable response from seal_epoch.")
        else:
            log.warning(f"[{origin_name}] Epoch sealing friction: {res.error}")

        receipt = exchange_adapter.finalize_settlement(
            entangled_state=entangled_state,
            signatures=signatures, 
            cost_metrics={"fuel_consumed": fuel_consumed, "accumulated_cost": cost},
            tier=Tier.STANDARD.value
        )
        log.info(f"[{origin_name}] 🧾 Transaction Receipt Issued: {receipt.job_id}")
        transition.reach_dominium(resource_address=f"urn:surgent:resource:resolved_task_{transition.id}")
        return receipt

    @staticmethod
    def handle_rupture(origin_name: str, transition: FlowTransition, source: str, error: str):
        log.critical(f"[{origin_name}] 🚨 Fatal topological rupture originating from [{source}]. Error: {error}")
        transition.record(f"System Error: {source} failed -> {error}")
        transition.fracture_topology(lmbda=0.2, tau=1.0, force_collapse=True)

# ==========================================
# 5. Integrated Infrastructure Scope Manager
# ==========================================

class SurfaceManager:
    def __init__(self, config: SurfaceConfig):
        self.config = config
        surface_type = getattr(config, "surface_type", "local")
        try:
            surface_class = get_surface_class(surface_type)
        except (ImportError, AttributeError, ValueError) as e:
            log.warning(f"🚨 Failed to load '{surface_type}' Surface: {e}. Fallback to default 'local' environment.")
            surface_class = get_surface_class("local")
            
        self.impl = surface_class(config)

    async def up(self):
        if asyncio.iscoroutinefunction(self.impl.up):
            await self.impl.up()
        else:
            self.impl.up()

    async def down(self):
        if asyncio.iscoroutinefunction(self.impl.down):
            await self.impl.down()
        else:
            self.impl.down()
    
    def get_engine(self):
        return self.impl.get_engine()

@asynccontextmanager
async def managed_scope(**surface_kwargs):
    config = SurfaceConfig(**surface_kwargs)
    manager = SurfaceManager(config)
    
    facet_type = "logical" if config.surface_type == "local" else "infra"
    surface_name = manager.impl.__class__.__name__.replace("Surface", "").lower()
    
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(scope_trace(name=surface_name, facet=facet_type))
        log.info(f"[*] Entered Trace Path: {get_current_trace_path()}")
        
        try:
            await manager.up()
            yield manager 
        except Exception as e:
            log.error(f"🚨 [managed_scope] pipeline exception: {type(e).__name__} - {e}")
            raise
        finally:
            log.info("[managed_scope] Triggering safe teardown sequence for infrastructure resources.")
            await asyncio.sleep(0.1)
            await manager.down()
            log.info("[+] Context Manager closed safely.")