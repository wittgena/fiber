# phase.scope.observer
## @lineage: agent.scope.observer
import asyncio
import json
import time
import hashlib
from enum import Enum
from typing import Dict, Any, Optional, List

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from xphi.arch.model.phase.flow import PhaseFlow, FlowState
from xphi.arch.contract.event.next import next_id
from xphi.arch.contract.event.mesh.transport import MeshP2PTransport
from xphi.kernel.space.topos.node.gan import Message, GanNode
from xphi.kernel.dphi.broker import DphiBroker
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("scope.observer")

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

class FlowMesh:
    def __init__(self, transport: MeshP2PTransport, broker: DphiBroker, topos_group: str = "global_nexus"):
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.peer_id = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()[:16]
        self.topos_group = topos_group
        self.transport = transport
        self.broker = broker
        self.local_tension = 0.0
        self.peer_tensions: Dict[str, float] = {}
        self.tension_threshold = 50.0

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
            avg_tension = sum(self.peer_tensions.values()) / max(1, len(self.peer_tensions))
            is_approved = avg_tension > (self.tension_threshold * 0.8)

            if is_approved:
                sig = self._sign_payload({"action": "SPLIT", "new_group": new_group})
                await self.transport.broadcast(self.topos_group, self._serialize({
                    "type": "SPLIT_VOTE", "sender": self.peer_id, "signature": sig, "new_group": new_group
                }))

    async def _evaluate_swarm_health(self):
        total_tension = sum(self.peer_tensions.values()) + self.local_tension
        require_split = total_tension > self.tension_threshold

        if require_split and self.topos_group == "global_nexus":
            new_sub_group = f"nexus_shard_{int(time.time())}"
            log.critical(f"[{self.peer_id}] TENSION SPIKE ({total_tension:.2f}). Proposing Split to '{new_sub_group}'!")
            await self.transport.broadcast(self.topos_group, self._serialize({
                "type": "PROPOSE_SPLIT", "sender": self.peer_id, "new_group": new_sub_group
            }))
            self.local_tension, self.peer_tensions = 0.0, {}

    async def broadcast_telemetry_loop(self):
        for _ in range(3):
            await asyncio.sleep(1)
            self.local_tension += 8.5
            await self.transport.broadcast(self.topos_group, self._serialize({"type": "TENSION_BEACON", "sender": self.peer_id, "tension_score": self.local_tension}))
            
    async def shutdown(self):
        await self.transport.close()

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