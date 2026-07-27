# topos.ops.scope.flow.folding
## @lineage: ops.scope.flow.folding
import asyncio
import json
from typing import Dict, Any, Optional, List, Tuple

from arch.topos.node.gan import Message, GanNode
from arch.gov.flow import PhaseFlow, FlowState
from watcher.plane.emitter import get_emitter

log = get_emitter("bound.folding")

class Bound:
    def __init__(self, broker: Any):
        self.broker = broker  # WasmBroker instance for FFI routing

    async def _execute_atomic_transition(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        payload = {
            "intent_action": f"trans_{flow.id[:8]}",
            "intent_payload": {"target": target_id, "manifold_keys": list(flow.payload.keys())},
            "evolution_ctx": {
                "phase_root": ctx.state.get("phase_root", {}),
                "external_rules": ctx.state.get("external_rules", [])
            }
        }
        try:
            res_raw = await self.broker.execute("execute_transition", payload)
            res = json.loads(res_raw)
            if not res.get("is_authorized"):
                log.warning(f"[Bound] Sealing blocked. WASM Kernel denied transition to '{target_id}'. Reason: {res.get('error_msg')}")
                return False
                
            if res.get("final_root"):
                ctx.state["phase_root"] = res["final_root"]
            if res.get("all_residues"):
                ctx.state.setdefault("residues", []).extend(res["all_residues"])
                
            return True
        except Exception as e:
            log.error(f"[Bound] FFI Transition Collapse failed: {e}")
            return False

    async def emit(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        """@trigger: Physically routes the flow to the target if the WASM kernel authorizes it."""
        raise NotImplementedError

class LocalBound(Bound):
    def __init__(self, local_registry: Dict[str, 'ToposNode'], broker: Any):
        super().__init__(broker)
        self.registry = local_registry

    async def emit(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        # 1. Kernel Validation & State Mutation (Atomic)
        is_authorized = await self._execute_atomic_transition(target_id, flow, ctx)
        if not is_authorized:
            return False

        # 2. Physical Transport
        if target_id in self.registry:
            target_node = self.registry[target_id]
            asyncio.create_task(target_node.receive(flow, ctx))
            return True
            
        log.error(f"[LocalBound] Target vertex '{target_id}' not found in the local manifold.")
        return False


class RemoteBound(Bound):
    def __init__(self, pool: Any, broker: Any):
        super().__init__(broker)
        self.pool = pool

    async def emit(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        is_authorized = await self._execute_atomic_transition(target_id, flow, ctx)
        if not is_authorized:
            return False

        packet = {
            "target_id": target_id,
            "flow_id": flow.id,
            "payload": flow.payload,
            "state_snapshot": ctx.state
        }
        # Inject into the Redis/Tunnel topology
        await self.pool.base_node.psi_queue.put((target_id, packet))
        return True


class ToposNode:
    def __init__(self, node_id: str, instance: Any):
        self.node_id = node_id
        self.instance = instance  
        self.boundaries: Dict[str, Bound] = {} 

    def attach_bound(self, target_id: str, bound: Bound):
        self.boundaries[target_id] = bound

    async def receive(self, flow: PhaseFlow, ctx: FlowState):
        if isinstance(self.instance, GanNode):
            # Map PhaseFlow to GanNode's internal Agentic Message schema
            msg = Message(f"flow_ingress", flow_id=flow.id, payload={"flow": flow, "ctx": ctx})
            self.instance.post_message(msg)
            
        elif hasattr(self.instance, "run"):
            # Duck typing for generic asynchronous runner nodes (legacy support)
            operator = ctx.state.get("operator")
            next_routes: List[Tuple[str, FlowState]] = await self.instance.run(flow, operator, ctx)
            
            for next_node_id, next_ctx in next_routes:
                if next_node_id != "END":
                    await self.route(next_node_id, flow, next_ctx)

    async def route(self, target_id: str, flow: PhaseFlow, ctx: FlowState):
        bound = self.boundaries.get(target_id)
        if bound:
            await bound.emit(target_id, flow, ctx)
        else:
            log.error(f"[ToposNode] No spatial boundary attached for route '{target_id}'.")


class ManifoldFolder:
    def __init__(self, broker: Any, redis_pool: Optional[Any] = None):
        self.local_registry: Dict[str, ToposNode] = {}
        self.redis_pool = redis_pool
        self.broker = broker  # Injected WasmBroker
        self.local_bound = LocalBound(self.local_registry, broker=self.broker)
        self.remote_bound = RemoteBound(self.redis_pool, broker=self.broker) if self.redis_pool else None

    def fold_manifold(self, active_nodes: Dict[str, Any], topology_spec: Dict[str, Any]) -> Dict[str, ToposNode]:
        for node_id, instance in active_nodes.items():
            self.local_registry[node_id] = ToposNode(node_id, instance)

        for node_id, spec in topology_spec.items():
            source_node = self.local_registry.get(node_id)
            if not source_node:
                continue

            # Attach spatial fences for each allowed outbound edge
            for target_id in spec.get("edges", []):
                target_spec = topology_spec.get(target_id, {})
                
                # Default to local boundary if unspecified
                is_local = target_spec.get("location", "local") == "local"
                
                if is_local:
                    source_node.attach_bound(target_id, self.local_bound)
                elif self.remote_bound:
                    source_node.attach_bound(target_id, self.remote_bound)
                else:
                    log.error(f"[ManifoldFolder] Remote routing to '{target_id}' requested, but no Redis pool provided.")

        return self.local_registry