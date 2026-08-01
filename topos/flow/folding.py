# topos.flow.folding
import asyncio
import json
from typing import Dict, Any, Optional, List, Tuple

from topos.flow.graph.executor import GraphExecutor
from topos.flow.graph.node import EngineNode

from arch.model.surge.blueprint import SurgeBlueprint
from arch.contract.gov.flow import PhaseFlow, FlowState
from arch.model.contract.graph import EntryNode

from arch.topos.node.gan import Message, GanNode
from arch.topos.space.organizer import SpaceNode
from watcher.plane.emitter import get_emitter

log = get_emitter("bound.folding")

# ==========================================
# 1. Spatial Boundaries (경계 및 라우팅)
# ==========================================

class Bound:
    def __init__(self, broker: Any):
        self.broker = broker  # WasmBroker instance for FFI routing

    async def _execute_atomic_transition(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        """@desc: WASM Kernel을 통해 상태 전이의 유효성을 검증하고 승인받습니다."""
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
                log.warning(f"[Bound] Kernel denied transition to '{target_id}'. Reason: {res.get('error_msg')}")
                return False
                
            if res.get("final_root"):
                ctx.state["phase_root"] = res["final_root"]
            if res.get("all_residues"):
                ctx.state.setdefault("residues", []).extend(res["all_residues"])
                
            return True
        except Exception as e:
            log.error(f"[Bound] FFI Transition failed: {e}")
            return False

    async def emit(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        raise NotImplementedError


class LocalBound(Bound):
    def __init__(self, local_registry: Dict[str, Any], broker: Any):
        super().__init__(broker)
        # ToposNode 래퍼 없이 GanNode 원본 인스턴스들을 직접 참조합니다.
        self.registry = local_registry

    async def emit(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        is_authorized = await self._execute_atomic_transition(target_id, flow, ctx)
        if not is_authorized:
            return False

        target_node = self.registry.get(target_id)
        if not target_node:
            log.error(f"[LocalBound] Target vertex '{target_id}' not found in the local manifold.")
            return False

        # 기존 ToposNode.receive()에 있던 로직을 Bound가 직접 수행하여 불필요한 계층 제거
        if isinstance(target_node, GanNode):
            msg = Message("flow_ingress", flow_id=flow.id, payload={"flow": flow, "ctx": ctx})
            target_node.post_message(msg)
        elif hasattr(target_node, "run"):
            # Legacy generic operator 지원
            asyncio.create_task(self._legacy_run(target_node, flow, ctx))
            
        return True

    async def _legacy_run(self, target_node: Any, flow: PhaseFlow, ctx: FlowState):
        operator = ctx.state.get("operator")
        next_routes: List[Tuple[str, FlowState]] = await target_node.run(flow, operator, ctx)
        for next_node_id, next_ctx in next_routes:
            if next_node_id != "END":
                await self.emit(next_node_id, flow, next_ctx)


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
        await self.pool.base_node.psi_queue.put((target_id, packet))
        return True


# ==========================================
# 2. Topology Management (위상 접기 및 라우팅 주입)
# ==========================================

class ManifoldFolder:
    """@desc: 노드 간의 공간적 경계(Bound)를 설정하고 라우팅 경로를 주입합니다."""
    
    def __init__(self, broker: Any, redis_pool: Optional[Any] = None):
        self.local_registry: Dict[str, Any] = {}
        self.redis_pool = redis_pool
        self.broker = broker
        self.local_bound = LocalBound(self.local_registry, broker=self.broker)
        self.remote_bound = RemoteBound(self.redis_pool, broker=self.broker) if self.redis_pool else None

    def fold_manifold(self, active_nodes: Dict[str, Any], topology_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        @desc: ToposNode 래퍼 클래스를 삭제하고, GanNode에 직접 boundaries 속성을 주입하여 
               객체 생성 비용과 호출 Depth를 줄입니다.
        """
        self.local_registry.update(active_nodes)

        for node_id, spec in topology_spec.items():
            source_node = self.local_registry.get(node_id)
            if not source_node:
                continue

            # 라우팅 경계 속성 동적 주입
            if not hasattr(source_node, "boundaries"):
                source_node.boundaries = {}

            for target_id in spec.get("edges", []):
                target_spec = topology_spec.get(target_id, {})
                is_local = target_spec.get("location", "local") == "local"
                
                if is_local:
                    source_node.boundaries[target_id] = self.local_bound
                elif self.remote_bound:
                    source_node.boundaries[target_id] = self.remote_bound
                else:
                    log.error(f"[ManifoldFolder] Remote routing to '{target_id}' requested, but no Redis pool provided.")

        return self.local_registry


# ==========================================
# 3. Topology Controller (root.py에서 위임된 조립/배포 로직)
# ==========================================

class TopologyController:
    """
    @desc: 기존 root.py에 파편화되어 있던 TopologyAssembler와 DeploymentDispatcher를 
           하나의 컨트롤러로 통합하여 네트워크 위상 관리 책임을 일원화합니다.
    """

    @staticmethod
    def assemble_and_mount(topos: GanNode, run_context: dict, broker: Any, topology_spec: dict) -> None:
        """@desc: 노드 인스턴스를 생성하고, 위상을 접은 뒤(fold), Root 토폴로지에 마운트합니다."""
        use_proxy = run_context.get("use_proxy", False)
        target_model = run_context.get("target_model")
        
        log.info(f"[TopologyController] Wiring hybrid nodes (Proxy Mode: {use_proxy}, Model: {target_model})")
        
        # 1. 노드 생성
        active_nodes = {
            "ConfigPolicyNode": GraphExecutor("ConfigPolicyNode"),
            "ConfigSettingsNode": EngineNode("ConfigSettingsNode", use_proxy=use_proxy, target_model=target_model),
            "DockerWorkspaceNode": SpaceNode("DockerWorkspaceNode", use_proxy=use_proxy)
        }
        
        # 2. 위상 접기 (라우팅 룰 주입)
        folder = ManifoldFolder(broker=broker)
        folder.fold_manifold(active_nodes=active_nodes, topology_spec=topology_spec)
        
        # 3. 마운트
        for node in active_nodes.values():
            topos.mount(node)
            
        log.info(f"[TopologyController] Topology spatial folding complete.")

    @staticmethod
    def dispatch_payload(policy_node: Any, blueprint: Optional[SurgeBlueprint], instruction: str, settings: dict) -> None:
        """@desc: 준비된 위상의 진입점(PolicyNode)에 초기 컨텍스트와 실행 지시를 배포합니다."""
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