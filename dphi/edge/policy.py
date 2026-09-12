# fiber.dphi.edge.policy
import sys
import os
import time
import asyncio
import json
import hashlib
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, AsyncGenerator

from xphi.kernel.space.topos.tunnel.factory import UniversalFacade
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("edge.policy", phase="MESH")

class SecurityError(Exception):
    pass

# ============================================================================
# 1. DATA MODELS (Schemas & Contexts)
# ============================================================================

class RoutingAction(str, Enum):
    CONTINUE = "CONTINUE"
    ROUTE_MUTATION = "ROUTE_MUTATION"
    DROP = "DROP"

@dataclass
class RoutingDecision:
    action: RoutingAction
    target_cluster: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)

@dataclass
class NodeHealthMetrics:
    status: str
    active_connections: int = 0
    # Extending this struct scales the cluster state visibility

@dataclass
class IngressContext:
    topo_id: int
    press_limit: int
    is_ruptured: bool
    reason: str = ""

def enforce_syscall_sandbox() -> None:
    def audit_hook(event: str, args: tuple) -> None:
        if event in {"os.system", "subprocess.Popen"}:
            raise SecurityError(f"[Sandbox] Privilege Escalation Blocked: Execution of '{event}' is forbidden.")
    
    sys.addaudithook(audit_hook)

class ToposSequencer:
    async def get_next_sequence(self, client_id: str) -> int:
        ts = int(time.time() * 1000)
        hash_val = int(hashlib.sha256(client_id.encode()).hexdigest()[:8], 16)
        return (ts % 100000000) + (hash_val % 1000)

class FuelAllocator:
    async def calculate_press_limit(self, client_id: str, action_type: str) -> int:
        seed_str = f"{client_id}:{action_type}"
        # 해시값을 정수로 변환하여 10~100 사이의 Press값 산출
        base_press = int(hashlib.md5(seed_str.encode()).hexdigest()[:4], 16) % 90
        return max(10, base_press)

class HealthMonitor:
    async def is_ruptured(self) -> tuple[bool, str]:
        """낮은 확률로 네트워크 균열(Byzantine 장애 등) 상태를 모사 (Mock)"""
        # 1% 확률로 Rupture 상태 반환
        if random.random() < 0.01:
            return True, "Byzantine divergence detected in consensus layer."
        return False, ""

class IngressPolicyEngine:
    """Facade for managing incoming request limits, sequences, and node health."""
    def __init__(self, sequencer: ToposSequencer, allocator: FuelAllocator, monitor: HealthMonitor):
        self.sequencer = sequencer
        self.allocator = allocator
        self.monitor = monitor

    async def resolve_context(self, client_id: str, action: str) -> IngressContext:
        ruptured, reason = await self.monitor.is_ruptured()
        topo_id = await self.sequencer.get_next_sequence(client_id)
        press_limit = await self.allocator.calculate_press_limit(client_id, action)

        return IngressContext(
            topo_id=topo_id,
            press_limit=press_limit,
            is_ruptured=ruptured,
            reason=reason
        )

class RoutingPolicyEngine:
    def __init__(self, broker: UniversalFacade):
        self.broker = broker
        self._routing_table: Dict[str, str] = {}

    async def synchronize_initial_state(self) -> None:
        raw_data = await self.broker.get("gateway:policy:routes")
        self._routing_table = json.loads(raw_data) if raw_data else {}

    async def watch_policy_updates(self) -> None:
        pubsub = self.broker.pubsub()
        await pubsub.subscribe("gateway:policy:mutations")
        
        async for msg in pubsub.listen():
            if msg and msg.get("type") == "message":
                mutation = json.loads(msg["data"])
                self._routing_table.update(mutation)

    def evaluate_intent(self, intent: str, cluster_state: Dict[str, NodeHealthMetrics]) -> RoutingDecision:
        target = self._routing_table.get(intent)
        if not target:
            return RoutingDecision(action=RoutingAction.CONTINUE)
            
        return RoutingDecision(action=RoutingAction.ROUTE_MUTATION, target_cluster=target)

class ClusterStateMesh:
    def __init__(self, broker: UniversalFacade):
        self.broker = broker
        self.peer_topology: Dict[str, NodeHealthMetrics] = {}

    async def start_mesh_sync(self) -> None:
        asyncio.create_task(self._subscribe_to_peer_telemetry())
        asyncio.create_task(self._broadcast_local_telemetry())

    async def _subscribe_to_peer_telemetry(self) -> None:
        pubsub = self.broker.pubsub()
        await pubsub.subscribe("gateway:mesh:telemetry")
        async for msg in pubsub.listen():
            if msg and msg.get("type") == "message":
                data = json.loads(msg["data"])
                node_id = data.pop("node_id", "unknown")
                self.peer_topology[node_id] = NodeHealthMetrics(**data)

    async def _broadcast_local_telemetry(self) -> None:
        payload = json.dumps({"node_id": "matrix-node-01", "status": "UP", "active_connections": 10})
        while True:
            await self.broker.publish("gateway:mesh:telemetry", payload)
            await asyncio.sleep(5.0)

class ExtProcStreamHandler:
    """Handles external processor streams (e.g., Envoy ExtProc) to dynamically mutate traffic."""
    def __init__(self, policy_engine: RoutingPolicyEngine, state_mesh: ClusterStateMesh):
        self.policy_engine = policy_engine
        self.state_mesh = state_mesh

    async def handle_bidirectional_stream(self, stream_iterator: AsyncGenerator) -> AsyncGenerator[Dict[str, str], None]:
        async for chunk in stream_iterator:
            intent = chunk.headers.get("x-matrix-intent", "default")
            decision: RoutingDecision = self.policy_engine.evaluate_intent(
                intent=intent, 
                cluster_state=self.state_mesh.peer_topology
            )
            if decision.action == RoutingAction.CONTINUE:
                yield {"status": decision.action.value}
            else:
                yield {"status": decision.action.value, "target": decision.target_cluster}

    async def serve(self) -> None:
        log.info("[Gateway] ExtProc Stream Handler bound to gRPC port 50051...")
        await asyncio.sleep(36000)


"""FACTORIES (Dependency Injection)"""
def get_topos_sequencer() -> ToposSequencer:
    return ToposSequencer()

def get_fuel_allocator() -> FuelAllocator:
    return FuelAllocator()

def get_health_monitor() -> HealthMonitor:
    return HealthMonitor()

def get_ingress_policy() -> IngressPolicyEngine:
    return IngressPolicyEngine(
        sequencer=ToposSequencer(),
        allocator=FuelAllocator(),
        monitor=HealthMonitor()
    )