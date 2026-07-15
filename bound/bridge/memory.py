# bound.bridge.memory
## @lineage: anchor.phase.bridge.memory
"""
@desc: 
- Environment-aware Topology Bridge. 
- Facilitates zero-latency, direct memory coupling between the Data Plane (Ingress) 
- Control Plane (Policy Engine) strictly for localized execution environments.
"""
import os
import asyncio
from typing import Optional, Dict, Any, cast

from bound.surface.mesh import RoutingPolicyEngine, ClusterStateMesh, RoutingDecision
from bound.bridge.protocol.acp.conn.base import Connection, StreamEvent, StreamDirection
from bound.bridge.protocol.acp.conn.queue import RpcTask, RpcTaskKind

from arch.topos.bound.tunnel import UniversalFacade
from watcher.plane.emitter import get_emitter

log = get_emitter("bridge.memory")

class AbstractRoutingBridge:
    """Interface for routing delegation."""
    async def dispatch(self, intent: str, payload: Dict[str, Any]) -> RoutingDecision:
        raise NotImplementedError

class DirectMemoryBridge(AbstractRoutingBridge):
    """
    @role: In-Memory Fast Path.
    @desc: Bypasses the distributed message broker entirely. Injects the Control Plane 
           directly into the Receptor for synchronous-like, zero-copy evaluation.
    """
    def __init__(self, policy_engine: RoutingPolicyEngine, state_mesh: ClusterStateMesh):
        self.engine = policy_engine
        self.mesh = state_mesh

    async def dispatch(self, intent: str, payload: Dict[str, Any]) -> RoutingDecision:
        ## No serialization, no pub/sub latency. Direct method invocation.
        log.info(f"[Bridge] Direct memory evaluation triggered for intent: {intent}")
        decision = self.engine.evaluate_intent(
            intent=intent, 
            cluster_state=self.mesh.peer_topology
        )
        return decision


class AcpMemoryBridge(AbstractRoutingBridge):
    """
    @role: In-Memory JSON-RPC Bridge.
    @desc: Bypasses network I/O by directly injecting RpcTasks into the ACP Connection's 
           MessageQueue and intercepting outgoing responses via StreamObserver.
    """
    def __init__(self, connection: Connection):
        self.connection = connection
        self.connection.add_observer(self._observe_outgoing)
        self._virtual_request_id = -1000
        self._pending_requests: Dict[int, asyncio.Future] = {}

    async def dispatch(self, intent: str, payload: Dict[str, Any]) -> RoutingDecision:
        """외부 트래픽을 ACP RpcTask로 변환하여 큐에 직접 밀어넣습니다."""
        req_id = self._virtual_request_id
        self._virtual_request_id -= 1

        future = asyncio.get_running_loop().create_future()
        self._pending_requests[req_id] = future
        message = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": intent,
            "params": payload
        }
        task = RpcTask(kind=RpcTaskKind.REQUEST, message=message)
        await self.connection._queue.publish(task)
        result = await future
        
        if isinstance(result, dict):
            return RoutingDecision(**result)
        return cast(RoutingDecision, result)

    async def _observe_outgoing(self, event: StreamEvent) -> None:
        if event.direction == StreamDirection.OUTGOING:
            msg = event.message
            req_id = msg.get("id")
            
            ## Bridge가 요청한 가상의 ID인지 확인
            if req_id in self._pending_requests:
                future = self._pending_requests.pop(req_id)
                if "result" in msg:
                    future.set_result(msg["result"])
                elif "error" in msg:
                    future.set_exception(RuntimeError(msg["error"]))

class BridgeFactory:
    @staticmethod
    def resolve_bridge(
        topology: str, 
        engine: RoutingPolicyEngine, 
        mesh: ClusterStateMesh,
        acp_conn: Optional[Connection] = None
    ) -> Optional[AbstractRoutingBridge]:
        
        if topology not in {"LOCAL_DAEMON", "EMBEDDED_BYPASS"}:
            return None
            
        ## ACP 커넥션이 제공되었다면 ACP 기반 인메모리 브릿지 반환
        if acp_conn:
            return AcpMemoryBridge(acp_conn)
            
        ## 기본 컨트롤 플레인 직접 연동 브릿지 반환
        return DirectMemoryBridge(engine, mesh)