# anchor.bind.surface.bridge
"""
@module: anchor.bind.surface.bridge
@desc: Environment-aware Topology Bridge. 
       Facilitates zero-latency, direct memory coupling between the Data Plane (Ingress) 
       and Control Plane (Policy Engine) strictly for localized execution environments.
"""
import os
from typing import Optional, Dict, Any

from anchor.bind.surface.matrix import RoutingPolicyEngine, ClusterStateMesh, RoutingDecision
from bound.adapter.protocol.acp.conn.base import Connection, StreamEvent, StreamDirection
from bound.adapter.protocol.acp.conn.queue import RpcTask, RpcTaskKind

from arch.bound.sandbox.tunnel import UniversalFacade


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
        # No serialization, no pub/sub latency. Direct method invocation.
        print(f"[Bridge] Direct memory evaluation triggered for intent: {intent}")
        
        decision = self.engine.evaluate_intent(
            intent=intent, 
            cluster_state=self.mesh.peer_topology
        )
        return decision

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
            
        # ACP 커넥션이 제공되었다면 ACP 기반 인메모리 브릿지 반환
        if acp_conn:
            return AcpMemoryBridge(acp_conn)
            
        # 기본 컨트롤 플레인 직접 연동 브릿지 반환
        return DirectMemoryBridge(engine, mesh)

class AcpMemoryBridge:
    """
    @role: In-Memory JSON-RPC Bridge.
    @desc: Bypasses network I/O by directly injecting RpcTasks into the ACP Connection's 
           MessageQueue and intercepting outgoing responses via StreamObserver.
    """
    def __init__(self, connection: Connection):
        self.connection = connection
        
        # 1. ACP Connection에 아웃바운드 패킷을 감청할 Observer 등록
        self.connection.add_observer(self._observe_outgoing)
        
        # 가상의 Request ID 발급 및 결과 대기열 (실제 클라이언트와 충돌 방지를 위해 음수 사용)
        self._virtual_request_id = -1000
        self._pending_requests: Dict[int, asyncio.Future] = {}

    async def dispatch(self, intent: str, payload: Dict[str, Any]) -> Any:
        """외부 트래픽을 ACP RpcTask로 변환하여 큐에 직접 밀어넣습니다."""
        req_id = self._virtual_request_id
        self._virtual_request_id -= 1

        future = asyncio.get_running_loop().create_future()
        self._pending_requests[req_id] = future

        # 2. JSON-RPC 규격으로 포장
        message = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": intent,
            "params": payload
        }
        task = RpcTask(kind=RpcTaskKind.REQUEST, message=message)

        # 3. I/O StreamReader를 우회하고 InMemoryMessageQueue에 직접 발행
        # (주의: _queue 접근을 위해 Connection 클래스에 public getter를 추가하는 것을 권장)
        await self.connection._queue.publish(task)

        # 4. Observer가 응답을 낚아채어 Future를 resolve할 때까지 대기 (Zero-latency)
        return await future

    async def _observe_outgoing(self, event: StreamEvent) -> None:
        """
        @desc: Dispatcher가 처리를 완료하고 전송(send)하려는 패킷을 메모리에서 가로챕니다.
        """
        if event.direction == StreamDirection.OUTGOING:
            msg = event.message
            req_id = msg.get("id")
            
            # Bridge가 요청한 가상의 ID인지 확인
            if req_id in self._pending_requests:
                future = self._pending_requests.pop(req_id)
                
                # 결과 Resolve
                if "result" in msg:
                    future.set_result(msg["result"])
                elif "error" in msg:
                    future.set_exception(RuntimeError(msg["error"]))