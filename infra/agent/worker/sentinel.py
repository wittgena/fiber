# fiber.infra.agent.worker.sentinel
## @lineage: fiber.a2a.worker.sentinel
## @lineage: fiber.infra.worker.agent.sentinel
import asyncio
import time
import json
from typing import List, Dict, Any

from fiber.dphi.rpc.client import InternalRpcClient
from fiber.infra.agent.bridge.protocol import AgentProtocol
from xphi.kernel.dphi.ledger.consensus import KernelLedger

class AgentSentinel(AgentProtocol):
    def __init__(self, ledger: KernelLedger, rpc_client: InternalRpcClient, sweep_interval: float = 5.0):
        super().__init__(agent_name="agent.sentinel")
        
        self.ledger = ledger
        self.rpc = rpc_client
        self.sweep_interval = sweep_interval
        self.running = False
        self.ttl_thresholds = {
            "dphi.transition.pending": 30.0,  # OTP/인간 개입 대기: 30초 초과 시 롤백
            "dphi.transition.invoke": 120.0   # 일반 블로킹 I/O 대기: 120초 초과 시 롤백
        }

    def handle_tools_list(self, req_id: Any):
        """MCP 2026 규격: 외부 AI나 관리자가 수동으로 Sentinel을 조작할 수 있도록 도구 노출"""
        tools = [{
            "name": "trigger_manual_reconciliation",
            "description": "Force an immediate ledger sweep to detect and rollback orphaned transactions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "override_threshold": {"type": "number", "description": "Override default TTL (seconds)"}
                }
            }
        }]
        self.send_response(req_id, {"tools": tools})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        """MCP 2026 규격: 도구 실행 라우팅"""
        if tool_name == "trigger_manual_reconciliation":
            override = arguments.get("override_threshold")
            # 비동기 로직을 동기 브릿지로 호출 (이벤트 루프에 태스크 위임)
            asyncio.create_task(self._manual_sweep_task(req_id, override))
        else:
            self.send_error(req_id, -32601, f"Unknown tool: {tool_name}")

    async def _manual_sweep_task(self, req_id: Any, override_ttl: float = None):
        """수동 스윕 실행 후 MCP 클라이언트에게 결과 반환"""
        try:
            original_pending = self.ttl_thresholds["dphi.transition.pending"]
            if override_ttl:
                self.ttl_thresholds["dphi.transition.pending"] = override_ttl
                
            swept_count = await self._sweep_and_reconcile()
            
            if override_ttl:
                self.ttl_thresholds["dphi.transition.pending"] = original_pending
                
            self.send_response(req_id, {
                "content": [{"type": "text", "text": f"Manual reconciliation complete. Swept {swept_count} orphaned streams."}],
                "isError": False
            })
        except Exception as e:
            self.log.error(f"Manual sweep failed: {e}")
            self.send_error(req_id, -32000, str(e))

    # =====================================================================
    # [기능 2] 자율 데몬 로직
    # =====================================================================
    async def ignite(self):
        """컨트롤 플레인의 백그라운드 태스크로 동작하는 무한 루프"""
        self.running = True
        self.log.info("👁️ Sentinel Agent Ignited. Commencing Autonomous Entropy Sweeps...")
        try:
            while self.running:
                await self._sweep_and_reconcile()
                await asyncio.sleep(self.sweep_interval)
        except asyncio.CancelledError:
            self.log.info("Sentinel shutting down gracefully.")
        except Exception as e:
            self.log.critical(f"Sentinel core fracture: {e}", exc_info=True)

    async def _sweep_and_reconcile(self) -> int:
        """원장을 스캔하여 임계 시간을 초과한 상태를 색출"""
        now = time.time()
        stale_streams = await self.ledger.query_stale_streams(now, self.ttl_thresholds)
        if not stale_streams:
            return 0

        self.log.warning(f"Detected {len(stale_streams)} orphaned transactions. Initiating Rollback Protocol.")
        tasks = [self._enforce_rollback(stream) for stream in stale_streams]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for idx, res in enumerate(results):
            if isinstance(res, Exception):
                handle_id = stale_streams[idx].id
                self.log.error(f"Rollback failed for {handle_id}: {res}")
                
        return len(stale_streams)

    async def _enforce_rollback(self, stream: Any):
        """단일 트랜잭션에 대한 강제 롤백 및 자원 회수 절차"""
        handle_id = stream.id
        target_node = stream.metadata.get("target")
        locked_fuel = stream.metadata.get("fuel_locked", 0)
        risk_score = stream.metadata.get("risk_score", 0)
        
        self.log.info(f"Reconciling {handle_id} on target [{target_node}]...")

        # Phase 1: 파이프라인 강제 해제 (Unblocking the Legacy Process)
        rollback_payload = {
            "handle_id": handle_id,
            "action": "RESUME_OR_KILL",
            "payload": {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32000, 
                    "message": "SYSTEM_SENTINEL_TIMEOUT: Transaction Forcefully Reverted."
                }
            }
        }
        await self.rpc.publish_intent(
            channel=f"mcp.intent.queue.{target_node}",
            payload=rollback_payload
        )

        # Phase 2: 원장 씰링 및 증거 기록 (Sealing the Truth)
        # 경제/Legacy 모듈이 사후에 환불 및 패널티를 비동기 처리할 수 있도록, 
        # 원장 메타데이터에 연료량과 위험도 증적을 명확히 브릿지(기록)해 둡니다.
        await self.ledger.force_transition(
            handle_id=handle_id,
            new_action="dphi.transition.resolve",
            metadata={
                "status": "FAULTED", 
                "reason": "SENTINEL_TIMEOUT",
                "locked_fuel": locked_fuel,
                "risk_score": risk_score
            }
        )
        self.log.info(f"✅ Reverted {handle_id}. Entropy cleared and ledger marked for future reconciliation.")


async def main():
    class MockLedger:
        async def query_stale_streams(self, current_time, thresholds):
            return []
        async def force_transition(self, *args, **kwargs):
            pass

    sentinel = AgentSentinel(ledger=MockLedger(), rpc_client=InternalRpcClient())
    await sentinel.ignite()

if __name__ == "__main__":
    asyncio.run(main())