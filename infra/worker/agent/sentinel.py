# fiber.infra.worker.agent.sentinel
import asyncio
import time
import logging
from typing import List, Dict, Any

from fiber.dphi.rpc.client import InternalRpcClient
from xphi.kernel.dphi.ledger.consensus import KernelLedger
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("agent.sentinel")

class AgentSentinel:
    def __init__(self, ledger: KernelLedger, rpc_client: InternalRpcClient, sweep_interval: float = 5.0):
        self.ledger = ledger
        self.rpc = rpc_client
        self.sweep_interval = sweep_interval
        self.running = False
        self.ttl_thresholds = {
            "dphi.transition.pending": 30.0,  # OTP/인간 개입 대기: 30초 초과 시 롤백
            "dphi.transition.invoke": 120.0   # 일반 블로킹 I/O 대기: 120초 초과 시 롤백
        }

    async def ignite(self):
        self.running = True
        log.info("👁️ Sentinel Agent Ignited. Commencing Entropy Sweeps...")
        try:
            while self.running:
                await self._sweep_and_reconcile()
                await asyncio.sleep(self.sweep_interval)
        except asyncio.CancelledError:
            log.info("Sentinel shutting down gracefully.")
        except Exception as e:
            log.critical(f"Sentinel core fracture: {e}", exc_info=True)

    async def _sweep_and_reconcile(self):
        """원장을 스캔하여 임계 시간을 초과한 상태를 색출"""
        now = time.time()
        stale_streams = await self.ledger.query_stale_streams(now, self.ttl_thresholds)
        if not stale_streams:
            return

        log.warning(f"Detected {len(stale_streams)} orphaned transactions. Initiating Rollback Protocol.")
        tasks = [self._enforce_rollback(stream) for stream in stale_streams]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for idx, res in enumerate(results):
            if isinstance(res, Exception):
                handle_id = stale_streams[idx].id
                log.error(f"Rollback failed for {handle_id}: {res}")

    async def _enforce_rollback(self, stream: Any):
        """단일 트랜잭션에 대한 강제 롤백 및 자원 회수 절차"""
        handle_id = stream.id
        target_node = stream.metadata.get("target")
        locked_fuel = stream.metadata.get("fuel_locked", 0)
        
        log.info(f"Reconciling {handle_id} on target [{target_node}]...")

        ## Phase 1: 파이프라인 강제 해제 (Unblocking the Legacy Process)
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

        ## Phase 2: A2A 경제 결산 (Economic Reconciliation)
        is_malicious = stream.metadata.get("risk_score", 0) > 80
        eco_action = "eco.compute.intent.slash" if is_malicious else "eco.compute.intent.refund"
        
        if locked_fuel > 0:
            await self.rpc.call(eco_action, {
                "handle_id": handle_id,
                "reason": "SENTINEL_TIMEOUT",
                "amount": locked_fuel
            })
            log.info(f"Budget reconciled ({eco_action.split('.')[-1]}): {locked_fuel} fuel.")

        ## Phase 3: 원장 씰링 (Sealing the Truth)
        await self.ledger.force_transition(
            handle_id=handle_id,
            new_action="dphi.transition.resolve",
            metadata={"status": "FAULTED", "reason": "SENTINEL_TIMEOUT"}
        )
        log.info(f"✅ Reverted {handle_id}. Entropy cleared.")

async def main():
    ## Mocking Ledger and RPC for standalone Sentinel ignition
    class MockLedger:
        async def query_stale_streams(self, current_time, thresholds):
            return []
        async def force_transition(self, *args, **kwargs):
            pass

    sentinel = AgentSentinel(ledger=MockLedger(), rpc_client=InternalRpcClient())
    await sentinel.ignite()

if __name__ == "__main__":
    asyncio.run(main())