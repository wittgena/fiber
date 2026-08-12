# phase.attach.scene.anchor
import time
import asyncio
import random
from typing import Any

from kernel.phase.runner import SchemeRunner
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.method import DphiMethod
from watcher.plane.emitter import get_emitter

from phase.epoch.scene.anchor import ActorIdentity 

log = get_emitter("attach.scene.anchor")

# =========================================================================
# [Scene 1] Live Anchor: Distributed Race Condition & Consensus
# =========================================================================
class LiveAnchorScene(SchemeRunner):
    """
    @desc: 라이브 클러스터 환경에서 다중 서명, 매칭 엔진 등의 
           분산 합의 병목(Deadlock) 및 경쟁 상태(Race Condition)를 검증하는 씬
    """
    def __init__(self, broker: Any):
        super().__init__(broker)
        self.node_a = ActorIdentity("Agent_A")
        self.node_b = ActorIdentity("Agent_B")
        self.field = ActorIdentity("Clearing_Field")

    async def run_all(self):
        log.info("\n=======================================================")
        log.info("⚔️ [START] Live Anchor: Distributed Race Condition Tests")
        log.info("=======================================================\n")
        
        await self._set_worker_policy("SYSTEM")
        
        await self._test_concurrent_ingress_bombardment()
        await self._test_concurrent_multisig_consensus()
        
        self.report()

    async def _test_concurrent_ingress_bombardment(self):
        log.info("\n--- [Live Anchor 1] Concurrent Order Ingress (Thundering Intents) ---")
        burst_size = 200
        log.info(f"Firing {burst_size} simultaneous Trade Intents (INIT_EPOCH) to induce Queue Contention...")
        
        tasks = []
        for i in range(burst_size):
            payload = {
                "ts": int(time.time() * 1000) + i, 
                "topo": 101, 
                "press": 5, 
                "rupture": False,
                "injected_tick": None
            }
            # Timeout을 타이트하게 주어 워커 큐의 처리 효율성(Backpressure) 검증
            tasks.append(self.broker.invoke(DphiMethod.INIT_EPOCH.value, payload, timeout=10.0))
            
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.time() - start_time) * 1000
        
        successes = sum(1 for r in results if getattr(r, 'success', False))
        
        if successes == burst_size:
            self._record_success(elapsed, f"Successfully processed {successes} concurrent trade intents without dropping.")
        else:
            self._record_fail(elapsed, f"Dropped intents! Only {successes}/{burst_size} succeeded.", "Concurrent Ingress")

    async def _test_concurrent_multisig_consensus(self):
        log.info("\n--- [Live Anchor 2] Concurrent Multi-sig Consensus Bombardment ---")
        burst_size = 100
        log.info(f"Bombarding {burst_size} SEAL_EPOCH consensus requests simultaneously...")
        
        parity = StateAdapter.build_parity_triplet("live_topos", 777, 888)
        commit = StateAdapter.build_anchor_commit(parity, 0, "genesis", {"repo": "live"}, {})
        
        signatures = [self.node_a.sign(commit), self.node_b.sign(commit), self.field.sign(commit)]
        signers = [self.node_a.pubkey_hex, self.node_b.pubkey_hex, self.field.pubkey_hex]
        
        payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state="genesis",
            repos={"repo": "live"}, cached_states={}, timestamp=int(time.time()),
            signers=signers, signatures=signatures, threshold=2, allowed_signers=signers
        )
        
        # 워커 노드들의 합의 락(Lock) 메커니즘이 데드락에 빠지지 않는지 검증
        tasks = [self.broker.invoke(DphiMethod.SEAL_EPOCH.value, payload, timeout=15.0) for _ in range(burst_size)]
        
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.time() - start_time) * 1000
        
        successes = sum(1 for r in results if getattr(r, 'success', False))
        
        if successes == burst_size:
            self._record_success(elapsed, f"Successfully sealed {successes} epochs concurrently. No Ledger Deadlocks.")
        else:
            self._record_fail(elapsed, f"Ledger contention/deadlock detected. {successes}/{burst_size} succeeded.", "Concurrent Consensus")


# =========================================================================
# [Scene 2] Live Cert: Mixed Chaos Isolation
# =========================================================================
class LiveCertScene(SchemeRunner):
    """
    @desc: 라이브 클러스터에 정상 연산과 악의적 공격(OOM, 무한루프)이 동시에 쏟아질 때의 
           혼합 카오스(Mixed Chaos) 격리 방어력을 증명하는 씬
    """
    async def run_all(self):
        log.info("\n=======================================================")
        log.info("🧪 [START] Live Cert: Mixed Chaos Isolation Tests")
        log.info("=======================================================\n")
        
        # STANDARD 티어로 강제하여 악성 요청이 Trap 되도록 세팅
        await self._set_worker_policy("STANDARD")
        await self._test_mixed_chaos_isolation()
        
        await self._set_worker_policy("SYSTEM")
        self.report()
        
    async def _test_mixed_chaos_isolation(self):
        log.info("\n--- [Live Cert 1] Mixed Chaos (Normal 50% vs Toxic 50%) ---")
        
        # 정상 요청: 시스템 안정성을 확인하기 위한 단순 핑 출력
        normal_code = "print('NORMAL_OK')"
        
        # 악성 요청: OOM(메모리 초과) 유도
        toxic_code = "arr = []\nwhile True: arr.append('A' * 1024 * 1024)"
        
        total_requests = 100
        log.info(f"Injecting {total_requests} requests concurrently into the cluster...")
        
        tasks = []
        expected_normal_count = 0
        
        # 동전 던지기로 정상/악성 요청 무작위 셔플링
        for _ in range(total_requests):
            if random.choice([True, False]):
                tasks.append(self.broker.execute(code=normal_code, timeout=5.0))
                expected_normal_count += 1
            else:
                # Toxic 요청은 금방 Trap되거나 Timeout이 나야 함
                tasks.append(self.broker.execute(code=toxic_code, timeout=2.0))
                
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.time() - start_time) * 1000
        
        normal_success = 0
        toxic_trapped = 0
        
        for res in results:
            if isinstance(res, Exception):
                toxic_trapped += 1
            elif getattr(res, 'success', False):
                # 정상적인 응답이 'NORMAL_OK'인지 확인
                if 'NORMAL_OK' in getattr(res, 'output', ''):
                    normal_success += 1
            else:
                # Toxic 연산이 WasmCG에 의해 Trap 되어 success=False 로 반환된 경우
                toxic_trapped += 1
                
        expected_toxic_count = total_requests - expected_normal_count
        log.info(f"🎯 Expected Ratio -> Normal: {expected_normal_count} | Toxic: {expected_toxic_count}")
        log.info(f"📊 Actual Result  -> Normal Success: {normal_success} | Toxic Trapped: {toxic_trapped}")
        
        # 악성 요청이 정상 워커 노드까지 다운시키지 않고 완벽히 격리(Isolation)되었는지 확인
        if normal_success == expected_normal_count and toxic_trapped == expected_toxic_count:
            self._record_success(elapsed, "Perfect Isolation: Toxic requests were trapped without affecting any normal requests.")
        else:
            self._record_fail(elapsed, f"Isolation leak! Normal requests dropped. (Normal: {normal_success}/{expected_normal_count})", "Mixed Chaos")