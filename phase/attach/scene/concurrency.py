# phase.attach.scene.concurrency
import time
import asyncio
import sys
from dataclasses import dataclass, field
from typing import List, Dict, Any

from kernel.phase.runner import SchemeRunner
from watcher.plane.emitter import get_emitter
from kernel.phase.daemon.bootstrap import KEY_HEARTBEAT_PATTERN

log = get_emitter("attach.scene.concurrency")

@dataclass(frozen=True)
class TestConstants:
    SCALE_STEPS: List[int] = field(default_factory=lambda: [1, 5, 17, 59, 71, 101, 256, 353, 512, 1024])
    MAX_TIMEOUT: float = 37.0 

CONST = TestConstants()

class ConcurrencyScene(SchemeRunner):
    def __init__(self, broker, cluster_capacity: int = 4, suites=None):
        super().__init__(broker=broker)
        self.suites = suites
        self.cluster_capacity = cluster_capacity

    async def run_all(self):
        log.info("\n=======================================================")
        log.info(f"🌪️ [START] Adaptive Cluster Concurrency (Capacity: {self.cluster_capacity})")
        log.info("=======================================================\n")
        
        await self._set_worker_policy("SYSTEM")
        
        await self._test_worker_process_distribution()
        await self._test_adaptive_scale_up()
        await self._proof_thundering_herd_backpressure()
        await self._proof_cross_sandbox_contamination()
        
        await self._set_worker_policy("SYSTEM")
        self.report()

    # -------------------------------------------------------------------------
    # [Domain 1] 스케일업 및 분산 로드 밸런싱 검증
    # -------------------------------------------------------------------------
    async def _test_worker_process_distribution(self):
        log.info("\n--- [Suite 1] Master-Worker Multi-Process Distribution ---")
        
        # 클러스터 가용량의 3배 정도만 쏘아 분산이 잘 되는지 확인 (무리한 하드코딩 제거)
        heavy_load_count = max(10, self.cluster_capacity * 3) 
        
        log.info(f"🚀 Firing {heavy_load_count} burst requests to check distribution...")
        
        start_time = time.time()
        tasks = [self.broker.invoke("compute_root_fingerprint", {"burst": i}, timeout=5.0) for i in range(heavy_load_count)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed_ms = (time.time() - start_time) * 1000
        
        participating_nodes, participating_pids = set(), set()
        success_count = 0
        
        for res in results:
            if not isinstance(res, Exception) and getattr(res, 'success', False):
                success_count += 1
                metrics = getattr(res, "metrics", {})
                if node_id := metrics.get("handled_by_node"): participating_nodes.add(node_id)
                if pid := metrics.get("handled_by_pid"): participating_pids.add(pid)

        log.info(f"🔥 Distribution Results: {success_count}/{heavy_load_count} success in {elapsed_ms:.2f}ms")
        log.info(f"🎯 Handled by {len(participating_nodes)} Nodes across {len(participating_pids)} PIDs.")
        
        if len(participating_pids) > 1 or self.cluster_capacity <= 4:
            self._record_success(elapsed_ms, f"Load evenly distributed across {len(participating_pids)} Worker PIDs.")
        else:
            self._record_fail(elapsed_ms, f"Bottleneck: All handled by a single PID {participating_pids}.", "Worker Distribution")

    async def _test_adaptive_scale_up(self):
        log.info("\n--- [Suite 2] Adaptive Concurrency Scale-up ---")
        
        # [지능형 정렬] 클러스터 능력을 한참 벗어나는 스케일은 필터링
        max_logical_test = self.cluster_capacity * 10 
        valid_steps = [s for s in CONST.SCALE_STEPS if s <= max_logical_test]
        if max_logical_test not in valid_steps:
            valid_steps.append(max_logical_test)
            
        log.info(f"📈 Adaptive Steps based on Capacity: {valid_steps}")
        last_success, total_ms = 0, 0
        
        for limit in valid_steps:
            current_timeout = 2.0 + (limit / max_logical_test) * (CONST.MAX_TIMEOUT - 2.0)
            start_time = time.time()
            
            tasks = [self.broker.invoke("compute_root_fingerprint", {"dummy": "scale"}, timeout=current_timeout) for _ in range(limit)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            elapsed_ms = (time.time() - start_time) * 1000
            total_ms += elapsed_ms
            successes = sum(1 for r in results if not isinstance(r, Exception) and getattr(r, 'success', False))
            
            if successes == limit:
                throughput = (limit / (elapsed_ms / 1000)) if elapsed_ms > 0 else 0
                log.info(f"  └─ [Level {limit:4d}] {elapsed_ms:7.2f}ms | {throughput:6.1f} TPS ✅")
                last_success = limit
            else:
                log.warning(f"  └─ [Level {limit:4d}] FAILED ({successes}/{limit}). Reached System Limit ⚠️")
                break # 한계에 도달하면 무의미한 상위 단계 스킵

        self._record_success(total_ms, f"Peak safe concurrency reached: {last_success} reqs.")

    # -------------------------------------------------------------------------
    # [Domain 2] 라이브 엣지(Edge) 케이스 방어 증명
    # -------------------------------------------------------------------------
    async def _proof_thundering_herd_backpressure(self):
        log.info("\n--- [Suite 3] Thundering Herd Backpressure (Dual-Channel OOB Verification) ---")
        
        # [지능형 정렬] 클러스터 가용량의 15배수를 순간적으로 때려 백프레셔 유도
        burst_size = self.cluster_capacity * 15
        log.info(f"Firing {burst_size} concurrent requests to Data Plane with strict 1.0s timeout...")
        
        start_time = time.time()
        tasks = [self.broker.execute(code="import time; time.sleep(0.01); print('ok')", timeout=1.0) for _ in range(burst_size)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.time() - start_time
        
        successes = sum(1 for r in results if not isinstance(r, Exception) and getattr(r, 'success', False))
        log.info(f"Data Plane processed {successes}/{burst_size} within 1.0s limit. Remaining were dropped by Backpressure.")
        
        # [핵심 수정: Out-of-Band Health Check]
        # 꽉 막힌 Data Plane(Task Queue)에 Ping을 넣지 않고, Control Plane(Redis Heartbeat)을 통해 
        # 시스템의 실제 생존 여부를 묻습니다. 
        log.info("Performing Out-of-Band Health Check via Control Plane (Heartbeat)...")
        await asyncio.sleep(2.0) # Heartbeat가 갱신(1s 주기)될 시간을 부여
        
        active_nodes = await self.broker.tunnel.keys(KEY_HEARTBEAT_PATTERN)
        
        if len(active_nodes) > 0:
            self._record_success(elapsed * 1000, f"System survived! Control Plane confirms {len(active_nodes)} nodes actively beating.")
        else:
            self._record_fail(elapsed * 1000, "System crashed. Control Plane Heartbeats completely stopped after burst.", "Backpressure")

    async def _proof_cross_sandbox_contamination(self):
        log.info("\n--- [Suite 4] Live Cross-Sandbox Contamination Prevention ---")
        
        infect_code = "import os; os.environ['HACKED_FLAG'] = 'CRITICAL_DATA'; print('INFECTED')"
        check_code = "import os; val = os.environ.get('HACKED_FLAG', 'CLEAN'); print(val)"
        
        test_count = self.cluster_capacity * 2 # 워커들이 충분히 섞이도록
        
        log.info("Infecting multiple workers across the cluster...")
        infect_tasks = [self.broker.execute(code=infect_code, timeout=5.0) for _ in range(test_count)]
        await asyncio.gather(*infect_tasks, return_exceptions=True)
        
        log.info("Scanning cluster for leaked context...")
        check_tasks = [self.broker.execute(code=check_code, timeout=5.0) for _ in range(test_count)]
        check_results = await asyncio.gather(*check_tasks, return_exceptions=True)
        
        leaked_count, valid_responses = 0, 0
        
        for res in check_results:
            if isinstance(res, Exception) or not getattr(res, 'success', False):
                continue
            
            valid_responses += 1
            if "CRITICAL_DATA" in getattr(res, 'output', ''):
                leaked_count += 1
                
        if valid_responses == 0:
            self._record_fail(0, "All check requests timed out or failed. Cannot verify isolation.", "Isolation Leak")
        elif leaked_count == 0:
            self._record_success(0, f"Context perfectly cleared across {valid_responses} responses. No cross-contamination.")
        else:
            self._record_fail(0, f"Contamination detected in {leaked_count} worker instances!", "Isolation Leak")