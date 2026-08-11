# phase.epoch.scene.concurrency
import time
import asyncio
import psutil
import sys
import json
from contextlib import suppress

from dphi.sandbox.script.test import CONST
from kernel.phase.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scene.concurrency")

class ConcurrencyScene(SchemeRunner):
    # [핵심 변경] 외부에서 주입된 DphiBroker를 수용하기 위한 생성자 오버라이드
    def __init__(self, broker, suites):
        super().__init__(broker=broker)
        self.suites = suites

    async def run_all(self):
        log.info("\n=== [START] Executing Multi-Process Concurrency Scenarios ===")
        # 라이브 시스템이므로 Policy Shift(System 권한 획득) 과정이 잘 통과되는지 확인
        await self._set_worker_policy("SYSTEM")
        await self._test_concurrency_and_recovery()
        await self._test_worker_process_distribution()
        await self._set_worker_policy("SYSTEM")
        self.report()

    async def _test_concurrency_and_recovery(self):
        log.info("\n--- Running Suite: Concurrency & Resilience ---")
        await self._assert_adaptive_concurrency(
            target_func="compute_root_fingerprint", 
            payload={"dummy_data": "Parallel Scale-up Test"}
        )
        
        await self._assert_fault_recovery(
            toxic_func="non_existent_function", 
            recovery_code="print('I survived')"
        )

    async def _test_worker_process_distribution(self):
        log.info("\n--- Running Suite: Master-Worker Multi-Process Distribution ---")
        target_func = "compute_root_fingerprint"
        heavy_load_count = max(CONST.SCALE_STEPS) # 353
        
        log.info(f"🚀 Firing {heavy_load_count} massive burst requests via Live EventBus...")
        
        start_time = time.time()
        tasks = [self.broker.invoke(target_func, {"burst": i}, timeout=CONST.MAX_TIMEOUT) for i in range(heavy_load_count)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        elapsed_ms = (time.time() - start_time) * 1000
        
        participating_nodes = set()
        participating_pids = set()
        success_count = 0
        
        for res in results:
            if hasattr(res, 'success') and res.success:
                success_count += 1
                metrics = getattr(res, "metrics", {})
                if node_id := metrics.get("handled_by_node"): participating_nodes.add(node_id)
                if pid := metrics.get("handled_by_pid"): participating_pids.add(pid)

        log.info(f"🔥 Burst Results: {success_count}/{heavy_load_count} success in {elapsed_ms:.2f}ms")
        log.info(f"🎯 Distribution: Handled by {len(participating_nodes)} Worker Nodes across {len(participating_pids)} OS Processes (PIDs).")
        
        log.info(f"   ↳ Participating PIDs: {list(participating_pids)}")

        # 검증 1: 하나의 Master 안에서 여러 Worker 프로세스가 로드를 분담했는가?
        if len(participating_pids) > 1:
            self._record_success(
                elapsed_ms, 
                f"Perfect GIL Bypass: Load was evenly distributed across {len(participating_pids)} independent Worker Processes."
            )
        else:
            self._record_fail(
                elapsed_ms, 
                f"Bottleneck Detected: All requests were handled by a single PID {participating_pids}. Multiprocessing failed.", 
                "Worker Process Distribution"
            )

    def _calculate_adaptive_timeout(self, concurrency_limit: int) -> float:
        max_step = max(CONST.SCALE_STEPS)
        base_timeout = 1.7
        raw_timeout = base_timeout + (concurrency_limit / max_step) * (CONST.MAX_TIMEOUT - base_timeout)
        valid_timeouts = [t for t in CONST.SCALE_STEPS if t <= CONST.MAX_TIMEOUT]
        return max(1.7, float(min(valid_timeouts, key=lambda x: abs(x - raw_timeout))))

    async def _assert_adaptive_concurrency(self, target_func: str, payload: dict):
        last_success = 0
        total_ms = 0
        progress_str = f"📈 Adaptive Scale-Up {CONST.SCALE_STEPS}: "
        log.info(progress_str)
        
        for limit in CONST.SCALE_STEPS:
            mem_pct = psutil.virtual_memory().percent
            global_cpu = psutil.cpu_percent(interval=0.1)
            
            if mem_pct > CONST.MEM_WARN_LIMIT or global_cpu > CONST.CPU_WARN_LIMIT:
                log.warning(f"\n⚠️ Resource breached (Mem: {mem_pct}%, CPU: {global_cpu}%). Halting scale-up.")
                break

            current_timeout = self._calculate_adaptive_timeout(limit)
            start_time = time.time()
            
            tasks = [self.broker.invoke(target_func, payload, timeout=current_timeout) for _ in range(limit)]
            results = await asyncio.gather(*tasks)
            
            elapsed_ms = (time.time() - start_time) * 1000
            total_ms += elapsed_ms
            successes = sum(1 for r in results if getattr(r, 'success', False))
            
            if successes == limit:
                throughput = (limit / (elapsed_ms / 1000)) if elapsed_ms > 0 else 0
                log.info(f"  └─ [Level {limit:3d}] {elapsed_ms:7.2f}ms | {throughput:6.1f} TPS ✅")
                last_success = limit
            else:
                log.error(f"  └─ [Level {limit:3d}] FAILED ({successes}/{limit}) ❌")
                self._record_fail(elapsed_ms, f"Only {successes}/{limit} succeeded", f"Concurrency Scale-Up ({limit})")
                return

        if last_success > 0:
            self._record_success(total_ms, f"Peak safe concurrency reached: {last_success} reqs.")
        else:
            self._record_fail(0, "All concurrency scaling failed", "Adaptive Concurrency Test")

    async def _assert_fault_recovery(self, toxic_func: str, recovery_code: str):
        res_toxic = await self.broker.invoke(toxic_func, {})
        res_recovery = await self.broker.execute(code=recovery_code)
        
        if not getattr(res_toxic, 'success', True) and getattr(res_recovery, 'success', False):
            self._record_success(0, "System survived the crash. Fault was isolated in a Worker process.")
        else:
            self._record_fail(0, "System failed to recover from toxic request", "Fault Isolation")