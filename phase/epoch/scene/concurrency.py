# phase.epoch.scene.concurrency
## @lineage: phase.epoch.flow.scene.concurrency
## @lineage: epoch.flow.scene.concurrency
import time
import asyncio
import psutil

from dphi.sandbox.script.test import CONST
from kernel.phase.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scene.concurrency")

class ConcurrencyScene(SchemeRunner):
    async def run_all(self):
        log.info("\n=== [START] Executing Sandbox Concurrency Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        await self._test_concurrency_and_recovery()
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

    def _calculate_adaptive_timeout(self, concurrency_limit: int) -> float:
        """SCALE_STEPS 배열 내에서 현재 동시성에 비례하는 타임아웃 값을 스냅(Snap)하여 반환합니다."""
        max_step = max(CONST.SCALE_STEPS)
        raw_timeout = 1.0 + (concurrency_limit / max_step) * (CONST.MAX_TIMEOUT - 1.0)
        valid_timeouts = [t for t in CONST.SCALE_STEPS if t <= CONST.MAX_TIMEOUT]
        return float(min(valid_timeouts, key=lambda x: abs(x - raw_timeout)))

    async def _assert_adaptive_concurrency(self, target_func: str, payload: dict):
        """SCALE_STEPS 기반 점진적 동시성 스케일업 및 리소스 서킷 브레이커 검증"""
        last_success = 0
        total_ms = 0
        
        log.info(f"Adaptive concurrency scale-up for '{target_func}' using {CONST.SCALE_STEPS}...")
        for limit in CONST.SCALE_STEPS:
            # 1. 서킷 브레이커 (리소스 보호 - 호스트 머신 다운 방지)
            mem_pct = psutil.virtual_memory().percent
            cpu_pct = psutil.cpu_percent(interval=0.1)
            if mem_pct > CONST.MEM_WARN_LIMIT or cpu_pct > CONST.CPU_WARN_LIMIT:
                log.warning(f"⚠️ Resource threshold breached (Mem: {mem_pct}%, CPU: {cpu_pct}%). Halting scale-up.")
                break

            # 2. 적응형 타임아웃 계산 및 주입
            current_timeout = self._calculate_adaptive_timeout(limit)
            log.info(f"  [Scale-Up] Firing {limit} requests (Adaptive Timeout: {current_timeout}s)...")
            
            start_time = time.time()
            tasks = [self.broker.invoke(target_func, payload, timeout=current_timeout) for _ in range(limit)]
            results = await asyncio.gather(*tasks)
            
            elapsed_ms = (time.time() - start_time) * 1000
            total_ms += elapsed_ms
            successes = sum(1 for r in results if r.success)
            
            # 3. 결과 집계
            if successes == limit:
                throughput = (limit / (elapsed_ms / 1000)) if elapsed_ms > 0 else 0
                log.info(f"  └─ [PASS] Handled {limit} tasks in {elapsed_ms:.2f}ms ({throughput:.1f} req/s)")
                last_success = limit
            else:
                self._record_fail(elapsed_ms, f"Only {successes}/{limit} succeeded", f"Concurrency Scale-Up ({limit})")
                return

        if last_success > 0:
            self._record_success(total_ms, f"Peak safe concurrency reached: {last_success} reqs.")
        else:
            self._record_fail(0, "All concurrency scaling failed", "Adaptive Concurrency Test")

    async def _assert_fault_recovery(self, toxic_func: str, recovery_code: str):
        """데몬 크래시(예외) 발생 후 시스템이 완벽히 격리되고 후속 요청을 처리(자가 복구)하는지 검증"""
        res_toxic = await self.broker.invoke(toxic_func, {})
        res_recovery = await self.broker.execute(code=recovery_code)
        if not res_toxic.success and res_recovery.success:
            self._record_success(0, "System survived the crash and isolated the fault.")
        else:
            self._record_fail(0, "System failed to recover from toxic request", "Fault Isolation")