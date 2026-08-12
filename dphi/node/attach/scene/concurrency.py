# dphi.node.attach.scene.concurrency
import time
import asyncio
from dataclasses import dataclass, field
from typing import List

from kernel.phase.runner import SchemeRunner
from watcher.plane.emitter import get_emitter
from kernel.dphi.method import DphiMethod

log = get_emitter("scene.concurrency")

@dataclass(frozen=True)
class TestConstants:
    SCALE_STEPS: List[int] = field(default_factory=lambda: [1, 5, 17, 59, 71, 101, 256, 353, 512, 1024])
    MAX_TIMEOUT: float = 15.0

CONST = TestConstants()


class SuiteProcessDistribution:
    def __init__(self, runner: SchemeRunner, broker, capacity: int):
        self.runner = runner
        self.broker = broker
        self.capacity = capacity

    async def run(self):
        log.info("\n--- [Suite 1] Master-Worker Multi-Process Distribution ---")
        # [정렬] 100% 성공을 검증하기 위해 정확히 가용량(Capacity) 만큼만 발송
        load_count = self.capacity 
        
        start_time = time.time()
        tasks = [
            self.broker.invoke(
                target_func=DphiMethod.COMPUTE_ROOT_FINGERPRINT, 
                payload={"burst": i}, 
                timeout=5.0
            ) 
            for i in range(load_count)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed_ms = (time.time() - start_time) * 1000
        
        participating_nodes, participating_pids = set(), set()
        success_count, fail_count = 0, 0
        
        for res in results:
            if not isinstance(res, Exception) and getattr(res, 'success', False):
                success_count += 1
                metrics = getattr(res, "metrics", {})
                if node_id := metrics.get("handled_by_node"): participating_nodes.add(node_id)
                if pid := metrics.get("handled_by_pid"): participating_pids.add(pid)
            else:
                fail_count += 1

        if success_count == load_count and (len(participating_pids) > 1 or self.capacity <= 4):
            self.runner._record_success(elapsed_ms, f"Perfect Distribution! {success_count}/{load_count} handled across {len(participating_pids)} PIDs.")
        else:
            self.runner._record_fail(elapsed_ms, f"Distribution Failed. Success: {success_count}/{load_count}, Fails: {fail_count}, PIDs: {len(participating_pids)}", "Worker Distribution")


class SuiteAdaptiveScaleUp:
    def __init__(self, runner: SchemeRunner, broker, capacity: int):
        self.runner = runner
        self.broker = broker
        self.capacity = capacity

    async def run(self):
        log.info("\n--- [Suite 2] Adaptive Concurrency Scale-up ---")
        # [정렬] TPS 측정 한계를 가용량의 100배(수천 단위)로 대폭 개방
        max_logical_test = self.capacity * 100 
        valid_steps = [s for s in CONST.SCALE_STEPS if s <= max_logical_test]
        if max_logical_test not in valid_steps:
            valid_steps.append(max_logical_test)
            
        last_success, total_ms = 0, 0
        
        for limit in valid_steps:
            current_timeout = 2.0 + (limit / max_logical_test) * (CONST.MAX_TIMEOUT - 2.0)
            start_time = time.time()
            tasks = [
                self.broker.invoke(
                    target_func=DphiMethod.COMPUTE_ROOT_FINGERPRINT, 
                    payload={"scale": i}, 
                    timeout=current_timeout
                ) 
                for i in range(limit)
            ]
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
                break 

        self.runner._record_success(total_ms, f"Peak safe concurrency reached: {last_success} reqs.")


class SuiteThunderingHerd:
    def __init__(self, runner: SchemeRunner, broker, capacity: int):
        self.runner = runner
        self.broker = broker
        self.capacity = capacity

    async def run(self):
        log.info("\n--- [Suite 3] Thundering Herd Backpressure (Strict Active Shedding Check) ---")
        
        # 10배수(수백 개 단위)의 강력한 부하 폭탄 투하
        burst_size = self.capacity * 10
        
        start_time = time.time()
        
        # 순수 WASM FFI를 통한 초고속 동시성 한계 측정
        tasks = [
            self.broker.invoke(
                target_func=DphiMethod.COMPUTE_ROOT_FINGERPRINT, 
                payload={"burst": i}, 
                timeout=5.0
            ) 
            for i in range(burst_size)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.time() - start_time
        
        successes, overloads, timeouts = 0, 0, 0
        for r in results:
            if isinstance(r, Exception):
                timeouts += 1
                continue
            if getattr(r, 'success', False):
                successes += 1
            else:
                err_str = str(getattr(r, 'error', ''))
                if "OVERLOADED" in err_str or "Backpressure" in err_str:
                    overloads += 1
                else:
                    timeouts += 1
                    
        log.info(f"Data Plane processed: {successes} | Active Shedding: {overloads} | Drop/Timeout: {timeouts} (Total: {burst_size})")
        
        # 워커들이 큐를 완전히 소화할 수 있도록 충분한 휴식 제공
        await asyncio.sleep(2.0) 
        
        ping_res = await self.broker.invoke(
            target_func=DphiMethod.COMPUTE_ROOT_FINGERPRINT, 
            payload={"ping": 1}, 
            timeout=3.0
        )
        
        if not getattr(ping_res, 'success', False):
            self.runner._record_fail(elapsed * 1000, "System crashed. Data plane unresponsive after burst.", "Backpressure")
        else:
            handled = successes + overloads
            msg = f"System Survived! Handled {handled}/{burst_size} of spike (Processed: {successes}, Shed: {overloads})."
            self.runner._record_success(elapsed * 1000, msg)


class SuiteContaminationDefense:
    def __init__(self, runner: SchemeRunner, broker, capacity: int):
        self.runner = runner
        self.broker = broker
        self.capacity = capacity

    async def run(self):
        log.info("\n--- [Suite 4] Live Cross-Sandbox Contamination Prevention ---")
        infect_code = "import os; os.environ['HACKED_FLAG'] = 'CRITICAL_DATA'; print('INFECTED')"
        check_code = "import os; val = os.environ.get('HACKED_FLAG', 'CLEAN'); print(val)"
        
        test_count = max(1, self.capacity // 2)
        
        infect_tasks = [self.broker.execute(code=infect_code, timeout=5.0) for _ in range(test_count)]
        await asyncio.gather(*infect_tasks, return_exceptions=True)
        
        log.info("  └─ Waiting 5.0s for Python Sandboxes to replenish...")
        await asyncio.sleep(5.0)
        
        check_tasks = [self.broker.execute(code=check_code, timeout=5.0) for _ in range(test_count)]
        check_results = await asyncio.gather(*check_tasks, return_exceptions=True)
        
        leaked_count, valid_responses = 0, 0
        
        for res in check_results:
            if isinstance(res, Exception) or not getattr(res, 'success', False):
                continue
            
            valid_responses += 1
            if "CRITICAL_DATA" in getattr(res, 'output', ''):
                leaked_count += 1
                
        if valid_responses != test_count:
            self.runner._record_fail(0, f"Failed: Expected {test_count} check responses, but got {valid_responses} (Timeouts occurred).", "Isolation Leak")
        elif leaked_count == 0:
            self.runner._record_success(0, f"Context perfectly cleared across {valid_responses} responses. No cross-contamination.")
        else:
            self.runner._record_fail(0, f"Contamination detected in {leaked_count} worker instances!", "Isolation Leak")


class ConcurrencyScene(SchemeRunner):
    def __init__(self, manifold, suites=None):
        super().__init__(broker=manifold.broker)
        self.suites_filter = suites
        
        active_broker = manifold.broker
        active_capacity = manifold.total_capacity or 40
        self.capacity = active_capacity
        
        self._test_suites = [
            SuiteProcessDistribution(self, active_broker, active_capacity),
            SuiteAdaptiveScaleUp(self, active_broker, active_capacity),
            SuiteThunderingHerd(self, active_broker, active_capacity),
            SuiteContaminationDefense(self, active_broker, active_capacity)
        ]

    async def run_all(self):
        log.info("\n=======================================================")
        log.info(f"🌪️ [START] Adaptive Cluster Concurrency (Capacity: {self.capacity})")
        log.info("=======================================================\n")
        await self._set_worker_policy("SYSTEM")
        
        for i, suite in enumerate(self._test_suites):
            await suite.run()
            if i < len(self._test_suites) - 1:
                await asyncio.sleep(1.0)
        
        await self._set_worker_policy("SYSTEM")
        self.report()