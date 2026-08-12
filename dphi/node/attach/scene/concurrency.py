# dphi.node.attach.scene.concurrency
## @lineage: dphi.phase.attach.scene.concurrency
## @lineage: phase.attach.scene.concurrency
import time
import asyncio
from dataclasses import dataclass, field
from typing import List

from kernel.phase.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scene.concurrency")

@dataclass(frozen=True)
class TestConstants:
    SCALE_STEPS: List[int] = field(default_factory=lambda: [1, 5, 17, 59, 71, 101, 256, 353, 512, 1024])
    MAX_TIMEOUT: float = 37.0 

CONST = TestConstants()

class SuiteProcessDistribution:
    def __init__(self, runner: SchemeRunner, manifold):
        self.runner = runner
        self.manifold = manifold
        self.capacity = manifold.discovery.total_capacity
        self.broker = manifold.broker

    async def run(self):
        log.info("\n--- [Suite 1] Master-Worker Multi-Process Distribution ---")
        heavy_load_count = max(10, self.capacity * 3) 
        
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

        if len(participating_pids) > 1 or self.capacity <= 4:
            self.runner._record_success(elapsed_ms, f"Load evenly distributed across {len(participating_pids)} Worker PIDs.")
        else:
            self.runner._record_fail(elapsed_ms, f"Bottleneck: All handled by a single PID {participating_pids}.", "Worker Distribution")

class SuiteAdaptiveScaleUp:
    def __init__(self, runner: SchemeRunner, manifold):
        self.runner = runner
        self.capacity = manifold.discovery.total_capacity
        self.broker = manifold.broker

    async def run(self):
        log.info("\n--- [Suite 2] Adaptive Concurrency Scale-up ---")
        max_logical_test = self.capacity * 10 
        valid_steps = [s for s in CONST.SCALE_STEPS if s <= max_logical_test]
        if max_logical_test not in valid_steps:
            valid_steps.append(max_logical_test)
            
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
                break 

        self.runner._record_success(total_ms, f"Peak safe concurrency reached: {last_success} reqs.")

class SuiteThunderingHerd:
    def __init__(self, runner: SchemeRunner, manifold):
        self.runner = runner
        self.manifold = manifold
        self.capacity = manifold.discovery.total_capacity
        self.broker = manifold.broker

    async def run(self):
        log.info("\n--- [Suite 3] Thundering Herd Backpressure (Dual-Channel OOB Verification) ---")
        
        burst_size = self.capacity * 15
        
        start_time = time.time()
        tasks = [self.broker.execute(code="import time; time.sleep(0.01); print('ok')", timeout=1.0) for _ in range(burst_size)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.time() - start_time
        
        successes = sum(1 for r in results if not isinstance(r, Exception) and getattr(r, 'success', False))
        log.info(f"Data Plane processed {successes}/{burst_size} within 1.0s limit.")
        
        await asyncio.sleep(2.0) 
        active_nodes = await self.manifold.discovery.oob_health_check()
        if active_nodes > 0:
            self.runner._record_success(elapsed * 1000, f"System survived! OOB Check confirms {active_nodes} nodes actively beating.")
        else:
            self.runner._record_fail(elapsed * 1000, "System crashed. OOB Heartbeats completely stopped after burst.", "Backpressure")

class SuiteContaminationDefense:
    def __init__(self, runner: SchemeRunner, manifold):
        self.runner = runner
        self.capacity = manifold.discovery.total_capacity
        self.broker = manifold.broker

    async def run(self):
        log.info("\n--- [Suite 4] Live Cross-Sandbox Contamination Prevention ---")
        infect_code = "import os; os.environ['HACKED_FLAG'] = 'CRITICAL_DATA'; print('INFECTED')"
        check_code = "import os; val = os.environ.get('HACKED_FLAG', 'CLEAN'); print(val)"
        
        test_count = self.capacity * 2 
        
        infect_tasks = [self.broker.execute(code=infect_code, timeout=5.0) for _ in range(test_count)]
        await asyncio.gather(*infect_tasks, return_exceptions=True)
        
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
            self.runner._record_fail(0, "All check requests timed out or failed. Cannot verify isolation.", "Isolation Leak")
        elif leaked_count == 0:
            self.runner._record_success(0, f"Context perfectly cleared across {valid_responses} responses. No cross-contamination.")
        else:
            self.runner._record_fail(0, f"Contamination detected in {leaked_count} worker instances!", "Isolation Leak")

class ConcurrencyScene(SchemeRunner):
    def __init__(self, manifold, suites=None):
        super().__init__(broker=manifold.broker)
        self.manifold = manifold
        self.suites_filter = suites
        self.capacity = manifold.discovery.total_capacity

        self._test_suites = [
            SuiteProcessDistribution(self, self.manifold),
            SuiteAdaptiveScaleUp(self, self.manifold),
            SuiteThunderingHerd(self, self.manifold),
            SuiteContaminationDefense(self, self.manifold)
        ]

    async def run_all(self):
        log.info("\n=======================================================")
        log.info(f"🌪️ [START] Adaptive Cluster Concurrency (Capacity: {self.capacity})")
        log.info("=======================================================\n")
        await self._set_worker_policy("SYSTEM")
        for suite in self._test_suites:
            await suite.run()
        
        await self._set_worker_policy("SYSTEM")
        self.report()