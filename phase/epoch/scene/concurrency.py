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
    async def run_all(self):
        log.info("\n=== [START] Executing Sandbox Concurrency Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        await self._test_concurrency_and_recovery()
        await self._test_n_core_scale_out()
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

    async def _test_n_core_scale_out(self):
        log.info("\n--- Running Suite: Physical N-Core Distribution (Scale-Out) ---")
        target_func = "compute_root_fingerprint"
        heavy_load_count = max(CONST.SCALE_STEPS) # 353
        
        log.info(f"🚀 Firing {heavy_load_count} massive burst requests... (Waiting for Tension Rupture)")
        async def _trigger_rupture():
            from arch.topos.tunnel.factory import TunnelFactory
            from watcher.receptor.kernel import CHANNEL_SIGNAL_MUTATION
            tunnel = await TunnelFactory.get_default()
            for i in range(15): 
                payload = {"signal_id": "node_load_synthetic_99", "value": float(i * 50)}
                await tunnel.publish(CHANNEL_SIGNAL_MUTATION, json.dumps(payload))
                await asyncio.sleep(0.1)
                
        asyncio.create_task(_trigger_rupture())
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
        log.info(f"🎯 Distribution: {len(participating_nodes)} Nodes / {len(participating_pids)} PIDs involved.")
        core_distribution = set()
        is_macos = sys.platform == 'darwin'

        for pid in participating_pids:
            try:
                proc = psutil.Process(int(pid))
                if is_macos:
                    core_distribution.add(f"Virtual_Core_for_{pid}")
                else:
                    if hasattr(proc, 'cpu_affinity'):
                        core_distribution.add(tuple(proc.cpu_affinity()))
            except (psutil.NoSuchProcess, Exception):
                pass

        if len(participating_nodes) > 1:
            if len(core_distribution) > 1:
                if is_macos:
                    self._record_success(elapsed_ms, f"Scale-Out Success: {len(participating_nodes)} Nodes spawned (macOS Virtual cores).")
                else:
                    self._record_success(elapsed_ms, f"Perfect Scale-Out: {len(participating_nodes)} Nodes mapped to {len(core_distribution)} distinct CPU Cores.")
            else:
                self._record_fail(elapsed_ms, f"Nodes scaled to {len(participating_nodes)}, but all bound to the same core {core_distribution}. Affinity failed.", "N-Core Scale-Out")
        else:
            self._record_fail(elapsed_ms, "Scale-Out failed. Watcher did not spawn new nodes, all handled by a single Node.", "N-Core Scale-Out")

    def _calculate_adaptive_timeout(self, concurrency_limit: int) -> float:
        max_step = max(CONST.SCALE_STEPS)
        raw_timeout = 1.0 + (concurrency_limit / max_step) * (CONST.MAX_TIMEOUT - 1.0)
        valid_timeouts = [t for t in CONST.SCALE_STEPS if t <= CONST.MAX_TIMEOUT]
        return float(min(valid_timeouts, key=lambda x: abs(x - raw_timeout)))

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
            successes = sum(1 for r in results if r.success)
            
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
        if not res_toxic.success and res_recovery.success:
            self._record_success(0, "System survived the crash and isolated the fault.")
        else:
            self._record_fail(0, "System failed to recover from toxic request", "Fault Isolation")