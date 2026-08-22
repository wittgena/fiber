# phase.dphi.bench.profile
## @lineage: nexus.phase.dphi.bench.profile
## @lineage: meta.phase.dphi.bench.profile
"""
@module: phase.dphi.bench.profile
@desc: Core WASM Engine & Cryptographic Micro-Benchmark Profiler (Config-Driven)
@target: Prove physical limits and cryptographic overhead in microseconds (µs) and core throughput (TPS).
"""

import time
import asyncio
import json
import statistics
import operator
from typing import Any, List, Dict, Callable

from dphi.scene.anchor import ActorIdentity
from dphi.node.attach.sandbox import TestScripts

from kernel.dphi.runner.phase import SchemeRunner
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.method import DphiMethod
from watcher.plane.emitter import get_emitter

log = get_emitter("bench.profile")

# =========================================================================
# 🎯 BENCHMARK TARGET CONFIGURATION
# =========================================================================
BENCH_TARGETS = {
    "cold_boot":      {"desc": "WASM Instantiation (Cold Boot)",         "target": 500,    "unit": "µs",  "op": "<",  "iters": 1000},
    "trap_latency":   {"desc": "Resource Trap Latency (OOM)",            "target": 100,    "unit": "µs",  "op": "<",  "iters": 200},
    "divergence":     {"desc": "State Divergence Rate",                  "target": 0.0,    "unit": "%",   "op": "==", "iters": 10000},
    "root_hashing":   {"desc": "Canonicalization & Root Hashing",        "target": 200,    "unit": "µs",  "op": "<",  "iters": 1000},
    "multisig_verify":{"desc": "Multi-Sig Consensus Verification",       "target": 150,    "unit": "µs",  "op": "<",  "iters": 1000},
    "merkle_proof":   {"desc": "Merkle Path Emission",                   "target": 500,    "unit": "µs",  "op": "<",  "iters": 1000},
    "entanglement":   {"desc": "State Entanglement Latency (Netting)",   "target": 1000,   "unit": "µs",  "op": "<",  "iters": 2000},
    "core_throughput":{"desc": "Single-Node Core Throughput",            "target": 100000, "unit": "TPS", "op": ">",  "iters": 50000},
}

OP_MAP = {"<": operator.lt, ">": operator.gt, "==": operator.eq, "<=": operator.le, ">=": operator.ge}


class ProfileContext:
    def __init__(self):
        self.system = ActorIdentity("System_Core")
        self.validators = [ActorIdentity(f"Validator_{i}") for i in range(3)]
        self.val_pubs = [v.pubkey_hex for v in self.validators]

class ProfileBenchmarker(SchemeRunner):
    def __init__(self, broker: Any):
        super().__init__(broker)
        self.ctx = ProfileContext()
        self.results = {}

    async def run_all(self):
        log.info("\n=======================================================")
        log.info("🚀 [START] DPHI Core Engine Micro-Benchmark Profiling")
        log.info("=======================================================\n")

        # 1. Microsecond-Level Isolation & Determinism
        await self._profile_wasm_cold_boot()
        await self._profile_resource_trap_latency()
        await self._profile_determinism_divergence()

        # 2. Zero-Friction Cryptographic Attestation
        await self._profile_canonicalization_overhead()
        await self._profile_multisig_verification()
        await self._profile_merkle_path_emission()
        
        # 3. High-Frequency Core Throughput
        await self._profile_core_throughput()
        await self._profile_entanglement_latency()

        self._print_final_report()

    # ---------------------------------------------------------
    # Helper: 타이머 측정 및 결과 평가 로직
    # ---------------------------------------------------------
    async def _measure_latency(self, iterations: int, task: Callable, *args, **kwargs) -> Dict[str, float]:
        latencies_us = []
        for _ in range(iterations):
            start_ns = time.perf_counter_ns()
            await task(*args, **kwargs)
            end_ns = time.perf_counter_ns()
            latencies_us.append((end_ns - start_ns) / 1000.0) # Convert to µs
        
        return {
            "avg_us": statistics.mean(latencies_us),
            "p95_us": statistics.quantiles(latencies_us, n=100)[94],
            "p99_us": statistics.quantiles(latencies_us, n=100)[98],
            "unit": "µs"
        }

    def _evaluate_and_log(self, key: str, actual_val: float, raw_metrics: dict):
        cfg = BENCH_TARGETS[key]
        is_passed = OP_MAP[cfg["op"]](actual_val, cfg["target"])
        status = "\033[92m[PASS]\033[0m" if is_passed else "\033[91m[FAIL]\033[0m"
        
        target_str = f"{cfg['op']} {cfg['target']:,.0f} {cfg['unit']}" if isinstance(cfg['target'], (int, float)) else f"{cfg['op']} {cfg['target']} {cfg['unit']}"
        
        if cfg["unit"] == "µs":
            res_str = f"Avg {raw_metrics['avg_us']:>6.2f} µs (p99: {raw_metrics['p99_us']:>6.2f} µs)"
        elif cfg["unit"] == "TPS":
            res_str = f"{actual_val:,.2f} TPS (Elapsed: {raw_metrics['elapsed_sec']:.4f}s)"
        else:
            res_str = f"{actual_val:.3f} {cfg['unit']}"

        log.info(f"{status} Target: {target_str:<12} | Result: {res_str}")
        
        raw_metrics["passed"] = is_passed
        self.results[cfg["desc"]] = raw_metrics

    # =========================================================================
    # Domain 1: Isolation & Determinism
    # =========================================================================
    async def _profile_wasm_cold_boot(self):
        cfg = BENCH_TARGETS["cold_boot"]
        log.info(f"\n--- [Profile] {cfg['desc']} ---")
        res = await self._measure_latency(cfg["iters"], self.broker.execute, code="pass")
        self._evaluate_and_log("cold_boot", res["avg_us"], res)

    async def _profile_resource_trap_latency(self):
        cfg = BENCH_TARGETS["trap_latency"]
        log.info(f"\n--- [Profile] {cfg['desc']} ---")
        
        script = TestScripts.OOM_ATTACK
        res = await self._measure_latency(cfg["iters"], self.broker.execute, code=script.code, tier=script.tier)
        self._evaluate_and_log("trap_latency", res["avg_us"], res)

    async def _profile_determinism_divergence(self):
        cfg = BENCH_TARGETS["divergence"]
        log.info(f"\n--- [Profile] {cfg['desc']} ---")
        
        complex_math_code = """
import math
print(f"{sum(math.sin(i * 0.1) * math.cos(i * 0.05) for i in range(100)):.15f}")
"""
        outputs = set()
        for _ in range(cfg["iters"]):
            res = await self.broker.execute(code=complex_math_code)
            if res.success:
                outputs.add(res.output.strip())
        
        divergence_rate = 0.0 if len(outputs) == 1 else 100.0
        self._evaluate_and_log("divergence", divergence_rate, {"unit": "%"})

    # =========================================================================
    # Domain 2: Cryptographic Attestation
    # =========================================================================
    async def _profile_canonicalization_overhead(self):
        cfg = BENCH_TARGETS["root_hashing"]
        log.info(f"\n--- [Profile] {cfg['desc']} ---")
        
        payload_str = json.dumps({"data": "A" * 100_000, "ts": int(time.time())})
        res = await self._measure_latency(cfg["iters"], self.broker.invoke, DphiMethod.COMPUTE_ROOT_FINGERPRINT.value, payload_str)
        self._evaluate_and_log("root_hashing", res["avg_us"], res)

    async def _profile_multisig_verification(self):
        cfg = BENCH_TARGETS["multisig_verify"]
        log.info(f"\n--- [Profile] {cfg['desc']} ---")
        
        parity = StateAdapter.build_parity_triplet("bench_topos", 1, 999)
        anchor_commit = StateAdapter.build_anchor_commit(parity, 0, "state-0", {"repo": "hash"}, {})
        signatures = [val.sign(anchor_commit) for val in self.ctx.validators]
        
        payload_str = json.dumps(StateAdapter.build_seal_epoch_payload(
            parity, 0, "state-0", {"repo": "hash"}, {}, int(time.time()),
            self.ctx.val_pubs, signatures, 3, self.ctx.val_pubs
        ))
        
        res = await self._measure_latency(cfg["iters"], self.broker.invoke, DphiMethod.SEAL_EPOCH.value, payload_str)
        self._evaluate_and_log("multisig_verify", res["avg_us"], res)

    async def _profile_merkle_path_emission(self):
        cfg = BENCH_TARGETS["merkle_proof"]
        log.info(f"\n--- [Profile] {cfg['desc']} ---")
        
        payload_str = json.dumps({"data": "audit_event_123", "verbose": True})
        res = await self._measure_latency(cfg["iters"], self.broker.invoke, DphiMethod.GENERATE_PROOF.value, payload_str)
        self._evaluate_and_log("merkle_proof", res["avg_us"], res)

    # =========================================================================
    # Domain 3: High-Frequency Core Throughput
    # =========================================================================
    async def _profile_entanglement_latency(self):
        cfg = BENCH_TARGETS["entanglement"]
        log.info(f"\n--- [Profile] {cfg['desc']} ---")
        
        def mock_entanglement():
            return StateAdapter.build_parity_triplet(f"clearing_{int(time.time())}", 101 ^ 102, 777)
        
        async def _wrap_sync(): return mock_entanglement()
        
        res = await self._measure_latency(cfg["iters"], _wrap_sync)
        self._evaluate_and_log("entanglement", res["avg_us"], res)

    async def _profile_core_throughput(self):
        cfg = BENCH_TARGETS["core_throughput"]
        log.info(f"\n--- [Profile] {cfg['desc']} ---")
        
        iterations = cfg["iters"]
        payload_str = json.dumps({"ts": int(time.time()), "topo": 1, "press": 5, "rupture": False, "injected_tick": None})
        method = DphiMethod.INIT_EPOCH.value
        
        chunk_size = 5000
        total_time_sec = 0.0
        
        for i in range(0, iterations, chunk_size):
            tasks = [self.broker.invoke(method, payload_str) for _ in range(chunk_size)]
            start_time = time.perf_counter()
            await asyncio.gather(*tasks)
            total_time_sec += (time.perf_counter() - start_time)
            
        tps = iterations / total_time_sec
        self._evaluate_and_log("core_throughput", tps, {"elapsed_sec": total_time_sec, "unit": "TPS"})

    # =========================================================================
    # Report
    # =========================================================================
    def _print_final_report(self):
        log.info("\n" + "="*65)
        log.info("📊 [BENCHMARK RESULTS] DPHI Core Profile Summary")
        log.info("="*65)
        
        all_passed = True
        for name, metrics in self.results.items():
            status = "✅" if metrics.get("passed") else "❌"
            all_passed = all_passed and metrics.get("passed", False)
            
            if metrics["unit"] == "TPS":
                log.info(f" {status} [ {name:<35} ] -> {metrics['val']:,.0f} TPS")
            elif metrics["unit"] == "µs":
                log.info(f" {status} [ {name:<35} ] -> Avg: {metrics['avg_us']:>6.2f} µs")
            else:
                log.info(f" {status} [ {name:<35} ] -> {metrics['val']:.3f} {metrics['unit']}")
        
        log.info("-" * 65)
        if all_passed:
            log.info("🏆 ALL BENCHMARK TARGETS MET OR EXCEEDED.")
        else:
            log.info("⚠️ SOME BENCHMARK TARGETS FAILED TO MEET THRESHOLDS.")
        log.info("="*65 + "\n")

if __name__ == "__main__":
    pass