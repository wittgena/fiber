# fiber.phase.e2e.wasm.bench.profile
## @lineage: fiber.dphi.workflow.wasm.bench.profile
## @lineage: fiber.workflow.wasm.bench.profile
## @lineage: workflow.bench.profile
import time
import asyncio
import json
import statistics
import operator
from typing import Any, List, Dict, Callable

from dphi.workflow.scene.anchor import ActorIdentity
from fiber.kernel.debug.sandbox import TestScripts

from xphi.kernel.space.runner import SchemeRunner
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.method import DphiMethod
from xphi.watcher.plane.emitter import get_emitter, set_log_level

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
    """
    @role: Micro-Benchmark Suite (Configurable)
    """
    # Entry에서 주입할 전역 설정 (Monkey-patch 방식)
    bench_config = {
        "targets": None,      # None이면 전체 실행
        "scale": 1.0,         # Iteration 배수 (가볍게 돌릴땐 0.1 등)
        "verbose": False      # True면 중간 측정 로그 출력
    }

    def __init__(self, broker: Any):
        super().__init__(broker)
        self.ctx = ProfileContext()
        self.results = {}
        self.success_count = 0
        self.fail_count = 0
        self.failed_cases = []
        
        # 동적 실행을 위한 메서드 라우팅 맵
        self._target_map = {
            "cold_boot": self._profile_wasm_cold_boot,
            "trap_latency": self._profile_resource_trap_latency,
            "divergence": self._profile_determinism_divergence,
            "root_hashing": self._profile_canonicalization_overhead,
            "multisig_verify": self._profile_multisig_verification,
            "merkle_proof": self._profile_merkle_path_emission,
            "entanglement": self._profile_entanglement_latency,
            "core_throughput": self._profile_core_throughput
        }

    async def run_all(self):
        scale = self.bench_config["scale"]
        is_verbose = self.bench_config["verbose"]
        
        # 타겟 필터링
        selected_targets = self.bench_config["targets"]
        if not selected_targets or "all" in selected_targets:
            selected_targets = list(self._target_map.keys())

        log.info(f"\n🚀 [START] DPHI Benchmark Profiling ({len(selected_targets)} targets, scale={scale}x)")
        if not is_verbose:
            log.info("🤫 Running in Silent Mode (Suppressing intermediate core logs...)")
            set_log_level("WARNING") # 코어 로그 일시적 억제 (시스템에 맞게 조정 필요)

        # 선택된 타겟만 동적 실행
        for target_key in selected_targets:
            if target_key in self._target_map:
                cfg = BENCH_TARGETS[target_key]
                actual_iters = max(1, int(cfg["iters"] * scale))
                if is_verbose:
                    log.info(f"\n--- [Profile] {cfg['desc']} ({actual_iters} iters) ---")
                
                # 측정 실행
                await self._target_map[target_key](actual_iters)
            else:
                log.warning(f"Unknown benchmark target: {target_key}")

        if not is_verbose:
            set_log_level("INFO") # 로그 레벨 복구
            
        self._print_final_report()

    # (이하 _measure_latency, _evaluate_and_log 로직은 기존과 거의 동일하나, 
    #  verbose 옵션에 따라 log.info 출력을 제어하도록 수정)
    async def _measure_latency(self, iterations: int, task: Callable, *args, **kwargs) -> Dict[str, float]:
        latencies_us = []
        for _ in range(iterations):
            start_ns = time.perf_counter_ns()
            await task(*args, **kwargs)
            end_ns = time.perf_counter_ns()
            latencies_us.append((end_ns - start_ns) / 1000.0)
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
            res_str = f"{actual_val:,.2f} TPS (Elapsed: {raw_metrics.get('elapsed_sec', 0):.4f}s)"
        else:
            res_str = f"{actual_val:.3f} {cfg['unit']}"

        if self.bench_config["verbose"]:
            log.info(f"{status} Target: {target_str:<12} | Result: {res_str}")
        
        raw_metrics["val"] = actual_val
        raw_metrics["passed"] = is_passed
        raw_metrics["target_str"] = target_str
        raw_metrics["res_str"] = res_str
        self.results[cfg["desc"]] = raw_metrics
        
        if is_passed: self.success_count += 1
        else: self.fail_count += 1

    # --- 프로파일링 실제 동작부 (파라미터로 iters를 받도록 서명 변경) ---
    async def _profile_wasm_cold_boot(self, iters: int):
        res = await self._measure_latency(iters, self.broker.execute, code="pass")
        self._evaluate_and_log("cold_boot", res["avg_us"], res)

    async def _profile_resource_trap_latency(self, iters: int):
        script = TestScripts.OOM_ATTACK
        res = await self._measure_latency(iters, self.broker.execute, code=script.code, tier=script.tier)
        self._evaluate_and_log("trap_latency", res["avg_us"], res)

    async def _profile_determinism_divergence(self, iters: int):
        code = 'import math; print(f"{sum(math.sin(i*0.1)*math.cos(i*0.05) for i in range(100)):.15f}")'
        outputs = set()
        for _ in range(iters):
            res = await self.broker.execute(code=code)
            if res.success: outputs.add(res.output.strip())
        divergence = 0.0 if len(outputs) == 1 else 100.0
        self._evaluate_and_log("divergence", divergence, {"unit": "%"})

    async def _profile_canonicalization_overhead(self, iters: int):
        payload = json.dumps({"data": "A" * 100_000, "ts": int(time.time())})
        res = await self._measure_latency(iters, self.broker.invoke, DphiMethod.COMPUTE_ROOT_FINGERPRINT.value, payload)
        self._evaluate_and_log("root_hashing", res["avg_us"], res)

    async def _profile_multisig_verification(self, iters: int):
        parity = StateAdapter.build_parity_triplet("bench_topos", 1, 999)
        anchor_commit = StateAdapter.build_anchor_commit(parity, 0, "state-0", {"repo": "hash"}, {})
        signatures = [val.sign(anchor_commit) for val in self.ctx.validators]
        payload = json.dumps(StateAdapter.build_seal_epoch_payload(parity, 0, "state-0", {"repo": "hash"}, {}, int(time.time()), self.ctx.val_pubs, signatures, 3, self.ctx.val_pubs))
        res = await self._measure_latency(iters, self.broker.invoke, DphiMethod.SEAL_EPOCH.value, payload)
        self._evaluate_and_log("multisig_verify", res["avg_us"], res)

    async def _profile_merkle_path_emission(self, iters: int):
        payload = json.dumps({"data": "audit_event_123", "verbose": True})
        res = await self._measure_latency(iters, self.broker.invoke, DphiMethod.GENERATE_PROOF.value, payload)
        self._evaluate_and_log("merkle_proof", res["avg_us"], res)

    async def _profile_entanglement_latency(self, iters: int):
        async def _wrap(): return StateAdapter.build_parity_triplet("cl", 1, 777)
        res = await self._measure_latency(iters, _wrap)
        self._evaluate_and_log("entanglement", res["avg_us"], res)

    async def _profile_core_throughput(self, iters: int):
        payload = json.dumps({"ts": int(time.time()), "topo": 1, "press": 5, "rupture": False, "injected_tick": None})
        method = DphiMethod.INIT_EPOCH.value
        chunk_size = min(5000, iters)
        total_time = 0.0
        
        for i in range(0, iters, chunk_size):
            tasks = [self.broker.invoke(method, payload) for _ in range(chunk_size)]
            start = time.perf_counter()
            await asyncio.gather(*tasks)
            total_time += (time.perf_counter() - start)
            
        tps = iters / total_time if total_time > 0 else 0
        self._evaluate_and_log("core_throughput", tps, {"elapsed_sec": total_time, "unit": "TPS"})

    def _print_final_report(self):
        log.info("\n" + "="*75)
        log.info("📊 [BENCHMARK RESULTS] DPHI Core Profile Summary".center(75))
        log.info("="*75)
        
        all_passed = True
        for name, metrics in self.results.items():
            status = "✅" if metrics.get("passed") else "❌"
            all_passed = all_passed and metrics.get("passed", False)
            log.info(f" {status} {name:<40} ➔ {metrics['res_str']}")
            
        log.info("-" * 75)
        if all_passed:
            log.info("🏆 ALL SELECTED BENCHMARK TARGETS MET OR EXCEEDED.")
        else:
            log.warning("⚠️ SOME BENCHMARK TARGETS FAILED TO MEET THRESHOLDS.")
        log.info("="*75 + "\n")