# fiber.workflow.scene.sandbox
## @lineage: workflow.scene.sandbox
## @lineage: dphi.workflow.scene.sandbox
import time
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List
from fiber.kernel.attach.sandbox import SandboxRunner, ScriptDef, TestScripts
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("scene.sandbox")

class TestPayloads:
    PHASE_GEN       = {"topo": 50, "press": -10, "rupture": False}
    INJECTED_STATE  = {"topo": 100, "press": 200, "rupture": True, "injected_anchor": 999999, "injected_tick": 77}
    MALFORMED_JSON  = '{"topo": 50, "press": -10, "rupture": '

@dataclass(frozen=True)
class TestConstants:
    PAYLOAD_10K: str = "A" * 10_000
    PAYLOAD_50K: str = "A" * 50_000
    PAYLOAD_150K: str = "A" * 150_000
    
    PAYLOAD_MASSIVE: str = "A" * (1024 * 1024 * 40) 
    SCALE_STEPS: List[int] = field(default_factory=lambda: [1, 5, 17, 46, 71, 128, 256, 353])
    MAX_TIMEOUT: float = 35.0 
    MEM_WARN_LIMIT: float = 85.0
    CPU_WARN_LIMIT: float = 95.0
    
    T_ID: int = 101010
    P_ID: int = 999999
    N_ID: int = 907049
    
    INJECTED_CTX: Dict[str, Any] = field(default_factory=lambda: {
        "timestamp": 1600000000, 
        "seed": "proof_of_compute_seed_777"
    })

CONST = TestConstants()


class SandboxScene(SandboxRunner):
    async def run_all(self):
        log.info("\n=== [START] Executing Sandbox & Compute Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        # 1. Isolation & Security
        await self._test_wasmcg_resilience()
        await self._test_legacy_isolation()
        
        # 2. Causality & State
        await self._test_topos_and_phase()
        await self._test_tripartite_parity()
        await self._test_ffi_robustness()
        
        # 3. Resources & Resilience
        await self._test_fuel_profiling()
        await self._set_worker_policy("SYSTEM")
        self.report()

    # --- Domain 1: Isolation & Security ---
    async def _test_wasmcg_resilience(self):
        log.info("\n--- Running Suite: WasmCG Resilience ---")
        await self._run_case(
            title="WasmCG: Unregistered API Call (O(1) Guard)", 
            target_func="hack_system_memory", 
            payload={}, 
            expected_success=False, 
            expected_match="unknown variant"
        )

    async def _test_legacy_isolation(self):
        log.info("\n--- Running Suite: Determinism & Legacy Isolation ---")
        
        # 1. 시맨틱 및 워크로드(Workload) 성능 검증
        await self._assert_script(TestScripts.LEGACY_NORMAL)
        await self._assert_script(TestScripts.COMPUTE_HEAVY)
        await self._assert_script(TestScripts.DATA_PROCESSING)
        
        # 2. 시스템 탈옥 방어 (Escape Defense)
        await self._assert_script(TestScripts.IO_VIOLATION)
        await self._assert_script(TestScripts.NET_VIOLATION)
        await self._assert_script(TestScripts.ENV_LEAK)
        await self._assert_script(TestScripts.SUBPROCESS_ATTACK)
        await self._assert_script(TestScripts.THREAD_ATTACK)
        await self._assert_script(TestScripts.SYS_EXIT_ATTACK)
        
        # 3. 악성 자원 소모 방어 (Cgroup Trap - STANDARD Tier 강제)
        await self._assert_script(TestScripts.INFINITE_LOOP_ATTACK)
        await self._assert_script(TestScripts.OOM_ATTACK)
        await self._assert_script(TestScripts.STACK_OVERFLOW_ATTACK)

        import asyncio
        await asyncio.sleep(3.0)
        
        # 4. 시간 누수 및 멱등성 검증 (Determinism)
        def validate_time_leak(out: str) -> bool:
            try:
                epoch_time, perf_time = map(float, out.strip().split('|'))
                return perf_time == 0.001 and epoch_time > 1500000000.0
            except Exception:
                return False
                
        await self._assert_script(
            TestScripts.TIME_LEAK, 
            validator=validate_time_leak
        )
        
        def validate_injection(out: str) -> bool:
            try:
                out_time, _ = out.strip().split('|')
                return int(float(out_time)) == CONST.INJECTED_CTX["timestamp"]
            except Exception:
                return False
            
        await self._assert_script(
            TestScripts.INJECTION, 
            context=CONST.INJECTED_CTX, 
            validator=validate_injection
        )
        
        # PRNG 결정론 검증
        r1 = await self.broker.execute(code=TestScripts.PRNG_IDEMPOTENT.code)
        r2 = await self.broker.execute(code=TestScripts.PRNG_IDEMPOTENT.code)
        if r1.success and r2.success and (r1.output == r2.output):
            self._record_success(0, f"PRNG sequences are 100% identical ({r1.output.strip()})")
        else:
            self._record_fail(0, "PRNG outputs diverge", "PRNG Idempotency Test")

    # --- Domain 2: Causality & State ---
    async def _test_topos_and_phase(self):
        log.info("\n--- Running Suite: Causality (Epoch-Tick) ---")
        await self._run_case("Event: Generate Topos Anchor ID", "generate_topos_id", {"ts": int(time.time() * 1000)}, True)
        await self._run_case("Event: Generate Phase & Nexus ID", "generate_phase_id", TestPayloads.PHASE_GEN, True)
        await self._run_case("DI: Phase with Injected State", "generate_phase_id", TestPayloads.INJECTED_STATE, True)

    async def _test_tripartite_parity(self):
        log.info("\n--- Running Suite: Tripartite Parity ---")
        p_all = {"topos_id_low32": CONST.T_ID, "phase_id": CONST.P_ID, "nexus_id": CONST.N_ID}
        await self._run_case("Parity: Validate All 3 IDs", "verify_parity", p_all, True)
        await self._run_case("Parity: Recover Missing Phase", "verify_parity", {"topos_id_low32": CONST.T_ID, "nexus_id": CONST.N_ID}, True)
        await self._run_case("Parity: Insufficient Info", "verify_parity", {"nexus_id": CONST.N_ID}, False)

    async def _test_ffi_robustness(self):
        log.info("\n--- Running Suite: FFI Robustness ---")
        await self._run_case("FFI Guard: Malformed JSON", "generate_phase_id", TestPayloads.MALFORMED_JSON, False, expected_match="Invalid")

    # --- Domain 3: Resources & Resilience ---
    async def _test_fuel_profiling(self):
        log.info("\n--- Running Suite: Resource Fuel Profiling ---")
        await self._run_case("Profile: 10KB Payload Hashing", "compute_root_fingerprint", {"dummy_data": CONST.PAYLOAD_10K}, True)
        await self._set_worker_policy("STANDARD")
        await self._run_case(
            title="Boundary Test: 40MB Exhaustion under STANDARD Tier (Expect Trap)", 
            target_func="compute_root_fingerprint", 
            payload={"dummy_data": CONST.PAYLOAD_MASSIVE}, 
            expected_success=False,
            expected_match=None
        )