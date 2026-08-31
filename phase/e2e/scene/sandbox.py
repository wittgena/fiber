# fiber.phase.e2e.scene.sandbox
import time
import asyncio
import json
import math
from dataclasses import dataclass, field
from typing import Any, List, Dict, Optional

from fiber.phase.debug.sandbox import SandboxRunner, ScriptDef, TestScripts
from fiber.dphi.infra.adapter.anchor import ActorIdentity

from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.method import DphiMethod
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("scene.sandbox")

# =========================================================================
# [Shared Constants & Payloads]
# =========================================================================
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


# =========================================================================
# 1. CertProofScene (Master System Certification Pipeline)
# =========================================================================
class CertProofScene(SandboxRunner):
    def __init__(self, broker: Any):
        super().__init__(broker)
        self.system = ActorIdentity("System_Core")
        self.node_a = ActorIdentity("Validator_A")
        self.node_b = ActorIdentity("Validator_B")
        self.node_rogue = ActorIdentity("Rogue_Node")

    async def run_all(self):
        log.info("\n=======================================================")
        log.info("🚀 [START] Master System Certification Pipeline (4 Core Proofs)")
        log.info("=======================================================\n")

        await self._set_worker_policy("SYSTEM")

        # Domain A: Resource & Payload Boundary
        await self._proof_gas_boundary_trap()

        # Domain B: Determinism & Consensus Integrity
        await self._proof_floating_point_determinism()
        await self._proof_byzantine_fault_tolerance()

        # Report Generation (상속받은 메서드)
        self.report()

    async def _proof_gas_boundary_trap(self):
        log.info("\n--- [Proof 1] Gas Boundary & Memory Trap ---")
        ## Fuel을 STANDARD로 낮춰 자원 고갈을 유도
        await self._set_worker_policy("STANDARD")
        toxic_code = """
state_mock = []
while True:
    state_mock.append('A' * 1024)
"""
        res = await self.broker.execute(code=toxic_code)
        
        if not getattr(res, 'success', True):
            self._record_success(0, f"Fuel/Memory Trap triggered successfully. Error: {getattr(res, 'error', 'Trap/OOM')}")
        else:
            self._record_fail(0, "System failed to trap infinite resource allocation.", "Gas Boundary")
            
        await self._set_worker_policy("SYSTEM")

    async def _proof_floating_point_determinism(self):
        log.info("\n--- [Proof 3] Floating-Point Determinism ---")
        # 아키텍처(ARM/x86) 간 부동소수점 오차 발생 가능성이 있는 난해한 연산
        fp_code = """
import math
# 비선형 연산 후 15자리 정밀도로 출력
val = sum(math.sin(i * 0.1) * math.cos(i * 0.05) for i in range(1000))
print(f"{val:.15f}")
"""
        results = []
        for _ in range(3):
            res = await self.broker.execute(code=fp_code)
            if res.success:
                results.append(res.output.strip())

        # 모든 샌드박스의 실행 결과가 단 1비트의 오차도 없이 동일해야 함
        if len(results) == 3 and len(set(results)) == 1:
            self._record_success(0, f"Perfect Determinism achieved. Result: {results[0]}")
        else:
            self._record_fail(0, f"Divergence detected in floating-point operations: {results}", "Determinism")

    async def _proof_byzantine_fault_tolerance(self):
        log.info("\n--- [Proof 4] Byzantine Fault & Quarantine ---")
        
        # 1. 정상적인 에포크 데이터 구성
        parity = StateAdapter.build_parity_triplet("topos_cert", 111, 222)
        valid_commit = StateAdapter.build_anchor_commit(parity, 0, "genesis", {"repo": "hash_A"}, {})
        
        # 2. 로그(Rogue) 노드가 데이터를 몰래 변조한 커밋 생성
        rogue_commit = StateAdapter.build_anchor_commit(parity, 0, "genesis", {"repo": "hash_B_MALICIOUS"}, {})
        
        # 3. 서명 수집 (2개는 정상, 1개는 조작된 데이터에 서명)
        signatures = [
            self.node_a.sign(valid_commit),
            self.node_b.sign(valid_commit),
            self.node_rogue.sign(rogue_commit) # 변조된 페이로드에 대한 서명 제출
        ]
        
        # 4. 봉인 시도 (Threshold는 2)
        payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state="genesis",
            repos={"repo": "hash_A"}, cached_states={}, timestamp=int(time.time()),
            signers=[self.node_a.pubkey_hex, self.node_b.pubkey_hex, self.node_rogue.pubkey_hex],
            signatures=signatures, threshold=2, 
            allowed_signers=[self.node_a.pubkey_hex, self.node_b.pubkey_hex, self.node_rogue.pubkey_hex]
        )
        
        # 브로커(엔드포인트)는 rogue_commit의 서명 불일치를 감지하여 Quarantine 시키고, 
        # 남은 2개의 유효 서명이 Threshold(2)를 만족하므로 합의를 성공시켜야 합니다.
        await self._run_case(
            title="Byzantine Defense: Quarantine rogue signature & accept 2-of-3 threshold",
            target_func=DphiMethod.SEAL_EPOCH.value,
            payload=payload,
            expected_success=True 
        )


# =========================================================================
# 2. SandboxScene (Core Sandbox & Compute Scenarios)
# =========================================================================
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


# =========================================================================
# 3. DynamicsScene (Field Dynamics & Math Kernels)
# =========================================================================
class DynamicsScene(SandboxRunner):
    """
    @spec: Tests the Rust-native O(N^2) Math Kernels for correct Phase calculations,
           wrapping behaviors, and system boundary conditions.
    """
    def __init__(self, broker: Any):
        super().__init__(broker)

    async def run_all(self):
        log.info("\n=======================================================")
        log.info("🌌 [START] Field Dynamics & Continuous Math Kernel Suite")
        log.info("=======================================================\n")

        # 고비용 연산이므로 SYSTEM Tier 사용
        await self._set_worker_policy("SYSTEM")

        await self._test_kuramoto_sync()
        await self._test_fitzhugh_spiking()
        await self._test_attractor_reflector_bypass()
        await self._test_dynamics_payload_robustness()

        self.report()

    # -------------------------------------------------------------------------
    # Domain A: Kuramoto Model (Phase Sync & Tension)
    # -------------------------------------------------------------------------
    async def _test_kuramoto_sync(self):
        log.info("\n--- [Kernel] Kuramoto Oscillator Sync ---")
        
        payload = {
            "states": {
                "node_1": {"phase": 0.0, "omega": 1.0, "state": "NORMAL", "tension": 0.0, "is_spiking": False},
                "node_2": {"phase": math.pi, "omega": 1.0, "state": "NORMAL", "tension": 0.0, "is_spiking": False},
            },
            "kernel_type": "kernel.kuramoto",
            "params": {"global_coupling": 1.0},
            "dt": 0.1
        }
        
        res = await self.broker.invoke(
            target_func=DphiMethod.PROCESS_FIELD_DYNAMICS, 
            payload=json.dumps(payload),
            tier="SYSTEM"
        )
        
        if res.success:
            deltas = json.loads(res.output)
            # node_1(0)과 node_2(pi)의 위상차는 pi. sin(pi)는 0이므로 coupling_force는 0이어야 함
            # 따라서 d_phase = (omega + 0) * dt = 1.0 * 0.1 = 0.1
            d_phase_n1 = deltas.get("node_1", {}).get("d_phase")
            if d_phase_n1 is not None and math.isclose(d_phase_n1, 0.1, abs_tol=1e-5):
                self._record_success(0, f"Kuramoto Math Verified. d_phase: {d_phase_n1:.4f}")
            else:
                self._record_fail(0, f"Unexpected d_phase output: {deltas}", "Kuramoto Sync")
        else:
            self._record_fail(0, f"WASM execution failed: {res.error}", "Kuramoto Sync")

    # -------------------------------------------------------------------------
    # Domain B: FitzHugh-Nagumo Model (Spiking & Recovery)
    # -------------------------------------------------------------------------
    async def _test_fitzhugh_spiking(self):
        log.info("\n--- [Kernel] FitzHugh-Nagumo Action Potential ---")
        
        payload = {
            "states": {
                # 높은 Phase(V)를 주어 강제 스파이킹 유발 (v = phase - pi > 1.0 이 되도록)
                "node_1": {"phase": math.pi + 2.0, "omega": 0.5, "state": "NORMAL", "tension": 0.0, "recovery": 0.0, "is_spiking": False},
            },
            "kernel_type": "kernel.fitzhugh",
            "params": {"global_coupling": 0.5, "a": 0.7, "b": 0.8, "fh_epsilon": 0.08},
            "dt": 0.1
        }
        
        res = await self.broker.invoke(target_func=DphiMethod.PROCESS_FIELD_DYNAMICS, payload=json.dumps(payload), tier="SYSTEM")
        
        if res.success:
            deltas = json.loads(res.output)
            is_spiking = deltas.get("node_1", {}).get("is_spiking", False)
            tension = deltas.get("node_1", {}).get("target_tension", 0.0)
            
            if is_spiking and tension == 100.0:
                self._record_success(0, "FHN Spiking logic and high tension (100.0) accurately triggered.")
            else:
                self._record_fail(0, f"Spike detection failed: {deltas}", "FitzHugh-Nagumo Spike")
        else:
            self._record_fail(0, f"WASM execution failed: {res.error}", "FitzHugh-Nagumo Spike")

    # -------------------------------------------------------------------------
    # Domain C: Topological Roles (Bypass Math)
    # -------------------------------------------------------------------------
    async def _test_attractor_reflector_bypass(self):
        log.info("\n--- [Topology] Attractor / Reflector Bypass ---")
        
        payload = {
            "states": {
                "node_attractor": {"phase": 0.0, "omega": 2.0, "state": "ATTRACTOR", "tension": 5.0, "is_spiking": False},
                "node_reflector": {"phase": 0.0, "omega": 2.0, "state": "REFLECTOR", "tension": 5.0, "is_spiking": False},
            },
            "kernel_type": "kernel.kuramoto",
            "params": {},
            "dt": 0.1
        }
        
        res = await self.broker.invoke(target_func=DphiMethod.PROCESS_FIELD_DYNAMICS, payload=json.dumps(payload), tier="SYSTEM")
        
        if res.success:
            deltas = json.loads(res.output)
            attractor_dphase = deltas.get("node_attractor", {}).get("d_phase", 0.0)
            reflector_dphase = deltas.get("node_reflector", {}).get("d_phase", 0.0)
            
            # Attractor: omega * 0.1 * dt = 2.0 * 0.1 * 0.1 = 0.02
            # Reflector: omega * 1.5 * dt = 2.0 * 1.5 * 0.1 = 0.3
            if math.isclose(attractor_dphase, 0.02) and math.isclose(reflector_dphase, 0.3):
                self._record_success(0, "Attractor & Reflector correctly bypassed O(N^2) interactions.")
            else:
                self._record_fail(0, f"Bypass multiplier incorrect. Output: {deltas}", "Role Bypass")
        else:
            self._record_fail(0, f"Execution failed: {res.error}", "Role Bypass")

    # -------------------------------------------------------------------------
    # Domain D: Robustness & FFI Guard
    # -------------------------------------------------------------------------
    async def _test_dynamics_payload_robustness(self):
        log.info("\n--- [Guard] Dynamics Payload FFI Boundary ---")
        
        bad_payloads = [
            ("Malformed JSON", '{"states": {"node_1": }, "kernel_type": "kuramoto"}'),
            ("Missing Type", '{"states": {}, "params": {}, "dt": 0.1}'),
        ]
        
        for title, raw_str in bad_payloads:
            res = await self.broker.invoke(target_func=DphiMethod.PROCESS_FIELD_DYNAMICS, payload=raw_str, tier="SYSTEM")
            if not res.success and "Invalid DynamicsPayload" in str(res.error) or "malformed" in str(res.error):
                self._record_success(0, f"Correctly trapped bad payload: {title}")
            else:
                self._record_fail(0, f"Failed to guard against: {title}", "Dynamics Payload Guard")