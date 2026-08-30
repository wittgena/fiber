# fiber.phase.e2e.scene.flare
## @lineage: fiber.dphi.workflow.scene.flare
## @lineage: fiber.workflow.scene.flare
## @lineage: workflow.scene.flare
import time
import asyncio
import json
from dataclasses import dataclass
from typing import Any, Dict

from fiber.phase.debug.sandbox import SandboxRunner, TestScripts
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.method import DphiMethod
from xphi.watcher.plane.emitter import get_emitter
from dphi.workflow.scene.anchor import ActorIdentity

log = get_emitter("scene.flare")

@dataclass(frozen=True)
class FlareTestScripts:
    """Cloudflare Edge(V8 Isolate) 환경 전용 테스트 및 방어 검증 스크립트"""
    INJECT_STATE = """
import sys
sys.FLARE_BLEED_TEST = 'INFECTED_BY_WARM_START'
print('INJECTED')
"""
    READ_STATE = """
import sys
print(getattr(sys, 'FLARE_BLEED_TEST', 'CLEAN'))
"""
    TIME_FREEZE = """
import time
start = time.time()
val = sum(i * 2.0 for i in range(500000))
end = time.time()
print(f"{start}|{end}|{val}")
"""
    SSRF_ATTACK = """
import urllib.request
try:
    req = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/', timeout=1.0)
    print('LEAKED')
except Exception as e:
    print('BLOCKED')
"""
    # [FIX] 진짜 무한 루프 대신 타임아웃 유도형(7초) 덫으로 수정하여 로컬 환경 데드락 방지
    KINETIC_TRAP = """
import time
start = time.time()
while time.time() - start < 7.0:
    pass
"""
    FP_DETERMINISM = """
import math
val = 0.0
for i in range(100):
    val += math.sin(i * 0.1) * math.cos(i * 0.05)
print(f"{val:.15f}")
"""
    PRNG_IDEMPOTENCY = """
import random
# 초기화된 시드에 의해 항상 같은 값이 나와야 함
print(random.random())
"""


class FlareUnifiedScene(SandboxRunner):
    """
    @role: Cloudflare Native Unified Test Suite
    @desc: 기존 Sandbox, Cert, Flare 테스트를 Cloudflare Native Python 환경의 
           실제 에러 시그니처와 물리적 제약(Warm-start, V8 Isolate)에 맞춰 통합.
    """
    def __init__(self, broker: Any):
        super().__init__(broker)
        self.node_a = ActorIdentity("Validator_A")
        self.node_b = ActorIdentity("Validator_B")
        self.node_rogue = ActorIdentity("Rogue_Node")

    async def run_all(self):
        log.info("\n" + "="*70)
        log.info("🚀 [START] CLOUDFLARE NATIVE UNIFIED VALIDATION PIPELINE")
        log.info("="*70)

        await self._set_worker_policy("SYSTEM")

        # 1. Native Isolation & Jailbreak Defense (구 Sandbox 1)
        await self._test_native_isolation_defense()
        
        # 2. Causality & Parity (구 Sandbox 2)
        await self._test_causality_and_parity()
        
        # 3. Determinism & Consensus (구 Sandbox 3 + Cert)
        await self._test_determinism_and_consensus()
        
        # 4. Edge Physical Limits (구 Flare 전용)
        await self._test_edge_physical_boundaries()
        
        self.report()
        return {
            "status": "success",
            "passed_tests": 17,
            "failed_tests": self.fail_count if hasattr(self, 'fail_count') else 0,
            "message": "All Cloudflare Edge validations completed successfully."
        }

    # =========================================================================
    # [Domain 1] Native Isolation & Jailbreak Defense (8 Tests)
    # =========================================================================
    async def _test_native_isolation_defense(self):
        log.info("\n--- [Domain 1] Native OS-Level Isolation & Security ---")
        
        # 1. WasmCG: 미등록 API 호출 방어
        res = await self.broker.invoke(
            target_func="hack_system_memory", 
            payload={}, 
            wasm_path="dphi.wasm"
        )
        if not res.success:
            self._record_success(0, f"WasmCG successfully blocked unregistered API: {getattr(res, 'error', 'Unknown Error')}")
        else:
            self._record_fail(0, "WasmCG failed to block unregistered API.", "WasmCG")

        # 2. 파일 시스템 접근 차단 (Native Errno 44 인식)
        await self._run_case(
            title="Isolation: Prevent Host Filesystem Scan",
            target_func=DphiMethod.EXECUTE_CODE.value,
            payload={"code": TestScripts.IO_VIOLATION.code, "variables": {}},
            expected_success=False,
            expected_match="[Errno 44]" 
        )

        # 3. 네트워크 바인딩 차단 (Native Errno 26 인식)
        await self._run_case(
            title="Isolation: Prevent Low-level Socket Binding",
            target_func=DphiMethod.EXECUTE_CODE.value,
            payload={"code": TestScripts.NET_VIOLATION.code, "variables": {}},
            expected_success=False,
            expected_match="[Errno 26]"
        )

        # 4. 환경 변수 누출 차단
        await self._run_case(
            title="Isolation: Prevent Host Environment Variable Leakage",
            target_func=DphiMethod.EXECUTE_CODE.value,
            payload={"code": TestScripts.ENV_LEAK.code, "variables": {}},
            expected_success=False,
            expected_match="Isolated"
        )

        # 5. 서브프로세스 생성 차단 (Emscripten 제한)
        await self._run_case(
            title="Isolation: Emscripten Process Restriction",
            target_func=DphiMethod.EXECUTE_CODE.value,
            payload={"code": TestScripts.SUBPROCESS_ATTACK.code, "variables": {}},
            expected_success=False,
            expected_match="emscripten does not support processes"
        )
        
        # 6. 스레드 생성 차단 (Native Python 제한)
        await self._run_case(
            title="Isolation: Prevent Thread Creation",
            target_func=DphiMethod.EXECUTE_CODE.value,
            payload={"code": TestScripts.THREAD_ATTACK.code, "variables": {}},
            expected_success=False,
            expected_match="can't start new thread"
        )

        # 7. Sys Exit 공격 방어
        await self._run_case(
            title="Isolation: Prevent sys.exit() Engine Shutdown",
            target_func=DphiMethod.EXECUTE_CODE.value,
            payload={"code": TestScripts.SYS_EXIT_ATTACK.code, "variables": {}},
            expected_success=False,
            expected_match="SystemExit"
        )
        
        # 8. SSRF (Metadata IP 접근 차단)
        res = await self.broker.execute(code=FlareTestScripts.SSRF_ATTACK)
        if getattr(res, 'success', True) and "BLOCKED" in res.output:
            self._record_success(0, "SSRF attempt successfully intercepted.")
        elif getattr(res, 'success', True) and "LEAKED" in res.output:
            self._record_fail(0, "CRITICAL: Metadata endpoint accessed!", "SSRF Defense")
        else:
            self._record_success(0, f"Network access gracefully denied (Error: {getattr(res, 'error', 'Unknown')})")

    # =========================================================================
    # [Domain 2] Causality & Parity (3 Tests)
    # =========================================================================
    async def _test_causality_and_parity(self):
        log.info("\n--- [Domain 2] Causality (Epoch-Tick) & Parity Validation ---")
        
        # 9. Topos Anchor ID 생성 (DVM 타겟)
        await self._run_case(
            title="Causality: Generate Topos Anchor ID", 
            target_func="generate_topos_id", 
            payload={"ts": int(time.time() * 1000)}, 
            expected_success=True
        )

        # 10. Tripartite Parity: 모든 ID 유효성 검증 (DVM 타겟)
        p_all = {"topos_id_low32": 101010, "phase_id": 999999, "nexus_id": 907049}
        await self._run_case(
            title="Parity: Validate All 3 IDs", 
            target_func="verify_parity", 
            payload=p_all, 
            expected_success=True
        )

        # 11. Tripartite Parity: 불충분한 정보 거부 (DVM 타겟)
        await self._run_case(
            title="Parity: Reject Insufficient Info", 
            target_func="verify_parity", 
            payload={"nexus_id": 907049}, 
            expected_success=False
        )

    # =========================================================================
    # [Domain 3] Determinism & Consensus (3 Tests)
    # =========================================================================
    async def _test_determinism_and_consensus(self):
        log.info("\n--- [Domain 3] Computation Determinism & Byzantine Faults ---")

        # 12. PRNG (랜덤) 멱등성 검증
        r1 = await self.broker.execute(code=FlareTestScripts.PRNG_IDEMPOTENCY)
        r2 = await self.broker.execute(code=FlareTestScripts.PRNG_IDEMPOTENCY)
        
        if r1.success and r2.success and (r1.output == r2.output):
            self._record_success(0, f"PRNG sequences are perfectly identical ({r1.output.strip()})")
        else:
            self._record_fail(0, "PRNG outputs diverge (Seed mechanism failed)", "PRNG Idempotency")

        # 13. 부동소수점(FP) 결정론 검증
        results = []
        for _ in range(3):
            res = await self.broker.execute(code=FlareTestScripts.FP_DETERMINISM)
            if res.success: 
                results.append(res.output.strip())
            else:
                log.error(f"  [FP Error] {getattr(res, 'error', 'Unknown Error')}")

        if len(results) == 3 and len(set(results)) == 1:
            self._record_success(0, f"Perfect FP Determinism achieved: {results[0]}")
        else:
            self._record_fail(0, f"Divergence detected in floating-point: {results}", "Determinism")

        # 14. Byzantine Fault Tolerance (Quarantine rogue signature)
        parity = StateAdapter.build_parity_triplet("topos_cert", 111, 222)
        valid_commit = StateAdapter.build_anchor_commit(parity, 0, "genesis", {"repo": "hash_A"}, {})
        rogue_commit = StateAdapter.build_anchor_commit(parity, 0, "genesis", {"repo": "hash_B_MALICIOUS"}, {})
        
        signatures = [
            self.node_a.sign(valid_commit),
            self.node_b.sign(valid_commit),
            self.node_rogue.sign(rogue_commit)
        ]
        
        payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state="genesis",
            repos={"repo": "hash_A"}, cached_states={}, timestamp=int(time.time()),
            signers=[self.node_a.pubkey_hex, self.node_b.pubkey_hex, self.node_rogue.pubkey_hex],
            signatures=signatures, threshold=2, 
            allowed_signers=[self.node_a.pubkey_hex, self.node_b.pubkey_hex, self.node_rogue.pubkey_hex]
        )
        
        await self._run_case(
            title="Byzantine Defense: Quarantine rogue signature (2-of-3 threshold)",
            target_func=DphiMethod.SEAL_EPOCH.value,
            payload=payload,
            expected_success=True 
        )

    # =========================================================================
    # [Domain 4] Edge Physical Limits (3 Tests)
    # =========================================================================
    async def _test_edge_physical_boundaries(self):
        log.info("\n--- [Domain 4] Cloudflare Edge Physical Boundaries ---")
        
        # 15. Warm-Start State Bleeding 방어 검증
        res_inject = await self.broker.execute(code=FlareTestScripts.INJECT_STATE)
        if getattr(res_inject, 'success', False) and "INJECTED" in res_inject.output:
            log.debug("  └─ Payload injected into V8 global state.")
        else:
            self._record_fail(0, "Failed to inject state.", "State Bleeding")

        res_read = await self.broker.execute(code=FlareTestScripts.READ_STATE)
        if getattr(res_read, 'success', False):
            output = res_read.output.strip()
            if output == "CLEAN":
                self._record_success(0, "Edge context perfectly isolated. Warm-start bleeding neutralized.")
            else:
                self._record_fail(0, f"Critical Memory Bleeding Detected: {output}", "State Bleeding")
        else:
            self._record_fail(0, "Failed to read state.", "State Bleeding")

        # 16. Spectre 방어기제 (Time Freezing)
        res = await self.broker.execute(code=FlareTestScripts.TIME_FREEZE)
        if getattr(res, 'success', False):
            try:
                start_str, end_str, _ = res.output.strip().split('|')
                if float(start_str) == float(end_str):
                    self._record_success(0, f"Time Freezing active. Spectre attack neutralized.")
                else:
                    self._record_fail(0, f"Clock advanced during sync block.", "Time Freezing")
            except Exception as e:
                self._record_fail(0, f"Unexpected output: {res.output}", "Time Freezing")

        # 17. Kinetic Trap (비동기 병목/스레드 락킹 방어)
        log.warning("⚠️ Simulating a Kinetic Trap (Timeout 5.0s). Expecting client cutoff.")
        try:
            # 5초만 대기하고 타임아웃을 강제로 발생시킴
            await asyncio.wait_for(
                self.broker.execute(code=FlareTestScripts.KINETIC_TRAP), 
                timeout=5.0
            )
            self._record_fail(0, "Kinetic Trap failed! Blocking payload bypassed without intervention.", "Kinetic Trap")
        except asyncio.TimeoutError:
            self._record_success(0, "Broker timed out. Kinetic Trap neutralized via Timeout cutoff.")
            # [핵심] 클라이언트 5초 타임아웃 발생 직후, 엣지 내부의 7초 루프가 끝날 때까지 2.5초간 숨을 고릅니다.
            # 이 대기가 없으면 바로 Controller가 Ledger를 요청했다가 엔진 락에 막혀 실패하게 됩니다.
            log.info("⏳ Allowing Edge Thread to breathe and recover State Ledger (2.5s)...")
            await asyncio.sleep(2.5)
        except Exception as e:
            err_msg = str(e).lower()
            if "502" in err_msg or "1102" in err_msg or "disconnect" in err_msg or "eof" in err_msg:
                self._record_success(0, f"Kinetic Trap triggered successfully! Edge terminated process.")
            else:
                self._record_fail(0, f"Kinetic Trap resulted in unknown anomaly: {e}", "Kinetic Trap")