# workflow.scene.flare
## @lineage: dphi.workflow.scene.flare
import time
import asyncio
from dataclasses import dataclass
from typing import Any

from fiber.phase.kernel.attach.sandbox import SandboxRunner
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("scene.flare")

@dataclass(frozen=True)
class FlareTestScripts:
    """Cloudflare Edge(V8 Isolate) 환경의 물리적 한계를 타격하기 위한 스크립트 모음"""
    
    # 1. State Bleeding (전역 상태 누수) 체크용
    INJECT_STATE = """
import sys
sys.FLARE_BLEED_TEST = 'INFECTED_BY_WARM_START'
print('INJECTED')
"""
    READ_STATE = """
import sys
# 이전 Request에서 오염된 전역 객체가 파괴되고 초기화되었는지 확인
print(getattr(sys, 'FLARE_BLEED_TEST', 'CLEAN'))
"""

    # 2. Time Freezing (Spectre Attack Mitigation) 검증
    TIME_FREEZE = """
import time
start = time.time()
# CPU 바운드 연산으로 인위적인 지연 유발 (약 5-10ms)
val = sum(i * 2.0 for i in range(500000))
end = time.time()
# 동기 블록 내에서 시간이 멈춰있어야 함 (start == end)
print(f"{start}|{end}|{val}")
"""

    # 3. SSRF & Network Boundary 검증 (로컬/메타데이터 IP 접근)
    SSRF_ATTACK = """
import urllib.request
try:
    # AWS/GCP/Cloudflare 메타데이터 엔드포인트 접근 시도
    req = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/', timeout=1.0)
    print('LEAKED')
except Exception as e:
    # 엣지 런타임에서 소켓을 차단하거나 에러를 내뿜어야 정상
    print('BLOCKED')
"""

    # 4. Kinetic Trap (CPU Time Limit Exceeded - 1102 Error 유도)
    KINETIC_TRAP = """
# Cloudflare Worker CPU 50ms Limit 돌파 유도 (무한 루프)
x = 0
while True:
    x += 1
"""


class FlareEdgeScene(SandboxRunner):
    """
    @role: Edge Physical Boundary Validation Suite
    @desc: 로컬 Wasmtime에서는 잡히지 않는 Cloudflare V8 Isolate 고유의 
           물리적 특성(Warm-start, Time Freeze, SSRF, CPU Limit)을 검증합니다.
    """
    def __init__(self, broker: Any):
        super().__init__(broker)

    async def run_all(self):
        log.info("\n" + "="*65)
        log.info("🚀 [START] CLOUDFLARE EDGE PHYSICAL BOUNDARY VALIDATION")
        log.info("="*65)

        await self._set_worker_policy("SYSTEM")

        # 1. 상태 격리 (State Isolation & Bleeding) 검증
        await self._test_isolate_state_isolation()
        
        # 2. Spectre 방어기제 (Time Freezing) 검증
        await self._test_time_freezing_defense()
        
        # 3. 네트워크 바운더리 (SSRF 차단) 검증
        await self._test_edge_network_boundary()

        # 4. 물리적 붕괴 (Kinetic Trap) 유도 - 이 테스트는 시스템을 다운시킬 수 있음
        await self._test_kinetic_trap_precision()

        self.report()

    # --- Domain 1: Isolate State & Memory ---
    async def _test_isolate_state_isolation(self):
        log.info("\n--- [Edge Physical] Phase 1: Warm-Start State Bleeding Defense ---")
        
        # 첫 번째 요청: V8 Isolate 내부에 악성 전역 변수 주입 (Warm-up)
        res_inject = await self.broker.execute(code=FlareTestScripts.INJECT_STATE)
        if getattr(res_inject, 'success', False) and "INJECTED" in res_inject.output:
            log.debug("  └─ [1/2] Payload successfully injected into V8 global state.")
        else:
            self._record_fail(0, "Failed to inject state for testing.", "State Isolation")
            return

        # V8 Isolate 재사용(Warm Start)을 유도하기 위해 지연 없이 즉시 재요청
        res_read = await self.broker.execute(code=FlareTestScripts.READ_STATE)
        
        if getattr(res_read, 'success', False):
            output = res_read.output.strip()
            if output == "CLEAN":
                self._record_success(0, "Edge context perfectly isolated. No state bleeding detected.")
            else:
                self._record_fail(0, f"Critical Memory Bleeding Detected! Output: {output}", "State Isolation")
        else:
            self._record_fail(0, "Failed to execute state read payload.", "State Isolation")

    # --- Domain 2: Timing & Spectre Mitigation ---
    async def _test_time_freezing_defense(self):
        log.info("\n--- [Edge Physical] Phase 2: Spectre Defense (Time Freezing) ---")
        
        res = await self.broker.execute(code=FlareTestScripts.TIME_FREEZE)
        if getattr(res, 'success', False):
            try:
                start_str, end_str, _ = res.output.strip().split('|')
                start_time, end_time = float(start_str), float(end_str)
                
                # Cloudflare Worker에서는 동기 루프 내에서 시간이 멈춰있어야 함
                if start_time == end_time:
                    self._record_success(0, f"Time Freezing active (Start: {start_time}, End: {end_time}). Spectre attack neutralized.")
                else:
                    delta = end_time - start_time
                    self._record_fail(0, f"Clock advanced by {delta}s during sync block. Edge time protection failed.", "Time Freezing")
            except Exception as e:
                self._record_fail(0, f"Unexpected output format: {res.output} (Err: {e})", "Time Freezing")
        else:
            self._record_fail(0, "Failed to execute Time Freeze payload.", "Time Freezing")

    # --- Domain 3: I/O & Subrequest Boundary ---
    async def _test_edge_network_boundary(self):
        log.info("\n--- [Edge Physical] Phase 3: SSRF & Subrequest Boundary ---")
        
        res = await self.broker.execute(code=FlareTestScripts.SSRF_ATTACK)
        
        if getattr(res, 'success', True):
            output = res.output.strip()
            if "BLOCKED" in output:
                self._record_success(0, "SSRF attempt successfully intercepted by Edge Boundary.")
            elif "LEAKED" in output:
                self._record_fail(0, "CRITICAL: Metadata endpoint accessed from within Sandbox!", "Network Boundary")
            else:
                self._record_fail(0, f"Unexpected Network Execution Result: {output}", "Network Boundary")
        else:
            # Wasm 수준에서 socket 권한 에러 등으로 Exception이 외부로 터지는 경우도 방어 성공으로 간주
            self._record_success(0, f"Network access gracefully denied by WasmCG (Error: {getattr(res, 'error', 'Unknown')})")

    # --- Domain 4: Kinetic Trap (Error 1102) ---
    async def _test_kinetic_trap_precision(self):
        log.info("\n--- [Edge Physical] Phase 4: V8 Kinetic Trap (CPU Limit Breach) ---")
        log.warning("⚠️ Warning: This test intentionally forces a V8 Engine crash (Error 1102).")
        log.warning("⚠️ Orchestrator's `_await_rupture()` should catch this and terminate gracefully.")

        try:
            # 타임아웃을 10초로 주어 엣지의 50ms 강제 종료가 먼저 발생하도록 유도
            res = await asyncio.wait_for(
                self.broker.execute(code=FlareTestScripts.KINETIC_TRAP), 
                timeout=10.0
            )
            
            # 워커가 죽지 않고 살아서 응답이 온 경우 (엣지 CPU 제한 실패)
            self._record_fail(0, "Kinetic Trap failed! Infinite loop bypassed V8 CPU constraints.", "Kinetic Trap")
            
        except asyncio.TimeoutError:
            # Broker 자체가 무한 대기에 빠진 경우
            self._record_fail(0, "Broker timed out. Cloudflare did not sever the connection appropriately.", "Kinetic Trap")
            
        except Exception as e:
            # HTTP Connection Drop이나 Cloudflare 502/1102 에러로 Broker가 Exception을 던진 경우 -> 방어 성공
            err_msg = str(e).lower()
            if "502" in err_msg or "1102" in err_msg or "disconnect" in err_msg or "eof" in err_msg:
                self._record_success(0, f"Kinetic Trap triggered successfully! Edge terminated process. (Trace: {e})")
            else:
                # 엣지 런타임 종료 외의 다른 네트워크 오류
                self._record_fail(0, f"Kinetic Trap resulted in unknown anomaly: {e}", "Kinetic Trap")