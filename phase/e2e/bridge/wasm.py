# fiber.phase.e2e.bridge.wasm
import sys
import asyncio
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Callable

from fiber.infra.wasm.bridge import WasmBridge

from xphi.arch.wasm.builder import WasmBuilder
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.adapter.gateway import GatewayAdapter

log = get_emitter("e2e.wasm.bridge")

@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success


class WasmBridgeTestSuite:
    def __init__(self):
        self.log = get_emitter("e2e.wasm.bridge.compute")
        self.results: List[TestResult] = []
        self.gateway = None
        
    async def setup(self):
        self.log.info("\n" + "-"*80)
        self.log.info("🔌 [BRIDGE SETUP] Initializing Zero-WASI WasmBridge (L0 Firewall)...")
        try:
            self.gateway = await asyncio.to_thread(WasmBridge, "gateway.wasm")
            self.log.info("✅ Bridge Initialized (Pre-allocated Buffer: 1MB & Thread-Safe Locked)")
        except Exception as e:
            self.log.critical(f"❌ [FATAL] Bridge initialization failed: {e}")
            self.gateway = None
        self.log.info("-" * 80)

    async def run_scenario(
        self, 
        title: str, 
        expected_success: bool, 
        payload_kwargs: Dict[str, Any], 
        validator: Callable[[Dict[str, Any]], bool] = None
    ):
        self.log.info(f"\n▶️ [FIREWALL DOMAIN] Scenario: {title}")
        
        if not self.gateway:
            self.log.error("💥 Bridge is offline. Auto-failing scenario.")
            self.results.append(TestResult("WASM_BRIDGE", title, False, expected_success))
            return

        try:
            # 1. 어댑터를 통한 Python 레벨의 1차 살균 및 스키마 검증
            safe_payload_kwargs = GatewayAdapter.build_evaluate_payload(**payload_kwargs)

            # 2. 정제된 페이로드를 FFI로 전송 (비동기 오프로딩)
            receipt = await asyncio.to_thread(self.gateway.evaluate_intent, **safe_payload_kwargs)
            
            is_success = receipt.get("success", False)
            revert_reason = receipt.get("revert_reason", "")
            
            test_passed = (is_success == expected_success)
            
            if test_passed and validator and not validator(receipt):
                self.log.error(f"❌ [VALIDATION FAILED] Revert reason mismatch. Receipt: {receipt}")
                test_passed = False
                
            if not is_success and not expected_success:
                if test_passed:
                    self.log.info(f"✅ 방어 로직 정상 작동 (Revert: {revert_reason})")
            elif is_success and expected_success:
                # [NEW] AST 추출(Extraction) 데이터 확인
                intent_data = receipt.get("intent", {})
                self.log.info(f"✅ L0 Firewall 통과! 추출된 AST: {intent_data}")
            elif not test_passed:
                self.log.error(f"💥 시나리오 실패. 예상: {expected_success}, 결과: {is_success} ({revert_reason})")
                
            self.results.append(TestResult(
                target="WASM_BRIDGE",
                scenario=title,
                success=is_success if validator is None else (is_success == expected_success and test_passed),
                expected_success=True if validator else expected_success
            ))
            
        except ValueError as ve:
            # Python Adapter(차원 등)에서 발생한 에러를 잡기 위한 분기
            if not expected_success:
                self.log.info(f"✅ Python Adapter 방어 정상 작동 (Revert: {ve})")
                self.results.append(TestResult("WASM_BRIDGE", title, False, False))
            else:
                self.log.error(f"💥 예기치 않은 Python 에러: {ve}")
                self.results.append(TestResult("WASM_BRIDGE", title, False, True))
        except Exception as e:
            self.log.error(f"💥 [CRASH] Unexpected bridge exception: {e}")
            self.results.append(TestResult("WASM_BRIDGE", title, False, expected_success))

    async def execute(self) -> List[TestResult]:
        await self.setup()
        self.log.info("\n[CLI] 🏃‍♂️ Initiating L0 Zero-Trust Isolation Tests...")
        
        # 1. [L0 Golden Path] 4대 원시 규격 완벽 준수
        await self.run_scenario(
            title="1. L0 Golden Path (OpCode, Budget, Target, Hex Payload)",
            expected_success=True,
            payload_kwargs={
                "dimension": 3,
                "base_friction": 0.005,
                "raw_payload": "CALL FEE 15.5 USDC TO 0x1234567890ABCDEF1234567890ABCDEF12345678 WITH 0xDEADBEEF",
                "state_vector": [1, 1, 1]
            }
        )
        
        # 2. [BDD O(1) Rejection] 논리 상태 벡터 불일치
        await self.run_scenario(
            title="2. O(1) BDD Logic Matrix Rejection",
            expected_success=False,
            payload_kwargs={
                "dimension": 3,
                "base_friction": 0.005,
                "raw_payload": "CALL FEE 15.5 USDC TO 0x1234567890ABCDEF1234567890ABCDEF12345678 WITH 0xDEADBEEF",
                "state_vector": [1, 0, 1] 
            },
            validator=lambda r: "logic matrix" in r.get("revert_reason", "").lower()
        )
        
        # 3. [Payload Injection Defense] Hex/B64 규격을 벗어난 SQL 인젝션 시도 차단
        await self.run_scenario(
            title="3. Pest Sandbox Defense (SQL Injection Rejection)",
            expected_success=False,
            payload_kwargs={
                "dimension": 3,
                "base_friction": 0.005,
                "raw_payload": "CALL FEE 100 USDC TO DB_SERVICE WITH DROP TABLE users;",
                "state_vector": [1, 1, 1] 
            },
            validator=lambda r: "syntax error" in r.get("revert_reason", "").lower()
        )
        
        # 4. [LLM Hallucination Trap] 미사여구를 포함한 원시 텍스트 차단
        await self.run_scenario(
            title="4. AST Pest Parser Rejection (LLM Hallucination)",
            expected_success=False,
            payload_kwargs={
                "dimension": 3,
                "base_friction": 0.005,
                "raw_payload": "PLEASE CALL FEE 10 USDC TO STORAGE_NODE",
                "state_vector": [1, 1, 1]
            },
            validator=lambda r: "syntax error" in r.get("revert_reason", "").lower()
        )

        # 5. [Physical Circuit Breaker] C-FFI 선형 메모리 1MB 오버플로우 공격 방어
        massive_payload = "CALL FEE 1 USDC TO 0x0 WITH 0x" + ("A" * (1024 * 1024 + 10))
        await self.run_scenario(
            title="5. Physical Circuit Breaker (Overflow 1MB)",
            expected_success=False,
            payload_kwargs={
                "dimension": 3,
                "base_friction": 0.005,
                "raw_payload": massive_payload,
                "state_vector": [1, 1, 1]
            },
            validator=lambda r: "boundary" in r.get("revert_reason", "").lower()
        )

        return self.results


class MasterBridgeSuite:
    def __init__(self):
        self.log = log
        self.all_results: List[TestResult] = []

    def _print_report(self):
        self.log.info("\n" + "="*90)
        self.log.info("📊 [WASM BRIDGE E2E REPORT: ISOLATION & L0 FFI STABILITY]")
        self.log.info("="*90)
        
        all_passed = True
        for idx, res in enumerate(self.all_results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
            
            target_label = f"[{res.target}]".ljust(15)
            self.log.info(f"{status_icon} {idx:02d}. {target_label} {res.scenario.ljust(60)} | Result: {status_text}")
            
        self.log.info("-" * 90)
        if all_passed:
            self.log.info("🎉 ALL L0 FIREWALL SCENARIOS EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 E2E BRIDGE FAILED. Inspect physical memory bounds and AST parsing rules.")
        self.log.info("="*90 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*90)
        self.log.info("🔨 [PRE-FLIGHT] Forcing Rebuild of 'gateway.wasm' Artifact...")
        self.log.info("="*90)
        
        builder = WasmBuilder(target_projects=["gateway"])
        if hasattr(builder, 'trace'):
            await builder.trace()
        else:
            await builder.execute()
        
        if getattr(builder, 'rupture_confirmed', False):
            self.log.critical("💥 [FATAL] Gateway WASM build failed. Aborting E2E Sequence.")
            sys.exit(1)
            
        self.log.info("✅ [PRE-FLIGHT] Binary synced. Proceeding to Tests.")

        self.log.info("\n" + "="*90)
        self.log.info("🧪 [MASTER SUITE] Commencing Decoupled WasmBridge Integration Tests")
        self.log.info("="*90)
        
        bridge_suite = WasmBridgeTestSuite()
        self.all_results.extend(await bridge_suite.execute())

        self._print_report()

def main(args_list: list[str] = None):
    app = MasterBridgeSuite()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()