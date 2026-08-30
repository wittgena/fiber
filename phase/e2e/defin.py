# fiber.phase.e2e.defin
import asyncio
import json
import uuid
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from fiber.dphi.workflow.pipeline.defin import DefinPipelineFactory
from fiber.dphi.workflow.pipeline.transaction import TransactionPipelineFactory

from xphi.kernel.phase.network.channel.pipeline import DuplexChannel, ChannelContext
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.defin")

@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success

class NodeIdentity:
    def __init__(self):
        self.key = ed25519.Ed25519PrivateKey.generate()
        self.pub_hex = self.key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()
        hash_seed = hashlib.sha1(self.pub_hex.encode()).hexdigest()
        self.evm_address = f"0x{hash_seed}"


class E2ETestSinkHandler(DuplexChannel):
    """Outbound 역방향 흐름의 종단점(Head)에서 최종 결과를 캡처"""
    def __init__(self):
        self.future = asyncio.Future()

    async def write(self, ctx: ChannelContext, msg: Any):
        if not self.future.done():
            if isinstance(msg, bytes):
                try:
                    msg = json.loads(msg.decode('utf-8').strip())
                except:
                    pass
            self.future.set_result(msg)
        
        # ✅ 최종 결과를 낚아챈 후, 네트워크 Transport로 내보내지 않고 삼킴(Swallow).
        return 


class MockE2EInfrastructure:
    class MockUtxoAdapter:
        async def execute_transaction(self, tx) -> str:
            return f"0x_mock_utxo_{uuid.uuid4().hex[:8]}"

    class MockBroker:
        async def execute(self, code, tier) -> Any:
            class MockResult:
                output = json.dumps({"success": True, "remaining_fuel": 5000})
            return MockResult()

    class E2EDvmInterpreterAdapter:
        async def execute_shadow(self, payload: dict) -> dict:
            calldata = payload.get("calldata", "0x")
            
            # 시나리오 3 대응: Calldata 훼손
            if "0xdeadbeef" in calldata:
                return {"success": False, "error": "Invalid Opcode"}
            
            # 시나리오 2 대응: 카오스 인젝터가 0x00...00으로 덮어쓴 스토리지 슬롯을 정확히 감지
            snapshot = payload.get("state_snapshot", {})
            for contract_data in snapshot.values():
                storage = contract_data.get("storage", {})
                if "0x0000000000000000000000000000000000000000000000000000000000000000" in storage.values():
                    return {"success": False, "error": "ERC20: insufficient allowance"}
                    
            return {"success": True, "data": {"state_diff": {"slot1": "0x1"}, "gas_used": 21000}}


class VmComputeTestSuite:
    def __init__(self):
        self.log = get_emitter("e2e.defin.compute")
        self.results: List[TestResult] = []

    async def run_scenario(self, title: str, agents: int, deposit: int, expected_success: bool = True, chaos_mode: str = "NORMAL"):
        self.log.info(f"\n\n{'='*80}\n🚀 [DEPIN SCENARIO] {title}\n{'='*80}")
        
        pipeline = DefinPipelineFactory.build(
            broker=MockE2EInfrastructure.MockBroker(),
            utxo_adapter=MockE2EInfrastructure.MockUtxoAdapter(),
            notary_keys=["mock_key_1"],
            chaos_mode=chaos_mode,
            concurrent_agents=agents
        )
        
        sink = E2ETestSinkHandler()
        pipeline.handlers.insert(0, sink)
        
        tenant = NodeIdentity()
        initial_payload = {
            "action": "START_COMPUTE",
            "caller_evm": tenant.evm_address,
            "signature": "valid_mock_signature",
            "deposit_usdc": deposit
        }
        raw_bytes = (json.dumps(initial_payload) + "\n").encode('utf-8')
        
        await pipeline._process_read(raw_bytes, 0)
        
        try:
            final_response = await asyncio.wait_for(sink.future, timeout=2.0)
            is_success = final_response.get("status") == "completed"
            if not is_success and not expected_success:
                self.log.info(f"✅ 방어 로직 정상 작동 (응답: {final_response})")
        except asyncio.TimeoutError:
            self.log.error("💥 Pipeline 응답 타임아웃")
            is_success = False

        self.results.append(TestResult(
            target="VM_BILLING_PIPELINE",
            scenario=title,
            success=is_success,
            expected_success=expected_success
        ))

    async def execute(self) -> List[TestResult]:
        self.log.info("\n[CLI] 🏃‍♂️ Initiating Pipeline-driven Cross-VM DePIN Tests")
        await self.run_scenario("1. Golden Path Compute (3 Agents)", agents=3, deposit=100, expected_success=True)
        await self.run_scenario("2. Negative Balance Halted by FSM", agents=3, deposit=0, expected_success=False)
        await self.run_scenario("3. Invalid EIP-712 Signature (Blocked at Edge)", agents=1, deposit=100, expected_success=False, chaos_mode="INVALID_SIGNATURE")
        return self.results


class SettlementTestSuite:
    def __init__(self):
        self.log = get_emitter("e2e.defin.settlement")
        self.results: List[TestResult] = []

    async def run_scenario(self, title: str, expected_success: bool, chaos_mode: str = "NORMAL"):
        self.log.info(f"\n▶️ [WALLET DOMAIN] Scenario: {title} (Chaos: {chaos_mode})")
        
        pipeline = TransactionPipelineFactory.build(
            dvm_adapter=MockE2EInfrastructure.E2EDvmInterpreterAdapter(),
            chaos_mode=chaos_mode
        )
        sink = E2ETestSinkHandler()
        pipeline.handlers.insert(0, sink) 
        
        clearing_node = NodeIdentity()
        
        # ✅ [수정됨] 카오스 인젝터가 타겟 컨트랙트를 정확히 찾을 수 있도록 키값을 일치시킴
        target_contract_address = "0x0000000000000000000000000000000000000000"
        raw_payload = {
            "action": "DEFERRED_CHARGE",
            "caller": clearing_node.evm_address,
            "charge_amount": 1000 * 10**6,
            "target_contract": target_contract_address,
            "calldata": "0x23b872dd00000000",
            "active_snapshot": {target_contract_address: {"balance": "0x1"}}
        }
        
        await pipeline._process_read((json.dumps(raw_payload) + "\n").encode('utf-8'), 0)
        
        try:
            res = await asyncio.wait_for(sink.future, timeout=2.0)
            is_success = res.get("status") == "completed"
            if not is_success and not expected_success:
                self.log.info(f"✅ 방어 로직 정상 작동 (응답: {res})")
        except:
            is_success = False

        self.results.append(TestResult(
            target="SETTLEMENT_PIPELINE",
            scenario=title,
            success=is_success,
            expected_success=expected_success
        ))

    async def execute(self) -> List[TestResult]:
        self.log.info("\n[CLI] 🏃‍♂️ Initiating Pipeline-driven Settlement Sequences...")
        await self.run_scenario("1. Standard Deferred Charge", expected_success=True)
        await self.run_scenario("2. State Reversion (Insufficient Allowance)", expected_success=False, chaos_mode="FORCE_INSUFFICIENT_ALLOWANCE")
        await self.run_scenario("3. VM Halt (Corrupted Calldata)", expected_success=False, chaos_mode="CORRUPT_CALLDATA")
        return self.results


class MasterDefinSuite:
    def __init__(self):
        self.log = log
        self.all_results: List[TestResult] = []

    def _print_report(self):
        self.log.info("\n" + "="*90)
        self.log.info("📊 [DPHI E2E INTEGRATION REPORT: PIPELINE ARCHITECTURE]")
        self.log.info("="*90)
        
        all_passed = True
        for idx, res in enumerate(self.all_results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
            
            target_label = f"[{res.target}]".ljust(25)
            self.log.info(f"{status_icon} {idx:02d}. {target_label} {res.scenario.ljust(50)} | Result: {status_text}")
            
        self.log.info("-" * 90)
        if all_passed:
            self.log.info("🎉 ALL PIPELINE & FSM SCENARIOS EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 E2E PIPELINE FAILED. Inspect structural logs for deviations.")
        self.log.info("="*90 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*90)
        self.log.info("🧪 [MASTER SUITE] Commencing Decoupled E2E Integration Tests")
        self.log.info("="*90)
        
        compute_suite = VmComputeTestSuite()
        self.all_results.extend(await compute_suite.execute())

        settlement_suite = SettlementTestSuite()
        self.all_results.extend(await settlement_suite.execute())

        self._print_report()

def main():
    app = MasterDefinSuite()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()