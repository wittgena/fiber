# fiber.dev.e2e.dphi.clearing
import asyncio
import json
import uuid
import hashlib
from dataclasses import dataclass
from typing import Any, List

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from fiber.dphi.eco.transaction.pipeline import ClearingPipelineFactory, TransactionPipelineFactory

from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.dphi.clearing")

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
    """Captures the final response of the outbound pipeline flow."""
    def __init__(self):
        self.future = asyncio.Future()

    async def write(self, ctx: ChannelContext, msg: Any):
        if not self.future.done():
            if isinstance(msg, bytes):
                try:
                    msg = json.loads(msg.decode('utf-8').strip())
                except json.JSONDecodeError:
                    pass
            self.future.set_result(msg)
        return 

class MockE2EInfrastructure:
    class MockPTAAdapter:
        async def execute_transaction(self, tx) -> str:
            return f"0x_mock_pta_{uuid.uuid4().hex[:8]}"

    class MockBroker:
        async def execute(self, code, tier) -> Any:
            class MockResult:
                output = json.dumps({"success": True, "remaining_fuel": 5000})
            return MockResult()

    class E2EDvmInterpreterAdapter:
        async def execute_shadow(self, payload: dict) -> dict:
            calldata = payload.get("calldata", "0x")
            
            # Scenario 3: Corrupted Calldata (Invalid Opcode)
            if "0xdeadbeef" in calldata:
                return {"success": False, "error": "Invalid Opcode"}
            
            # Scenario 2: Storage slot mutation (Insufficient allowance)
            snapshot = payload.get("state_snapshot", {})
            for contract_data in snapshot.values():
                storage = contract_data.get("storage", {})
                if "0x0000000000000000000000000000000000000000000000000000000000000000" in storage.values():
                    return {"success": False, "error": "ERC20: insufficient allowance"}
                    
            return {"success": True, "data": {"state_diff": {"slot1": "0x1"}, "gas_used": 21000}}


class VmComputeTestSuite:
    def __init__(self):
        self.log = get_emitter("e2e.clearing.compute")
        self.results: List[TestResult] = []

    async def run_scenario(self, title: str, agents: int, deposit: int, expected_success: bool = True, chaos_mode: str = "NORMAL"):
        self.log.info(f"▶ [Compute] {title} (Chaos: {chaos_mode})")
        
        pipeline = ClearingPipelineFactory.build(
            broker=MockE2EInfrastructure.MockBroker(),
            pta_adapter=MockE2EInfrastructure.MockPTAAdapter(),
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
        
        await pipeline._process_read((json.dumps(initial_payload) + "\n").encode('utf-8'), 0)
        
        try:
            res = await asyncio.wait_for(sink.future, timeout=2.0)
            is_success = res.get("status") == "completed"
            
            if not is_success and not expected_success:
                reason = res.get('reason', 'Unknown Exception')
                self.log.info(f"  └ ✅ Defense logic triggered successfully (Reason: {reason})")
        except asyncio.TimeoutError:
            self.log.error("  └ 💥 Pipeline response timeout")
            is_success = False

        self.results.append(TestResult(
            target="VM_COMPUTE",
            scenario=title,
            success=is_success,
            expected_success=expected_success
        ))

    async def execute(self) -> List[TestResult]:
        self.log.info("🏃‍♂️ Initiating Pipeline-driven Cross-VM Compute Tests")
        await self.run_scenario("1. Golden Path Compute (3 Agents)", agents=3, deposit=100, expected_success=True)
        await self.run_scenario("2. Negative Balance Halted by FSM", agents=3, deposit=0, expected_success=False)
        await self.run_scenario("3. Invalid EIP-712 Signature", agents=1, deposit=100, expected_success=False, chaos_mode="INVALID_SIGNATURE")
        return self.results


class TransactionClearingSuite:
    def __init__(self):
        self.log = get_emitter("e2e.clearing.tx")
        self.results: List[TestResult] = []

    async def run_scenario(self, title: str, expected_success: bool, chaos_mode: str = "NORMAL"):
        self.log.info(f"▶ [Transaction] {title} (Chaos: {chaos_mode})")
        
        pipeline = TransactionPipelineFactory.build(
            dvm_adapter=MockE2EInfrastructure.E2EDvmInterpreterAdapter(),
            chaos_mode=chaos_mode
        )
        sink = E2ETestSinkHandler()
        pipeline.handlers.insert(0, sink) 
        
        clearing_node = NodeIdentity()
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
                reason = res.get('reason', 'Unknown Exception')
                self.log.info(f"  └ ✅ Defense logic triggered successfully (Reason: {reason})")
        except asyncio.TimeoutError:
            self.log.error("  └ 💥 Pipeline response timeout")
            is_success = False

        self.results.append(TestResult(
            target="TX_CLEARING",
            scenario=title,
            success=is_success,
            expected_success=expected_success
        ))

    async def execute(self) -> List[TestResult]:
        self.log.info("🏃‍♂️ Initiating Pipeline-driven Transaction Clearing Tests")
        await self.run_scenario("1. Standard Deferred Charge", expected_success=True)
        await self.run_scenario("2. State Reversion (Insufficient Allowance)", expected_success=False, chaos_mode="FORCE_INSUFFICIENT_ALLOWANCE")
        await self.run_scenario("3. VM Halt (Corrupted Calldata)", expected_success=False, chaos_mode="CORRUPT_CALLDATA")
        return self.results


class MasterClearingSuite:
    def __init__(self):
        self.log = log
        self.all_results: List[TestResult] = []

    def _print_report(self):
        self.log.info("=" * 80)
        self.log.info("📊 [CLEARING PIPELINE] E2E INTEGRATION REPORT")
        self.log.info("-" * 80)
        
        all_passed = True
        for idx, res in enumerate(self.all_results, 1):
            status = "✅ PASS" if res.passed else "❌ FAIL"
            if not res.passed: all_passed = False
            
            target_label = f"[{res.target}]".ljust(15)
            self.log.info(f"{idx:02d}. {status} | {target_label} | {res.scenario}")
            
        self.log.info("-" * 80)
        if all_passed:
            self.log.info("🎉 ALL CLEARING PIPELINE & FSM SCENARIOS EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 E2E PIPELINE FAILED. Inspect structural logs for deviations.")
        self.log.info("=" * 80)

    async def execute(self):
        self.log.info("=" * 80)
        self.log.info("🧪 [MASTER SUITE] Commencing Decoupled E2E Integration Tests")
        self.log.info("=" * 80)
        
        compute_suite = VmComputeTestSuite()
        self.all_results.extend(await compute_suite.execute())

        tx_suite = TransactionClearingSuite()
        self.all_results.extend(await tx_suite.execute())

        self._print_report()


def main(args_list: list[str] = None):
    app = MasterClearingSuite()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()