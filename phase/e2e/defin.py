# fiber.phase.e2e.defin
import asyncio
import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, List

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from fiber.dphi.workflow.defin import (
    VmComputeConfig, CrossVmBillingWorkflow, 
    SettlementConfig, ShadowWalletWorkflow
)
from fiber.dphi.infra.adapter.dvm import DvmAdapter

from xphi.kernel.phase.network.bridge import RpcBridge
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.kernel.phase.inter.dvm import DvmInterpreter
from xphi.kernel.space.runner import SchemeRunner
from xphi.kernel.dphi.broker import DphiBroker
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.defin")

# =========================================================================
# [1] E2E 공통 유틸리티 (Identity, TestResult, Chaos Injector)
# =========================================================================

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

class WalletChaosInjector:
    @staticmethod
    def force_insufficient_allowance(snapshot: Dict[str, Any], agent_address: str, contract_address: str) -> Dict[str, Any]:
        if contract_address in snapshot:
            if "storage" not in snapshot[contract_address]:
                snapshot[contract_address]["storage"] = {}
            snapshot[contract_address]["storage"]["0x0000000000000000000000000000000000000000000000000000000000000000"] = "0x0000000000000000000000000000000000000000000000000000000000000000" 
        return snapshot

    @staticmethod
    def corrupt_erc20_calldata(calldata: str) -> str:
        return "0xdeadbeef" + calldata[10:]

# =========================================================================
# [2] Mock Infrastructure (Mock DVM Bridge)
# =========================================================================

class MockDvmRollupBridge(RpcBridge):
    def __init__(self):
        super().__init__()
        self.log = get_emitter("e2e.mock_bridge")

    async def request(self, payload: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        action = payload.get("action")
        await asyncio.sleep(0.05)

        if action == "shadow_execute_vm":
            self.log.info("🔬 [Mock Bridge] Instantiating REVM sandbox for deterministic state derivation...")
            vm_args = payload.get("payload", {})
            vm_target = vm_args.get("vm_target", "EVM")
            target_address = vm_args.get("target_address", "0x00")
            calldata = vm_args.get("calldata", "0x")
            state_snapshot = vm_args.get("state_snapshot", {})
            caller = vm_args.get("caller_address", "0x00")
            
            try:
                with DvmInterpreter(wasm_module_name="dvm.wasm") as dvm:
                    res = dvm.execute(
                        vm_target=vm_target,
                        target_address=target_address,
                        calldata=calldata,
                        state_snapshot=state_snapshot,
                        context={"caller": caller, "value": "0x0"}
                    )
                    
                    if res.success:
                        self.log.info("✅ [Mock Bridge] State derivation completed successfully.")
                        return {"status": 200, "data": json.loads(res.output)}
                    else:
                        # 💡 [핵심 개선] REVM Reverted (None) 문제 수정
                        error_detail = str(res.error)
                        try:
                            # 만약 JSON 파싱이 가능하다면 세부 revert_reason 추출
                            if res.output:
                                out_json = json.loads(res.output)
                                if "revert_reason" in out_json:
                                    error_detail = out_json["revert_reason"]
                        except Exception:
                            pass
                        
                        return {"status": 200, "data": {"success": False, "revert_reason": error_detail}}
            except Exception as e:
                self.log.error(f"💥 [Mock Bridge] Sandbox Engine Exception: {e}")
                return {"status": 500, "error": str(e)}
            
        return {"status": 404, "error": f"Unknown action: {action}"}


# =========================================================================
# [3] DePIN 과금 워크플로우 테스트 스위트 (VmComputeTestSuite)
# =========================================================================

class VmComputeTestSuite(SchemeRunner):
    def __init__(self, broker: DphiBroker):
        super().__init__(broker)
        self.log = get_emitter("e2e.defin.compute")
        self.results: List[TestResult] = []

    async def run_scenario(self, title: str, agents: int, cycles: int, cost: int, expected_success: bool = True, expected_error_match: str = None):
        self.log.info(f"\n\n{'='*80}\n🚀 [DEPIN SCENARIO] {title}\n{'='*80}")
        
        run_config = VmComputeConfig(
            concurrent_agents=agents,
            billing_cycles=cycles,
            cost_per_call=cost
        )
        
        workflow = CrossVmBillingWorkflow(config=run_config)
        success = await workflow.start()

        self.results.append(TestResult(
            target="VM_BILLING",
            scenario=title,
            success=success,
            expected_success=expected_success
        ))

        if not success and expected_error_match:
            error_output = getattr(workflow, "last_error_message", "")
            if expected_error_match not in error_output:
                self.log.warning(f"Expected error '{expected_error_match}' not found. Got: {error_output}")

    async def execute(self) -> List[TestResult]:
        self.log.info("\n[CLI] 🏃‍♂️ Initiating Cross-VM DePIN Pipeline (WASM Compute + UTXO Billing)")
        
        await self.run_scenario(
            "1. Standard API Traffic (1 Agent, 10 calls)", 
            agents=1, cycles=10, cost=50_000, 
            expected_success=True
        )
        await self.run_scenario(
            "2. Parallel Agent Inferencing (3 Agents, 10 calls each)", 
            agents=3, cycles=10, cost=20_000, 
            expected_success=True
        )
        await self.run_scenario(
            "3. Fuel Exhaustion (Circuit Breaker Test)", 
            agents=2, cycles=5, cost=20_000_000,
            expected_success=False, 
            expected_error_match="OOM or Fuel Exhaustion"
        )
        return self.results


# =========================================================================
# [4] DVM 정산 워크플로우 테스트 스위트 (SettlementTestSuite)
# =========================================================================

@dataclass
class SettlementScenario:
    name: str
    expected_success: bool
    snapshot_injector: Optional[Callable[[Dict[str, Any], str, str], Dict[str, Any]]] = None
    calldata_injector: Optional[Callable[[str], str]] = None


class SettlementTestSuite:
    def __init__(self):
        self.log = get_emitter("e2e.defin.settlement")
        self.results: List[TestResult] = []

    def build_domain_config(self, scenario: SettlementScenario) -> SettlementConfig:
        clearing_node = NodeIdentity()
        agent_node = NodeIdentity()
        contract_address = "0x" + "c".rjust(40, "0")
        charge_amount = 1000 * (10 ** 6)

        # 💡 [핵심 개선] DvmAdapter를 활용한 우아한 E2E 환경 구축 (하드코딩 제거)
        base_calldata = DvmAdapter.build_erc20_transfer_from_calldata(
            from_address=agent_node.evm_address, 
            to_address=clearing_node.evm_address, 
            amount_wei=charge_amount
        )
        active_calldata = scenario.calldata_injector(base_calldata) if scenario.calldata_injector else base_calldata

        valid_mock_erc20_bytecode = "0x6000341160165760003560e01c6323b872dd14601b575b600080fd5b6000541560165700"
        
        # DvmAdapter.build_evm_account_data 활용
        base_snapshot = {
            clearing_node.evm_address: DvmAdapter.build_evm_account_data(balance_wei=10**18, nonce=1),
            contract_address: DvmAdapter.build_evm_account_data(balance_wei=0, code_hex=valid_mock_erc20_bytecode),
            agent_node.evm_address: DvmAdapter.build_evm_account_data(balance_wei=charge_amount * 10, nonce=0)
        }
        # 테스트를 위해 컨트랙트에 허용량(Allowance) 스토리지 슬롯 주입
        base_snapshot[contract_address]["storage"] = {
            "0x0000000000000000000000000000000000000000000000000000000000000000": "0x0000000000000000000000000000000000000000000000000000000000000001"
        }
        
        active_snapshot = scenario.snapshot_injector(base_snapshot, agent_node.evm_address, contract_address) if scenario.snapshot_injector else base_snapshot

        return SettlementConfig(
            target_contract=contract_address,
            caller_address=clearing_node.evm_address,
            agent_address=agent_node.evm_address,
            charge_amount=charge_amount,
            active_calldata=active_calldata,
            active_snapshot=active_snapshot
        )

    async def execute(self) -> List[TestResult]:
        self.log.info("\n▶️ [WALLET DOMAIN] Initiating Deferred Settlement Sequences...")
        
        scenarios = [
            SettlementScenario(
                name="1. Standard Deferred Charge (Successful Pull within Allowance)",
                expected_success=True
            ),
            SettlementScenario(
                name="2. State Reversion (Insufficient Allowance / Mandate Expired)",
                expected_success=False,
                snapshot_injector=WalletChaosInjector.force_insufficient_allowance
            ),
            SettlementScenario(
                name="3. VM Halt (Malformed Calldata / Invalid Opcode Injection)", 
                expected_success=False,
                calldata_injector=WalletChaosInjector.corrupt_erc20_calldata
            )
        ]

        mock_bridge = MockDvmRollupBridge()

        for idx, s in enumerate(scenarios, 1):
            config = self.build_domain_config(s)
            workflow = ShadowWalletWorkflow(config=config, rpc_bridge=mock_bridge, log_context=f"e2e_settlement_{idx}")
            
            is_success = await workflow.start()
            
            self.results.append(TestResult(
                target="DEFERRED_CHARGE",
                scenario=s.name,
                success=is_success,
                expected_success=s.expected_success
            ))
            await asyncio.sleep(0.2)
            
        return self.results


# =========================================================================
# [5] Master Suite Executor
# =========================================================================

class MasterDefinSuite:
    def __init__(self):
        self.log = log
        self.all_results: List[TestResult] = []

    def _print_report(self):
        self.log.info("\n" + "="*90)
        self.log.info("📊 [DPHI DEFIN & SETTLEMENT E2E INTEGRATION REPORT]")
        self.log.info("="*90)
        
        all_passed = True
        for idx, res in enumerate(self.all_results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
                
            target_label = f"[{res.target}]".ljust(20)
            self.log.info(f"{status_icon} {idx:02d}. {target_label} {res.scenario.ljust(50)} | Result: {status_text}")
            
        self.log.info("-" * 90)
        if all_passed:
            self.log.info("🎉 ALL CROSS-VM DEPIN & SETTLEMENT SCENARIOS EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 E2E PIPELINE FAILED. Inspect structural logs for deviations.")
        self.log.info("="*90 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*90)
        self.log.info("🧪 [MASTER SUITE] Commencing DePIN Billing & Settlement Integration Tests")
        self.log.info("="*90)
        
        # 1. Run DePIN CosmBilling Tests
        broker = DphiBroker()
        compute_suite = VmComputeTestSuite(broker=broker)
        compute_results = await compute_suite.execute()
        self.all_results.extend(compute_results)

        # 2. Run DVM Settlement Tests
        settlement_suite = SettlementTestSuite()
        settlement_results = await settlement_suite.execute()
        self.all_results.extend(settlement_results)

        # 3. Aggregate Report
        self._print_report()


def main():
    app = MasterDefinSuite()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()