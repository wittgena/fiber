# phase.epoch.workflow.wallet
## @lineage: bound.exchange.wallet.workflow
import asyncio
import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, List

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from phase.anchor.config.dphi import dphi_env
from arch.topos.network.bridge import RpcBridge
from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.phase.reactor import PhaseReactor

from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.ledger.consensus import KernelLedger, ToposBlob
from kernel.bind.inter.dvm import DvmInterpreter
from watcher.plane.emitter import get_emitter

log = get_emitter("wallet.workflow")

@dataclass
class ScenarioConfig:
    name: str
    snapshot_injector: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    calldata_injector: Optional[Callable[[str], str]] = None

@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success

class SwStartMsg(WorkflowMessage): pass
class SwPrepareMsg(WorkflowMessage): pass
class SwExecuteMsg(WorkflowMessage): pass
class SwCommitMsg(WorkflowMessage): pass


class WalletChaosInjector:
    @staticmethod
    def force_insufficient_allowance(snapshot: Dict[str, Any], agent_address: str) -> Dict[str, Any]:
        """
        [지연 정산 룰 검증] 
        에이전트가 DPHI 청산소에 부여한 Allowance(위임 한도)가 부족한 상태를 
        논리적으로 스냅샷에 주입하여 DVM Revert를 강제함.
        """
        if agent_address in snapshot:
            # 잔고는 있지만 위임 한도가 없다고 논리적 마킹 (EVM Storage Slot 조작 시뮬레이션)
            snapshot[agent_address]["allowance"] = "0x0" 
        return snapshot

    @staticmethod
    def corrupt_erc20_calldata(calldata: str) -> str:
        # 정상적인 0x23b872dd 대신 잘못된 함수 선택자를 주입
        return "0xdeadbeef" + calldata[10:]


class NodeIdentity:
    def __init__(self):
        self.key = ed25519.Ed25519PrivateKey.generate()
        self.pub_hex = self.key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()
        hash_seed = hashlib.sha1(self.pub_hex.encode()).hexdigest()
        self.evm_address = f"0x{hash_seed}"


class DvmRollupBridge(RpcBridge):
    def __init__(self):
        super().__init__()
        self.log = get_emitter("rpc.bridge.dvm")

    async def request(self, payload: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        action = payload.get("action")
        await asyncio.sleep(0.05)

        if action == "shadow_execute_vm":
            self.log.info("🔬 [DVM Engine] Instantiating REVM sandbox for deterministic state derivation...")
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
                        self.log.info("✅ [DVM Engine] State derivation completed successfully.")
                        return {"status": 200, "data": json.loads(res.output)}
                    else:
                        self.log.error(f"🛑 [DVM Reverted] Execution Halted: {res.error}")
                        return {"status": 200, "data": {"success": False, "revert_reason": str(res.error)}}
            except Exception as e:
                self.log.error(f"💥 [DVM Fatality] Sandbox Engine Exception: {e}")
                return {"status": 500, "error": str(e)}
            
        return {"status": 404, "error": f"Unknown action: {action}"}


class ShadowWalletWorkflow(Workflow):
    def __init__(self, scenario: ScenarioConfig):
        super().__init__(name=f"WALLET_SHADOW [{scenario.name}]")
        self.scenario = scenario
        self.log = get_emitter(f"workflow.wallet.{uuid.uuid4().hex[:4]}")
        
        self.rpc_bridge: Optional[DvmRollupBridge] = None
        self.clearing_node = NodeIdentity()  # DPHI 청산소 시스템 계정
        self.agent_node = NodeIdentity()     # 과금을 당하는 외부 에이전트 계정
        self.ledger = KernelLedger()
        
        self.contract_address = "0x" + "c".rjust(40, "0")
        self.charge_amount = 1000 * (10 ** 6)
        
        self.active_calldata = ""
        self.active_snapshot = {}
        self.dvm_result = {}
        self.sealed_hash = None

    async def start(self) -> bool:
        self.log.info(f"\n{'='*70}\n🚀 [START] Sequence: {self.scenario.name}\n{'='*70}")
        self.rpc_bridge = DvmRollupBridge()
        self.post_message(SwStartMsg())
        await self.run()
        return self.sealed_hash is not None

    @step
    async def phase_prepare(self, msg: SwStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Deferred Charge Assembly & State Sync ---")
        
        clean_from = self.agent_node.evm_address.replace("0x", "").rjust(64, "0")
        clean_to = self.clearing_node.evm_address.replace("0x", "").rjust(64, "0")
        clean_amount = hex(self.charge_amount).replace("0x", "").rjust(64, "0")
        
        # 지연 정산을 위한 대리 수금 (transferFrom)
        base_calldata = f"0x23b872dd{clean_from}{clean_to}{clean_amount}" 
        
        self.active_calldata = self.scenario.calldata_injector(base_calldata) if self.scenario.calldata_injector else base_calldata

        # 0x23b872dd (transferFrom) 호출 시 STOP 하도록 패치된 Mock 바이트코드
        valid_mock_erc20_bytecode = "0x6080604052348015600f57600080fd5b506004361060285760003560e01c806323b872dd14602d575b600080fd5b00"

        base_snapshot = {
            self.clearing_node.evm_address: {"balance": hex(10**18), "nonce": 1},
            self.contract_address: {"balance": "0x0", "code": valid_mock_erc20_bytecode},
            self.agent_node.evm_address: {"balance": hex(self.charge_amount * 10), "allowance": "unlimited", "nonce": 0}
        }
        
        self.active_snapshot = self.scenario.snapshot_injector(base_snapshot, self.agent_node.evm_address) if self.scenario.snapshot_injector else base_snapshot
        
        return SwPrepareMsg()

    @step
    async def phase_execute(self, msg: SwPrepareMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] DVM Shadow Execution (Pull) ---")
        
        dvm_payload = {
            "vm_target": "EVM",
            "target_address": self.contract_address,
            "caller_address": self.clearing_node.evm_address, # Caller는 DPHI 마스터
            "calldata": self.active_calldata,
            "gas_limit": 150000,
            "gas_price": hex(10**9), 
            "state_snapshot": self.active_snapshot
        }
        
        res = await self.rpc_bridge.request({"action": "shadow_execute_vm", "payload": dvm_payload})
        data = res.get("data", {})
        
        if res.get("status") != 200 or not data.get("success"):
            return ErrorMessage(f"REVM Reverted: {data.get('revert_reason', 'Unknown Error')}")

        self.dvm_result = data
        return SwExecuteMsg()

    @step
    async def phase_commit(self, msg: SwExecuteMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 3] Deferred Charge Ledger Sealing ---")
        
        state_diff = self.dvm_result.get("state_diff", {})
        gas_used = self.dvm_result.get("gas_used", 0)
        
        payload_dict = {
            "caller": self.clearing_node.evm_address,
            "contract": self.contract_address,
            "state_diff": state_diff,
            "gas": gas_used,
            "timestamp": time.time()
        }
        
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        rollup_hash = hashlib.sha256(canonical_bytes).hexdigest()
        
        blob = ToposBlob(
            action="DEFERRED_SETTLEMENT_CHARGE",
            from_state="dvm.wasm.execution",
            to_state="ledger.sealed",
            tension=0.5,
            details=f"Gas: {gas_used} | Modified: {len(state_diff)}"
        )
        
        self.sealed_hash = self.ledger.save_transition(blob)
        self.log.info(f"✨ [Sealed] Deferred Charge Rollup Hash generated: 0x{rollup_hash[:16]}...")
        
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"❌ [HALTED] Pipeline execution terminated: {msg.msg}")
        return StopMessage(result=False)


class WalletDomainRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def _run_domain_workflows(self):
        self.log.info("\n▶️ [WALLET DOMAIN] Initiating Deferred Settlement Sequences...")

        scenarios = [
            {
                "config": ScenarioConfig(
                    name="Standard Deferred Charge (Successful Pull within Allowance)",
                    snapshot_injector=None,
                    calldata_injector=None
                ),
                "expected": True
            },
            {
                "config": ScenarioConfig(
                    name="State Reversion (Insufficient Allowance / Mandate Expired)",
                    snapshot_injector=WalletChaosInjector.force_insufficient_allowance,
                    calldata_injector=None
                ),
                "expected": False
            },
            {
                "config": ScenarioConfig(
                    name="VM Halt (Malformed Calldata / Invalid Opcode Injection)", 
                    snapshot_injector=None,
                    calldata_injector=WalletChaosInjector.corrupt_erc20_calldata
                ),
                "expected": False
            }
        ]

        for item in scenarios:
            scenario_config = item["config"]
            expected = item["expected"]
            
            workflow = ShadowWalletWorkflow(scenario=scenario_config)
            is_success = await workflow.start()
            
            self.results.append(TestResult(
                target="DEFERRED_CHARGE",
                scenario=scenario_config.name,
                success=is_success,
                expected_success=expected
            ))
            await asyncio.sleep(0.2)

    def _print_report(self):
        self.log.info("\n" + "="*85)
        self.log.info("📊 [DEFERRED SETTLEMENT SHADOW EXECUTION REPORT]")
        self.log.info("="*85)
        
        all_passed = True
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
                
            target_label = f"[{res.target}]"
            self.log.info(f"{status_icon} {idx:02d}. {target_label.ljust(18)} {res.scenario.ljust(60)} | Result: {status_text}")
            
        self.log.info("-" * 85)
        if all_passed:
            self.log.info("🎉 ALL DEFERRED SETTLEMENT SCENARIOS EXECUTED AS EXPECTED.")
        else:
            self.log.critical("💥 SETTLEMENT EXECUTION FAILED. Inspect structural logs for deviations.")
        self.log.info("="*85 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*85)
        self.log.info("🧪 [DPHI WALLET SUITE] Commencing Shadow Execution Reactor")
        self.log.info("="*85)
        
        await self._run_domain_workflows()
        self._print_report()

def main():
    app = WalletDomainRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()