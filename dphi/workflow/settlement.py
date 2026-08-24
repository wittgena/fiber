# dphi.workflow.settlement
import asyncio
import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, List

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from fiber.dphi.adapter.config import dphi_env
from xphi.kernel.space.topos.network.bridge import RpcBridge
from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.phase.reactor import PhaseReactor

from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.ledger.consensus import KernelLedger, ToposBlob
from xphi.kernel.phase.inter.dvm import DvmInterpreter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("workflow.settlement")

@dataclass
class ScenarioConfig:
    name: str
    snapshot_injector: Optional[Callable[[Dict[str, Any], str, str], Dict[str, Any]]] = None
    calldata_injector: Optional[Callable[[str], str]] = None
    is_negative_path: bool = False

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
    def force_insufficient_allowance(snapshot: Dict[str, Any], agent_address: str, contract_address: str) -> Dict[str, Any]:
        if contract_address in snapshot:
            if "storage" not in snapshot[contract_address]:
                snapshot[contract_address]["storage"] = {}
            snapshot[contract_address]["storage"]["0x0000000000000000000000000000000000000000000000000000000000000000"] = "0x0000000000000000000000000000000000000000000000000000000000000000" 
        return snapshot

    @staticmethod
    def corrupt_erc20_calldata(calldata: str) -> str:
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
        self.clearing_node = NodeIdentity()  
        self.agent_node = NodeIdentity()     
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
        
        base_calldata = f"0x23b872dd{clean_from}{clean_to}{clean_amount}" 
        self.active_calldata = self.scenario.calldata_injector(base_calldata) if self.scenario.calldata_injector else base_calldata

        valid_mock_erc20_bytecode = "0x6000341160165760003560e01c6323b872dd14601b575b600080fd5b6000541560165700"
        base_snapshot = {
            self.clearing_node.evm_address: {"balance": hex(10**18), "nonce": 1},
            self.contract_address: {
                "balance": "0x0", 
                "code": valid_mock_erc20_bytecode,
                "storage": {
                    "0x0000000000000000000000000000000000000000000000000000000000000000": "0x0000000000000000000000000000000000000000000000000000000000000001"
                }
            },
            self.agent_node.evm_address: {"balance": hex(self.charge_amount * 10), "nonce": 0}
        }
        
        if self.scenario.snapshot_injector:
            self.active_snapshot = self.scenario.snapshot_injector(base_snapshot, self.agent_node.evm_address, self.contract_address)
        else:
            self.active_snapshot = base_snapshot
        
        func_sig = self.active_calldata[:10]
        self.log.info(
            f"  └─ 🧩 Assembled DVM Payload:\n"
            f"     ├─ 🎯 Target Contract : {self.contract_address}\n"
            f"     ├─ 👤 Caller Node     : {self.clearing_node.evm_address}\n"
            f"     └─ 📦 Func Signature  : {func_sig}"
        )
        return SwPrepareMsg()

    @step
    async def phase_execute(self, msg: SwPrepareMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] DVM Shadow Execution (Pull) ---")
        
        dvm_payload = {
            "vm_target": "EVM",
            "target_address": self.contract_address,
            "caller_address": self.clearing_node.evm_address,
            "calldata": self.active_calldata,
            "gas_limit": 150000,
            "gas_price": hex(10**9), 
            "state_snapshot": self.active_snapshot
        }
        
        res = await self.rpc_bridge.request({"action": "shadow_execute_vm", "payload": dvm_payload})
        data = res.get("data", {})
        
        if res.get("status") != 200 or not data.get("success"):
            revert_reason = data.get('revert_reason', 'Unknown Error')
            return ErrorMessage(f"REVM Reverted ({revert_reason})")

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
        receipt = (
            f"\n    🧾 [SHADOW SETTLEMENT RECEIPT]\n"
            f"     ├─ ⛽ EVM Gas Used   : {gas_used} Units\n"
            f"     ├─ 🔄 State Mutations: {len(state_diff)} Storage slot(s) modified\n"
            f"     └─ 💎 L2 Rollup Hash : 0x{rollup_hash[:16]}..."
        )
        self.log.info(receipt)
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        if self.scenario.is_negative_path:
            self.log.info(f"  └─ 🛡️ Defense Triggered: Shadow execution correctly halted. ({msg.msg})")
        else:
            self.log.error(f"  └─ ❌ [HALTED] Pipeline execution terminated: {msg.msg}")
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
                    calldata_injector=None,
                    is_negative_path=False
                ),
                "expected": True
            },
            {
                "config": ScenarioConfig(
                    name="State Reversion (Insufficient Allowance / Mandate Expired)",
                    snapshot_injector=WalletChaosInjector.force_insufficient_allowance,
                    calldata_injector=None,
                    is_negative_path=True
                ),
                "expected": False
            },
            {
                "config": ScenarioConfig(
                    name="VM Halt (Malformed Calldata / Invalid Opcode Injection)", 
                    snapshot_injector=None,
                    calldata_injector=WalletChaosInjector.corrupt_erc20_calldata,
                    is_negative_path=True
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
        self.log.info("🧪 [DPHI WORKFLOW SETTLEMENT] Commencing Shadow Execution Reactor")
        self.log.info("="*85)
        
        await self._run_domain_workflows()
        self._print_report()

def main():
    app = WalletDomainRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()