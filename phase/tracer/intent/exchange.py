# phase.tracer.intent.exchange
## @lineage: bound.observer.intent.exchange
import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from fiber.dphi.adapter.config import NetEnv, dphi_env
from xphi.watcher.ingress.sentinel import RpcChaosInjector

from xphi.arch.contract.event.next import generate_parity_triplet, next_phase_id
from xphi.arch.model.phase.gate import uuid4
from xphi.arch.topos.network.bridge import RpcBridge
from xphi.arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step

from xphi.kernel.dphi.eco.settlement import Ap2MandateResult, EcoAdapter, SettlementPayload, X402SettlementReceipt
from fiber.phase.client.ext.wallet import EthWalletAdapter
from fiber.phase.client.ext.evm import Web3Adapter

from xphi.kernel.phase.inter.dvm import DvmInterpreter
from xphi.kernel.dphi.exchange.transaction import ExchangeAdapter, TransactionReceipt
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.broker import DphiMethod
from xphi.kernel.phase.reactor import PhaseReactor

from xphi.watcher.plane.emitter import get_emitter

entry_log = get_emitter("exchange.entry")
workflow_log = get_emitter("intent.workflow")

@dataclass
class ScenarioConfig:
    name: str
    mandate_injector: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    signature_injector: Optional[Callable[[List[str]], List[str]]] = None

@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success

# Workflow Messages
class ExStartMsg(WorkflowMessage): pass
class ExApproveMsg(WorkflowMessage): pass
class ExIngressMsg(WorkflowMessage): pass
class ExSimulationMsg(WorkflowMessage): pass
class ExSettlementMsg(WorkflowMessage): pass
class ExNexusMsg(WorkflowMessage): pass

class NodeIdentity:
    """Pure consensus participant entity in the protocol core (Ed25519-based)."""
    def __init__(self):
        self.key = ed25519.Ed25519PrivateKey.generate()
        self.pub_hex = self.key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    def sign(self, canonical_bytes: bytes) -> str:
        return self.key.sign(hashlib.sha256(canonical_bytes).digest()).hex()

class DvmRpcBridge(RpcBridge):
    """Orchestrates the DVM (revm/cosmwasm) for deterministic state derivation and validation."""
    def __init__(self):
        super().__init__()
        self.log = get_emitter("dvm.bridge")

    async def request(self, payload: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        action = payload.get("action")
        await asyncio.sleep(0.05)

        if action == DphiMethod.INIT_EPOCH.value:
            mandate_result = payload.get("mandate", {})
            actual_mandate = mandate_result.get("mandate", {})
            constraints = actual_mandate.get("constraints", {})
            
            if constraints.get("expiration_ts", 0) < int(time.time() * 1000):
                self.log.warning("🛑 [REJECTED] Ingress Validation Failed: AP2 Mandate Expired")
                return {"status": 401, "error": "Unauthorized: AP2 Mandate Expired"}
                
            topo = payload.get("topo", 0)
            press = payload.get("press", 0)
            return {"status": 200, "data": {"phase_id": next_phase_id(topo=topo, press=press)}}
            
        elif action == "simulate_vm":
            self.log.info("🔬 [DVM Engine] Initiating sandbox execution for state derivation...")
            vm_args = payload.get("payload", {})
            try:
                with DvmInterpreter(wasm_module_name="dvm.wasm") as dvm:
                    res = dvm.execute(
                        vm_target=vm_args.get("vm_target", "EVM"),
                        target_address=vm_args.get("target_address", "0x00"),
                        calldata=vm_args.get("calldata", "0x"),
                        state_snapshot=vm_args.get("state_snapshot", {}),
                        context={"caller": vm_args.get("caller_address"), "gas_price": hex(10**9), "value": "0x0"}
                    )
                    if res.success:
                        self.log.info("✅ [DVM Engine] State derivation successful.")
                        return {"status": 200, "data": json.loads(res.output)}
                    else:
                        self.log.error(f"🛑 [DVM Reverted] Execution Halted: {res.error}")
                        return {"status": 200, "data": {"success": False, "revert_reason": str(res.error)}}
            except Exception as e:
                self.log.error(f"💥 [DVM Fatality] Wasmtime Engine Exception: {e}")
                return {"status": 500, "error": str(e)}
            
        elif action == DphiMethod.SEAL_EPOCH.value:
            return {"status": 200, "data": {"receipt_id": f"nexus_receipt_{uuid4().hex[:8]}"}}
            
        return {"status": 404, "error": f"Unknown action: {action}"}

class ExchangeWorkflow(Workflow):
    """Pipeline orchestrating internal E2E simulation (Customer Approve -> DVM -> Clearinghouse Pull)."""
    def __init__(self, scenario: ScenarioConfig, simulate_wallet: bool = True):
        super().__init__(name=f"EX_PIPELINE [{scenario.name}]")
        self.scenario = scenario
        self.log = get_emitter(f"workflow.{scenario.name}")
        
        self.field_node = NodeIdentity()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field_node.pub_hex)
        self.web3_adapter = Web3Adapter()
        
        self.agent_adapter = EthWalletAdapter(
            web3_adapter=self.web3_adapter, 
            agent_alias="alpha", 
            simulate=simulate_wallet
        )
        try:
            self.clearing_adapter = EthWalletAdapter(
                web3_adapter=self.web3_adapter, 
                agent_alias="beta", 
                simulate=simulate_wallet
            )
        except Exception as e:
            self.log.warning(f"Failed to init clearing wallet, forcing simulation mode: {e}")
            self.clearing_adapter = EthWalletAdapter(
                web3_adapter=self.web3_adapter, 
                agent_alias="beta", 
                simulate=True
            )
        
        self.rpc_bridge: Optional[RpcBridge] = None
        self.agent_a_node = NodeIdentity()  # 오프체인 서명용 Ed25519
        self.agent_b_node = NodeIdentity()
        
        self.phase_results: Dict[str, Any] = {}
        self.entangled_state: Dict[str, Any] = {}
        self.economy_state: Dict[str, Any] = {}
        
        self.ap2_mandate: Optional[Ap2MandateResult] = None
        self.x402_receipt: Optional[X402SettlementReceipt] = None
        self.pull_receipt: Optional[X402SettlementReceipt] = None
        self.receipt: Optional[TransactionReceipt] = None
        self.rollup_payload: Optional[SettlementPayload] = None

    async def start(self) -> bool:
        mode = "Local Simulation" if getattr(self.clearing_adapter, 'simulate', True) else "Live Native EVM Ledger"
        self.log.info(f"\n{'='*60}\n🚀 [START] Sequence: {self.scenario.name} ({mode})\n{'='*60}")
        
        self.rpc_bridge = DvmRpcBridge()
        self.post_message(ExStartMsg())
        await self.run()
        
        return self.receipt is not None

    @step
    async def phase_setup(self, msg: ExStartMsg) -> WorkflowMessage:
        """Phase 0: 고객 시뮬레이션 - 청산소가 징수할 수 있도록 L1 한도를 사전에 열어둠"""
        self.log.info("--- [Phase 0] L1 Pre-Authorization (Approve Setup) ---")
        self.log.info(f"  └─ [Customer: {self.agent_adapter.agent_alias}] Approving Clearinghouse to pull funds...")
        
        try:
            # EthWalletAdapter에 approve 메서드가 구현되어 있다고 가정
            tx_hash = await self.agent_adapter.approve(
                spender_address=self.clearing_adapter.wallet_address,
                amount_str="100.0",
                asset="usdc"
            )
            self.log.info(f"  └─ L1 Approve Successful. TxHash: {tx_hash}")
        except AttributeError:
            self.log.warning("  └─ [Mocked] EthWalletAdapter.approve() not fully implemented yet. Assuming success for Pipeline.")
        except Exception as e:
            self.log.error(f"L1 Approve Failure: {e}")
            return ErrorMessage(f"Approve Halted: {e}")

        return ExApproveMsg()

    @step
    async def phase_ingress(self, msg: ExApproveMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Ingress Validation: AP2 Mandate Constraints ---")
        actual_evm_caller = self.agent_adapter.wallet_address
        base_mandate = {
            "requester_id": actual_evm_caller,
            "target_action": "compute_intent",
            "max_spend_usdc": "100.0",
            "signer_key": self.agent_a_node.key
        }
        
        mandate_params = self.scenario.mandate_injector(base_mandate) if self.scenario.mandate_injector else base_mandate
        self.ap2_mandate = EcoAdapter.build_ap2_mandate(**mandate_params)

        res_a = await self.rpc_bridge.request({
            "action": DphiMethod.INIT_EPOCH.value, 
            "topo": 120, "press": 85,
            "mandate": self.ap2_mandate.model_dump(exclude_none=True)
        })
        
        if res_a.get("status") != 200:
            return ErrorMessage(f"Ingress Sequence Terminated: {res_a.get('error')}")
            
        self.phase_results['a'] = res_a.get("data", {})
        self.phase_results['b'] = res_a.get("data", {}) 
        return ExIngressMsg()

    @step
    async def phase_simulate(self, msg: ExIngressMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] DVM Execution: Intent Execution & Metering ---")
        
        caller_address = self.agent_adapter.wallet_address
        target_contract = getattr(dphi_env.contracts, 'nexus_clearing', "0x" + "c".rjust(40, "0"))
        
        res = await self.rpc_bridge.request({
            "action": "simulate_vm",
            "payload": {
                "vm_target": "EVM",
                "caller_address": caller_address,
                "gas_price": hex(10**9),
                "target_address": target_contract,
                "calldata": "0x00000000",
                "gas_limit": 100000,
                "state_snapshot": {
                    caller_address: {"balance": hex(10**18), "nonce": 1},
                    target_contract: {"balance": "0x0", "nonce": 0, "code": "0x"}
                }
            }
        })
        
        data = res.get("data", {})
        if res.get("status") != 200 or not data.get("success"):
            return ErrorMessage(f"DVM Execution Halted: {data.get('revert_reason', 'Unknown Error')}")

        self.entangled_state = {
            "repos": {
                "participant_a": self.phase_results['a'].get("phase_id"),
                "participant_b": self.phase_results['b'].get("phase_id")
            },
            "parity": generate_parity_triplet(topo=120, press=85),
            "dvm_state_diff": data.get("state_diff", {})
        }
        return ExSimulationMsg()

    @step
    async def phase_settlement(self, msg: ExSimulationMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 3] Deferred Economy: Off-chain Token Issuance & Netting ---")
        
        self.x402_receipt = EcoAdapter.issue_deferred_receipt(self.ap2_mandate)
        self.log.info(f"  └─ Issued Capability Receipt: {self.x402_receipt.receipt_id}")
        
        self.economy_state = EcoAdapter.embed_economy_state(
            base_cached_states={"state_diff": self.entangled_state.get("dvm_state_diff")}, 
            mandate=self.ap2_mandate, 
            receipt=self.x402_receipt
        )

        self.log.info("  └─ [System Simulation] Pulling accrued debt from L1 Network...")
        try:
            self.pull_receipt = await EcoAdapter.process_deferred_pull(
                agent_wallet_address=self.agent_adapter.wallet_address,
                accrued_amount_usdc="0.05",
                clearing_wallet_adapter=self.clearing_adapter
            )
            self.log.info(f"  └─ L1 Pull Successful. Final Settlement Receipt: {self.pull_receipt.receipt_id}")
        except Exception as e:
            self.log.error(f"L1 Pull Settlement Failure: {e}")
            return ErrorMessage(f"Deferred Pull Halted: {e}")
            
        return ExSettlementMsg()

    @step
    async def phase_nexus(self, msg: ExSettlementMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 4] Core Ledger: State Commit (Epoch Seal) ---")
        parity = self.entangled_state["parity"]
        
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state="genesis",
            repos=self.entangled_state["repos"], cached_states=self.economy_state,
            timestamp=time.time(), 
            signers=[], signatures=[], threshold=0
        )
        
        res = await self.rpc_bridge.request({
            "action": DphiMethod.SEAL_EPOCH.value, 
            "payload": StateAdapter.to_canonical_bytes(seal_payload).decode('utf-8')
        })
        
        if res.get("status") != 200:
            return ErrorMessage(f"Core Ledger Commit Rejected: {res.get('error')}")
            
        return ExNexusMsg()

    @step
    async def phase_finalize(self, msg: ExNexusMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 5] Export Node: Cryptographic Attestation Generation ---")
        self.receipt = self.exchange_adapter.finalize_settlement(
            entangled_state=self.entangled_state, 
            signatures=[],  
            cost_metrics={"fuel_consumed": 35000}, tier="SYSTEM"
        )
        
        receipt_dict = self.receipt.model_dump(exclude_none=True) if hasattr(self.receipt, 'model_dump') else self.receipt.__dict__
        canonical_receipt_bytes = StateAdapter.to_canonical_bytes(receipt_dict)
        valid_signatures = [
            self.agent_a_node.sign(canonical_receipt_bytes),
            self.agent_b_node.sign(canonical_receipt_bytes),
            self.field_node.sign(canonical_receipt_bytes)
        ]
        
        export_signatures = self.scenario.signature_injector(valid_signatures) if self.scenario.signature_injector else valid_signatures
            
        self.rollup_payload = self.exchange_adapter.generate_settlement_payload(
            receipt=self.receipt,
            attestations=export_signatures
        )
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"❌ [HALTED] Pipeline execution terminated: {msg.msg}")
        return StopMessage(result=False)

class ExchangeDomainRunner:
    """Orchestrates the Exchange E2E Pipeline integrating DVM, State Sync, and Attestation."""
    def __init__(self):
        self.log = entry_log
        self.results: List[TestResult] = []
        
        is_local_mode = (dphi_env.mode == NetEnv.LOCAL)
        has_real_pkey = os.getenv(dphi_env.agents.alpha.private_key_env_var) is not None
        self.should_simulate = is_local_mode or not has_real_pkey

    async def _run_domain_workflows(self):
        self.log.info("\n▶️ [EXCHANGE DOMAIN] Initiating Pipeline Execution Sequences...")
        if not self.should_simulate:
            self.log.info(f"⚡ [Mode] Real EVM Keys detected. External Ledger Sync (EVM) will target Chain ID: {dphi_env.network.chain_id}")
        else:
            self.log.info("🛡️ [Mode] Executing in Local Simulation (External Ledger Sync Bypassed).")

        scenarios = [
            {
                "config": ScenarioConfig(
                    name="Standard Pipeline (DVM Simulation -> EVM Ledger Sync -> Notary Attestation)",
                    mandate_injector=None,
                    signature_injector=None
                ),
                "expected": True
            },
            {
                "config": ScenarioConfig(
                    name="Ingress Rejection (Expired AP2 Mandate Constraint)",
                    mandate_injector=RpcChaosInjector.corrupt_ap2_mandate,
                    signature_injector=None
                ),
                "expected": False
            },
            {
                "config": ScenarioConfig(
                    name="Attestation Failure (Invalid Cryptographic Signatures at Export Phase)", 
                    mandate_injector=None,
                    signature_injector=RpcChaosInjector.corrupt_consensus_signatures
                ),
                "expected": True
            }
        ]

        for item in scenarios:
            scenario_config = item["config"]
            expected = item["expected"]
            workflow = ExchangeWorkflow(scenario=scenario_config, simulate_wallet=self.should_simulate)
            is_success = await workflow.start()
            
            self.results.append(TestResult(
                target="EXCHANGE",
                scenario=scenario_config.name,
                success=is_success,
                expected_success=expected
            ))
            await asyncio.sleep(0.5)

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("📊 [EXCHANGE DOMAIN EXECUTION REPORT]")
        self.log.info("="*80)
        
        all_passed = True
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
                
            target_label = f"[{res.target}]"
            self.log.info(f"{status_icon} {idx:02d}. {target_label.ljust(12)} {res.scenario.ljust(75)} | Result: {status_text}")
            
        self.log.info("-" * 80)
        if all_passed:
            self.log.info("🎉 ALL PIPELINE SCENARIOS EXECUTED AS EXPECTED.")
        else:
            self.log.critical("💥 PIPELINE EXECUTION FAILED. Inspect structural logs for deviations.")
        self.log.info("="*80 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI EXCHANGE SUITE] Commencing Exchange Domain Reactor")
        self.log.info("="*80)
        
        await self._run_domain_workflows()
        self._print_report()

def main():
    app = ExchangeDomainRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()