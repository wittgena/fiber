# dphi.exchange.workflow
import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, List

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from dphi.adapter.config.client import PhaseBuilder
from dphi.adapter.config.dphi import mock_env
from dphi.adapter.eco import EcoAdapter, X402SettlementReceipt, Ap2MandateResult, SettlementPayload
from dphi.adapter.local.wallet import LocalWalletClient

from arch.model.phase.gate import uuid4
from arch.topos.network.bridge import RpcBridge
from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from arch.contract.event.next import next_phase_id, generate_parity_triplet

from kernel.dphi.adapter.exchange import ExchangeAdapter, TransactionReceipt
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.broker import DphiMethod
from kernel.bind.inter.dvm import DvmInterpreter
from watcher.plane.emitter import get_emitter

log = get_emitter("exchange.workflow")

@dataclass
class ScenarioConfig:
    name: str
    mandate_injector: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    signature_injector: Optional[Callable[[List[str]], List[str]]] = None

class ExStartMsg(WorkflowMessage): pass
class ExIngressMsg(WorkflowMessage): pass
class ExSimulationMsg(WorkflowMessage): pass
class ExSettlementMsg(WorkflowMessage): pass
class ExNexusMsg(WorkflowMessage): pass

class NodeIdentity:
    """Pure consensus participant entity in the protocol core (Ed25519-based, independent of EVM)."""
    def __init__(self):
        self.key = ed25519.Ed25519PrivateKey.generate()
        self.pub_hex = self.key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    def sign(self, canonical_bytes: bytes) -> str:
        return self.key.sign(hashlib.sha256(canonical_bytes).digest()).hex()


class DvmRpcBridge(RpcBridge):
    """
    Physical entry point to the Core Node's validation engine.
    Orchestrates the DVM (revm/cosmwasm) for deterministic state derivation and validation.
    """
    def __init__(self):
        super().__init__()
        self.log = get_emitter("rpc.bridge.dvm")

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
            vm_target = vm_args.get("vm_target", "EVM")
            target_address = vm_args.get("target_address", "0x00")
            calldata = vm_args.get("calldata", "0x")
            state_snapshot = vm_args.get("state_snapshot", {})
            
            try:
                with DvmInterpreter(wasm_module_name="dvm.wasm") as dvm:
                    res = dvm.execute(
                        vm_target=vm_target,
                        target_address=target_address,
                        calldata=calldata,
                        state_snapshot=state_snapshot,
                        context={"caller": "0x_workflow_agent"}
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
    """Pipeline orchestrating validation, local simulation, ledger sync, and final attestation."""
    def __init__(self, scenario: ScenarioConfig, simulate_wallet: bool = True):
        super().__init__(name=f"EX_PIPELINE [{scenario.name}]")
        self.scenario = scenario
        self.log = get_emitter(f"workflow.{scenario.name}")
        
        self.field_node = NodeIdentity()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field_node.pub_hex)
        
        self.wallet_client: LocalWalletClient = PhaseBuilder.get_testnet_wallet()
        self.wallet_client.simulate = simulate_wallet
        
        self.rpc_bridge: Optional[RpcBridge] = None
        self.agent_a = NodeIdentity()
        self.agent_b = NodeIdentity()
        
        self.phase_results: Dict[str, Any] = {}
        self.entangled_state: Dict[str, Any] = {}
        self.economy_state: Dict[str, Any] = {}
        
        self.ap2_mandate: Optional[Ap2MandateResult] = None
        self.x402_receipt: Optional[X402SettlementReceipt] = None
        self.receipt: Optional[TransactionReceipt] = None
        self.rollup_payload: Optional[SettlementPayload] = None

    async def start(self) -> bool:
        mode = "Local Simulation" if getattr(self.wallet_client, 'simulate', True) else "Live External Ledger"
        self.log.info(f"\n{'='*60}\n🚀 [START] Sequence: {self.scenario.name} ({mode})\n{'='*60}")
        
        self.rpc_bridge = DvmRpcBridge()
        self.post_message(ExStartMsg())
        await self.run()
        
        return self.receipt is not None

    @step
    async def phase_ingress(self, msg: ExStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Ingress Validation: AP2 Mandate Constraints ---")
        
        base_mandate = PhaseBuilder.ap2_mandate_params(self.agent_a.pub_hex, self.agent_a.key)
        
        if self.scenario.mandate_injector:
            mandate_params = self.scenario.mandate_injector(base_mandate)
        else:
            mandate_params = base_mandate
            
        self.ap2_mandate = EcoAdapter.build_ap2_mandate(**mandate_params)

        req_payload = {
            "action": DphiMethod.INIT_EPOCH.value, 
            "topo": 120, "press": 85,
            "mandate": self.ap2_mandate.model_dump(exclude_none=True)
        }
        res_a = await self.rpc_bridge.request(req_payload)
        
        if res_a.get("status") != 200:
            return ErrorMessage(f"Ingress Sequence Terminated: {res_a.get('error')}")
            
        self.phase_results['a'] = res_a.get("data", {})
        self.phase_results['b'] = res_a.get("data", {}) 
        return ExIngressMsg()

    @step
    async def phase_simulate(self, msg: ExIngressMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] DVM Execution: State Differential Derivation ---")
        
        dvm_payload = {
            "vm_target": "EVM",
            "payload": {
                "target_address": mock_env.contracts.nexus_clearing,
                "calldata": "0x_x402_settlement_calldata",
                "gas_limit": 100000,
                "state_snapshot": {
                    mock_env.contracts.nexus_clearing: {
                        "balance": "0x0",
                        "nonce": 0
                    }
                }
            }
        }
        
        res = await self.rpc_bridge.request({
            "action": "simulate_vm",
            "payload": dvm_payload
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
        self.log.info("--- [Phase 3] External Plug: Ledger State Synchronization ---")
        
        payee_address = mock_env.contracts.nexus_clearing
        amount = "0.05"
        resource_id = "compute_fee"

        try:
            raw_receipt = await self.wallet_client.process_x402_payment(
                payee_address=payee_address,
                amount_usdc=amount,
                resource_id=resource_id
            )
            
            receipt_data = raw_receipt.get("receipt") if isinstance(raw_receipt, dict) and "receipt" in raw_receipt else raw_receipt
            self.x402_receipt = X402SettlementReceipt(**receipt_data)
            
        except Exception as e:
            self.log.error(f"External Ledger Sync Failure: {e}")
            return ErrorMessage(f"Ledger Sync Halted: {e}")
        
        self.economy_state = EcoAdapter.embed_economy_state(
            base_cached_states={"state_diff": self.entangled_state.get("dvm_state_diff")}, 
            mandate=self.ap2_mandate, 
            receipt=self.x402_receipt
        )
        return ExSettlementMsg()

    @step
    async def phase_nexus(self, msg: ExSettlementMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 4] Core Ledger: State Commit (Epoch Seal) ---")
        parity = self.entangled_state["parity"]
        
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state="genesis",
            repos=self.entangled_state["repos"], cached_states=self.economy_state,
            timestamp=time.time(), 
            signers=[],
            signatures=[],
            threshold=0
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
            self.agent_a.sign(canonical_receipt_bytes),
            self.agent_b.sign(canonical_receipt_bytes),
            self.field_node.sign(canonical_receipt_bytes)
        ]
        
        if self.scenario.signature_injector:
            export_signatures = self.scenario.signature_injector(valid_signatures)
        else:
            export_signatures = valid_signatures
        
        self.rollup_payload = self.exchange_adapter.generate_settlement_payload(
            receipt=self.receipt,
            attestations=export_signatures
        )
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"❌ [HALTED] Pipeline execution terminated: {msg.msg}")
        return StopMessage(result=False)