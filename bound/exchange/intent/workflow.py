# bound.exchange.intent.workflow
import asyncio
import hashlib
import json
import time
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, List

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from bound.client.local.wallet import LocalWalletClient
from phase.anchor.config.client import PhaseBuilder
from phase.anchor.config.dphi import dphi_env

from arch.model.phase.gate import uuid4
from arch.topos.network.bridge import RpcBridge
from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from arch.contract.event.next import next_phase_id, generate_parity_triplet

# 🌟 업데이트된 EcoAdapter 및 Settlement 모델 임포트
from bound.exchange.eco import EcoAdapter, X402SettlementReceipt, Ap2MandateResult, SettlementPayload
from kernel.dphi.adapter.exchange import ExchangeAdapter, TransactionReceipt
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.broker import DphiMethod
from kernel.bind.inter.dvm import DvmInterpreter
from watcher.plane.emitter import get_emitter

log = get_emitter("intent.workflow")

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
        # 🌟 지연 정산 시뮬레이션을 위한 파생 EVM 주소 추가
        hash_seed = hashlib.sha1(self.pub_hex.encode()).hexdigest()
        self.evm_address = f"0x{hash_seed}"

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
            caller_address = vm_args.get("caller_address", "0x" + "1".rjust(40, "0"))
            gas_price = vm_args.get("gas_price", hex(10**9))
            state_snapshot = vm_args.get("state_snapshot", {})
            
            try:
                with DvmInterpreter(wasm_module_name="dvm.wasm") as dvm:
                    res = dvm.execute(
                        vm_target=vm_target,
                        target_address=target_address,
                        calldata=calldata,
                        state_snapshot=state_snapshot,
                        context={"caller": caller_address, "gas_price": gas_price, "value": "0x0"}
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
        self.pull_receipt: Optional[X402SettlementReceipt] = None
        self.receipt: Optional[TransactionReceipt] = None
        self.rollup_payload: Optional[SettlementPayload] = None

    async def start(self) -> bool:
        mode = "Local Simulation" if getattr(self.wallet_client, 'simulate', True) else "Live Native EVM Ledger"
        self.log.info(f"\n{'='*60}\n🚀 [START] Sequence: {self.scenario.name} ({mode})\n{'='*60}")
        
        self.rpc_bridge = DvmRpcBridge()
        self.post_message(ExStartMsg())
        await self.run()
        
        return self.receipt is not None

    @step
    async def phase_ingress(self, msg: ExStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Ingress Validation: AP2 Mandate Constraints ---")
        
        # 🌟 지연 정산을 위해 에이전트가 오프체인 서명(Mandate) 제출
        base_mandate = {
            "requester_id": self.agent_a.evm_address,
            "target_action": "compute_intent",
            "max_spend_usdc": "100.0",
            "signer_key": self.agent_a.key
        }
        
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
        self.log.info("--- [Phase 2] DVM Execution: Intent Execution & Metering ---")
        
        caller_address = self.agent_a.evm_address
        target_contract = getattr(dphi_env.contracts, 'nexus_clearing', "0x" + "c".rjust(40, "0"))
        
        dvm_payload = {
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
        self.log.info("--- [Phase 3] Deferred Economy: Off-chain Token Issuance & Netting ---")
        
        # 🌟 1. 즉시 결제가 아닌, Mandate 기반 오프체인 역량 토큰(Capability Token) 발급
        self.x402_receipt = EcoAdapter.issue_deferred_receipt(self.ap2_mandate)
        self.log.info(f"  └─ Issued Capability Receipt: {self.x402_receipt.receipt_id} (Type: {self.x402_receipt.receipt_type})")
        
        self.economy_state = EcoAdapter.embed_economy_state(
            base_cached_states={"state_diff": self.entangled_state.get("dvm_state_diff")}, 
            mandate=self.ap2_mandate, 
            receipt=self.x402_receipt
        )

        # 🌟 2. (E2E 검증용) 백그라운드 워커의 L1 징수(Pull) 시뮬레이션
        self.log.info("  └─ [Background Worker Mock] Pulling accrued debt from L1 Network...")
        try:
            # 테스트 환경이므로 LocalWalletClient를 DPHI 청산소 시스템 지갑으로 가정하고 대리 호출
            self.pull_receipt = await EcoAdapter.process_deferred_pull(
                agent_wallet_address=self.agent_a.evm_address,
                accrued_amount_usdc="0.05",
                clearing_wallet_adapter=self.wallet_client
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