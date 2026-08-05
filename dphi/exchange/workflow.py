# dphi.exchange.workflow
import asyncio
import hashlib
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from dphi.exchange.mock.net import MockNetBuilder
from dphi.exchange.mock.config import mock_env

from arch.model.phase.gate import uuid4
from arch.topos.network.bridge import FlowPropagator, RpcBridge
from arch.topos.network.channel.codec import JsonMessageCodec
from arch.topos.network.factory import ProtocolFactory
from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from arch.contract.event.next import next_phase_id, generate_parity_triplet

from kernel.dphi.adapter.eco import (
    EcoAdapter, ExchangeAdapter, TransactionReceipt, WalletAdapter, 
    X402SettlementReceipt, Ap2MandateResult, SettlementPayload
)
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.broker import WasmMethod

from watcher.plane.emitter import get_emitter

log = get_emitter("exchange.workflow")

@dataclass
class ScenarioConfig:
    name: str
    mandate_injector: Optional[Callable] = None
    signature_injector: Optional[Callable] = None

class ExStartMsg(WorkflowMessage): pass
class ExIngressMsg(WorkflowMessage): pass
class ExEntanglementMsg(WorkflowMessage): pass
class ExSettlementMsg(WorkflowMessage): pass
class ExNexusMsg(WorkflowMessage): pass

class NodeIdentity:
    """프로토콜 코어에 참여하는 순수 합의 주체 (EVM과 무관한 Ed25519 기반)"""
    def __init__(self):
        self.key = ed25519.Ed25519PrivateKey.generate()
        self.pub_hex = self.key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    def sign(self, canonical_bytes: bytes) -> str:
        return self.key.sign(hashlib.sha256(canonical_bytes).digest()).hex()

class MockRpcBridge(RpcBridge):
    """실제 통신을 대신하여 네트워크 검증 노드(WASM 샌드박스) 역할을 수행하는 방어벽"""
    async def request(self, payload: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        action = payload.get("action")
        await asyncio.sleep(0.05)

        if action == WasmMethod.INIT_EPOCH.value:
            mandate_result = payload.get("mandate", {})
            actual_mandate = mandate_result.get("mandate", {})
            constraints = actual_mandate.get("constraints", {})
            
            # Mandate 유효기간 검증
            if constraints.get("expiration_ts", 0) < int(time.time() * 1000):
                log.warning("[MockRPC] 🛑 REJECTED: AP2 Mandate is expired!")
                return {"status": 401, "error": "Unauthorized: AP2 Mandate Expired"}
                
            topo = payload.get("topo", 0)
            press = payload.get("press", 0)
            return {"status": 200, "data": {"phase_id": next_phase_id(topo=topo, press=press)}}
            
        elif action == WasmMethod.SEAL_EPOCH.value:
            # 코어는 결정론적 연산 결과가 무결한지만 확인하고 순수 영수증을 발급합니다. (BFT 서명 확인 안 함)
            return {"status": 200, "data": {"receipt_id": f"nexus_receipt_{uuid4().hex[:8]}"}}
            
        return {"status": 404, "error": f"Unknown action: {action}"}

class ExchangeWorkflow(Workflow):
    def __init__(self, scenario: ScenarioConfig, simulate_wallet: bool = True):
        super().__init__(name=f"EX_E2E [{scenario.name}]")
        self.scenario = scenario
        self.log = get_emitter(f"workflow.{scenario.name}")
        
        self.field_node = NodeIdentity()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field_node.pub_hex)
        
        # 외부 접점: 자본주의 네트워크(EVM)와 통신하기 위한 Wallet Plug-in 연결
        self.wallet_adapter = MockNetBuilder.get_testnet_wallet()
        if simulate_wallet:
            self.wallet_adapter.simulate = True
        
        self.rpc_bridge: Optional[RpcBridge] = None
        self.agent_a = NodeIdentity()
        self.agent_b = NodeIdentity()
        
        self.phase_results = {}
        self.entangled_state = {}
        self.ap2_mandate: Optional[Ap2MandateResult] = None
        self.x402_receipt: Optional[X402SettlementReceipt] = None
        self.economy_state: Dict[str, Any] = {}
        self.receipt: Optional[TransactionReceipt] = None
        self.rollup_payload: Optional[SettlementPayload] = None

    async def start(self) -> bool:
        mode = "Simulated" if self.wallet_adapter.simulate else "Testnet Live"
        self.log.info(f"\n{'='*60}\n🚀 [START] Scenario: {self.scenario.name} ({mode})\n{'='*60}")
        self.rpc_bridge = MockRpcBridge()
        self.post_message(ExStartMsg())
        await self.run()
        return self.receipt is not None

    @step
    async def phase_ingress(self, msg: ExStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Protocol Core: Verify Intent Mandate ---")
        if self.scenario.mandate_injector:
            mandate_params = self.scenario.mandate_injector(self.agent_a.pub_hex, self.agent_a.key)
        else:
            mandate_params = MockNetBuilder.ap2_mandate_params(self.agent_a.pub_hex, self.agent_a.key)
            
        self.ap2_mandate = EcoAdapter.build_ap2_mandate(**mandate_params)

        req_payload = {
            "action": WasmMethod.INIT_EPOCH.value, 
            "topo": 120, "press": 85,
            "mandate": self.ap2_mandate.model_dump(exclude_none=True)
        }
        res_a = await self.rpc_bridge.request(req_payload)
        
        # 여기서 반환된 ErrorMessage가 시스템 멈춤 없이 on_error로 가려면 on_error 구현이 필수입니다.
        if res_a.get("status") != 200:
            return ErrorMessage(f"Ingress Rejected: {res_a.get('error')}")
            
        self.phase_results['a'] = res_a.get("data", {})
        self.phase_results['b'] = res_a.get("data", {}) 
        return ExIngressMsg()

    @step
    async def phase_entanglement(self, msg: ExIngressMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] Protocol Core: State Entanglement ---")
        parity = generate_parity_triplet(topo=120, press=85)
        self.entangled_state = {
            "repos": {
                "participant_a": self.phase_results['a'].get("phase_id"),
                "participant_b": self.phase_results['b'].get("phase_id")
            },
            "parity": parity
        }
        return ExEntanglementMsg()

    @step
    async def phase_settlement(self, msg: ExEntanglementMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 3] External Plug: EVM X402 Settlement ---")
        payee_address = mock_env.settlement_target.clearing_contract_address
        payer_address = mock_env.agents.alpha.evm_address
        
        invoice = EcoAdapter.build_x402_invoice(payee_address, "0.05", "compute_fee")
        
        self.x402_receipt = EcoAdapter.process_x402_settlement(
            invoice, payer_address, self.wallet_adapter
        )
        
        self.economy_state = EcoAdapter.embed_economy_state(
            base_cached_states={}, mandate=self.ap2_mandate, receipt=self.x402_receipt
        )
        return ExSettlementMsg()

    @step
    async def phase_nexus(self, msg: ExSettlementMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 4] Protocol Core: Deterministic Epoch Collapse ---")
        parity = self.entangled_state["parity"]
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state="genesis",
            repos=self.entangled_state["repos"], cached_states=self.economy_state,
            timestamp=time.time(), 
            signers=[],    # WASM 코어 자체는 증명인을 필요로 하지 않음
            signatures=[],
            threshold=0
        )
        
        res = await self.rpc_bridge.request({
            "action": WasmMethod.SEAL_EPOCH.value, 
            "payload": StateAdapter.to_canonical_bytes(seal_payload).decode('utf-8')
        })
        
        if res.get("status") != 200:
            return ErrorMessage(f"Nexus Settlement Rejected: {res.get('error')}")
            
        return ExNexusMsg()

    @step
    async def phase_finalize(self, msg: ExNexusMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 5] Export Plug: Attest & Generate Payload ---")
        
        self.receipt = self.exchange_adapter.finalize_settlement(
            entangled_state=self.entangled_state, 
            signatures=[],  
            cost_metrics={"fuel_consumed": 35000}, tier="SYSTEM"
        )
        
        # 🌟 안정적인 직렬화(Serialization)를 통한 Canonical Bytes 생성
        receipt_dict = self.receipt.model_dump(exclude_none=True) if hasattr(self.receipt, 'model_dump') else self.receipt.__dict__
        canonical_receipt_bytes = StateAdapter.to_canonical_bytes(receipt_dict)
        
        if self.scenario.signature_injector:
            export_signatures = self.scenario.signature_injector([])
        else:
            # 외부 제출용 겉면 포장 서명(Attestation)
            export_signatures = [
                self.agent_a.sign(canonical_receipt_bytes),
                self.agent_b.sign(canonical_receipt_bytes),
                self.field_node.sign(canonical_receipt_bytes)
            ]
        
        self.rollup_payload = self.exchange_adapter.generate_settlement_payload(
            receipt=self.receipt,
            attestations=export_signatures
        )
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"❌ [HALTED] Scenario aborted: {msg.msg}")
        return StopMessage(result=False)