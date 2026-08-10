# dphi.workflow.exchange
import asyncio
import hashlib
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, List

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from phase.epoch.config.client import PhaseBuilder
from ext.client import ExtWalletClient
from phase.epoch.config.dphi import mock_env

from arch.model.phase.gate import uuid4
from arch.topos.network.bridge import RpcBridge
from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from arch.contract.event.next import next_phase_id, generate_parity_triplet

from dphi.adapter.eco import EcoAdapter, X402SettlementReceipt, Ap2MandateResult, SettlementPayload
from kernel.dphi.adapter.exchange import ExchangeAdapter, TransactionReceipt
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.broker import DphiMethod

from watcher.plane.emitter import get_emitter

log = get_emitter("shadow.workflow")

@dataclass
class ScenarioConfig:
    name: str
    mandate_injector: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    signature_injector: Optional[Callable[[List[str]], List[str]]] = None

class ExStartMsg(WorkflowMessage): pass
class ExIngressMsg(WorkflowMessage): pass
class ExEntanglementMsg(WorkflowMessage): pass
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

class MockRpcBridge(RpcBridge):
    """Acts as a defensive firewall and simulates the WASM sandbox network validation node."""
    def __init__(self):
        super().__init__()
        self.log = get_emitter("rpc.bridge.mock")

    async def request(self, payload: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        action = payload.get("action")
        await asyncio.sleep(0.05)

        if action == DphiMethod.INIT_EPOCH.value:
            mandate_result = payload.get("mandate", {})
            actual_mandate = mandate_result.get("mandate", {})
            constraints = actual_mandate.get("constraints", {})
            
            # Mandate 만료 시간 검증 (Chaos Injector에 의해 조작될 수 있음)
            if constraints.get("expiration_ts", 0) < int(time.time() * 1000):
                self.log.warning("🛑 [REJECTED] AP2 Mandate is expired!")
                return {"status": 401, "error": "Unauthorized: AP2 Mandate Expired"}
                
            topo = payload.get("topo", 0)
            press = payload.get("press", 0)
            return {"status": 200, "data": {"phase_id": next_phase_id(topo=topo, press=press)}}
            
        elif action == DphiMethod.SEAL_EPOCH.value:
            return {"status": 200, "data": {"receipt_id": f"nexus_receipt_{uuid4().hex[:8]}"}}
            
        return {"status": 404, "error": f"Unknown action: {action}"}

class ExchangeWorkflow(Workflow):
    def __init__(self, scenario: ScenarioConfig, simulate_wallet: bool = True):
        super().__init__(name=f"EX_E2E [{scenario.name}]")
        self.scenario = scenario
        self.log = get_emitter(f"workflow.{scenario.name}")
        
        self.field_node = NodeIdentity()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field_node.pub_hex)
        
        # ExtWalletClient를 통한 Edge.ext 통신
        self.wallet_client: ExtWalletClient = PhaseBuilder.get_testnet_wallet()
        
        # 동적 속성 부여 (이전의 AttributeError 방지용)
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
        mode = "Simulated" if getattr(self.wallet_client, 'simulate', True) else "Testnet Live"
        self.log.info(f"\n{'='*60}\n🚀 [START] Scenario: {self.scenario.name} ({mode})\n{'='*60}")
        
        self.rpc_bridge = MockRpcBridge()
        self.post_message(ExStartMsg())
        await self.run()
        
        return self.receipt is not None

    @step
    async def phase_ingress(self, msg: ExStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Protocol Core: Verify Intent Mandate ---")
        
        base_mandate = PhaseBuilder.ap2_mandate_params(self.agent_a.pub_hex, self.agent_a.key)
        
        if self.scenario.mandate_injector:
            mandate_params = self.scenario.mandate_injector(base_mandate)
        else:
            mandate_params = base_mandate
            
        # 블록체인 통신 없이 순수 도메인 로직으로 JSON 페이로드 생성
        self.ap2_mandate = EcoAdapter.build_ap2_mandate(**mandate_params)

        req_payload = {
            "action": DphiMethod.INIT_EPOCH.value, 
            "topo": 120, "press": 85,
            "mandate": self.ap2_mandate.model_dump(exclude_none=True)
        }
        res_a = await self.rpc_bridge.request(req_payload)
        
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
        
        payee_address = mock_env.contracts.nexus_clearing
        amount = "0.05"
        resource_id = "compute_fee"

        # 1. Edge.ext 외부 서버로 결제 위임 (API 호출)
        try:
            raw_receipt = await self.wallet_client.process_x402_payment(
                payee_address=payee_address,
                amount_usdc=amount,
                resource_id=resource_id
            )
            
            # 방어적 파싱: 응답이 {"receipt": {...}} 이거나 바로 {...} 인 경우 모두 처리
            receipt_data = raw_receipt.get("receipt") if isinstance(raw_receipt, dict) and "receipt" in raw_receipt else raw_receipt
            self.x402_receipt = X402SettlementReceipt(**receipt_data)
            
        except Exception as e:
            self.log.error(f"X402 Settlement via API failed: {e}")
            return ErrorMessage(f"Settlement failed: {e}")
        
        # 2. 결과(Receipt)를 바탕으로 순수 도메인 경제 상태 임베딩
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
            signers=[],
            signatures=[],
            threshold=0
        )
        
        res = await self.rpc_bridge.request({
            "action": DphiMethod.SEAL_EPOCH.value, 
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
        self.log.error(f"❌ [HALTED] Scenario aborted: {msg.msg}")
        return StopMessage(result=False)