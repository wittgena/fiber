# dphi.exchange.workflow
import asyncio
import hashlib
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

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
from dphi.exchange.mock.net import MockNetBuilder

log = get_emitter("exchange.workflow")

@dataclass
class ScenarioConfig:
    name: str
    # 외부에서 주입되는 동적 카오스 인젝터
    mandate_injector: Optional[Callable] = None
    signature_injector: Optional[Callable] = None

class ExStartMsg(WorkflowMessage): pass
class ExIngressMsg(WorkflowMessage): pass
class ExEntanglementMsg(WorkflowMessage): pass
class ExSettlementMsg(WorkflowMessage): pass
class ExNexusMsg(WorkflowMessage): pass

class NodeIdentity:
    def __init__(self):
        self.key = ed25519.Ed25519PrivateKey.generate()
        self.pub_hex = self.key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    def sign(self, canonical_bytes: bytes) -> str:
        return self.key.sign(hashlib.sha256(canonical_bytes).digest()).hex()

class MockRpcBridge(RpcBridge):
    """실제 통신을 대신하여 네트워크 검증 노드 역할을 수행하는 방어벽"""
    async def request(self, payload: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        action = payload.get("action")
        await asyncio.sleep(0.05)

        if action == WasmMethod.INIT_EPOCH.value:
            # 수신한 AP2 Mandate의 만료 시간 검증
            mandate_result = payload.get("mandate", {})
            actual_mandate = mandate_result.get("mandate", {})
            constraints = actual_mandate.get("constraints", {})
            
            if constraints.get("expiration_ts", 0) < int(time.time() * 1000):
                log.warning("[MockRPC] 🛑 REJECTED: AP2 Mandate is expired!")
                return {"status": 401, "error": "Unauthorized: AP2 Mandate Expired"}
                
            topo = payload.get("topo", 0)
            press = payload.get("press", 0)
            return {"status": 200, "data": {"phase_id": next_phase_id(topo=topo, press=press)}}
            
        elif action == WasmMethod.SEAL_EPOCH.value:
            # 수신한 Parity Triplet 서명의 훼손 여부 감지 (BFT)
            if "BAD_SIGNATURE" in payload.get("payload", ""):
                log.warning("[MockRPC] 🛑 REJECTED: Invalid Ed25519 Signature detected in Parity Triplet!")
                return {"status": 403, "error": "Consensus Failed: Invalid Signatures"}
                
            return {"status": 200, "data": {"receipt_id": "nexus_receipt_0x123"}}
            
        return {"status": 404, "error": f"Unknown action: {action}"}

class ExchangeWorkflow(Workflow):
    def __init__(self, scenario: ScenarioConfig, simulate_wallet: bool = True):
        super().__init__(name=f"EX_E2E [{scenario.name}]")
        self.scenario = scenario
        self.log = get_emitter(f"workflow.{scenario.name}")
        
        self.field_node = NodeIdentity()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field_node.pub_hex)
        self.wallet_adapter = WalletAdapter(network_id="base-sepolia", simulate=simulate_wallet)
        
        self.rpc_bridge: Optional[RpcBridge] = None
        self.protocol_transport = None
        
        self.agent_a = NodeIdentity()
        self.agent_b = NodeIdentity()
        
        self.phase_results = {}
        self.entangled_state = {}
        self.signatures = []
        self.ap2_mandate: Optional[Ap2MandateResult] = None
        self.x402_receipt: Optional[X402SettlementReceipt] = None
        self.economy_state: Dict[str, Any] = {}
        self.receipt: Optional[TransactionReceipt] = None
        self.rollup_payload: Optional[SettlementPayload] = None

    async def start(self) -> bool:
        self.log.info(f"\n{'='*60}\n🚀 [START] Scenario: {self.scenario.name}\n{'='*60}")
        self.rpc_bridge = MockRpcBridge()
        self.post_message(ExStartMsg())
        await self.run()
        return self.receipt is not None

    @step
    async def phase_ingress(self, msg: ExStartMsg) -> WorkflowMessage:
        # [제어의 역전] 외부 Injector가 존재하면 파괴된 데이터를 사용하고, 없으면 정상 Mock 데이터를 사용
        if self.scenario.mandate_injector:
            mandate_params = self.scenario.mandate_injector(self.agent_a.pub_hex, self.agent_a.key)
        else:
            mandate_params = MockNetBuilder.ap2_mandate_params(self.agent_a.pub_hex, self.agent_a.key)
            
        self.ap2_mandate = EcoAdapter.build_ap2_mandate(**mandate_params)

        req_payload = {
            "action": WasmMethod.INIT_EPOCH.value, 
            "topo": 120, "press": 85,
            "mandate": self.ap2_mandate.model_dump()
        }
        res_a = await self.rpc_bridge.request(req_payload)
        
        if res_a.get("status") != 200:
            return ErrorMessage(f"Ingress Rejected: {res_a.get('error')}")
            
        self.phase_results['a'] = res_a.get("data", {})
        self.phase_results['b'] = res_a.get("data", {}) 
        return ExIngressMsg()

    @step
    async def phase_entanglement(self, msg: ExIngressMsg) -> WorkflowMessage:
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
        invoice = EcoAdapter.build_x402_invoice(payee_address=self.field_node.pub_hex, amount_usdc="0.05", resource_id="compute_fee")
        self.x402_receipt = EcoAdapter.process_x402_settlement(invoice=invoice, agent_wallet_address=self.agent_a.pub_hex, wallet_adapter=self.wallet_adapter)
        
        self.economy_state = EcoAdapter.embed_economy_state(
            base_cached_states={}, mandate=self.ap2_mandate, receipt=self.x402_receipt
        )
        return ExSettlementMsg()

    @step
    async def phase_nexus(self, msg: ExSettlementMsg) -> WorkflowMessage:
        parity = self.entangled_state["parity"]
        canonical_bytes = StateAdapter.to_canonical_bytes({"parity": parity})
        self.signatures = [
            self.agent_a.sign(canonical_bytes),
            self.agent_b.sign(canonical_bytes),
            self.field_node.sign(canonical_bytes)
        ]
        
        # [제어의 역전] 외부 Injector에게 서명 배열의 조작을 위임함
        if self.scenario.signature_injector:
            self.signatures = self.scenario.signature_injector(self.signatures)
            
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state="genesis",
            repos=self.entangled_state["repos"], cached_states=self.economy_state,
            timestamp=time.time(), signers=[self.agent_a.pub_hex, self.agent_b.pub_hex, self.field_node.pub_hex],
            signatures=self.signatures, threshold=2
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
        self.receipt = self.exchange_adapter.finalize_settlement(
            entangled_state=self.entangled_state, signatures=self.signatures, 
            cost_metrics={"fuel_consumed": 35000}, tier="SYSTEM"
        )
        self.rollup_payload = self.exchange_adapter.generate_settlement_payload(self.receipt)
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"❌ [HALTED] Scenario aborted: {msg.msg}")
        return StopMessage(result=False)

    async def graceful_teardown(self):
        if self.protocol_transport:
            self.protocol_transport.close()