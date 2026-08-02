# dphi.net.exchange.server
## @lineage: dphi.wasm.exchange
import time
import json
import hashlib
import asyncio
import abc
from dataclasses import dataclass, field
from typing import Dict, Any, List

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from arch.topos.network.bridge import FlowPropagator, RpcBridge
from arch.topos.network.channel.pipeline import DuplexChannel, ChannelContext
from arch.topos.network.channel.codec import JsonMessageCodec, XelogUniversalTracer
from arch.topos.network.factory import ProtocolFactory

from watcher.dphi.adapter.exchange import ExchangeAdapter
from watcher.dphi.adapter.state import StateAdapter
from watcher.dphi.adapter.eco import EcoAdapter
from dphi.adapter.wallet import WalletAdapter
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("xelog.exchange")

class NodeIdentity:
    """참여자의 암호화 키 생성 및 서명 책임을 분리한 클래스"""
    def __init__(self):
        self.key = ed25519.Ed25519PrivateKey.generate()
        self.pub_hex = self.key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    def sign(self, canonical_bytes: bytes) -> str:
        return self.key.sign(hashlib.sha256(canonical_bytes).digest()).hex()


@dataclass
class ExchangeContext:
    rpc_bridge: RpcBridge
    agent_a: NodeIdentity
    agent_b: NodeIdentity
    field_node: NodeIdentity
    wallet_adapter: WalletAdapter
    
    # 워크플로우 진행 중 채워질 데이터들
    phases: Dict[str, Any] = field(default_factory=dict)
    entangled_state: Dict[str, Any] = field(default_factory=dict)
    signatures: List[str] = field(default_factory=list)
    x402_receipt: Any = None
    receipt: Any = None

class ExchangePhase(abc.ABC):
    @abc.abstractmethod
    async def execute(self, ctx: ExchangeContext):
        pass

class GatewayIngressPhase(ExchangePhase):
    """[Step 1] Gateway Ingress: 양측 에이전트의 의도(Intent)를 서버에 제출 및 검증"""
    async def execute(self, ctx: ExchangeContext):
        log.info("--- [Gateway Ingress] Validating intents from Agents... ---")
        ctx.phases['a'] = await self._ingress(ctx.rpc_bridge, ctx.agent_a.pub_hex, "offer_tokenX_for_tokenY")
        ctx.phases['b'] = await self._ingress(ctx.rpc_bridge, ctx.agent_b.pub_hex, "offer_tokenY_for_tokenX")

    async def _ingress(self, rpc_bridge: RpcBridge, agent_pub: str, intent_action: str) -> dict:
        log.info(f"  └─ Validating intent from {agent_pub[:8]}...")
        payload = {
            "action": "init_epoch",
            "topo": 101,
            "press": 5,
            "injected_intent": {"agent_id": agent_pub, "action": intent_action}
        }
        res = await rpc_bridge.request(payload)
        if res.get("status") != 200:
            raise ValueError(f"Ingress Failed: {res.get('error')}")
            
        parity_triplet = res["data"]
        log.info(f"  └─ [Ingress Validated] Phase ID: {parity_triplet['phase_id']}")
        return parity_triplet

class EntanglementPhase(ExchangePhase):
    """[Step 2] Matching Engine: 두 에이전트의 상태를 결합(Entangle)"""
    async def execute(self, ctx: ExchangeContext):
        log.info("\n--- [Matching Engine] Binding Execution State A (Bid) and State B (Ask) ---")
        phase_a = ctx.phases['a']
        phase_b = ctx.phases['b']
        
        unified_topos = f"clearing_batch_{int(time.time())}"
        unified_phase = phase_a["phase_id"] ^ phase_b["phase_id"]
        
        ctx.entangled_state = {
            "repos": {
                "participant_a": phase_a["phase_id"],
                "participant_b": phase_b["phase_id"],
                "field_status": "matched_fully_filled"
            },
            "parity": StateAdapter.build_parity_triplet(
                topos_id=unified_topos,
                phase_id=unified_phase,
                nexus_id=777777
            )
        }
        log.info("  └─ [Matched] Opposite intents paired successfully. Imbalance = 0.")

class PaymentSettlementPhase(ExchangePhase):
    """[Step 2.5] x402 Micropayment: 매칭된 상태에 대해 비용을 청구하고 온체인 정산"""
    async def execute(self, ctx: ExchangeContext):
        log.info("\n--- [Micropayment] Processing x402 Settlement via Base Network ---")
        
        # 1. 서버(Field Node)가 청구서(Invoice) 발행
        invoice = EcoAdapter.build_x402_invoice(
            payee_address="0xFieldNodeTreasury", 
            amount_usdc="0.05", 
            resource_id="exchange_matching_fee"
        )
        
        # 2. Agent A(구매자)가 WalletAdapter를 통해 온체인/시뮬레이션 결제
        ctx.x402_receipt = EcoAdapter.process_x402_settlement(
            invoice=invoice,
            agent_wallet_address="0xAgentAWallet",
            wallet_adapter=ctx.wallet_adapter
        )
        
        log.info(f"  └─ [Paid] Amount: {invoice['amount_usdc']} USDC, Tx Hash: {ctx.x402_receipt['tx_hash']}")

class NexusCollapsePhase(ExchangePhase):
    """[Step 3] Settlement Commit: 3-of-3 다중 서명을 통한 상태 확정 및 서버 제출"""
    async def execute(self, ctx: ExchangeContext):
        log.info("\n--- [Trade Settlement] Finalizing clearing via 3-of-3 Multi-sig Consensus ---")
        
        # 결제 영수증을 상태 트리에 병합
        cached_states = {"x402_tx": ctx.x402_receipt} if ctx.x402_receipt else {}
        
        parity = ctx.entangled_state["parity"]
        canonical_bytes = StateAdapter.to_canonical_bytes(
            StateAdapter.build_anchor_commit(
                parity=parity, parent_nexus_id=0, parent_commit_id="genesis",
                repos=ctx.entangled_state["repos"], cached_states=cached_states
            )
        )
        
        # 3-of-3 Multi-sig 생성 (Agent A, Agent B, Field Node)
        ctx.signatures = [
            ctx.agent_a.sign(canonical_bytes),
            ctx.agent_b.sign(canonical_bytes),
            ctx.field_node.sign(canonical_bytes)
        ]
        
        signers = [ctx.agent_a.pub_hex, ctx.agent_b.pub_hex, ctx.field_node.pub_hex]
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state="genesis",
            repos=ctx.entangled_state["repos"], cached_states=cached_states, timestamp=time.time(),
            signers=signers, signatures=ctx.signatures, threshold=3, allowed_signers=signers
        )
        
        # 서버(Nexus)에 검증 및 봉인(Seal) 요청
        res = await ctx.rpc_bridge.request({
            "action": "seal_epoch",
            "payload": seal_payload
        })
        if res.get("status") != 200:
            raise ValueError(f"Settlement Failed: {res.get('error')}")
            
        log.info("  └─ [Settlement Completed] 3-of-3 Multi-sig State Committed to Nexus.")

class FinalizeExchangePhase(ExchangePhase):
    """[Step 4] Finalize: 최종 영수증 발행 및 외부(Rollup) 전송용 페이로드 생성"""
    def __init__(self, exchange_adapter: ExchangeAdapter):
        self.exchange_adapter = exchange_adapter

    async def execute(self, ctx: ExchangeContext):
        ctx.receipt = self.exchange_adapter.finalize_settlement(
            entangled_state=ctx.entangled_state,
            signatures=ctx.signatures,
            cost_metrics={"fuel_consumed": 35000},
            tier="SYSTEM"
        )
        
        external_payload = self.exchange_adapter.generate_settlement_payload(ctx.receipt)
        log.info(f"\n[Exchange Ready] Payload for External Network (Rollup Sequencer):")
        log.info(json.dumps(external_payload, indent=2))

class ExchangeNet:
    def __init__(self, simulate_wallet: bool = True):
        # 중앙 관리용 Field Node 키 생성
        self.field_node = NodeIdentity()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field_node.pub_hex)
        
        # WalletAdapter 초기화
        self.wallet_adapter = WalletAdapter(network_id="base-sepolia", simulate=simulate_wallet)
        if not self.wallet_adapter.simulate:
            self.wallet_adapter.fund_wallet()
        
        # 비즈니스 워크플로우 정의 (순서대로 실행됨)
        self.workflow = [
            GatewayIngressPhase(),
            EntanglementPhase(),
            PaymentSettlementPhase(),  # x402 결제 단계 추가
            NexusCollapsePhase(),
            FinalizeExchangePhase(self.exchange_adapter)
        ]

    async def execute(self, host: str, port: int) -> bool:
        """단일 파이프라인 세션 안에서 구성된 워크플로우를 모두 관통합니다."""
        with flow_scope(phase="EXCHANGE_E2E"):
            log.info("\n=== [START] P2P Order Ingress & Deterministic Settlement (PhiNet) ===")
            
            # 1. 네트워크 계층 구성 (RPC Bridge 등)
            rpc_bridge = RpcBridge()
            bootstrap = ProtocolFactory() \
                .child_handler(lambda: FlowPropagator("EXCHANGE_AGENT")) \
                .child_handler(lambda: JsonMessageCodec()) \
                .child_handler(lambda: XelogUniversalTracer()) \
                .child_handler(lambda: rpc_bridge)
                
            protocol = await bootstrap.connect(host, port)
            
            # 파이프라인의 channel_active 이벤트가 끝까지 전파될 시간을 미세하게 확보
            await asyncio.sleep(0.1) 
            
            # 2. 거래 세션 상태(Context) 초기화
            ctx = ExchangeContext(
                rpc_bridge=rpc_bridge,
                agent_a=NodeIdentity(),
                agent_b=NodeIdentity(),
                field_node=self.field_node,
                wallet_adapter=self.wallet_adapter
            )

            try:
                # 3. 워크플로우 순차 실행
                for phase in self.workflow:
                    await phase.execute(ctx)
                    
                log.info("[SUCCESS] Exchange Scenario Completed over PhiNet Pipeline.")
                return True
                
            except Exception as e:
                log.exception(f"[FAIL] Exchange Scenario aborted: {e}")
                return False
                
            finally:
                # 4. 리소스 정리
                if protocol and protocol.pipeline and protocol.pipeline.transport:
                    protocol.pipeline.transport.close()

class ExchangeServer(DuplexChannel):
    """A2A, Exchange, Ledger의 모든 요청을 받아주는 통합 백엔드"""
    async def channel_read(self, ctx: ChannelContext, msg: dict):
        if not isinstance(msg, dict): return
        
        action = msg.get("action")
        req_id = msg.get("_req_id") # Correlation ID 유지
        
        response = {"_req_id": req_id, "status": 200, "data": {}}
        
        try:
            # 1. Exchange Scenarios
            if action == "init_epoch":
                response["data"] = {"topos_id": msg.get("topo"), "phase_id": 9999}
            elif action == "seal_epoch":
                # 다중 서명 임계값(Threshold) 검증 로직 시뮬레이션
                if len(msg.get("signatures", [])) < msg.get("threshold", 3):
                    response["status"] = 403
                    response["error"] = "Insufficient Signatures"
                else:
                    response["data"] = {"nexus_id": "committed_hash_123"}
                    
            # 2. A2A Scenarios
            elif action == "validate_intent":
                response["data"] = {"validation": "passed"}
            elif action == "generate_proof":
                response["data"] = {"proof": "zk_snark_dummy_proof"}
                
            else:
                response["status"] = 404
                response["error"] = "Unknown Action"
                
        except Exception as e:
            response["status"] = 500
            response["error"] = str(e)
            
        # 파이프라인을 역류하여(Codec 통과) 클라이언트에게 응답 전송
        await ctx.fire_write(response)