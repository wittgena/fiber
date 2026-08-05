# dphi.exchange.mock.net
import time
import uuid
from typing import Any, Dict

from cryptography.hazmat.primitives.asymmetric import ed25519

from arch.contract.model.receptor import (
    TradeIngressRequest,
    AnchorProposalRequest,
    ParityTripletSchema
)
from watcher.receptor.contract.otlp import ExportLogsServiceRequest
from watcher.receptor.edge.core import StreamAppendRequest, LedgerEventSchema

class MockNetBuilder:
    __domain_metadata__ = {
        "otlp_payload": "OTel + Datadog/LangSmith. Tracks LLM GenAI metrics (tokens/latency) for billing.",
        "trade_intent": "W3C DID + UniswapX/Fetch.ai. Intent-centric A2A (Agent-to-Agent) resource swap with slippage.",
        "ledger_append": "Celestia/EigenDA + RISC Zero. Immutable DA (Data Availability) & ZK-verifiable compute logs.",
        "anchor_proposal": "Ethereum L2 (OP Stack) Sequencer. Rollup of state roots (Merkle Parity) for global consensus.",
        "ap2_mandate": "Agent-to-Agent(A2A) Authorization. Verifiable Credential for delegated spending limits."
    }

    @staticmethod
    def ap2_mandate_params(
        agent_pub_hex: str, 
        agent_key: ed25519.Ed25519PrivateKey,
        target_action: str = "M2M_INFERENCE_SWAP",
        max_spend_usdc: str = "0.50",
        is_expired: bool = False  # [개선] 만료된 권한 테스트용 플래그
    ) -> Dict[str, Any]:
        """@desc: AP2 Mandate 발급 파라미터 동적 생성"""
        return {
            "requester_id": agent_pub_hex,
            "target_action": target_action,
            "max_spend_usdc": max_spend_usdc,
            "signer_key": agent_key,
            # 만료 테스트 시 validity_ms를 과거 시간(-1시간)으로 조작
            "validity_ms": -3600000 if is_expired else 3600000
        }

    @staticmethod
    def otlp_payload(
        model_name: str = "gemini-1.5-pro", 
        prompt_tokens: int = 128450,
        completion_tokens: int = 2048,
        latency_ms: int = 1450,
        is_malformed: bool = False # [개선] OTLP 스키마 파괴 테스트용 플래그
    ) -> Dict[str, Any]:
        """@desc: 다양한 LLM 메트릭 및 악의적 페이로드 생성"""
        trace_id = uuid.uuid4().hex
        agent_did = f"did:pkh:eip155:0x{uuid.uuid4().hex[:40]}"
        
        req = ExportLogsServiceRequest(
            resourceLogs=[{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "xelog-agent-gateway"}},
                        {"key": "cloud.provider", "value": {"stringValue": "gcp"}},
                        {"key": "agent.did", "value": {"stringValue": agent_did}}
                    ]
                },
                "scopeLogs": [{
                    "scope": {"name": "genai.instrumentation"},
                    "logRecords": [{
                        "timeUnixNano": str(int(time.time() * 1e9)),
                        "traceId": trace_id,
                        "body": {"stringValue": "LLM Inference completed successfully."},
                        "attributes": [
                            {"key": "genai.system", "value": {"stringValue": model_name.split('-')[0]}},
                            {"key": "genai.request.model", "value": {"stringValue": model_name}},
                            {"key": "genai.response.latency_ms", "value": {"intValue": str(latency_ms)}},
                        ]
                    }]
                }]
            }]
        )
        payload = req.model_dump(exclude_none=True)
        
        if is_malformed:
            # 필수 필드 삭제로 파서 에러 유도
            del payload["resourceLogs"] 
        else:
            estimated_cost = (prompt_tokens * 0.000001) + (completion_tokens * 0.000002)
            payload["genai_metrics"] = {
                "tenant_id": agent_did,
                "model": model_name,
                "usage": {
                    "prompt_tokens": prompt_tokens, 
                    "completion_tokens": completion_tokens,
                    "estimated_cost_usd": round(estimated_cost, 4)
                }
            }
        return payload

    @staticmethod
    def trade_intent(
        action: str = "API_RESOURCE_SWAP",
        token: str = "USDC",
        max_fee: str = "0.50",
        slippage: int = 50,
        should_fail_policy: bool = False # [개선] 정책 위반 인텐트(초고위험) 생성 플래그
    ) -> Dict[str, Any]:
        """@desc: 자율 에이전트 거래 인텐트 동적 생성"""
        
        # 정책 위반 테스트 시: 지원하지 않는 토큰(DOGE) 및 과도한 슬리피지(50%) 요청
        if should_fail_policy:
            token = "DOGE"
            slippage = 5000 
            
        req = TradeIngressRequest(
            agent_id=f"did:pkh:eip155:0x{uuid.uuid4().hex[:40]}",
            action=action,
            parameters={
                "target_service": "did:web:financial-analyzer-agent.com",
                "payment_token": "USDC" if not should_fail_policy else token,
                "max_fee_amount": max_fee,          
                "slippage_tolerance_bps": slippage,      
                "deadline_ts": int(time.time()) + (10 if should_fail_policy else 300) 
            }
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def ledger_append(action_name: str, root_hash: str) -> Dict[str, Any]:
        event = LedgerEventSchema(
            action=action_name,
            user_id="system_clearing_engine",
            pii_data=None, 
            details=f"Settlement finalized for intent hash {root_hash}. Billed: 0.45 USDC."
        )
        
        req = StreamAppendRequest(
            stream_name="deai_mainnet_audit_stream",
            verbose=True,
            events=[event]
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def anchor_proposal(
        state_roots: Dict[str, str], 
        inject_fault: bool = False # [개선] 상태 무결성 파괴(Parity Mismatch) 테스트 플래그
    ) -> Dict[str, Any]:
        """@desc: L2 Rollup Sequencer 제출용 데이터 및 혼돈 주입"""
        
        # 의도적 무결성 훼손 (해시 충돌 유발)
        ledger_root = state_roots.get("ledger_root", f"0x{uuid.uuid4().hex}")
        if inject_fault:
            ledger_root = "0xBAD_HASH_CORRUPTED_STATE"
            
        parity = ParityTripletSchema(
            topos_id="epoch_20260805_batch_01",
            phase_id=1,
            nexus_id=14592,
            state_hash=ledger_root
        )
        
        req = AnchorProposalRequest(
            receptor_id=f"rollup_node_{uuid.uuid4().hex[:8]}",
            proposed_parity=parity,
            parent_nexus_id=14591,
            self_parent_state="genesis",
            repos={"exchange_merkle_root": state_roots.get("exchange_root", "0x00")},
            signers=["0xValidatorNodeAlpha", "0xValidatorNodeBeta"],
            signatures=[f"0x{uuid.uuid4().hex}{uuid.uuid4().hex}", f"0x{uuid.uuid4().hex}{uuid.uuid4().hex}"],
            timestamp=int(time.time() * 1000)
        )
        return req.model_dump(exclude_none=True)