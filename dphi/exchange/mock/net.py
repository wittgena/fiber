# dphi.exchange.mock.net
import time
import uuid
import random
from typing import Any, Dict, List

from cryptography.hazmat.primitives.asymmetric import ed25519

from arch.contract.model.receptor import (
    TradeIngressRequest,
    AnchorProposalRequest,
    ParityTripletSchema
)
from watcher.receptor.contract.model import ExportLogsServiceRequest
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
        is_expired: bool = False  
    ) -> Dict[str, Any]:
        """@desc: AP2 Mandate 발급 파라미터 동적 생성 (다양한 VC 클레임 추가)"""
        return {
            "requester_id": agent_pub_hex,
            "target_action": target_action,
            "max_spend_usdc": max_spend_usdc,
            "signer_key": agent_key,
            "validity_ms": -3600000 if is_expired else 3600000,
            "delegation_tier": "TIER_2_ORACLE",
            "allowed_networks": ["base-mainnet", "arbitrum-one"],
            "ip_restrictions": ["192.168.1.0/24", "10.0.0.0/8"]
        }

    @staticmethod
    def otlp_payload(
        model_name: str = None, 
        prompt_tokens: int = None,
        completion_tokens: int = None,
        latency_ms: int = None,
        is_malformed: bool = False 
    ) -> Dict[str, Any]:
        """@desc: 다종 LLM 메트릭, 하이퍼파라미터 및 에러 로그 등 풍부한 페이로드 생성"""
        models = ["gemini-1.5-pro", "gpt-4o", "claude-3-5-sonnet", "llama-3.1-70b-instruct"]
        providers = ["gcp", "aws", "azure", "together-ai"]
        
        ## 동적 랜덤 할당 (값이 명시되지 않은 경우)
        model_name = model_name or random.choice(models)
        prompt_tokens = prompt_tokens or random.randint(100, 150000)
        completion_tokens = completion_tokens or random.randint(10, 4096)
        latency_ms = latency_ms or random.randint(500, 15000)
        
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        agent_did = f"did:pkh:eip155:0x{uuid.uuid4().hex[:40]}"
        
        ## 단순 과금 외에 엔지니어링 분석을 위한 풍부한 메타데이터 추가
        req = ExportLogsServiceRequest(
            resourceLogs=[{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "xelog-agent-gateway"}},
                        {"key": "cloud.provider", "value": {"stringValue": random.choice(providers)}},
                        {"key": "cloud.region", "value": {"stringValue": "us-west-2"}},
                        {"key": "agent.did", "value": {"stringValue": agent_did}},
                        {"key": "tenant.id", "value": {"stringValue": f"tenant_{uuid.uuid4().hex[:8]}"}}
                    ]
                },
                "scopeLogs": [{
                    "scope": {"name": "genai.instrumentation", "version": "1.2.0"},
                    "logRecords": [{
                        "timeUnixNano": str(int(time.time() * 1e9)),
                        "traceId": trace_id,
                        "spanId": span_id,
                        "severityText": "INFO",
                        "body": {"stringValue": f"[{model_name}] LLM Inference completed successfully."},
                        "attributes": [
                            {"key": "llm.model", "value": {"stringValue": model_name}},
                            {"key": "gen_ai.request.model", "value": {"stringValue": model_name}},
                            {"key": "gen_ai.response.latency_ms", "value": {"intValue": str(latency_ms)}},
                            {"key": "prompt_tokens", "value": {"intValue": str(prompt_tokens)}},
                            {"key": "completion_tokens", "value": {"intValue": str(completion_tokens)}},
                            {"key": "reasoning_tokens", "value": {"intValue": str(random.randint(0, completion_tokens))}},
                            {"key": "gen_ai.request.temperature", "value": {"doubleValue": "0.7"}},
                            {"key": "gen_ai.response.finish_reason", "value": {"stringValue": "stop"}},
                        ]
                    }]
                }]
            }]
        )
        payload = req.model_dump(exclude_none=True)
        
        if is_malformed:
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
        action: str = "A2A_COMPUTE_LEASE",
        token: str = "USDC",
        max_fee: str = "2.50",
        slippage: int = 50,
        should_fail_policy: bool = False 
    ) -> Dict[str, Any]:
        """@desc: 단순 API 호출을 넘어선 물리/가상 자원 임대 형태의 복합 인텐트 생성"""
        if should_fail_policy:
            token = "DOGE"
            slippage = 5000 
            
        req = TradeIngressRequest(
            agent_id=f"did:pkh:eip155:0x{uuid.uuid4().hex[:40]}",
            action=action,
            parameters={
                "target_service": "did:web:compute-provider.akash.network",
                "payment_token": "USDC" if not should_fail_policy else token,
                "max_fee_amount": max_fee,          
                "slippage_tolerance_bps": slippage,      
                "deadline_ts": int(time.time()) + (10 if should_fail_policy else 300),
                "execution_environment": {
                    "hardware": "NVIDIA_H100_80GB",
                    "duration_seconds": 3600,
                    "dataset_cid": "ipfs://QmYwAPJzv5CZsnA625s3Xf2sm5DcgXU1G"
                }
            }
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def ledger_append(action_name: str, root_hash: str, event_count: int = 3) -> Dict[str, Any]:
        """@desc: 다중 이벤트(Batch) 생성 및 PII 데이터 주입을 통한 SecretAuditor 실질 검증"""
        events = []
        for i in range(event_count):
            pii_payload = {
                "user_email": f"agent_{i}@dphi.network",
                "kyc_wallet_ip": f"192.168.1.{10+i}",
                "auth_token": f"Bearer eyJhbGci...{uuid.uuid4().hex[:8]}"
            }
            
            event = LedgerEventSchema(
                action=f"{action_name}_STEP_{i+1}",
                user_id="system_clearing_engine",
                pii_data=pii_payload, 
                details=f"State transition step {i+1} for intent hash {root_hash}."
            )
            events.append(event)
            
        req = StreamAppendRequest(
            stream_name="deai_mainnet_audit_stream",
            verbose=True,
            events=events
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def anchor_proposal(
        state_roots: Dict[str, str], 
        inject_fault: bool = False 
    ) -> Dict[str, Any]:
        """@desc: L2 Rollup Sequencer 제출용 데이터. 다중 검증자 서명 등 합의 복잡성 모사"""
        
        ledger_root = state_roots.get("ledger_root", f"0x{uuid.uuid4().hex}")
        if inject_fault:
            ledger_root = "0xBAD_HASH_CORRUPTED_STATE"
            
        parity = ParityTripletSchema(
            topos_id=f"epoch_{time.strftime('%Y%m%d')}_batch_01",
            phase_id=1,
            nexus_id=14592,
            state_hash=ledger_root
        )
        
        validators = [
            "0xValidatorNodeAlpha", 
            "0xValidatorNodeBeta", 
            "0xValidatorNodeGamma", 
            "0xValidatorNodeDelta"
        ]
        
        req = AnchorProposalRequest(
            receptor_id=f"rollup_node_{uuid.uuid4().hex[:8]}",
            proposed_parity=parity,
            parent_nexus_id=14591,
            self_parent_state="genesis",
            repos={
                "exchange_merkle_root": state_roots.get("exchange_root", "0x00"),
                "otlp_telemetry_root": state_roots.get("otlp_root", "0x00")
            },
            signers=validators[:3], # 4명 중 3명(정족수) 서명 모사
            signatures=[f"0x{uuid.uuid4().hex}{uuid.uuid4().hex}" for _ in range(3)],
            timestamp=int(time.time() * 1000)
        )
        return req.model_dump(exclude_none=True)