# phase.dphi.adapter.anchor
import json
import time
import uuid
import random
import hashlib
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from ator.client.local.wallet import LocalWalletClient
from phase.dphi.config import dphi_env
from phase.dphi.adapter.exchange import TransactionReceipt

from arch.contract.model.receptor import (
    TradeIngressRequest,
    AnchorProposalRequest,
    ParityTripletSchema
)
from kernel.dphi.broker import DphiBroker
from kernel.dphi.adapter.state import StateAdapter
from watcher.receptor.contract.model import ExportLogsServiceRequest
from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.anchor")


# =====================================================================
# 1. DATA STRUCTURES
# =====================================================================

@dataclass
class AnchorProposal:
    receptor_id: str
    proposed_parity: Dict[str, Any]
    parent_nexus_id: int
    self_parent_state: str
    repos: Dict[str, str]
    signers: List[str]          # 합의에 참여한 노드/에이전트들의 공개키
    signatures: List[str]       # 각 노드의 Ed25519 서명
    timestamp: float = field(default_factory=time.time)


@dataclass
class AnchorResult:
    is_sealed: bool
    nexus_id: Optional[int] = None
    commit_hash: Optional[str] = None
    receipt: Optional[TransactionReceipt] = None
    rupture_reason: Optional[str] = None

class LedgerEventSchema(BaseModel):
    action: str
    user_id: str
    pii_data: Optional[Dict[str, Any]] = None
    details: str

class StreamAppendRequest(BaseModel):
    stream_name: str
    events: List[LedgerEventSchema]
    verbose: bool = False

class NexusAnchor:
    def __init__(self, broker: DphiBroker, consensus_threshold: int = 1, allowed_committee: List[str] = None):
        self.broker = broker
        self.consensus_threshold = consensus_threshold
        self.allowed_committee = allowed_committee or []

    async def _verify_tripartite_parity(self, parity_dict: dict) -> bool:
        payload_json = StateAdapter.to_canonical_bytes(parity_dict).decode('utf-8')
        res = await self.broker.invoke("verify_parity", payload_json)
        
        if not res.success:
            log.error(f"[Nexus] 🚨 Parity Verification Crashed: {res.error}")
            return False
            
        output = json.loads(res.output)
        is_valid = output.get("is_valid", False)
        
        if not is_valid and "recovered_missing" in output:
            log.warning(f"[Nexus] ⚠️ Parity fractured but recovered via XOR! "
                        f"Restored {output.get('recovered_type')}: {output.get('recovered_missing')}")
            return True
            
        return is_valid

    async def anchor_state(self, proposal: AnchorProposal) -> AnchorResult:
        # 로그에도 연결되는 부모(Parent)의 해시 정보를 함께 출력하도록 개선
        log.info(f"[Nexus] ⚓ Anchoring topological state from [{proposal.receptor_id}] (Parent: {proposal.self_parent_state[:8]})...")
        
        if not await self._verify_tripartite_parity(proposal.proposed_parity):
            log.critical("[Nexus] 💥 Topological Rupture Detected! Parity Check Failed.")
            return AnchorResult(
                is_sealed=False, 
                rupture_reason="Tripartite Parity Check Failed"
            )

        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=proposal.proposed_parity,
            parent_nexus_id=proposal.parent_nexus_id,
            self_parent_state=proposal.self_parent_state,
            repos=proposal.repos,
            cached_states={},
            timestamp=proposal.timestamp,
            signers=proposal.signers,
            signatures=proposal.signatures,
            threshold=self.consensus_threshold,
            allowed_signers=self.allowed_committee
        )

        canonical_payload = StateAdapter.to_canonical_bytes(seal_payload).decode('utf-8')
        seal_res = await self.broker.invoke("seal_epoch", canonical_payload)

        if not seal_res.success:
            log.error(f"[Nexus] 🚫 Consensus Failed (UNAUTHORIZED_PROPOSER): {seal_res.error}")
            return AnchorResult(
                is_sealed=False,
                rupture_reason=f"Consensus Failed: {seal_res.error}"
            )

        seal_data = json.loads(seal_res.output)
        commit_hash = seal_data.get("anchor_result", {}).get("commit_hash", "UNKNOWN_HASH")
        new_nexus_id = proposal.proposed_parity.get("nexus_id", 0)

        log.info(f"[Nexus] ⏳ Epoch successfully sealed. Commit: {commit_hash[:8]}...")
        receipt = TransactionReceipt(
            job_id=f"nexus_{new_nexus_id}_{int(time.time())}",
            topos_id=proposal.proposed_parity.get("topos_id", "0"),
            parity_hash=commit_hash,
            clearing_signatures=proposal.signatures,
            fuel_consumed=getattr(seal_res, 'fuel_consumed', 0),
            settlement_status="COMMITTED_TO_NEXUS"
        )
        log.info(f"[Nexus] 🧾 Deterministic Truth Emitted. (Receipt: {receipt.job_id})")
        return AnchorResult(
            is_sealed=True,
            nexus_id=new_nexus_id,
            commit_hash=commit_hash,
            receipt=receipt
        )


# =====================================================================
# 3. SWARM & BUILDERS (Test & Payload Generators)
# =====================================================================

class NotarySwarm:
    def __init__(self, size: int = 3):
        self.notaries = []
        for i in range(size):
            seed = hashlib.sha256(f"dphi_notary_node_{i}".encode()).digest()
            private_key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
            public_hex = private_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, 
                format=serialization.PublicFormat.Raw
            ).hex()
            self.notaries.append({"priv": private_key, "pub": public_hex})
            
        # [핵심 수리] 
        # NotarySwarm이 인스턴스화 될 때, 자신이 생성한 공개키들을 
        # mock_env의 witness_pubkeys(커널이 신뢰하는 화이트리스트)에 강제로 덮어씌웁니다.
        # 이렇게 하면 Gateway가 만든 서명이 커널 검증을 100% 통과합니다.
        dphi_env.export_attestation.__class__.witness_pubkeys = property(lambda self: [node["pub"] for node in self.notaries])

    @property
    def public_keys(self) -> List[str]:
        return [node["pub"] for node in self.notaries]

    def attest_payload(self, canonical_hash: bytes) -> List[str]:
        return [node["priv"].sign(canonical_hash).hex() for node in self.notaries]


class PhaseBuilder:
    __domain_metadata__ = {
        "otlp_payload": "OTel + Datadog/LangSmith. Tracks LLM GenAI metrics (tokens/latency) for billing.",
        "trade_intent": "W3C DID + UniswapX/Fetch.ai. Intent-centric A2A (Agent-to-Agent) resource swap with slippage.",
        "ledger_append": "Celestia/EigenDA + RISC Zero. Immutable DA (Data Availability) & ZK-verifiable compute logs.",
        "anchor_proposal": "Ethereum L2 (OP Stack) Sequencer. Rollup of state roots (Merkle Parity) for global consensus."
    }

    @staticmethod
    def get_testnet_wallet(edge_server_url: str = "http://localhost:8000/v1/ext") -> 'ExtWalletClient':
        return LocalWalletClient(base_url=edge_server_url)

    @staticmethod
    def ap2_mandate_params(
        agent_pub_hex: str, 
        agent_key: ed25519.Ed25519PrivateKey,
        target_action: str = "M2M_INFERENCE_SWAP",
        max_spend_usdc: str = "0.50",
        is_expired: bool = False  
    ) -> Dict[str, Any]:
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
    def trade_intent(
        action: str = "A2A_COMPUTE_LEASE",
        token: str = "USDC",
        max_fee: str = "2.50",
        slippage: int = 50,
        should_fail_policy: bool = False 
    ) -> Dict[str, Any]:
        if should_fail_policy:
            token = "DOGE"
            slippage = 5000 
            
        req = TradeIngressRequest(
            agent_id=dphi_env.agents.alpha.did,
            action=action,
            parameters={
                "target_service": dphi_env.agents.beta.did,
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
    def otlp_payload(
        model_name: str = None, 
        prompt_tokens: int = None,
        completion_tokens: int = None,
        latency_ms: int = None,
        is_malformed: bool = False 
    ) -> Dict[str, Any]:
        models = ["gemini-1.5-pro", "gpt-4o", "claude-3-5-sonnet", "llama-3.1-70b-instruct"]
        providers = ["gcp", "aws", "azure", "together-ai"]
        
        model_name = model_name or random.choice(models)
        prompt_tokens = prompt_tokens or random.randint(100, 150000)
        completion_tokens = completion_tokens or random.randint(10, 4096)
        latency_ms = latency_ms or random.randint(500, 15000)
        
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        agent_did = dphi_env.agents.alpha.did 
        
        req = ExportLogsServiceRequest(
            resourceLogs=[{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "xelog-agent-gateway"}},
                        {"key": "cloud.provider", "value": {"stringValue": random.choice(providers)}},
                        {"key": "cloud.region", "value": {"stringValue": "us-west-2"}},
                        {"key": "agent.did", "value": {"stringValue": agent_did}},
                        {"key": "tenant.id", "value": {"stringValue": agent_did}} 
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
    def ledger_append(action_name: str, root_hash: str, event_count: int = 3) -> Dict[str, Any]:
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
            stream_name=dphi_env.da_layer.namespace_id,
            verbose=True,
            events=events
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def anchor_proposal(
        state_roots: Dict[str, str], 
        inject_fault: bool = False 
    ) -> Dict[str, Any]:
        ledger_root = state_roots.get("ledger_root", f"0x{uuid.uuid4().hex}")
        if inject_fault:
            ledger_root = "0xBAD_HASH_CORRUPTED_STATE"
            
        parity = ParityTripletSchema(
            topos_id=f"epoch_{time.strftime('%Y%m%d')}_batch_01",
            phase_id=1,
            nexus_id=14592,
            state_hash=ledger_root
        )
        
        witnesses = dphi_env.export_attestation.witness_pubkeys
        mock_signatures = [f"{uuid.uuid4().hex}{uuid.uuid4().hex}" for _ in range(3)]
        req = AnchorProposalRequest(
            receptor_id=dphi_env.contracts.nexus_clearing,
            proposed_parity=parity,
            parent_nexus_id=14591,
            self_parent_state="genesis",
            repos={
                "exchange_merkle_root": state_roots.get("exchange_root", "0x00"),
                "otlp_telemetry_root": state_roots.get("otlp_root", "0x00")
            },
            signers=witnesses[:3], 
            signatures=mock_signatures,
            timestamp=int(time.time() * 1000)
        )
        return req.model_dump(exclude_none=True)