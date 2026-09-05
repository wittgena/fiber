# fiber.dphi.adapter.anchor
## @lineage: fiber.dphi.infra.adapter.anchor
import json
import time
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from fiber.dphi.adapter.settlement import TransactionReceipt
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("adapter.anchor")

class ActorIdentity:
    def __init__(self, name: str = "Anonymous"):
        self.name = name
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.pubkey_hex = self._generate_pub_hex()

    def _generate_pub_hex(self) -> str:
        return self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, 
            format=serialization.PublicFormat.Raw
        ).hex()

    def sign(self, commit_dict: dict) -> str:
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest()
        return self.private_key.sign(commit_hash.encode('utf-8')).hex()

"""CORE DOMAIN STRUCTURES"""
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
    """@desc: 검증된 L2 상태(Parity)를 DPhi Broker를 통해 합의 레이어(Nexus)에 앵커링하는 순수 어댑터"""
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
        log.info(f"[Nexus] ⚓ Anchoring topological state from [{proposal.receptor_id}] (Parent: {proposal.self_parent_state[:8]})...")
        
        # 1. State Parity Check
        if not await self._verify_tripartite_parity(proposal.proposed_parity):
            log.critical("[Nexus] 💥 Topological Rupture Detected! Parity Check Failed.")
            return AnchorResult(
                is_sealed=False, 
                rupture_reason="Tripartite Parity Check Failed"
            )

        # 2. Build Canonical Seal Payload
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
        
        # 3. Invoke Consensus Broker
        seal_res = await self.broker.invoke("seal_epoch", canonical_payload)

        if not seal_res.success:
            log.error(f"[Nexus] 🚫 Consensus Failed (UNAUTHORIZED_PROPOSER): {seal_res.error}")
            return AnchorResult(
                is_sealed=False,
                rupture_reason=f"Consensus Failed: {seal_res.error}"
            )

        # 4. Process Result
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