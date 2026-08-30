# fiber.dphi.eco.shadow
## @lineage: xphi.arch.eco.adapter.shadow
## @lineage: xphi.eco.adapter.shadow
import time
import json
import hashlib
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from xphi.arch.model.surge.model import DynamicSurgeModel
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("adapter.shadow")

class StateOverride(DynamicSurgeModel):
    slot_hash: str
    injected_value: str

class ShadowStateProjection(DynamicSurgeModel):
    target_address: str
    access_list_state: Dict[str, Any]
    overrides: Optional[List[StateOverride]] = None
    projected_at: int

class DeterministicIntent(DynamicSurgeModel):
    caller: str
    calldata: str
    value_wei: str
    gas_limit: int
    scenario_type: str

class ExecutionProofReceipt(DynamicSurgeModel):
    receipt_id: str
    status: str  # "PASS" or "REVERTED" (둘 다 성공적인 증명으로 취급)
    canonical_hash: str
    gas_used: int
    sealed_at: int
    witness_signatures: List[str]

class ShadowAdapter:
    @classmethod
    def project_shadow_state(
        cls, 
        target_address: str, 
        base_state: Dict[str, Any], 
        overrides: Optional[List[Dict[str, str]]] = None
    ) -> ShadowStateProjection:
        parsed_overrides = []
        if overrides:
            for override in overrides:
                parsed_overrides.append(StateOverride(
                    slot_hash=override["slot_hash"],
                    injected_value=override["injected_value"]
                ))
                
        return ShadowStateProjection(
            target_address=target_address,
            access_list_state=base_state,
            overrides=parsed_overrides,
            projected_at=int(time.time() * 1000)
        )

    @classmethod
    def forge_intent(
        cls, 
        caller: str, 
        calldata: str, 
        scenario_type: str, 
        gas_limit: int = 30_000_000
    ) -> DeterministicIntent:
        return DeterministicIntent(
            caller=caller,
            calldata=calldata,
            value_wei="0",
            gas_limit=gas_limit,
            scenario_type=scenario_type
        )

    @classmethod
    def seal_execution_proof(
        cls, 
        execution_output: Dict[str, Any], 
        notary_keys: List[ed25519.Ed25519PrivateKey]
    ) -> ExecutionProofReceipt:
        canonical_bytes = json.dumps(execution_output, sort_keys=True, separators=(',', ':')).encode('utf-8')
        canonical_hash = hashlib.sha256(canonical_bytes).hexdigest()
        
        signatures = []
        for key in notary_keys:
            sig = key.sign(hashlib.sha256(canonical_bytes).digest()).hex()
            signatures.append(sig)

        is_success = execution_output.get("success", False)
        status_str = "PASS" if is_success else f"REVERTED_{execution_output.get('revert_reason', 'UNKNOWN')}"

        return ExecutionProofReceipt(
            receipt_id=f"proof_{canonical_hash[:12]}",
            status=status_str,
            canonical_hash=canonical_hash,
            gas_used=int(execution_output.get("gas_used", 0)),
            sealed_at=int(time.time() * 1000),
            witness_signatures=signatures
        )

    @classmethod
    def embed_shadow_context(
        cls, 
        base_cached_states: Dict[str, Any], 
        projection: Optional[ShadowStateProjection] = None, 
        proof: Optional[ExecutionProofReceipt] = None
    ) -> Dict[str, Any]:
        updated_state = dict(base_cached_states) if base_cached_states else {}
        if projection is not None:
            updated_state["shadow_projection"] = projection.model_dump(exclude_none=True)
            
        if proof is not None:
            updated_state["execution_proof"] = proof.model_dump(exclude_none=True)
            
        return updated_state