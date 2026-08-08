# receptor.surface.adapter.shadow
## @lineage: surface.adapter.shadow
## @lineage: dphi.adapter.shadow
## @lineage: kernel.dphi.adapter.shadow
import time
import json
import hashlib
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from arch.xor.surge.model import DynamicSurgeModel
from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.shadow")

# =========================================================================
# 1. Structural Models (타협 없는 데이터 정합성 검증)
# =========================================================================

class StateOverride(DynamicSurgeModel):
    """특정 컨트랙트의 스토리지 슬롯을 오프체인에서 강제로 조작(God Mode)하기 위한 규격"""
    slot_hash: str
    injected_value: str

class ShadowStateProjection(DynamicSurgeModel):
    """WASM 샌드박스에 주입될 외부 상태의 구조적 스냅샷"""
    target_address: str
    access_list_state: Dict[str, Any]
    overrides: Optional[List[StateOverride]] = None
    projected_at: int

class DeterministicIntent(DynamicSurgeModel):
    """결정론적 연산을 위해 EVM(또는 WASM)에 주입되는 순수 입력 벡터"""
    caller: str
    calldata: str
    value_wei: str
    gas_limit: int
    scenario_type: str

class ExecutionProofReceipt(DynamicSurgeModel):
    """연산 결과가 해시와 서명으로 봉인(Sealing)된 수학적 증명서"""
    receipt_id: str
    status: str  # "PASS" or "REVERTED" (둘 다 성공적인 증명으로 취급)
    canonical_hash: str
    gas_used: int
    sealed_at: int
    witness_signatures: List[str]

# =========================================================================
# 2. Shadow Adapter (상태 주입 및 증명 산출 인터페이스)
# =========================================================================

class ShadowAdapter:
    @classmethod
    def project_shadow_state(
        cls, 
        target_address: str, 
        base_state: Dict[str, Any], 
        overrides: Optional[List[Dict[str, str]]] = None
    ) -> ShadowStateProjection:
        """
        @desc: 노드에서 가져온 Raw 상태에 오프체인 조작(Override)을 더해 샌드박스용 스냅샷을 생성합니다.
        (예: 유니스왑 시나리오에서 WETH Allowance 슬롯을 0xfff...로 덮어쓰는 행위)
        """
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
        """@desc: 실행 환경에 주입될 불변의 입력(Intent) 구조체를 벼려냅니다(Forge)."""
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
        """
        @desc: WASM 엔진이 뱉어낸 결과(Output)를 해싱하고 서명하여, 
               '시뮬레이션이 곧 기반(Proof)이 되는' 영수증을 산출합니다.
        """
        # 1. 상태 변이와 가스 사용량을 정렬하여 해시(Canonical Hash) 생성
        canonical_bytes = json.dumps(execution_output, sort_keys=True, separators=(',', ':')).encode('utf-8')
        canonical_hash = hashlib.sha256(canonical_bytes).hexdigest()
        
        # 2. 결과가 Revert이든 Pass이든 연산 자체의 증명으로 서명(Attestation)
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
        """@desc: 증명된 섀도우 연산 결과를 시스템의 거시적 캐시/원장에 병합합니다."""
        updated_state = dict(base_cached_states) if base_cached_states else {}
        if projection is not None:
            updated_state["shadow_projection"] = projection.model_dump(exclude_none=True)
            
        if proof is not None:
            updated_state["execution_proof"] = proof.model_dump(exclude_none=True)
            
        return updated_state