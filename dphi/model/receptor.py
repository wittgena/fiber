# xphi.arch.contract.model.receptor
## @lineage: arch.contract.model.receptor
from enum import Enum
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional

class EdgeState(str, Enum):
    INTENT_VALIDATED = "INTENT_VALIDATED"
    EXECUTION_SUCCESS = "EXECUTION_SUCCESS"
    PROOF_GENERATED = "PROOF_GENERATED"
    SEALED_AND_COMMITTED = "SEALED_AND_COMMITTED"
    INTENT_ACCEPTED = "INTENT_ACCEPTED"
    RECEIPT_GENERATED = "RECEIPT_GENERATED"
    SUCCESS = "success"
    ERROR = "error"

class EdgeHeader(str, Enum):
    STATE = "X-Edge-State"
    CONTENT_HASH = "X-Content-Hash"
    FINGERPRINT = "X-Kernel-Fingerprint"
    ERROR_DETAIL = "X-Error-Detail"

class IntentValidationRequest(BaseModel):
    requester_id: str = Field(..., description="요청 에이전트 식별자")
    responder_id: str = Field(..., description="응답(수행) 에이전트 식별자")
    action: str = Field(..., description="수행할 작업 명칭")
    max_fuel_budget: int = Field(..., ge=0, le=4294967295, description="연산에 허용된 최대 Fuel (u32)")
    signature: Optional[str] = None 
    sig_algo: str = "ECDSA_SECP256K1"

class ExecuteComputeRequest(BaseModel):
    code: str = Field(..., description="WASM 샌드박스에서 실행할 Python 스크립트")
    variables: Dict[str, Any] = Field(default_factory=dict, description="스크립트에 주입할 변수 컨텍스트")

class ProofGenerationRequest(BaseModel):
    execution_hash: str = Field(..., description="실행 결과의 결정론적 해시")
    fuel_consumed: int = Field(..., ge=0, le=4294967295, description="소모된 Fuel (u32)")
    verification_seed: str = Field(..., description="증명 생성을 위한 랜덤 시드")

class IntentValidationResponse(BaseModel):
    status: EdgeState
    clearance: Dict[str, Any]

class ExecuteComputeResponse(BaseModel):
    status: EdgeState
    output: str

class ProofGenerationResponse(BaseModel):
    status: EdgeState
    zk_receipt: Dict[str, Any]

class TradeIngressRequest(BaseModel):
    agent_id: str = Field(..., description="에이전트 식별자")
    action: str = Field(..., description="거래 액션 (예: buy, sell, swap)")

class TradeIngressResponse(BaseModel):
    status: EdgeState
    session: Dict[str, Any] = Field(..., description="발급된 세션 정보")

class ClearingReceiptRequest(BaseModel):
    entangled_state: Dict[str, Any] = Field(default_factory=dict, description="매칭되어 얽힌(Entangled) 상태 데이터")
    signatures: List[str] = Field(default_factory=list, description="참여자들의 서명 리스트")
    cost_metrics: Dict[str, Any] = Field(default_factory=dict, description="정산 비용 및 가스 메트릭 정보")

class ClearingReceiptResponse(BaseModel):
    status: EdgeState
    rollup_payload: Dict[str, Any] = Field(..., description="EVM/Rollup 전송용 외부 페이로드")

class EpochInitPayload(BaseModel):
    ts: int = Field(..., description="인입 밀리초 타임스탬프")
    topo: int = Field(..., description="Sequencer가 발급한 위상(Topology) ID")
    press: int = Field(..., description="Allocator가 할당한 연산 한도(Fuel)")
    rupture: bool = Field(..., description="네트워크 균열(장애) 여부")
    injected_intent: TradeIngressRequest = Field(..., description="사용자가 주입한 원본 인텐트")

class ParityTripletSchema(BaseModel):
    topos_id: str = Field(..., description="위상 식별자")
    phase_id: int = Field(ge=0, le=4294967295, description="Rust u32 constraint")
    nexus_id: int = Field(ge=0, le=4294967295, description="Rust u32 constraint")

class AnchorProposalRequest(BaseModel):
    receptor_id: str = Field(..., description="수용자 에이전트 식별자")
    proposed_parity: ParityTripletSchema = Field(..., description="제안된 패리티 삼중항")
    parent_nexus_id: int = Field(default=0, ge=0, le=4294967295, description="부모 넥서스 ID")

    self_parent_state: str = Field(..., description="부모 상태(Parent State)의 해시값")

    repos: Dict[str, str] = Field(default_factory=dict, description="상태 저장소 맵")
    signers: List[str] = Field(default_factory=list, description="서명자 목록")
    signatures: List[str] = Field(default_factory=list, description="암호학적 서명 목록")
    timestamp: float = Field(..., description="제안 타임스탬프")

class AnchorSealResponse(BaseModel):
    status: EdgeState = Field(..., description="합의 상태 영수증")
    nexus_id: int = Field(..., description="확정된 Nexus ID")
    commit_hash: str = Field(..., description="커밋 결정론적 해시")
    receipt: Dict[str, Any] = Field(default_factory=dict, description="봉인된 세부 영수증 데이터")