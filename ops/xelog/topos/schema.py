# ops.xelog.topos.schema
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional

class ParityTriplet(BaseModel):
    """상태의 시공간적 식별자 (결정론적 고유 ID)"""
    topos_id: str
    phase_id: int
    nexus_id: int

class ConsensusContext(BaseModel):
    """M-of-N 다중 서명 및 접근 제어(ACL) 컨텍스트"""
    signers: List[str] = Field(..., description="서명자 퍼블릭 키 목록 (Hex)")
    signatures: List[str] = Field(..., description="서명 배열 (Ed25519)")
    threshold: int = Field(default=1, description="합의에 필요한 최소 서명 수")
    allowed_signers: List[str] = Field(..., description="현재 Epoch에서 허용된 위원회 명단")