# anchor.phase.ingress.proxy.schema
## @lineage: bound.ingress.proxy.schema
## @lineage: xphi.proxy.ingress.schema
## @lineage: bound.transport.stream.ingress.schema
from pydantic import BaseModel, Field
from typing import Any, Dict
from enum import Enum
from uuid import UUID, uuid4

class ProtocolSource(str, Enum):
    MCP_1_0 = "1.0"
    MCP_2_0 = "2.0"
    UNKNOWN = "unknown"

class ActionIntent(str, Enum):
    INITIALIZE = "initialize"
    INVOKE_TOOL = "invoke_tool"
    READ_RESOURCE = "read_resource"

class StreamIdentity(BaseModel):
    """
    [BRN-SEC-MCP-001] 방어: 
    ASGI 게이트키퍼가 헤더를 뜯어내고 검증한 결과물. 
    내부 로직은 더 이상 Authorization 헤더를 직접 파싱하지 않음.
    """
    is_authenticated: bool = Field(default=False)
    stateless_token_id: str | None = None
    granted_scopes: list[str] = Field(default_factory=list)

class StreamMetadata(BaseModel):
    """
    [BRN-SEC-MCP-002] 방어:
    밀반입(Smuggling) 검사가 끝난 순수 트래픽의 메타데이터.
    """
    stream_id: UUID = Field(default_factory=uuid4)
    original_protocol: ProtocolSource
    content_length: int
    client_ip: str

class LogicPayload(BaseModel):
    """
    [BRN-SEC-MCP-003] 방어:
    Pydantic의 무한 백트래킹(Union DoS)을 막기 위해 
    ActionIntent를 기반으로 단일 스키마로 강제 매핑된 페이로드.
    """
    intent: ActionIntent
    parameters: Dict[str, Any] = Field(default_factory=dict)
    # 필요시 parameters는 intent에 따라 xphi.scope에서 
    # 특정 스키마(예: ToolCallParams)로 지연 파싱(Lazy Parsing)됨.

class LogicStream(BaseModel):
    """
    Brane 내부 시스템으로 인입되는 최종 단일 토폴로지.
    외부 라우터는 오직 이 객체만 생성하여 비즈니스 로직에 넘김.
    """
    meta: StreamMetadata
    identity: StreamIdentity
    payload: LogicPayload
    model_config = {"frozen": True, "extra": "forbid"}