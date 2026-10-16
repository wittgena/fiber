# fiber.llm.compat.schema
from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, ConfigDict

PathType = Union[str, List[str]]


class UsageTokenMappingSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt_tokens: Optional[PathType] = None
    completion_tokens: Optional[PathType] = None
    total_tokens: Optional[PathType] = None


class FinishedConditionSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    value: Any


class ExtractionRuleSchema(BaseModel):
    """단일화된 LLM 응답(Sync & Stream) 추출 규칙 스키마"""
    model_config = ConfigDict(extra="forbid")

    # 1. 공통 데이터 추출 경로
    text: Optional[PathType] = None
    finish_reason: Optional[PathType] = None
    usage: Optional[Union[PathType, UsageTokenMappingSchema, List[Union[str, UsageTokenMappingSchema]]]] = None
    tool_calls: Optional[PathType] = None
    logprobs: Optional[PathType] = None
    id: Optional[PathType] = None
    system_fingerprint: Optional[PathType] = None
    provider_specific_fields: Optional[PathType] = None
    error: Optional[PathType] = None

    # 2. 스트림 종료 판별 조건
    is_finished_cond: Optional[FinishedConditionSchema] = None
    is_finished_static: Optional[bool] = None
    finish_reason_static: Optional[str] = None

    # 3. Tool Call 복구 경로 및 Defaults 정적 상수
    fallback_tool_name: Optional[PathType] = None
    fallback_tool_args: Optional[PathType] = None
    role: Optional[str] = None
    finish_stop: Optional[str] = None
    finish_tool: Optional[str] = None

class ProviderParamRuleSchema(BaseModel):
    """요청 파라미터 변환 규칙 스키마"""
    model_config = ConfigDict(extra="allow")

    message_schema: Optional[str] = "openai_array"
    mapping: Optional[Dict[str, str]] = None