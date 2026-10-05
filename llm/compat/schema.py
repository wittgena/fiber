# fiber.llm.compat.schema
from typing import List, Dict, Union, Any, Optional, Literal
from pydantic import BaseModel, Field

class UsageDictSchema(BaseModel):
    """Usage 매핑 규칙 - (예: {"prompt_tokens": "usage.input_tokens", ...})"""
    prompt_tokens: Union[str, List[str]]
    completion_tokens: Union[str, List[str]]
    total_tokens: Optional[Union[str, List[str]]] = None

class ConditionSchema(BaseModel):
    """스트림 종료 조건을 평가하기 위한 구조체"""
    path: str
    value: Any

# STATE_EXTRACTION_RULES 스키마
class StateExtractionRuleSchema(BaseModel):
    fallback_tool_name: Optional[str] = None
    fallback_tool_args: Optional[str] = None
    sync_content_paths: Optional[List[str]] = None
    
    # usage는 "usage" 같은 단순 문자열일 수도 있고, 상세 매핑을 담은 Dict일 수도 있음
    sync_usage_paths: Optional[List[Union[str, UsageDictSchema]]] = None
    
    # defaults 용 (기본값 설정)
    role: Optional[str] = None
    finish_stop: Optional[str] = None
    finish_tool: Optional[str] = None
    stream_content_paths: Optional[List[str]] = None

class ProviderParamRuleSchema(BaseModel):
    supported: List[str] = Field(default_factory=list)
    mapping: Dict[str, str] = Field(default_factory=dict)
    wrap_in: Dict[str, List[str]] = Field(default_factory=dict)
    tool_format: Literal["standard", "gemini_strict", "anthropic"] = "standard"
    
    role_mapping: Dict[str, str] = Field(default_factory=dict)
    system_param: Optional[str] = None
    supports_tools: bool = False

class StreamExtractionRuleSchema(BaseModel):
    # text와 finish_reason 등은 단일 경로(str) 혹은 폴백 경로(List[str])를 가짐
    text: Union[str, List[str]]
    finish_reason: Optional[Union[str, List[str]]] = None
    logprobs: Optional[Union[str, List[str]]] = None
    tool_calls: Optional[Union[str, List[str]]] = None
    error: Optional[Union[str, List[str]]] = None
    
    # STREAM의 usage는 "usage" 문자열, 상세 Dict, 혹은 이 둘의 혼합 리스트 형태를 띰
    usage: Optional[Union[str, UsageDictSchema, List[Union[str, UsageDictSchema]]]] = None
    
    # 정적/동적 종료 조건
    is_finished_cond: Optional[ConditionSchema] = None
    is_finished_static: Optional[bool] = None
    finish_reason_static: Optional[str] = None