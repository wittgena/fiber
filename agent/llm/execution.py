# agent.llm.execution
## @lineage: agent.anchor.llm.execution
## @lineage: ator.client.model.execution
## @lineage: eco.client.model.execution
## @lineage: engine.client.metadata.context
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

from fiber.agent.anchor.model.types.general import EmbeddingResponse
from fiber.agent.llm.param import ModelResponse

@dataclass
class ExecutionMetadata:
    """프레임워크 전역에서 공유되는 시스템 메타데이터 및 추적(Tracking) 정보"""
    session_id: Optional[str] = None
    trace_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    preset_cache_key: Optional[str] = None
    data_residency: Optional[str] = None
    
    call_id: Optional[str] = None
    completion_call_id: Optional[str] = None
    model_info: Optional[Dict[str, Any]] = None
    model_alias_map: Optional[Dict[str, str]] = None
    proxy_server_request: Optional[Any] = None
    base_model: Optional[str] = None
    
    input_cost_per_token: Optional[float] = None
    output_cost_per_token: Optional[float] = None
    input_cost_per_second: Optional[float] = None
    output_cost_per_second: Optional[float] = None
    cost_per_query: Optional[float] = None
    
    prompt_id: Optional[str] = None
    prompt_variables: Optional[dict] = None
    
    provider_auth: Dict[str, Any] = field(default_factory=dict)
    framework_flags: Dict[str, Any] = field(default_factory=dict)

    def to_legacy_dict(self) -> dict:
        res = {**self.__dict__, **self.provider_auth, **self.framework_flags}
        res.pop("provider_auth", None)
        res.pop("framework_flags", None)
        return res


@dataclass
class CompletionContext:
    """전처리가 완료된 안전한 Completion 상태 벡터 (어댑터 전달용 DTO)"""
    model: str
    messages: List[Any]
    custom_llm_provider: str
    model_response: ModelResponse
    system_meta: ExecutionMetadata  
    
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    timeout: Any = 60.0
    
    optional_params: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)
    original_kwargs: dict = field(default_factory=dict)
    
    stream: Optional[bool] = False
    acompletion: bool = False
    shared_session: Optional[Any] = None
    client_instance: Optional[Any] = None
    deployment_id: Optional[str] = None


@dataclass
class EmbeddingContext:
    """전처리가 완료된 안전한 Embedding 상태 벡터 (어댑터 전달용 DTO)"""
    model: str
    input: Union[str, List[str]]
    custom_llm_provider: str
    
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    timeout: Union[float, int] = 60.0
    aembedding: bool = False
    
    optional_params: Dict[str, Any] = field(default_factory=dict)
    original_kwargs: Dict[str, Any] = field(default_factory=dict)
    model_response: EmbeddingResponse = field(default_factory=EmbeddingResponse)