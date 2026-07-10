# anchor.provider.param.legacy
## @lineage: bound.adapter.mapper.param.legacy
## @lineage: anchor.provider.model.param.legacy
## @lineage: anchor.surface.model.param.legacy
## @lineage: anchor.provider.info.router
import datetime
import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple, Union, get_type_hints
import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Required, TypedDict

if TYPE_CHECKING:
    from bound.surface.legacy.types import ModelResponse
    ModelResponseType = ModelResponse
else:
    ModelResponseType = Any

class ConfigurableClientsideParamsCustomAuth(TypedDict):
    api_base: str

CONFIGURABLE_CLIENTSIDE_AUTH_PARAMS = Optional[List[Union[str, ConfigurableClientsideParamsCustomAuth]]]

class CustomPricingLiteLLMParams(BaseModel):
    input_cost_per_token: Optional[float] = None
    output_cost_per_token: Optional[float] = None
    input_cost_per_second: Optional[float] = None
    output_cost_per_second: Optional[float] = None
    output_cost_per_second_1080p: Optional[float] = None
    input_cost_per_pixel: Optional[float] = None
    output_cost_per_pixel: Optional[float] = None

    input_cost_per_token_flex: Optional[float] = None
    input_cost_per_token_priority: Optional[float] = None
    cache_creation_input_token_cost: Optional[float] = None
    cache_creation_input_token_cost_above_1hr: Optional[float] = None
    cache_creation_input_token_cost_above_200k_tokens: Optional[float] = None
    cache_creation_input_audio_token_cost: Optional[float] = None
    cache_read_input_token_cost: Optional[float] = None
    cache_read_input_token_cost_flex: Optional[float] = None
    cache_read_input_token_cost_priority: Optional[float] = None
    cache_read_input_token_cost_above_200k_tokens: Optional[float] = None
    cache_read_input_audio_token_cost: Optional[float] = None
    input_cost_per_character: Optional[float] = None
    input_cost_per_character_above_128k_tokens: Optional[float] = None
    input_cost_per_audio_token: Optional[float] = None
    input_cost_per_token_cache_hit: Optional[float] = None
    input_cost_per_token_above_128k_tokens: Optional[float] = None
    input_cost_per_token_above_200k_tokens: Optional[float] = None
    input_cost_per_query: Optional[float] = None
    input_cost_per_image: Optional[float] = None
    input_cost_per_image_above_128k_tokens: Optional[float] = None
    input_cost_per_audio_per_second: Optional[float] = None
    input_cost_per_audio_per_second_above_128k_tokens: Optional[float] = None
    input_cost_per_video_per_second: Optional[float] = None
    input_cost_per_video_per_second_above_128k_tokens: Optional[float] = None
    input_cost_per_video_per_second_above_15s_interval: Optional[float] = None
    input_cost_per_video_per_second_above_8s_interval: Optional[float] = None
    input_cost_per_token_batches: Optional[float] = None
    output_cost_per_token_batches: Optional[float] = None
    output_cost_per_token_flex: Optional[float] = None
    output_cost_per_token_priority: Optional[float] = None
    output_cost_per_character: Optional[float] = None
    output_cost_per_audio_token: Optional[float] = None
    output_cost_per_token_above_128k_tokens: Optional[float] = None
    output_cost_per_token_above_200k_tokens: Optional[float] = None
    output_cost_per_character_above_128k_tokens: Optional[float] = None
    output_cost_per_image: Optional[float] = None
    output_cost_per_image_token: Optional[float] = None
    output_cost_per_reasoning_token: Optional[float] = None
    output_cost_per_video_per_second: Optional[float] = None
    output_cost_per_audio_per_second: Optional[float] = None
    search_context_cost_per_query: Optional[Dict[str, Any]] = None
    citation_cost_per_token: Optional[float] = None
    tiered_pricing: Optional[List[Dict[str, Any]]] = None

class CredentialLiteLLMParams(BaseModel):
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    api_version: Optional[str] = None
    vertex_project: Optional[str] = None
    vertex_location: Optional[str] = None
    vertex_credentials: Optional[Union[str, dict]] = None
    region_name: Optional[str] = None

    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region_name: Optional[str] = None
    aws_bedrock_runtime_endpoint: Optional[str] = None
    watsonx_region_name: Optional[str] = None

class GenericLiteLLMParams(CredentialLiteLLMParams, CustomPricingLiteLLMParams):
    custom_llm_provider: Optional[str] = None
    tpm: Optional[int] = None
    rpm: Optional[int] = None
    timeout: Optional[Union[float, str, httpx.Timeout]] = None
    stream_timeout: Optional[Union[float, str]] = None
    max_retries: Optional[int] = None
    organization: Optional[str] = None
    configurable_clientside_auth_params: CONFIGURABLE_CLIENTSIDE_AUTH_PARAMS = None
    litellm_credential_name: Optional[str] = None
    litellm_trace_id: Optional[str] = None
    max_file_size_mb: Optional[float] = None
    default_api_key_tpm_limit: Optional[int] = None
    default_api_key_rpm_limit: Optional[int] = None
    max_budget: Optional[float] = None
    budget_duration: Optional[str] = None
    use_in_pass_through: Optional[bool] = False
    use_litellm_proxy: Optional[bool] = False
    use_chat_completions_api: Optional[bool] = None
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)
    merge_reasoning_content_in_choices: Optional[bool] = False
    model_info: Optional[Dict] = None
    mock_response: Optional[Union[str, ModelResponseType, Exception, Any]] = None

    tags: Optional[List[str]] = None
    tag_regex: Optional[List[str]] = None

    auto_router_config_path: Optional[str] = None
    auto_router_config: Optional[str] = None
    auto_router_default_model: Optional[str] = None
    auto_router_embedding_model: Optional[str] = None

    complexity_router_config: Optional[Dict] = None
    complexity_router_default_model: Optional[str] = None

    adaptive_router_default_model: Optional[str] = None
    adaptive_router_config: Optional[Dict] = None
    quality_router_config: Optional[Dict] = None
    quality_router_default_model: Optional[str] = None

    s3_bucket_name: Optional[str] = None
    s3_encryption_key_id: Optional[str] = None
    gcs_bucket_name: Optional[str] = None

    vector_store_id: Optional[str] = None
    milvus_text_field: Optional[str] = None
    milvus_db_name: Optional[str] = None
    milvus_partition_names: Optional[List[str]] = None

    @model_validator(mode="before")
    @classmethod
    def preprocess_input_data(cls, data: Any) -> Any:
        if isinstance(data, dict):
            filtered = {k: v for k, v in data.items() if k not in _RESERVED_INIT_KEYS}
            if "max_retries" in filtered and isinstance(filtered["max_retries"], str):
                filtered["max_retries"] = int(filtered["max_retries"])
            return filtered
        return data

    def __contains__(self, key):
        return hasattr(self, key)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

_RESERVED_INIT_KEYS = frozenset({"self", "params", "__class__"})

class LiteLLM_Params(GenericLiteLLMParams):
    model: str
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    def __contains__(self, key):
        return hasattr(self, key)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)