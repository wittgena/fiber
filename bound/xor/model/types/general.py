# bound.xor.model.types.general
## @lineage: eco.model.types.general
## @lineage: engine.model.types.general
## @lineage: bound.model.types.general
## @lineage: llm.types.general
## @lineage: eco.mesh.model.types.general
## @lineage: runtime.mesh.model.types.general
## @lineage: mesh.model.types.general
import json
import time
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator
from typing_extensions import Required, TypedDict

## OpenAI SDK Base Models
from openai._models import BaseModel as OpenAIObject
from openai.types.completion_usage import CompletionTokensDetails, PromptTokensDetails
from openai.types.images_response import Image as OpenAIImage
from openai.types.images_response import ImagesResponse as OpenAIImageResponse

from bound.xor.model.types.param.legacy import PricingParams
from bound.xor.model.protype import ProviderSpecificModelInfo, ProviderTypes
from bound.xor.model.types.core import Usage
from bound.xor.model.types.openai import ChatCompletionToolCallChunk, ChatCompletionUsageBlock
from bound.xor.model.types.openai import OpenAIChatCompletionFinishReason

from arch.model.phase.gate import uuid
from arch.model.surge.model import DynamicSurgeModel
from watcher.plane.emitter import get_emitter

log = get_emitter("types.general")

FINISH_REASON_MAP: dict[str, OpenAIChatCompletionFinishReason] = {
    ## Anthropic
    "stop_sequence": "stop",
    "end_turn": "stop",
    "max_tokens": "length",
    "tool_use": "tool_calls",
    "refusal": "content_filter",
    "compaction": "length",
    ## Cohere
    "COMPLETE": "stop",
    "ERROR_TOXIC": "content_filter",
    "ERROR": "stop",
    ## HuggingFace / Together AI
    "eos_token": "stop",
    "eos": "stop",
    ## Gemini / Vertex AI
    "STOP": "stop",
    "MAX_TOKENS": "length",
    "SAFETY": "content_filter",
    "RECITATION": "content_filter",
    "FINISH_REASON_UNSPECIFIED": "stop",
    "MALFORMED_FUNCTION_CALL": "stop",
    "LANGUAGE": "content_filter",
    "OTHER": "content_filter",
    "BLOCKLIST": "content_filter",
    "PROHIBITED_CONTENT": "content_filter",
    "SPII": "content_filter",
    "IMAGE_SAFETY": "content_filter",
    "IMAGE_PROHIBITED_CONTENT": "content_filter",
    "TOO_MANY_TOOL_CALLS": "stop",
    "MALFORMED_RESPONSE": "stop",
    ## Zhipu GLM
    "network_error": "stop",
    "sensitive": "content_filter",
    ## Bedrock
    "guardrail_intervened": "content_filter",
    ## OpenAI passthrough
    "stop": "stop",
    "length": "length",
    "tool_calls": "tool_calls",
    "function_call": "function_call",
    "content_filter": "content_filter",
    ## Anthropic Sonnet 4
    "content_filtered": "content_filter",
}

def map_finish_reason(finish_reason: str) -> OpenAIChatCompletionFinishReason:
    mapped = FINISH_REASON_MAP.get(finish_reason)
    if mapped is None:
        log.warning("Unmapped finish_reason '%s', defaulting to 'stop'", finish_reason)
        return "stop"
    return mapped

# =========================================================
# 1. Base Helpers & Wrappers
# =========================================================
def _generate_id():  # private helper function
    return "chatcmpl-" + str(uuid.uuid4())

class SafeAttributeModel:
    """A base model that provides safe attribute access."""
    def __delattr__(self, name):
        try:
            super().__delattr__(name)
        except AttributeError:
            pass

class HiddenParams(DynamicSurgeModel):
    original_response: Optional[Union[str, Any]] = None
    model_id: Optional[str] = None
    api_base: Optional[str] = None
    _response_ms: Optional[float] = None
    response_cost: Optional[float] = None
    
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        extra="allow",
        protected_namespaces=()
    )

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def model_dump(self, **kwargs):
        data = super().model_dump(**kwargs)
        data["_response_ms"] = getattr(self, "_response_ms", None)
        return data

    def json(self, **kwargs):
        return self.model_dump(**kwargs)


# =========================================================
# 2. Enums, Constants & Basic Types
# =========================================================
class ServiceTier(Enum):
    FLEX = "flex"
    PRIORITY = "priority"

class DataResidency(Enum):
    US = "us"
    EU = "eu"

OPENAI_RESPONSE_HEADERS = [
    "x-ratelimit-remaining-requests",
    "x-ratelimit-remaining-tokens",
    "x-ratelimit-limit-requests",
    "x-ratelimit-limit-tokens",
    "x-ratelimit-reset-requests",
    "x-ratelimit-reset-tokens",
]


# =========================================================
# 3. Tokenizer & Credentials
# =========================================================
class CustomHuggingfaceTokenizer(TypedDict):
    identifier: str
    revision: str
    auth_token: Optional[str]

class SelectTokenizerResponse(TypedDict):
    type: Literal["openai_tokenizer", "huggingface_tokenizer"]
    tokenizer: Any

class CredentialBase(BaseModel):
    credential_name: str
    credential_info: dict

class CredentialItem(CredentialBase):
    credential_values: dict


# =========================================================
# 4. Functions & Embeddings
# =========================================================
class FunctionCall(OpenAIObject):
    arguments: str
    name: Optional[str] = None

class Function(OpenAIObject):
    arguments: str
    name: Optional[str]

    def __init__(self, arguments: Optional[Union[Dict, str]] = None, name: Optional[str] = None, **params):
        if arguments is None:
            if params.get("parameters", None) is not None and isinstance(params["parameters"], dict):
                arguments = json.dumps(params["parameters"])
                params.pop("parameters")
            else:
                arguments = ""
        elif isinstance(arguments, Dict):
            arguments = json.dumps(arguments)
        
        super(Function, self).__init__(arguments=arguments, name=name)

    def __contains__(self, key):
        return hasattr(self, key)
    def get(self, key, default=None):
        return getattr(self, key, default)
    def __getitem__(self, key):
        return getattr(self, key)
    def __setitem__(self, key, value):
        setattr(self, key, value)

class Embedding(OpenAIObject):
    embedding: Union[list, str] = []
    index: int
    object: Literal["embedding"]

    def get(self, key, default=None):
        return getattr(self, key, default)
    def __getitem__(self, key):
        return getattr(self, key)
    def __setitem__(self, key, value):
        setattr(self, key, value)

class EmbeddingResponse(OpenAIObject):
    model: Optional[str] = None
    data: List
    object: Literal["list"]
    usage: Optional[Usage] = None
    _hidden_params: dict = {}
    _response_headers: Optional[Dict] = None
    _response_ms: Optional[float] = None

    def __init__(self, model: Optional[str] = None, usage: Optional[Usage] = None, response_ms=None, data: Optional[Union[List, List[Embedding]]] = None, hidden_params=None, _response_headers=None, **params):
        object = "list"
        _response_ms = response_ms if response_ms else None
        data = data if data else []
        usage = usage if usage else Usage()
        if _response_headers:
            self._response_headers = _response_headers
        super().__init__(model=model, object=object, data=data, usage=usage)
        if hidden_params:
            self._hidden_params = hidden_params

    def __contains__(self, key): return hasattr(self, key)
    def get(self, key, default=None): return getattr(self, key, default)
    def __getitem__(self, key): return getattr(self, key)
    def __setitem__(self, key, value): setattr(self, key, value)
    def json(self, **kwargs):
        try: return self.model_dump()
        except Exception: return self.dict()


# =========================================================
# 5. Tokens, Costs & Model Info
# =========================================================
class CostPerToken(TypedDict, total=False):
    input_cost_per_token: Required[float]
    output_cost_per_token: Required[float]
    cache_read_input_token_cost: float
    cache_creation_input_token_cost: float

class SearchContextCostPerQuery(TypedDict, total=False):
    search_context_size_low: float
    search_context_size_medium: float
    search_context_size_high: float

class ModelInfoBase(ProviderSpecificModelInfo, total=False):
    key: Required[str]
    max_tokens: Required[Optional[int]]
    max_input_tokens: Required[Optional[int]]
    max_output_tokens: Required[Optional[int]]
    input_cost_per_token: Required[Optional[float]]
    output_cost_per_token: Required[Optional[float]]
    litellm_provider: Required[str]
    mode: Required[Literal["completion", "embedding", "image_generation", "chat", "audio_transcription", "responses", "ocr"]]
    
    # Optional Pricing / Multipliers
    input_cost_per_token_flex: Optional[float]
    cache_creation_input_token_cost: Optional[float]
    cache_read_input_token_cost: Optional[float]
    regional_processing_uplift_multiplier_eu: Optional[float]
    search_context_cost_per_query: Optional[SearchContextCostPerQuery]
    tiered_pricing: Optional[List[Dict[str, Any]]]
    tpm: Optional[int]
    rpm: Optional[int]

class ModelInfo(ModelInfoBase, total=False):
    supported_openai_params: Required[Optional[List[str]]]

class CacheCreationTokenDetails(BaseModel):
    ephemeral_5m_input_tokens: Optional[int] = None
    ephemeral_1h_input_tokens: Optional[int] = None

class CompletionTokensDetailsWrapper(CompletionTokensDetails):
    text_tokens: Optional[int] = None
    image_tokens: Optional[int] = None
    video_tokens: Optional[int] = None

class PromptTokensDetailsWrapper(SafeAttributeModel, PromptTokensDetails):
    text_tokens: Optional[int] = None
    image_tokens: Optional[int] = None
    video_tokens: Optional[int] = None
    web_search_requests: Optional[int] = None
    character_count: Optional[int] = None
    image_count: Optional[int] = None
    video_length_seconds: Optional[float] = None
    cache_creation_tokens: Optional[int] = None
    cache_creation_token_details: Optional[CacheCreationTokenDetails] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.character_count is None: del self.character_count
        if self.image_count is None: del self.image_count
        if self.video_length_seconds is None: del self.video_length_seconds
        if self.web_search_requests is None: del self.web_search_requests
        if self.cache_creation_tokens is None: del self.cache_creation_tokens
        if self.cache_creation_token_details is None: del self.cache_creation_token_details


# =========================================================
# 6. Text & Generic Streaming
# =========================================================
class GenericStreamingChunk(TypedDict, total=False):
    text: Required[str]
    tool_use: Optional[ChatCompletionToolCallChunk]
    is_finished: Required[bool]
    finish_reason: Required[str]
    usage: Required[Optional[ChatCompletionUsageBlock]]
    index: int
    provider_specific_fields: Optional[Dict[str, Any]]

class Logprobs(OpenAIObject):
    text_offset: Optional[List[int]]
    token_logprobs: Optional[List[Union[float, None]]]
    tokens: Optional[List[str]]
    top_logprobs: Optional[List[Union[Dict[str, float], None]]]

class TextChoices(OpenAIObject):
    def __init__(self, finish_reason=None, index=0, text=None, logprobs=None, **params):
        super(TextChoices, self).__init__(**params)
        self.finish_reason = map_finish_reason(finish_reason) if finish_reason else None
        self.index = index
        self.text = text
        if logprobs is None: self.logprobs = None
        elif isinstance(logprobs, dict): self.logprobs = Logprobs(**logprobs)
        else: self.logprobs = logprobs

    def __contains__(self, key): return hasattr(self, key)
    def get(self, key, default=None): return getattr(self, key, default)
    def __getitem__(self, key): return getattr(self, key)
    def __setitem__(self, key, value): setattr(self, key, value)
    def json(self, **kwargs):
        try: return self.model_dump()
        except Exception: return self.dict()

class TextCompletionResponse(OpenAIObject):
    id: str
    object: str
    created: int
    model: Optional[str]
    choices: List[TextChoices]
    usage: Optional[Usage]
    _response_ms: Optional[int] = None
    _hidden_params: HiddenParams

    def __init__(self, id=None, choices=None, created=None, model=None, usage=None, stream=False, response_ms=None, object=None, **params):
        if stream:
            object = "text_completion.chunk"
            choices = [TextChoices()]
        else:
            object = "text_completion"
            if choices is not None and isinstance(choices, list):
                choices = [TextChoices(**c) if isinstance(c, dict) else c for c in choices]
            else:
                choices = [TextChoices()]
                
        id = id if id else _generate_id()
        created = created if created else int(time.time())
        usage = usage if usage else Usage()

        super(TextCompletionResponse, self).__init__(id=id, object=object, created=created, model=model, choices=choices, usage=usage, **params)
        self._response_ms = response_ms if response_ms else None
        self._hidden_params = HiddenParams()

    def __contains__(self, key): return hasattr(self, key)
    def get(self, key, default=None): return getattr(self, key, default)
    def __getitem__(self, key): return getattr(self, key)
    def __setitem__(self, key, value): setattr(self, key, value)


# =========================================================
# 7. Images
# =========================================================
class ImageObject(OpenAIImage):
    b64_json: Optional[str] = None
    url: Optional[str] = None
    revised_prompt: Optional[str] = None
    provider_specific_fields: Optional[Dict[str, Any]] = None

    def __init__(self, b64_json=None, url=None, revised_prompt=None, provider_specific_fields=None, **kwargs):
        super().__init__(b64_json=b64_json, url=url, revised_prompt=revised_prompt)
        if provider_specific_fields:
            self.provider_specific_fields = provider_specific_fields

    def __contains__(self, key): return hasattr(self, key)
    def get(self, key, default=None): return getattr(self, key, default)
    def __getitem__(self, key): return getattr(self, key)
    def __setitem__(self, key, value): setattr(self, key, value)
    def json(self, **kwargs):
        try: return self.model_dump()
        except Exception: return self.dict()

class ImageUsageInputTokensDetails(DynamicSurgeModel):
    image_tokens: int
    text_tokens: int

class ImageUsage(DynamicSurgeModel):
    input_tokens: int
    input_tokens_details: ImageUsageInputTokensDetails
    output_tokens: int
    total_tokens: int

class ImageResponse(OpenAIImageResponse, DynamicSurgeModel):
    _hidden_params: dict = {}
    usage: Optional[ImageUsage] = None
    model_config = ConfigDict(extra="allow", protected_namespaces=())

    def __init__(self, created: Optional[int] = None, data: Optional[List[ImageObject]] = None, response_ms=None, usage: Optional[ImageUsage] = None, hidden_params: Optional[dict] = None, **kwargs):
        _response_ms = response_ms if response_ms else None
        data = data if data else []
        created = created if created else int(time.time())

        _data = [ImageObject(**d) if isinstance(d, dict) else ImageObject(**d.model_dump()) if isinstance(d, BaseModel) else d for d in data]
        _usage = usage or ImageUsage(input_tokens=0, input_tokens_details=ImageUsageInputTokensDetails(image_tokens=0, text_tokens=0), output_tokens=0, total_tokens=0)
        
        super().__init__(created=created, data=_data, usage=_usage)
        self.quality = kwargs.get("quality", None)
        self.output_format = kwargs.get("output_format", None)
        self.size = kwargs.get("size", None)
        self._hidden_params = hidden_params or {}

    def __contains__(self, key): return hasattr(self, key)
    def get(self, key, default=None): return getattr(self, key, default)
    def __getitem__(self, key): return getattr(self, key)
    def __setitem__(self, key, value): setattr(self, key, value)
    def json(self, **kwargs):
        try: return self.model_dump()
        except Exception: return self.dict()


# =========================================================
# 8. Logging & Configurations
# =========================================================
class StandardCallbackDynamicParams(TypedDict, total=False):
    langfuse_public_key: Optional[str]
    langfuse_secret: Optional[str]
    langfuse_secret_key: Optional[str]
    langfuse_host: Optional[str]
    langfuse_prompt_version: Optional[int]
    gcs_bucket_name: Optional[str]
    gcs_path_service_account: Optional[str]
    langsmith_api_key: Optional[str]
    langsmith_project: Optional[str]
    langsmith_base_url: Optional[str]
    langsmith_sampling_rate: Optional[float]
    langsmith_tenant_id: Optional[str]
    humanloop_api_key: Optional[str]
    arize_api_key: Optional[str]
    arize_space_key: Optional[str]
    arize_space_id: Optional[str]
    posthog_api_key: Optional[str]
    posthog_api_url: Optional[str]
    wandb_api_key: Optional[str]
    weave_project_id: Optional[str]
    turn_off_message_logging: Optional[bool]
    litellm_disabled_callbacks: Optional[List[str]]

all_litellm_params = (
    [
        "metadata", "litellm_metadata", "litellm_trace_id", "litellm_request_debug",
        "guardrails", "tags", "acompletion", "aimg_generation", "atext_completion",
        "text_completion", "caching", "mock_response", "mock_timeout",
        "disable_add_transform_inline_image_block", "litellm_proxy_rate_limit_response",
        "api_key", "api_version", "prompt_id", "prompt_variables", "litellm_system_prompt",
        "provider_specific_header", "prompt_version", "api_base", "force_timeout",
        "logger_fn", "verbose", "custom_llm_provider", "model_file_id_mapping",
        "log_delegator", "call_id", "use_client", "id", "fallbacks", "azure",
        "headers", "model_list", "num_retries", "context_window_fallback_dict",
        "retry_policy", "retry_strategy", "roles", "final_prompt_value", "bos_token",
        "eos_token", "request_timeout", "complete_response", "self", "client", "rpm",
        "tpm", "max_parallel_requests", "input_cost_per_token", "output_cost_per_token",
        "input_cost_per_second", "output_cost_per_second", "hf_model_name", "model_info",
        "proxy_server_request", "secret_fields", "preset_cache_key", "caching_groups",
        "ttl", "cache", "no-log", "base_model", "stream_timeout", "supports_system_message",
        "region_name", "allowed_model_region", "model_config", "fastest_response",
        "cooldown_time", "cache_key", "max_retries", "azure_ad_token_provider", "tenant_id",
        "client_id", "azure_username", "azure_password", "azure_scope", "client_secret",
        "user_continue_message", "configurable_clientside_auth_params", "weight",
        "ensure_alternating_roles", "assistant_continue_message", "fallback_depth",
        "max_fallbacks", "max_budget", "budget_duration", "use_in_pass_through",
        "merge_reasoning_content_in_choices", "litellm_credential_name", "allowed_openai_params",
        "litellm_session_id", "use_litellm_proxy", "use_chat_completions_api",
        "prompt_label", "shared_session", "search_tool_name", "order", "enable_json_schema_validation"
    ]
    + list(StandardCallbackDynamicParams.__annotations__.keys())
    + list(PricingParams.model_fields.keys())
)