# bound.adapter.surface.legacy.info
## @lineage: bound.surface.legacy.info
## @lineage: bound.surface.legacy.provider
from enum import Enum
from typing import Optional, Literal, List, Dict
from typing_extensions import TypedDict

class ProviderSpecificModelInfo(TypedDict, total=False):
    supports_system_messages: Optional[bool]
    supports_response_schema: Optional[bool]
    supports_vision: Optional[bool]
    supports_function_calling: Optional[bool]
    supports_tool_choice: Optional[bool]
    supports_assistant_prefill: Optional[bool]
    supports_prompt_caching: Optional[bool]
    supports_computer_use: Optional[bool]
    supports_audio_input: Optional[bool]
    supports_embedding_image_input: Optional[bool]
    supports_audio_output: Optional[bool]
    supports_pdf_input: Optional[bool]
    supports_native_streaming: Optional[bool]
    supports_native_structured_output: Optional[bool]
    supports_parallel_function_calling: Optional[bool]
    supports_web_search: Optional[bool]
    supports_reasoning: Optional[bool]
    supports_url_context: Optional[bool]
    supports_none_reasoning_effort: Optional[bool]
    supports_minimal_reasoning_effort: Optional[bool]
    supports_low_reasoning_effort: Optional[bool]
    supports_xhigh_reasoning_effort: Optional[bool]
    supports_max_reasoning_effort: Optional[bool]
    supports_output_config: Optional[bool]
    supports_image_size: Optional[bool]
    bedrock_output_config_effort_ceiling: Optional[Literal["low", "medium", "high", "max", "xhigh"]]

class ProviderTypes(str, Enum):
    OPENAI = "openai"
    CHATGPT = "chatgpt"
    OPENAI_LIKE = "openai_like"
    ANTHROPIC = "anthropic"
    ANTHROPIC_TEXT = "anthropic_text"
    HUGGINGFACE = "huggingface"
    VERTEX_AI = "vertex_ai"
    GEMINI = "gemini"
    BEDROCK = "bedrock"
    OLLAMA = "ollama"
    PERPLEXITY = "perplexity"
    MISTRAL = "mistral"
    A2A = "a2a"
    DEEPSEEK = "deepseek"
    SAMBANOVA = "sambanova"
    DATABRICKS = "databricks"
    GITHUB = "github"
    CUSTOM = "custom"
    LITELLM_PROXY = "litellm_proxy"
    HOSTED_VLLM = "hosted_vllm"
    LM_STUDIO = "lm_studio"
    AIOHTTP_OPENAI = "aiohttp_openai"
    LANGFUSE = "langfuse"
    GITHUB_COPILOT = "github_copilot"
    SNOWFLAKE = "snowflake"
    LLAMA = "meta_llama"
    CURSOR = "cursor"

ProviderTypesSet = {provider.value for provider in ProviderTypes}