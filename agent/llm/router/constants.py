# agent.llm.router.constants
## @lineage: bound.agent.constants
import os
import sys
from typing import List, Literal, Optional

def get_env_int(env_var: str, default: int) -> int:
    raw = os.getenv(env_var)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (ValueError, TypeError):
        return default

def get_env_float(env_var: str, default: float) -> float:
    raw = os.getenv(env_var)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except (ValueError, TypeError):
        return default

DEFAULT_REQUEST_TIMEOUT_SECONDS: float = 60.0
REQUEST_TIMEOUT: float = get_env_float("REQUEST_TIMEOUT", DEFAULT_REQUEST_TIMEOUT_SECONDS)

AIOHTTP_CONNECTOR_LIMIT = get_env_int("AIOHTTP_CONNECTOR_LIMIT", 1000)
AIOHTTP_KEEPALIVE_TIMEOUT = get_env_int("AIOHTTP_KEEPALIVE_TIMEOUT", 120)
COMPLETION_HTTP_FALLBACK_SECONDS: float = 600.0

OPENAI_EMBEDDING_PARAMS = ["dimensions", "encoding_format", "user"]
DEFAULT_EMBEDDING_PARAM_VALUES = {
    **{k: None for k in OPENAI_EMBEDDING_PARAMS},
    "model": None,
    "custom_llm_provider": "",
    "input": None,
}

DEFAULT_IMAGE_WIDTH = get_env_int("DEFAULT_IMAGE_WIDTH", 300)
DEFAULT_IMAGE_HEIGHT = get_env_int("DEFAULT_IMAGE_HEIGHT", 300)
DEFAULT_MAX_LRU_CACHE_SIZE = get_env_int("DEFAULT_MAX_LRU_CACHE_SIZE", 64)
DEFAULT_MAX_RECURSE_DEPTH = get_env_int("DEFAULT_MAX_RECURSE_DEPTH", 100)
DEFAULT_IMAGE_TOKEN_COUNT = get_env_int("DEFAULT_IMAGE_TOKEN_COUNT", 250)

DEFAULT_REPLICATE_GPU_PRICE_PER_SECOND = get_env_float("DEFAULT_REPLICATE_GPU_PRICE_PER_SECOND", 0.001400)

DEFAULT_SSL_CIPHERS = os.getenv(
    "DEFAULT_SSL_CIPHERS",
    "TLS_AES_256_GCM_SHA384:"  # Fastest observed in testing
    "TLS_AES_128_GCM_SHA256:"  # Slightly faster than 256-bit
    "TLS_CHACHA20_POLY1305_SHA256:"  # Fast on ARM/mobile
    "ECDHE-RSA-AES256-GCM-SHA384:"
    "ECDHE-RSA-AES128-GCM-SHA256:"
    "ECDHE-ECDSA-AES256-GCM-SHA384:"
    "ECDHE-ECDSA-AES128-GCM-SHA256:"
    "ECDHE-RSA-CHACHA20-POLY1305:" "ECDHE-ECDSA-CHACHA20-POLY1305:"
    "ECDHE-RSA-AES256-SHA384:"  # Common fallback
    "ECDHE-RSA-AES128-SHA256:"  # Very widely supported
    "AES256-GCM-SHA384:"  # Non-PFS fallback (compatibility)
    "AES128-GCM-SHA256"   # Last resort (maximum compatibility)
)

DEFAULT_TRIM_RATIO = get_env_float("DEFAULT_TRIM_RATIO", 0.75)

HTTP_HANDLER_CONNECT_TIMEOUT_SECONDS: float = 5.0
MAX_STREAMING_DURATION_SECONDS: float = 300.0
REPLICATE_MODEL_NAME_WITH_ID_LENGTH = get_env_int("REPLICATE_MODEL_NAME_WITH_ID_LENGTH", 64)

MAX_IMAGE_URL_DOWNLOAD_SIZE_MB = get_env_float("MAX_IMAGE_URL_DOWNLOAD_SIZE_MB", 50.0)
MAX_LONG_SIDE_FOR_IMAGE_HIGH_RES = get_env_int("MAX_LONG_SIDE_FOR_IMAGE_HIGH_RES", 2000)
MAX_SHORT_SIDE_FOR_IMAGE_HIGH_RES = get_env_int("MAX_SHORT_SIDE_FOR_IMAGE_HIGH_RES", 768)
MAX_TILE_WIDTH = get_env_int("MAX_TILE_WIDTH", 512)
MAX_TILE_HEIGHT = get_env_int("MAX_TILE_HEIGHT", 512)

DEFAULT_CHAT_COMPLETION_PARAM_VALUES = {
    "functions": None,
    "function_call": None,
    "temperature": None,
    "top_p": None,
    "n": None,
    "stream": None,
    "stream_options": None,
    "stop": None,
    "max_tokens": None,
    "max_completion_tokens": None,
    "modalities": None,
    "prediction": None,
    "audio": None,
    "presence_penalty": None,
    "frequency_penalty": None,
    "logit_bias": None,
    "user": None,
    "model": None,
    "custom_llm_provider": "",
    "response_format": None,
    "seed": None,
    "tools": None,
    "tool_choice": None,
    "max_retries": None,
    "logprobs": None,
    "top_logprobs": None,
    "extra_headers": None,
    "api_version": None,
    "parallel_tool_calls": None,
    "drop_params": None,
    "allowed_openai_params": None,
    "additional_drop_params": None,
    "messages": None,
    "reasoning_effort": None,
    "verbosity": None,
    "thinking": None,
    "web_search_options": None,
    "include_server_side_tool_invocations": None,
    "service_tier": None,
    "safety_identifier": None,
    "prompt_cache_key": None,
    "prompt_cache_retention": None,
    "store": None,
    "metadata": None,
    "context_management": None,
}

DEFAULT_TEMPERATURE = 0.7
DEFAULT_NUM_OUTPUTS = 2048
DEFAULT_CONTEXT_WINDOW = 8192 
DEFAULT_EMBED_BATCH_SIZE = 10