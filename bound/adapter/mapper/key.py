# bound.adapter.mapper.key
## @lineage: bound.bridge.channel.mapper.key
## @lineage: bound.channel.bridge.mapper.key
"""
@desc: Legacy LiteLLM Key Mapping
Preserves the mapping history of legacy 'litellm_' keys removed from the internal system.
Provides bidirectional adaptation between internal clean keys and external legacy keys.
"""
from typing import Dict, Any

## @desc: Comprehensive mapping (Legacy Key -> Clean Internal Key)
LEGACY_KEY_MAP = {
    "litellm_logging_obj": "log_delegator",
    "litellm_call_id": "call_id",
    "litellm_trace_id": "trace_id",
    "litellm_credential_name": "credential_name",
    "litellm_metadata": "system_metadata",
    "litellm_params": "provider_params",
    "litellm_debug_info": "debug_info",
    "litellm_provider": "provider",
    "litellm_overhead_time_ms": "overhead_time_ms",
    "litellm_model_name": "model_name",
    "litellm_disabled_callbacks": "disabled_callbacks",
    "litellm_request_debug": "request_debug",
    "litellm_proxy_rate_limit_response": "proxy_rate_limit_response",
    "litellm_system_prompt": "system_prompt",
    "litellm_session_id": "session_id",
    "use_litellm_proxy": "use_proxy",
    "litellm_parent_otel_span": "parent_otel_span",
    "litellm_cache_args": "cache_args"
}

## @desc: Cached reverse mapping for O(1) lookups (Clean Internal Key -> Legacy Key)
REVERSE_KEY_MAP = {v: k for k, v in LEGACY_KEY_MAP.items()}

def adapt_payload_for_external_litellm(internal_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforms the payload for external LiteLLM consumption.
    (Write/Outbound) Clean Keys -> Legacy Keys
    """
    payload = internal_kwargs.copy()
    
    for legacy_key, internal_key in LEGACY_KEY_MAP.items():
        if internal_key in payload:
            payload[legacy_key] = payload.pop(internal_key)
            
    return payload

def normalize_payload_for_internal_use(external_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforms an external payload back into a clean internal payload.
    (Read/Inbound) Legacy Keys -> Clean Keys
    """
    payload = external_kwargs.copy()
    
    for legacy_key, internal_key in LEGACY_KEY_MAP.items():
        if legacy_key in payload:
            payload[internal_key] = payload.pop(legacy_key)
            
    return payload

def get_legacy_key(internal_key: str) -> str:
    """Helper to safely fetch the legacy key name for targeted external reads."""
    return REVERSE_KEY_MAP.get(internal_key, internal_key)

def get_internal_key(legacy_key: str) -> str:
    """Helper to safely fetch the clean internal key name for targeted internal reads."""
    return LEGACY_KEY_MAP.get(legacy_key, legacy_key)