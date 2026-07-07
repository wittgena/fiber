# bound.channel.bridge.mapper.key
"""
@desc: Legacy LiteLLM Key Mapping
- Preserves the mapping history of legacy 'litellm_' keys removed from the internal system
"""
from typing import Dict, Any

## @desc: History reference mapping legacy keys to clean internal keys
LEGACY_KEY_MAP = {
    "litellm_logging_obj": "log_delegator",
    "litellm_call_id": "call_id",
    "litellm_trace_id": "trace_id",
    "litellm_credential_name": "credential_name",
    "litellm_metadata": "system_metadata",
    "litellm_params": "provider_params",
}

def adapt_payload_for_external_litellm(internal_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Transforms the payload for external LiteLLM consumption"""
    payload = internal_kwargs.copy()
    
    ## @desc: Convert clean internal keys back to mandatory legacy keys for the external library
    if "log_delegator" in payload:
        payload["litellm_logging_obj"] = payload.pop("log_delegator")
        
    if "call_id" in payload:
        payload["litellm_call_id"] = payload.pop("call_id")
        
    if "trace_id" in payload:
        payload["litellm_trace_id"] = payload.pop("trace_id")
        
    if "credential_name" in payload:
        payload["litellm_credential_name"] = payload.pop("credential_name")
        
    if "system_metadata" in payload:
        payload["litellm_metadata"] = payload.pop("system_metadata")
        
    if "provider_params" in payload:
        payload["litellm_params"] = payload.pop("provider_params")
        
    return payload