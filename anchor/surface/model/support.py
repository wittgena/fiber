# anchor.surface.model.support
## @lineage: anchor.provider.model.support
from __future__ import annotations

from typing import Optional, Literal, List, Dict, ClassVar
from anchor.provider.types import ProviderTypes, ProviderTypesSet
from anchor.provider.model.support import ModelSupport
from watcher.plane.emitter import get_emitter

log = get_emitter("model.support")

"""
@phase: 
- External Capability Facade (Registry)
@desc:
- Maintains backward compatibility for external imports and provides a structured registry for future capability flags.
"""
def get_supported_openai_params(
    model: str, custom_llm_provider: Optional[str] = None, request_type: str = "chat_completion", base_model: Optional[str] = None
) -> Optional[list]:
    return ModelSupport.get_supported_openai_params(model, custom_llm_provider, request_type, base_model)

def get_supported_regions(model: str, custom_llm_provider: Optional[str] = None) -> Optional[List[str]]:
    return ModelSupport.get_supported_regions(model, custom_llm_provider)

def supports_httpx_timeout(custom_llm_provider: str) -> bool:
    return custom_llm_provider in ["openai"]

"""Capability Flags"""
def supports_system_messages(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_system_messages")

def supports_web_search(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_web_search")

def supports_url_context(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_url_context")

def supports_native_streaming(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    ## @note: Defaults to True if explicitly undefined in registries
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_native_streaming", default_if_none=True)

def supports_response_schema(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_response_schema")

def supports_parallel_function_calling(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_parallel_function_calling")

def supports_function_calling(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_function_calling")

def supports_tool_choice(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_tool_choice")

def supports_audio_input(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_audio_input")

def supports_pdf_input(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_pdf_input")

def supports_audio_output(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_audio_output")

def supports_prompt_caching(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_prompt_caching")

def supports_computer_use(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_computer_use")

def supports_vision(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_vision")

def supports_reasoning(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_reasoning")

def supports_native_structured_output(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_native_structured_output")

def supports_embedding_image_input(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_embedding_image_input")