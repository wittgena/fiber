# anchor.registry.model.support
from __future__ import annotations
from typing import Optional, Literal, List, Dict, ClassVar

from anchor.registry.model.cost import model_cost, lookup_base_model_info
from anchor.registry.router.locator import get_llm_provider
from anchor.registry.router.config import get_provider_info, ProviderConfigManager

from bound.surface.legacy.info import ProviderTypes, ProviderTypesSet
from bound.surface.exception import BadRequestError
from anchor.registry.model.config.resolver import config

from watcher.plane.emitter import get_emitter

log = get_emitter("model.support")

"""@phase: Model Capability & Support Resolution"""
class ModelSupport:
    """@desc: Centralized resolver for model capabilities, API parameters, and provider support."""
    _lowercase_map: ClassVar[Dict[str, str] | None] = None

    @classmethod
    def _get_cost_key(cls, potential_key: str) -> Optional[str]:
        """@desc: Resolves the canonical key for cost mapping with cache handling."""
        if potential_key in model_cost:
            return potential_key

        if cls._lowercase_map is None:
            cls._lowercase_map = {k.lower(): k for k in model_cost}

        potential_key_lower = potential_key.lower()
        matched_key = cls._lowercase_map.get(potential_key_lower)
        
        if matched_key and matched_key in model_cost:
            return matched_key

        ## @flow: Rebuild cache to handle stale entries
        cls._lowercase_map = {k.lower(): k for k in model_cost}
        matched_key = cls._lowercase_map.get(potential_key_lower)
        if matched_key and matched_key in model_cost:
            return matched_key

        return None

    @classmethod
    def check_capability(
        cls, 
        model: str, 
        custom_llm_provider: Optional[str], 
        key: str, 
        default_if_none: bool = False
    ) -> bool:
        """@desc: Dynamically checks if a model/provider supports a specific feature flag."""
        try:
            resolved_model, resolved_provider, _, _ = get_llm_provider(model=model, custom_llm_provider=custom_llm_provider)
            model_info = lookup_base_model_info(model=resolved_model, custom_llm_provider=resolved_provider)
            if model_info.get(key) is not None:
                return bool(model_info.get(key))

            bare_model_key = cls._get_cost_key(resolved_model)
            if bare_model_key:
                bare_entry = model_cost.get(bare_model_key) or {}
                if bare_entry.get(key) is not None:
                    return bool(bare_entry.get(key))

            provider_info = get_provider_info(
                model=resolved_model, custom_llm_provider=resolved_provider
            )
            if provider_info and provider_info.get(key) is not None:
                return bool(provider_info.get(key))

            return default_if_none

        except Exception as e:
            log.debug(f"Capability check failed for {key}. model={model}, provider={custom_llm_provider}. Error: {e}")
            provider_info = get_provider_info(model=model, custom_llm_provider=custom_llm_provider)
            if provider_info and provider_info.get(key) is not None:
                return bool(provider_info.get(key))
            return default_if_none

    @classmethod
    def get_supported_openai_params(
        cls,
        model: str,
        custom_llm_provider: Optional[str] = None,
        request_type: Literal["chat_completion", "embeddings", "transcription"] = "chat_completion",
        base_model: Optional[str] = None,
    ) -> Optional[list]:
        """@desc: Retrieves supported OpenAI API parameters for a given provider/model combo."""
        if not custom_llm_provider:
            try:
                custom_llm_provider = get_llm_provider(model=model)[1]
            except BadRequestError:
                return None

        ## @flow: Resolve config via ProviderConfigManager
        provider_config = None
        if custom_llm_provider in ProviderTypesSet:
            provider_config = ProviderConfigManager.get_provider_chat_config(
                model=model, provider=ProviderTypes(custom_llm_provider), base_model=base_model
            )
        elif custom_llm_provider.split("/")[0] in ProviderTypesSet:
            provider_config = ProviderConfigManager.get_provider_chat_config(
                model=model, provider=ProviderTypes(custom_llm_provider.split("/")[0]), base_model=base_model
            )

        if provider_config and request_type == "chat_completion":
            supported_params = provider_config.get_supported_openai_params(model=model)
            if base_model and base_model != model:
                base_model_params = provider_config.get_supported_openai_params(model=base_model)
                supported_params = list(dict.fromkeys([*supported_params, *base_model_params]))
            return supported_params

        ## @flow: Fallback to static provider config mapping
        if custom_llm_provider == "ollama":
            return config.OllamaConfig().get_supported_openai_params(model=model)
        elif custom_llm_provider == "anthropic":
            return config.AnthropicConfig().get_supported_openai_params(model=model)
        elif custom_llm_provider == "openai":
            if request_type == "transcription":
                transcription_config = ProviderConfigManager.get_provider_audio_transcription_config(
                    model=model, provider=ProviderTypes.OPENAI
                )
                if isinstance(transcription_config, config.OpenAIGPTAudioTranscriptionConfig):
                    return transcription_config.get_supported_openai_params(model=model)
                raise ValueError(f"Unsupported transcription config for model: {model}")
            return config.OpenAIConfig().get_supported_openai_params(model=model)
        elif custom_llm_provider == "huggingface":
            return config.HuggingFaceChatConfig().get_supported_openai_params(model=model)
        return None

    @classmethod
    def get_supported_regions(
        cls, model: str, custom_llm_provider: Optional[str] = None
    ) -> Optional[List[str]]:
        """@desc: Extracts region support mapping from model cost data."""
        try:
            resolved_model, resolved_provider, _, _ = get_llm_provider(model=model, custom_llm_provider=custom_llm_provider)
            model_info = lookup_base_model_info(model=resolved_model, custom_llm_provider=resolved_provider)
            model_key = model_info.get("key")
            if model_key:
                model_cost_data = model_cost.get(model_key, {})
                supported_regions = model_cost_data.get("supported_regions")
                if isinstance(supported_regions, list):
                    return supported_regions
            return None
        except Exception as e:
            log.debug(f"Failed to check supported_regions. model={model}. Error: {e}")
            return None

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