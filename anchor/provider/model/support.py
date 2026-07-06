# anchor.provider.model.support
## @lineage: anchor.surface.provider.support
## @lineage: anchor.channel.compat.switch.model.support
from typing import Optional, Literal, List, Dict
from typing_extensions import TypedDict

from bound.channel.config.resolver import config

from bound.router.provider.locator import get_llm_provider
from bound.router.provider.config import get_provider_info, ProviderConfigManager
from anchor.provider.types import ProviderTypes, ProviderTypesSet
from anchor.surface.registry.provider import model_cost, _get_model_info_helper
from anchor.surface.exception import BadRequestError

from watcher.plane.emitter import get_emitter

log = get_emitter("model.support")

def get_supported_openai_params(
    model: str,
    custom_llm_provider: Optional[str] = None,
    request_type: Literal[
        "chat_completion", "embeddings", "transcription"
    ] = "chat_completion",
    base_model: Optional[str] = None,
) -> Optional[list]:
    if not custom_llm_provider:
        try:
            custom_llm_provider = get_llm_provider(model=model)[1]
        except BadRequestError:
            return None

    if custom_llm_provider in ProviderTypesSet:
        provider_config = ProviderConfigManager.get_provider_chat_config(
            model=model,
            provider=ProviderTypes(custom_llm_provider),
            base_model=base_model,
        )
    elif custom_llm_provider.split("/")[0] in ProviderTypesSet:
        provider_config = ProviderConfigManager.get_provider_chat_config(
            model=model,
            provider=ProviderTypes(custom_llm_provider.split("/")[0]),
            base_model=base_model,
        )
    else:
        provider_config = None

    if provider_config and request_type == "chat_completion":
        supported_params = provider_config.get_supported_openai_params(model=model)
        if base_model and base_model != model:
            base_model_params = provider_config.get_supported_openai_params(
                model=base_model
            )
            supported_params = list(
                dict.fromkeys([*supported_params, *base_model_params])
            )
        return supported_params

    if custom_llm_provider == "ollama":
        return config.OllamaConfig().get_supported_openai_params(model=model)
    elif custom_llm_provider == "anthropic":
        return config.AnthropicConfig().get_supported_openai_params(model=model)
    elif custom_llm_provider == "openai":
        if request_type == "transcription":
            transcription_provider_config = ProviderConfigManager.get_provider_audio_transcription_config(model=model, provider=ProviderTypes.OPENAI)
            if isinstance(transcription_provider_config, config.OpenAIGPTAudioTranscriptionConfig):
                return transcription_provider_config.get_supported_openai_params(model=model)
            else:
                raise ValueError(
                    f"Unsupported provider config: {transcription_provider_config} for model: {model}"
                )
        return config.OpenAIConfig().get_supported_openai_params(model=model)
    elif custom_llm_provider == "huggingface":
        return litellm.HuggingFaceChatConfig().get_supported_openai_params(model=model)
    return None


def supports_httpx_timeout(custom_llm_provider: str) -> bool:
    supported_providers = ["openai"]
    if custom_llm_provider in supported_providers:
        return True
    return False

def supports_system_messages(model: str, custom_llm_provider: Optional[str]) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_system_messages",
    )

def supports_web_search(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_web_search",
    )

def supports_url_context(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_url_context",
    )

def supports_native_streaming(model: str, custom_llm_provider: Optional[str]) -> bool:
    try:
        model, custom_llm_provider, _, _ = get_llm_provider(model=model, custom_llm_provider=custom_llm_provider)
        model_info = _get_model_info_helper(model=model, custom_llm_provider=custom_llm_provider)
        supports_native_streaming = model_info.get("supports_native_streaming", True)
        if supports_native_streaming is None:
            supports_native_streaming = True
        return supports_native_streaming
    except Exception as e:
        log.debug(
            f"Model not found or error in checking supports_native_streaming support. You passed model={model}, custom_llm_provider={custom_llm_provider}. Error: {str(e)}"
        )
        return False

def supports_response_schema(
    model: str, custom_llm_provider: Optional[str] = None
) -> bool:
    try:
        model, custom_llm_provider, _, _ = get_llm_provider(model=model, custom_llm_provider=custom_llm_provider)
    except Exception as e:
        log.debug(f"Model not found or error in checking response schema support. You passed model={model}, custom_llm_provider={custom_llm_provider}. Error: {str(e)}")
        return False
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_response_schema",
    )

def supports_parallel_function_calling(
    model: str, custom_llm_provider: Optional[str] = None
) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_parallel_function_calling",
    )

def supports_function_calling(
    model: str, custom_llm_provider: Optional[str] = None
) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_function_calling",
    )

def supports_tool_choice(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return _supports_factory(
        model=model, custom_llm_provider=custom_llm_provider, key="supports_tool_choice"
    )

def _supports_provider_info_factory(
    model: str, custom_llm_provider: Optional[str], key: str
) -> Optional[Literal[True]]:
    provider_info = get_provider_info(model=model, custom_llm_provider=custom_llm_provider)
    if provider_info is not None and provider_info.get(key, False) is True:
        return True
    return None

def _supports_factory(model: str, custom_llm_provider: Optional[str], key: str) -> bool:
    try:
        model, custom_llm_provider, _, _ = get_llm_provider(model=model, custom_llm_provider=custom_llm_provider)
        model_info = _get_model_info_helper(model=model, custom_llm_provider=custom_llm_provider)
        if model_info.get(key, False) is True:
            return True
        elif model_info.get(key) is None:  # don't check if 'False' explicitly set
            bare_model_key = _get_model_cost_key(model)
            if bare_model_key is not None:
                bare_entry = model_cost.get(bare_model_key) or {}
                if bare_entry.get(key, False) is True:
                    return True

            supported_by_provider = _supports_provider_info_factory(
                model, custom_llm_provider, key
            )
            if supported_by_provider is not None:
                return supported_by_provider

        return False
    except Exception as e:
        log.debug(f"Model not found or error in checking {key} support. You passed model={model}, custom_llm_provider={custom_llm_provider}. Error: {str(e)}")
        supported_by_provider = _supports_provider_info_factory(
            model, custom_llm_provider, key
        )
        if supported_by_provider is not None:
            return supported_by_provider

        return False

def supports_audio_input(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return _supports_factory(
        model=model, custom_llm_provider=custom_llm_provider, key="supports_audio_input"
    )

def supports_pdf_input(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return _supports_factory(
        model=model, custom_llm_provider=custom_llm_provider, key="supports_pdf_input"
    )

def supports_audio_output(
    model: str, custom_llm_provider: Optional[str] = None
) -> bool:
    return _supports_factory(
        model=model, custom_llm_provider=custom_llm_provider, key="supports_audio_output"
    )

def supports_prompt_caching(
    model: str, custom_llm_provider: Optional[str] = None
) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_prompt_caching",
    )

def supports_computer_use(
    model: str, custom_llm_provider: Optional[str] = None
) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_computer_use",
    )

def supports_vision(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_vision",
    )


def supports_reasoning(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return _supports_factory(
        model=model, custom_llm_provider=custom_llm_provider, key="supports_reasoning"
    )

def supports_native_structured_output(
    model: str, custom_llm_provider: Optional[str] = None
) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_native_structured_output",
    )

def get_supported_regions(
    model: str, custom_llm_provider: Optional[str] = None
) -> Optional[List[str]]:
    try:
        model, custom_llm_provider, _, _ = get_llm_provider(model=model, custom_llm_provider=custom_llm_provider)
        model_info = _get_model_info_helper(model=model, custom_llm_provider=custom_llm_provider)
        model_key = model_info.get("key")
        if model_key is None:
            return None

        model_cost_data = model_cost.get(model_key, {})
        supported_regions = model_cost_data.get("supported_regions", None)
        if supported_regions is None:
            return None

        if isinstance(supported_regions, list):
            return supported_regions
        else:
            return None
    except Exception as e:
        log.debug(
            f"Model not found or error in checking supported_regions support. You passed model={model}, custom_llm_provider={custom_llm_provider}. Error: {str(e)}"
        )
        return None


def supports_embedding_image_input(
    model: str, custom_llm_provider: Optional[str] = None
) -> bool:
    return _supports_factory(
        model=model,
        custom_llm_provider=custom_llm_provider,
        key="supports_embedding_image_input",
    )

def _get_model_cost_key(potential_key: str) -> Optional[str]:
    global _model_cost_lowercase_map

    if potential_key in litellm.model_cost:
        return potential_key

    if _model_cost_lowercase_map is None:
        _model_cost_lowercase_map = _rebuild_model_cost_lowercase_map()

    potential_key_lower = potential_key.lower()
    matched_key = _model_cost_lowercase_map.get(potential_key_lower)
    if matched_key is not None and matched_key in litellm.model_cost:
        return matched_key

    if matched_key is not None:
        matched_key = _handle_stale_map_entry_rebuild(potential_key_lower)
        if matched_key is not None:
            return matched_key

    return None

def _rebuild_model_cost_lowercase_map() -> Dict[str, str]:
    global _model_cost_lowercase_map
    _model_cost_lowercase_map = {k.lower(): k for k in litellm.model_cost}
    return _model_cost_lowercase_map

def _handle_stale_map_entry_rebuild(
    potential_key_lower: str,
) -> Optional[str]:
    global _model_cost_lowercase_map
    _model_cost_lowercase_map = _rebuild_model_cost_lowercase_map()
    matched_key = _model_cost_lowercase_map.get(potential_key_lower)
    if matched_key is not None and matched_key in litellm.model_cost:
        return matched_key
    return None