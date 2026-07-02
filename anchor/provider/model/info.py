# anchor.provider.model.info
## @lineage: anchor.model.info
## @lineage: anchor.channel.compat.switch.model.info
from typing import Optional
from functools import lru_cache, wraps

from bound.channel.client.action.param.format import BaseLLMModelInfo, type_to_response_format_param
from bound.channel.config.constants import DEFAULT_MAX_LRU_CACHE_SIZE
from bound.channel.config.resolver import config

from bound.router.provider.config import get_provider_info
from anchor.provider.legacy.types import ModelInfo

def get_model_info(
    model: str,
    custom_llm_provider: Optional[str] = None,
    api_base: Optional[str] = None,
    api_key: Optional[str] = None,
) -> ModelInfo:
    if api_key is not None:
        return _build_model_info(model, custom_llm_provider, api_base, api_key)
    return _cached_get_model_info(model, custom_llm_provider, api_base)

def _build_model_info(
    model: str,
    custom_llm_provider: Optional[str] = None,
    api_base: Optional[str] = None,
    api_key: Optional[str] = None,
) -> ModelInfo:
    from anchor.surface.registry.provider import _get_model_info_helper

    supported_openai_params = config.get_supported_openai_params(
        model=model, custom_llm_provider=custom_llm_provider
    )
    _model_info = _get_model_info_helper(
        model=model,
        custom_llm_provider=custom_llm_provider,
        api_base=api_base,
        api_key=api_key,
    )
    provider_info = get_provider_info(model=model, custom_llm_provider=custom_llm_provider)
    if provider_info:
        for key, value in provider_info.items():
            if value is not None:
                _model_info[key] = value  # type: ignore
    return ModelInfo(**_model_info, supported_openai_params=supported_openai_params)

@lru_cache(maxsize=DEFAULT_MAX_LRU_CACHE_SIZE)
def _cached_get_model_info(
    model: str,
    custom_llm_provider: Optional[str] = None,
    api_base: Optional[str] = None,
) -> ModelInfo:
    return _build_model_info(model=model, custom_llm_provider=custom_llm_provider, api_base=api_base)