# bound.channel.client.action.support.base
## @lineage: anchor.channel.client.action.support.base
## @lineage: anchor.channel.action.support.base
## @lineage: bound.channel.action.support.base
## @lineage: bound.channel.support.api.base
## @lineage: bound.channel.api.get_api_base
## @lineage: bound.bridge.api.get_api_base
## @lineage: bound.client.api.get_api_base
from typing import Optional, Union
from bound.channel.config.resolver import config
from anchor.provider.router.locator import get_llm_provider
from anchor.provider.model.param.legacy import LiteLLM_Params
from watcher.plane.emitter import get_emitter

log = get_emitter("api.base")

def get_api_base(
    model: str, optional_params: Union[dict, LiteLLM_Params]
) -> Optional[str]:
    try:
        if isinstance(optional_params, LiteLLM_Params):
            _optional_params = optional_params
        elif "model" in optional_params:
            _optional_params = LiteLLM_Params(**optional_params)
        else:  # prevent needing to copy and pop the dict
            _optional_params = LiteLLM_Params(
                model=model, **optional_params
            )  # convert to pydantic object
    except Exception:
        return None

    if _optional_params.api_base is not None:
        return _optional_params.api_base

    if config.model_alias_map and model in config.model_alias_map:
        model = config.model_alias_map[model]

    try:
        (
            model,
            custom_llm_provider,
            dynamic_api_key,
            dynamic_api_base,
        ) = get_llm_provider(
            model=model,
            custom_llm_provider=_optional_params.custom_llm_provider,
            api_base=_optional_params.api_base,
            api_key=_optional_params.api_key,
        )
    except Exception as e:
        log.debug("Error occurred in getting api base - {}".format(str(e)))
        custom_llm_provider = None
        dynamic_api_base = None

    if dynamic_api_base is not None:
        return dynamic_api_base

    stream: bool = getattr(optional_params, "stream", False)

    if (
        _optional_params.vertex_location is not None
        and _optional_params.vertex_project is not None
    ):
        if stream:
            _api_base = "{}-aiplatform.googleapis.com/v1/projects/{}/locations/{}/publishers/google/models/{}:streamGenerateContent".format(
                _optional_params.vertex_location,
                _optional_params.vertex_project,
                _optional_params.vertex_location,
                model,
            )
        else:
            _api_base = "{}-aiplatform.googleapis.com/v1/projects/{}/locations/{}/publishers/google/models/{}:generateContent".format(
                _optional_params.vertex_location,
                _optional_params.vertex_project,
                _optional_params.vertex_location,
                model,
            )
        return _api_base

    if custom_llm_provider is None:
        return None

    if custom_llm_provider == "gemini":
        if stream:
            _api_base = "https://generativelanguage.googleapis.com/v1beta/models/{}:streamGenerateContent".format(
                model
            )
        else:
            _api_base = "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent".format(
                model
            )
        return _api_base
    elif custom_llm_provider == "openai":
        _api_base = "https://api.openai.com"
        return _api_base
    return None
