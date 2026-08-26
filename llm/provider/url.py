# llm.provider.url
from typing import Optional, Union

from fiber.llm.model.types.param.legacy import LegacyParams
from fiber.llm.provider.registry import get_llm_provider

from xphi.arch.model.config import config
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("provider.url")

def get_api_base(model: str, optional_params: Union[dict, LegacyParams]) -> Optional[str]:
    """@desc: Resolves the dynamic API Base URL based on the model, provider, and runtime parameters"""
    if isinstance(optional_params, LegacyParams):
        params = optional_params
    elif isinstance(optional_params, dict):
        try:
            params_dict = optional_params.copy()
            if "model" not in params_dict:
                params_dict["model"] = model
            params = LegacyParams(**params_dict)
        except Exception as e:
            log.debug(f"Failed to parse optional_params into LiteLLM_Params: {e}")
            return None
    else:
        return None

    ## Explicit API Base Override
    if params.api_base is not None:
        return params.api_base

    ## Model Alias Resolution
    resolved_model = config.model_alias_map.get(model, model) if config.model_alias_map else model

    ## Dynamic Provider Inference
    try:
        _, provider, _, dynamic_api_base = get_llm_provider(
            model=resolved_model,
            custom_llm_provider=params.custom_llm_provider,
            api_base=params.api_base,
            api_key=params.api_key,
        )
    except Exception as e:
        log.debug(f"Error inferring LLM provider for api_base resolution: {e}")
        provider, dynamic_api_base = None, None

    if dynamic_api_base is not None:
        return dynamic_api_base

    ## Stream endpoint suffix determination
    is_stream = getattr(optional_params, "stream", False) or getattr(params, "stream", False)
    content_endpoint = "streamGenerateContent" if is_stream else "generateContent"

    ## Vertex AI Routing
    if params.vertex_location and params.vertex_project:
        return (
            f"{params.vertex_location}-aiplatform.googleapis.com/v1/"
            f"projects/{params.vertex_project}/locations/{params.vertex_location}/"
            f"publishers/google/models/{resolved_model}:{content_endpoint}"
        )

    ## Standard Provider Routing
    if not provider:
        return None

    if provider == "gemini":
        return f"https://generativelanguage.googleapis.com/v1beta/models/{resolved_model}:{content_endpoint}"
        
    if provider == "openai":
        return "https://api.openai.com"

    return None