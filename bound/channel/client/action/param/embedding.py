# bound.channel.client.action.param.embedding
## @lineage: anchor.channel.client.action.param.embedding
## @lineage: anchor.channel.action.param.embedding
## @lineage: bound.channel.action.param.embedding
## @lineage: bound.channel.support.param.embedding
## @lineage: bound.channel.action.handler.param.embedding
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
    get_args,
)
from bound.channel.config.resolver import config
from bound.channel.config.constants import (
    DEFAULT_EMBEDDING_PARAM_VALUES,
    OPENAI_EMBEDDING_PARAMS,
)
from anchor.surface.exception import (
    AuthenticationError,
    BadRequestError,
    UnsupportedParamsError,
    MockException
)
from bound.channel.client.action.param.optional import PreProcessNonDefaultParams, add_provider_specific_params_to_optional_params
from anchor.provider.model.support import get_supported_openai_params
from watcher.plane.emitter import get_emitter

log = get_emitter("provider.param_embedding")

def get_optional_params_embeddings(
    model: str,
    user: Optional[str] = None,
    encoding_format: Optional[str] = None,
    dimensions: Optional[int] = None,
    custom_llm_provider="",
    drop_params: Optional[bool] = None,
    additional_drop_params: Optional[List[str]] = None,
    allowed_openai_params: Optional[List[str]] = None,
    **kwargs,
):
    passed_params = locals()
    custom_llm_provider = passed_params.pop("custom_llm_provider", None)
    special_params = passed_params.pop("kwargs")
    drop_params = passed_params.pop("drop_params", None)
    additional_drop_params = passed_params.pop("additional_drop_params", None)
    allowed_openai_params = passed_params.pop("allowed_openai_params", None) or []
    passed_params.pop("get_supported_openai_params", None)

    def _check_valid_arg(supported_params: Optional[list]):
        if supported_params is None:
            return
        unsupported_params = {}
        for k in non_default_params.keys():
            if k not in supported_params:
                unsupported_params[k] = non_default_params[k]
        if unsupported_params:
            if config.drop_params is True or (drop_params is not None and drop_params is True):
                pass
            else:
                raise UnsupportedParamsError(
                    status_code=500,
                    message=f"{custom_llm_provider} does not support parameters: {unsupported_params}, for model={model}. To drop these, set `litellm.drop_params=True` or for proxy:\n\n`litellm_settings:\n drop_params: true`\n",
                )

    non_default_params = (
        PreProcessNonDefaultParams.embedding_pre_process_non_default_params(
            passed_params=passed_params,
            special_params=special_params,
            custom_llm_provider=custom_llm_provider,
            additional_drop_params=additional_drop_params,
            model=model,
        )
    )

    provider_config: Optional[BaseEmbeddingConfig] = None
    optional_params = {}
    if provider_config is not None:
        supported_params: Optional[list] = provider_config.get_supported_openai_params(
            model=model
        )
        _check_valid_arg(supported_params=supported_params)
        optional_params = provider_config.map_openai_params(
            non_default_params=non_default_params,
            optional_params={},
            model=model,
            drop_params=drop_params if drop_params is not None else False,
        )
        if supported_params:
            for param in supported_params:
                if param in OPENAI_EMBEDDING_PARAMS:
                    continue
                if (
                    param in passed_params
                    and passed_params[param] is not None
                    and param not in optional_params
                ):
                    optional_params[param] = passed_params[param]
    elif custom_llm_provider == "openai":
        if (
            model is not None
            and "text-embedding-3" not in model
            and "dimensions" in non_default_params.keys()
            and "dimensions" not in (allowed_openai_params or [])
        ):
            if config.drop_params is True or drop_params is True:
                non_default_params.pop("dimensions", None)
            else:
                raise UnsupportedParamsError(
                    status_code=500,
                    message="Setting dimensions is not supported for OpenAI `text-embedding-3` and later models. To drop it from the call, set `litellm.drop_params = True`.",
                )
        optional_params = non_default_params
    elif custom_llm_provider == "vertex_ai" or custom_llm_provider == "gemini":
        supported_params = get_supported_openai_params(
            model=model,
            custom_llm_provider="vertex_ai",
            request_type="embeddings",
        )
        _check_valid_arg(supported_params=supported_params)
        (
            optional_params,
            kwargs,
        ) = config.VertexAITextEmbeddingConfig().map_openai_params(
            non_default_params=non_default_params, optional_params={}, kwargs=kwargs
        )
    elif custom_llm_provider == "ollama":
        if "dimensions" in non_default_params:
            optional_params["dimensions"] = non_default_params.pop("dimensions")
        if len(non_default_params.keys()) > 0:
            if (config.drop_params is True or drop_params is True):
                keys = list(non_default_params.keys())
                for k in keys:
                    non_default_params.pop(k, None)
            else:
                raise UnsupportedParamsError(
                    status_code=500,
                    message=f"Setting {non_default_params} is not supported by {custom_llm_provider}. To drop it from the call, set `litellm.drop_params = True`.",
                )
    elif (
        custom_llm_provider != "openai"
        and custom_llm_provider != "azure"
        and custom_llm_provider not in config.openai_compatible_providers
    ):
        if len(non_default_params.keys()) > 0:
            if (config.drop_params is True or drop_params is True):
                keys = list(non_default_params.keys())
                for k in keys:
                    non_default_params.pop(k, None)
            else:
                raise UnsupportedParamsError(
                    status_code=500,
                    message=f"Setting {non_default_params} is not supported by {custom_llm_provider}. To drop it from the call, set `litellm.drop_params = True`.",
                )
        else:
            optional_params = non_default_params
    else:
        optional_params = non_default_params

    final_params = add_provider_specific_params_to_optional_params(
        optional_params=optional_params,
        passed_params=passed_params,
        custom_llm_provider=custom_llm_provider,
        openai_params=list(DEFAULT_EMBEDDING_PARAM_VALUES.keys()),
        additional_drop_params=kwargs.get("additional_drop_params", None),
    )

    if "extra_body" in final_params and len(final_params["extra_body"]) == 0:
        final_params.pop("extra_body", None)

    return final_params
