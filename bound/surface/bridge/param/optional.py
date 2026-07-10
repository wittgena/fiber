# bound.surface.bridge.param.optional
## @lineage: bound.surface.bridge.channel.param.optional
## @lineage: bound.channel.param.optional
## @lineage: bound.bridge.channel.param.optional
## @lineage: bound.channel.action.param.optional
## @lineage: bound.channel.client.action.param.optional
import copy
import inspect
import io
import json
import os
import random
import re
import struct
import sys
from dataclasses import dataclass, field
from functools import lru_cache, wraps
from os.path import abspath, dirname, join
import httpx
import openai
import tiktoken
from httpx import Proxy
from openai.lib import _parsing, _pydantic
from pydantic import BaseModel
from tiktoken import Encoding
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, TypeVar, Union

from bound.surface.exception import UnsupportedParamsError
from bound.surface.bridge.param.format import type_to_response_format_param
from anchor.registry.model.config.base import BaseConfig

from anchor.registry.model.config.resolver import config
from anchor.registry.model.config.constants import DEFAULT_CHAT_COMPLETION_PARAM_VALUES

from bound.surface.legacy.anthropic import AnthropicThinkingParam
from bound.surface.legacy.openai.types import AllMessageValues, OpenAIWebSearchOptions
from bound.surface.legacy.types import Embedding, Function, ProviderTypes
from anchor.registry.router.config import ProviderConfigManager
from anchor.registry.model.support import get_supported_openai_params

from watcher.plane.emitter import get_emitter

CustomLogger = Any

log = get_emitter("provider.optional_params")

T = TypeVar("T")

def get_nested_value(data: Dict[str, Any], key_path: str, default: Optional[T] = None) -> Optional[T]:
    if not key_path:
        return default

    # Remove metadata. prefix if it exists
    key_path = (
        key_path.replace("metadata.", "", 1)
        if key_path.startswith("metadata.")
        else key_path
    )

    # Split the key path into parts, respecting escaped dots (\.)
    # Use a temporary placeholder, split on unescaped dots, then restore
    placeholder = "\x00"
    parts = key_path.replace("\\.", placeholder).split(".")
    parts = [p.replace(placeholder, ".") for p in parts]

    # Traverse through the dictionary
    current: Any = data
    for part in parts:
        try:
            current = current[part]
        except (KeyError, TypeError):
            return default

    # If default is None, we can return any type
    if default is None:
        return current

    # Otherwise, ensure the type matches the default
    return current if isinstance(current, type(default)) else default


def _parse_path_segments(path: str) -> list:
    import re
    pattern = r"[^\.\[]+|\[[^\]]*\]"
    segments = re.findall(pattern, path)
    return segments


def _delete_nested_value_custom(
    data: Union[Dict[str, Any], List[Any]],
    segments: list,
    segment_index: int = 0,
) -> None:
    if segment_index >= len(segments):
        return

    segment = segments[segment_index]
    is_last = segment_index == len(segments) - 1

    # Handle array wildcard: [*]
    if segment == "[*]":
        if isinstance(data, list):
            for item in data:
                if is_last:
                    # Can't delete array elements themselves, skip
                    pass
                else:
                    # Only recurse if item is a dict or list (nested structure)
                    if isinstance(item, (dict, list)):
                        _delete_nested_value_custom(item, segments, segment_index + 1)
        return

    # Handle array index: [0], [1], [2], etc.
    if segment.startswith("[") and segment.endswith("]"):
        try:
            index = int(segment[1:-1])
            if isinstance(data, list) and 0 <= index < len(data):
                if is_last:
                    # Can't delete array elements themselves, skip
                    pass
                else:
                    # Only recurse if element is a dict or list (nested structure)
                    element = data[index]
                    if isinstance(element, (dict, list)):
                        _delete_nested_value_custom(
                            element, segments, segment_index + 1
                        )
        except (ValueError, IndexError):
            # Invalid index, skip
            pass
        return

    # Handle regular field navigation
    if isinstance(data, dict):
        if is_last:
            # Delete the field
            data.pop(segment, None)
        else:
            # Navigate deeper
            if segment in data:
                next_segment = (
                    segments[segment_index + 1]
                    if segment_index + 1 < len(segments)
                    else None
                )

                # If next segment is array notation, current field should be list
                if next_segment and (next_segment.startswith("[")):
                    if isinstance(data[segment], list):
                        _delete_nested_value_custom(
                            data[segment], segments, segment_index + 1
                        )
                # Otherwise navigate into dict
                elif isinstance(data[segment], dict):
                    _delete_nested_value_custom(
                        data[segment], segments, segment_index + 1
                    )


def delete_nested_value(
    data: Dict[str, Any],
    path: str,
    depth: int = 0,
    max_depth: int = 20,
) -> Dict[str, Any]:
    import copy
    result = copy.deepcopy(data)

    try:
        # Parse path into segments
        segments = _parse_path_segments(path)

        if not segments:
            return result

        # Delete using custom recursive implementation
        _delete_nested_value_custom(result, segments, 0)

    except Exception:
        # Invalid path or parsing error - silently skip
        pass

    return result

def is_nested_path(path: str) -> bool:
    return "." in path or "[" in path


def get_optional_params(
    model: str,
    functions=None,
    function_call=None,
    temperature=None,
    top_p=None,
    n=None,
    stream=False,
    stream_options=None,
    stop=None,
    max_tokens=None,
    max_completion_tokens=None,
    modalities=None,
    prediction=None,
    audio=None,
    presence_penalty=None,
    frequency_penalty=None,
    logit_bias=None,
    user=None,
    custom_llm_provider="",
    response_format=None,
    seed=None,
    tools=None,
    tool_choice=None,
    max_retries=None,
    logprobs=None,
    top_logprobs=None,
    extra_headers=None,
    api_version=None,
    parallel_tool_calls=None,
    drop_params=None,
    allowed_openai_params: Optional[List[str]] = None,
    reasoning_effort=None,
    verbosity=None,
    additional_drop_params=None,
    messages: Optional[List[AllMessageValues]] = None,
    thinking: Optional[AnthropicThinkingParam] = None,
    web_search_options: Optional[OpenAIWebSearchOptions] = None,
    safety_identifier: Optional[str] = None,
    base_model: Optional[str] = None,
    **kwargs,
):
    passed_params = locals().copy()
    special_params = passed_params.pop("kwargs")
    passed_params.pop("base_model", None)
    provider_config: Optional[BaseConfig] = None

    if custom_llm_provider is not None and custom_llm_provider in [provider.value for provider in ProviderTypes]:
        provider_config = ProviderConfigManager.get_provider_chat_config(
            model=model,
            provider=ProviderTypes(custom_llm_provider),
            base_model=base_model,
        )

    non_default_params = pre_process_non_default_params(
        passed_params=passed_params,
        special_params=special_params,
        custom_llm_provider=custom_llm_provider,
        additional_drop_params=additional_drop_params,
        model=model,
        provider_config=provider_config,
    )
    optional_params = pre_process_optional_params(
        passed_params=passed_params,
        non_default_params=non_default_params,
        custom_llm_provider=custom_llm_provider,
    )

    def _check_valid_arg(supported_params: List[str]):
        log.info(f"\nLiteLLM completion() model= {model}; provider = {custom_llm_provider}")
        # log.debug(f"\nLiteLLM: Params passed to completion() {passed_params}")
        # log.debug(f"\nLiteLLM: Non-Default params passed to completion() {non_default_params}")
        unsupported_params = {}
        for k in non_default_params.keys():
            if k not in supported_params:
                if k == "user" or k == "stream_options" or k == "stream":
                    continue
                if k == "n" and n == 1:  # langchain sends n=1 as a default value
                    continue  # skip this param
                if (k == "max_retries"):
                    continue  # skip this param
                else:
                    unsupported_params[k] = non_default_params[k]

        if unsupported_params:
            if config.drop_params is True or (drop_params is not None and drop_params is True):
                for k in unsupported_params.keys():
                    non_default_params.pop(k, None)
            else:
                raise UnsupportedParamsError(
                    status_code=500,
                    message=f"{custom_llm_provider} does not support parameters: {list(unsupported_params.keys())}, for model={model}. To drop these, set `litellm.drop_params=True` or for proxy:\n\n`litellm_settings:\n drop_params: true`\n. \n If you want to use these params dynamically send allowed_openai_params={list(unsupported_params.keys())} in your request.",
                )

    supported_params = get_supported_openai_params(model=model, custom_llm_provider=custom_llm_provider, base_model=base_model)
    if supported_params is None:
        supported_params = get_supported_openai_params(model=model, custom_llm_provider="openai")

    supported_params = supported_params or []
    allowed_openai_params = allowed_openai_params or []
    supported_params.extend(allowed_openai_params)
    _check_valid_arg(supported_params=supported_params or [],)

    ## raise exception if provider doesn't support passed in param
    if custom_llm_provider == "anthropic":
        ## check if unsupported param passed in
        optional_params = config.AnthropicConfig().map_openai_params(
            model=model,
            non_default_params=non_default_params,
            optional_params=optional_params,
            drop_params=(
                drop_params
                if drop_params is not None and isinstance(drop_params, bool)
                else False
            ),
        )
    elif custom_llm_provider == "huggingface":
        optional_params = config.HuggingFaceChatConfig().map_openai_params(
            non_default_params=non_default_params,
            optional_params=optional_params,
            model=model,
            drop_params=(
                drop_params
                if drop_params is not None and isinstance(drop_params, bool)
                else False
            ),
        )
    elif custom_llm_provider == "gemini":
        optional_params = config.GoogleAIStudioGeminiConfig().map_openai_params(
            non_default_params=non_default_params,
            optional_params=optional_params,
            model=model,
            drop_params=(
                drop_params
                if drop_params is not None and isinstance(drop_params, bool)
                else False
            ),
        )
    elif custom_llm_provider == "ollama":
        optional_params = config.OllamaConfig().map_openai_params(
            non_default_params=non_default_params,
            optional_params=optional_params,
            model=model,
            drop_params=(
                drop_params
                if drop_params is not None and isinstance(drop_params, bool)
                else False
            ),
        )
    elif custom_llm_provider == "openai":
        optional_params = config.OpenAIConfig().map_openai_params(
            non_default_params=non_default_params,
            optional_params=optional_params,
            model=model,
            drop_params=(
                drop_params
                if drop_params is not None and isinstance(drop_params, bool)
                else False
            ),
        )
    elif provider_config is not None:
        optional_params = provider_config.map_openai_params(
            non_default_params=non_default_params,
            optional_params=optional_params,
            model=model,
            drop_params=(
                drop_params
                if drop_params is not None and isinstance(drop_params, bool)
                else False
            ),
        )
    else:  # assume passing in params for openai-like api
        optional_params = config.OpenAILikeChatConfig().map_openai_params(
            non_default_params=non_default_params,
            optional_params=optional_params,
            model=model,
            drop_params=(
                drop_params
                if drop_params is not None and isinstance(drop_params, bool)
                else False
            ),
        )
    # if user passed in non-default kwargs for specific providers/models, pass them along
    optional_params = add_provider_specific_params_to_optional_params(
        optional_params=optional_params,
        passed_params=passed_params,
        custom_llm_provider=custom_llm_provider,
        openai_params=list(DEFAULT_CHAT_COMPLETION_PARAM_VALUES.keys()),
        additional_drop_params=additional_drop_params,
    )
    optional_params = _apply_openai_param_overrides(
        optional_params=optional_params,
        non_default_params=non_default_params,
        allowed_openai_params=allowed_openai_params,
    )

    # Apply nested drops from additional_drop_params
    if additional_drop_params:
        is_nested_path = getattr(sys.modules[__name__], "is_nested_path")
        delete_nested_value = getattr(sys.modules[__name__], "delete_nested_value")
        nested_paths = [p for p in additional_drop_params if is_nested_path(p)]
        for path in nested_paths:
            optional_params = delete_nested_value(optional_params, path)

    return optional_params

def pre_process_non_default_params(
    passed_params: dict,
    special_params: dict,
    custom_llm_provider: str,
    additional_drop_params: Optional[List[str]],
    model: str,
    remove_sensitive_keys: bool = False,
    add_provider_specific_params: bool = False,
    provider_config: Optional[BaseConfig] = None,
) -> dict:
    """
    Pre-process non-default params to a standardized format
    """
    # retrieve all parameters passed to the function

    non_default_params = PreProcessNonDefaultParams.base_pre_process_non_default_params(
        passed_params=passed_params,
        special_params=special_params,
        custom_llm_provider=custom_llm_provider,
        additional_drop_params=additional_drop_params,
        default_param_values=DEFAULT_CHAT_COMPLETION_PARAM_VALUES,
        additional_endpoint_specific_params=["messages"],
    )

    if "response_format" in non_default_params:
        if provider_config is not None:
            non_default_params["response_format"] = (
                provider_config.get_json_schema_from_pydantic_object(
                    response_format=non_default_params["response_format"]
                )
            )
        else:
            non_default_params["response_format"] = type_to_response_format_param(
                response_format=non_default_params["response_format"]
            )

    if "tools" in non_default_params and isinstance(
        non_default_params, list
    ):  # fixes https://github.com/BerriAI/litellm/issues/4933
        tools = non_default_params["tools"]
        for tool in tools:
            tool_function = tool.get("function", {})
            parameters = tool_function.get("parameters", None)
            if parameters is not None:
                new_parameters = copy.deepcopy(parameters)
                if (
                    "additionalProperties" in new_parameters
                    and new_parameters["additionalProperties"] is False
                ):
                    new_parameters.pop("additionalProperties", None)
                tool_function["parameters"] = new_parameters

    if add_provider_specific_params:
        non_default_params = add_provider_specific_params_to_optional_params(
            optional_params=non_default_params,
            passed_params=passed_params,
            custom_llm_provider=custom_llm_provider,
            openai_params=list(DEFAULT_CHAT_COMPLETION_PARAM_VALUES.keys()),
            additional_drop_params=additional_drop_params,
        )

    if remove_sensitive_keys:
        non_default_params = remove_sensitive_keys_from_dict(non_default_params)
    return non_default_params

def remove_sensitive_keys_from_dict(d: dict) -> dict:
    """Remove sensitive keys from a dictionary"""
    sensitive_key_phrases = ["key", "secret", "access", "credential"]
    remove_keys = []
    for key in d.keys():
        if any(phrase in key.lower() for phrase in sensitive_key_phrases):
            remove_keys.append(key)
    for key in remove_keys:
        d.pop(key)
    return d

def pre_process_optional_params(passed_params: dict, non_default_params: dict, custom_llm_provider: str) -> dict:
    """For .completion(), preprocess optional params"""
    optional_params: Dict = {}

    common_auth_dict = config.common_cloud_provider_auth_params
    if custom_llm_provider in common_auth_dict["providers"]:
        """
        Check if params = ["project", "region_name", "token"]
        and correctly translate for = ["azure", "vertex_ai", "watsonx", "aws"]
        """
        if custom_llm_provider == "azure":
            optional_params = config.AzureOpenAIConfig().map_special_auth_params(
                non_default_params=passed_params, optional_params=optional_params
            )
        elif custom_llm_provider == "bedrock":
            optional_params = (
                config.AmazonBedrockGlobalConfig().map_special_auth_params(
                    non_default_params=passed_params, optional_params=optional_params
                )
            )
        elif (
            custom_llm_provider == "vertex_ai"
            or custom_llm_provider == "vertex_ai_beta"
        ):
            optional_params = config.VertexAIConfig().map_special_auth_params(
                non_default_params=passed_params, optional_params=optional_params
            )
        elif custom_llm_provider == "watsonx":
            optional_params = config.IBMWatsonXAIConfig().map_special_auth_params(
                non_default_params=passed_params, optional_params=optional_params
            )

    ## raise exception if function calling passed in for a provider that doesn't support it
    if (
        "functions" in non_default_params
        or "function_call" in non_default_params
        or "tools" in non_default_params
    ):
        if (
            custom_llm_provider == "ollama"
            and custom_llm_provider != "text-completion-openai"
            and custom_llm_provider != "azure"
            and custom_llm_provider != "vertex_ai"
            and custom_llm_provider != "anyscale"
            and custom_llm_provider != "together_ai"
            and custom_llm_provider != "groq"
            and custom_llm_provider != "nvidia_nim"
            and custom_llm_provider != "cerebras"
            and custom_llm_provider != "xai"
            and custom_llm_provider != "ai21_chat"
            and custom_llm_provider != "volcengine"
            and custom_llm_provider != "deepseek"
            and custom_llm_provider != "codestral"
            and custom_llm_provider != "mistral"
            and custom_llm_provider != "anthropic"
            and custom_llm_provider != "cohere_chat"
            and custom_llm_provider != "cohere"
            and custom_llm_provider != "bedrock"
            and custom_llm_provider != "ollama_chat"
            and custom_llm_provider != "openrouter"
            and custom_llm_provider != "vercel_ai_gateway"
            and custom_llm_provider != "nebius"
            and custom_llm_provider != "wandb"
            and custom_llm_provider not in config.openai_compatible_providers
        ):
            if custom_llm_provider == "ollama":
                # ollama actually supports json output
                optional_params["format"] = "json"
                litellm.add_function_to_prompt = (
                    True  # so that main.py adds the function call to the prompt
                )
                if "tools" in non_default_params:
                    optional_params["functions_unsupported_model"] = (
                        non_default_params.pop("tools")
                    )
                    non_default_params.pop(
                        "tool_choice", None
                    )  # causes ollama requests to hang
                elif "functions" in non_default_params:
                    optional_params["functions_unsupported_model"] = (
                        non_default_params.pop("functions")
                    )
            elif (
                config.add_function_to_prompt
            ):  # if user opts to add it to prompt instead
                optional_params["functions_unsupported_model"] = non_default_params.pop(
                    "tools", non_default_params.pop("functions", None)
                )
            else:
                raise UnsupportedParamsError(
                    status_code=500,
                    message=f"Function calling is not supported by {custom_llm_provider}.",
                )

    return optional_params

def add_provider_specific_params_to_optional_params(
    optional_params: dict,
    passed_params: dict,
    custom_llm_provider: str,
    openai_params: List[str],
    additional_drop_params: Optional[list] = None,
) -> dict:
    if (
        custom_llm_provider
        in ["openai", "azure", "text-completion-openai"]
        + config.openai_compatible_providers
    ):
        if (_should_drop_param(k="extra_body", additional_drop_params=additional_drop_params) is False):
            extra_body = dict(passed_params.pop("extra_body", None) or {})
            for k in passed_params.keys():
                if k not in openai_params and passed_params[k] is not None:
                    extra_body[k] = passed_params[k]
            if not isinstance(optional_params.get("extra_body"), dict):
                optional_params["extra_body"] = {}
            initial_extra_body = {
                **optional_params["extra_body"],
                **extra_body,
            }

            if additional_drop_params is not None:
                processed_extra_body = {
                    k: v
                    for k, v in initial_extra_body.items()
                    if k not in additional_drop_params
                }
            else:
                processed_extra_body = initial_extra_body
            optional_params["extra_body"] = _ensure_extra_body_is_safe(extra_body=processed_extra_body)
    else:
        for k in passed_params.keys():
            if k not in openai_params and passed_params[k] is not None:
                if _should_drop_param(k=k, additional_drop_params=additional_drop_params):
                    continue
                optional_params[k] = passed_params[k]
    return optional_params

def _apply_openai_param_overrides(optional_params: dict, non_default_params: dict, allowed_openai_params: list):
    if allowed_openai_params:
        for param in allowed_openai_params:
            if param in optional_params:
                continue
            if param not in non_default_params:
                continue
            optional_params[param] = non_default_params.pop(param)
    return optional_params

def _should_drop_param(k, additional_drop_params) -> bool:
    if (
        additional_drop_params is not None
        and isinstance(additional_drop_params, list)
        and k in additional_drop_params
    ):
        return True  # allow user to drop specific params for a model - e.g. vllm - logit bias

    return False

def _ensure_extra_body_is_safe(extra_body: Optional[Dict]) -> Optional[Dict]:
    if extra_body is None:
        return None

    if not isinstance(extra_body, dict):
        return extra_body

    if "metadata" in extra_body and isinstance(extra_body["metadata"], dict):
        if "prompt" in extra_body["metadata"]:
            _prompt = extra_body["metadata"].get("prompt")

            # users can send Langfuse TextPromptClient objects, so we need to convert them to dicts
            # Langfuse TextPromptClients have .__dict__ attribute
            if _prompt is not None and hasattr(_prompt, "__dict__"):
                extra_body["metadata"]["prompt"] = _prompt.__dict__

    return extra_body

class PreProcessNonDefaultParams:
    @staticmethod
    def base_pre_process_non_default_params(
        passed_params: dict,
        special_params: dict,
        custom_llm_provider: str,
        additional_drop_params: Optional[List[str]],
        default_param_values: dict,
        additional_endpoint_specific_params: List[str],
    ) -> dict:
        for k, v in special_params.items():
            if k.startswith("aws_") and (
                custom_llm_provider != "bedrock"
                and not custom_llm_provider.startswith("sagemaker")
            ):  # allow dynamically setting boto3 init logic
                continue
            elif k == "hf_model_name" and custom_llm_provider != "sagemaker":
                continue
            elif (
                k.startswith("vertex_")
                and custom_llm_provider != "vertex_ai"
                and custom_llm_provider != "vertex_ai_beta"
            ):  # allow dynamically setting vertex ai init logic
                continue
            passed_params[k] = v

        # filter out those parameters that were passed with non-default values
        non_default_params = {
            k: v
            for k, v in passed_params.items()
            if (
                k != "model"
                and k != "custom_llm_provider"
                and k != "api_version"
                and k != "drop_params"
                and k != "allowed_openai_params"
                and k != "additional_drop_params"
                and k not in additional_endpoint_specific_params
                and k in default_param_values
                and v != default_param_values[k]
                and _should_drop_param(
                    k=k, additional_drop_params=additional_drop_params
                )
                is False
            )
        }

        return non_default_params

    @staticmethod
    def embedding_pre_process_non_default_params(
        passed_params: dict,
        special_params: dict,
        custom_llm_provider: str,
        additional_drop_params: Optional[List[str]],
        model: str,
        remove_sensitive_keys: bool = False,
        add_provider_specific_params: bool = False,
    ) -> dict:
        non_default_params = (
            PreProcessNonDefaultParams.base_pre_process_non_default_params(
                passed_params=passed_params,
                special_params=special_params,
                custom_llm_provider=custom_llm_provider,
                additional_drop_params=additional_drop_params,
                default_param_values={k: None for k in OPENAI_EMBEDDING_PARAMS},
                additional_endpoint_specific_params=["input"],
            )
        )

        return non_default_params