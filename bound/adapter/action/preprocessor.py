# bound.adapter.action.preprocessor
## @lineage: bound.transport.channel.action.preprocessor
## @lineage: bound.channel.action.preprocessor
## @lineage: bound.channel.client.action.preprocessor
from __future__ import annotations
import asyncio
import contextvars
import datetime
import inspect
import json
import os
import random
import sys
import time
import traceback
import httpx
from copy import deepcopy
from functools import partial
from typing import Any, Dict, List, Literal, Callable, Optional, Tuple, Type, Union, cast
from dataclasses import dataclass, field

from bound.surface.legacy.config.constants import COMPLETION_HTTP_FALLBACK_SECONDS, DEFAULT_REQUEST_TIMEOUT_SECONDS
from bound.surface.legacy.types import EmbeddingResponse
from anchor.bind.switch.params import ModelResponse
from bound.surface.legacy.types import all_litellm_params
from bound.surface.legacy.config.resolver import config
from anchor.registry.model.support import supports_httpx_timeout
from anchor.registry.router.config import ProviderConfigManager
from anchor.registry.router.locator import get_llm_provider
from bound.surface.legacy.provider import ProviderTypes
from bound.surface.legacy.openai.types import AllMessageValues
from bound.surface.bridge.param.optional import get_optional_params
from bound.surface.bridge.param.litellm import get_litellm_params
from bound.surface.bridge.param.validator import (
    validate_and_fix_openai_messages,
    validate_and_fix_openai_tools,
    validate_and_fix_thinking_param,
    validate_chat_completion_tool_choice,
    validate_openai_optional_params
)
from bound.watcher.delegator import LogDelegator
from watcher.plane.emitter import get_emitter

log = get_emitter("action.preprocessor")

def get_non_default_completion_params(kwargs: dict) -> dict:
    openai_params = config.OPENAI_CHAT_COMPLETION_PARAMS
    default_params = openai_params + all_litellm_params
    return {k: v for k, v in kwargs.items() if k not in default_params}

def _should_allow_input_examples(custom_llm_provider: Optional[str], model: str) -> bool:
    return True

def _drop_input_examples_from_tool(tool: dict) -> dict:
    tool_copy = tool.copy()
    tool_copy.pop("input_examples", None)
    function = tool_copy.get("function")
    if isinstance(function, dict):
        function = function.copy()
        function.pop("input_examples", None)
        tool_copy["function"] = function
    return tool_copy

def _drop_input_examples_from_tools(tools: Optional[List[dict]]) -> Optional[List[dict]]:
    if tools is None:
        return None
    return [_drop_input_examples_from_tool(tool) if isinstance(tool, dict) else tool for tool in tools]

@dataclass
class CompletionContext:
    """전처리가 완료된 안전한 상태 벡터"""
    model: str
    messages: List[Any]
    custom_llm_provider: str
    api_key: Optional[str]
    api_base: Optional[str]
    timeout: Any
    model_response: ModelResponse
    optional_params: dict
    litellm_params: dict
    headers: dict
    
    # 제어 및 유틸리티 플래그
    stream: Optional[bool]
    acompletion: bool
    shared_session: Optional[Any]
    client_instance: Optional[Any]
    logging_obj: Optional[LogDelegator]
    deployment_id: Optional[str]
    original_kwargs: dict  # 예외 처리를 위해 원본 보존

class CompletionPreprocessor:
    """모든 파라미터 검증, 매핑, Provider 추론을 담당하는 빌더"""
    def __init__(self, model: str, messages: List, kwargs: dict):
        self.model = model
        self.messages = messages
        self.kwargs = kwargs
        
        self.tools = kwargs.get("tools")
        self.tool_choice = kwargs.get("tool_choice")
        self.stop = kwargs.get("stop")
        self.thinking = kwargs.get("thinking")
        self.api_key = kwargs.get("api_key")
        self.api_base = kwargs.get("api_base") or kwargs.get("base_url")
        self.custom_llm_provider = kwargs.get("custom_llm_provider")
        self.deployment_id = kwargs.get("deployment_id")
        
        self.headers = kwargs.get("headers", {}) or {}
        if kwargs.get("extra_headers"):
            self.headers.update(kwargs.get("extra_headers"))

    def build(self) -> CompletionContext:
        self._validate_inputs()
        self._resolve_provider()
        self._prepare_messages()
        model_response, timeout = self._prepare_base_objects()
        optional_params, litellm_params = self._prepare_parameters()
        logging_obj = self._setup_logging(optional_params, litellm_params)

        return CompletionContext(
            model=self.model,
            messages=self.messages,
            custom_llm_provider=self.custom_llm_provider,
            api_key=self.api_key,
            api_base=self.api_base,
            timeout=timeout,
            model_response=model_response,
            optional_params=optional_params,
            litellm_params=litellm_params,
            headers=self.headers,
            stream=self.kwargs.get("stream"),
            acompletion=self.kwargs.get("acompletion", False),
            shared_session=self.kwargs.get("shared_session"),
            client_instance=self.kwargs.get("client"),
            logging_obj=logging_obj,
            deployment_id=self.deployment_id,
            original_kwargs=self.kwargs
        )

    def _validate_inputs(self):
        self.messages = validate_and_fix_openai_messages(messages=self.messages)
        self.tools = validate_and_fix_openai_tools(tools=self.tools)
        self.tool_choice = validate_chat_completion_tool_choice(tool_choice=self.tool_choice)
        self.stop = validate_openai_optional_params(stop=self.stop)
        self.thinking = validate_and_fix_thinking_param(thinking=self.thinking)

    def _resolve_provider(self):
        if self.kwargs.get("azure", False) is True:
            self.custom_llm_provider = "azure"
        if self.deployment_id is not None:
            self.model = self.deployment_id
            self.custom_llm_provider = "azure"

        effective_api_key = None if self.api_key == "not-needed" else self.api_key
        self.model, self.custom_llm_provider, dynamic_api_key, self.api_base = get_llm_provider(
            model=self.model,
            custom_llm_provider=self.custom_llm_provider,
            api_base=self.api_base,
            api_key=effective_api_key,
        )
        self.api_key = dynamic_api_key or effective_api_key
        
        if not _should_allow_input_examples(custom_llm_provider=self.custom_llm_provider, model=self.model):
            self.tools = _drop_input_examples_from_tools(tools=self.tools)

    def _prepare_messages(self):
        base_model = self.kwargs.get("base_model") or (self.kwargs.get("model_info", {}).get("base_model"))
        provider_config = None
        if self.custom_llm_provider in [p.value for p in ProviderTypes]:
            provider_config = ProviderConfigManager.get_provider_chat_config(
                model=self.model, provider=ProviderTypes(self.custom_llm_provider), base_model=base_model,
            )

        if provider_config is not None:
            self.messages = provider_config.translate_developer_role_to_system_role(messages=self.messages)

        if self.kwargs.get("supports_system_message") is False:
            self.messages = map_system_message_pt(messages=self.messages)

        if self.kwargs.get("litellm_system_prompt"):
            self.messages = add_system_prompt_to_messages(
                messages=self.messages, system_prompt=self.kwargs.get("litellm_system_prompt"), merge_with_first_system=True
            )

    def _prepare_base_objects(self) -> Tuple[ModelResponse, Any]:
        model_response = ModelResponse()
        setattr(model_response, "usage", config.Usage())
        if hasattr(model_response, "_hidden_params"):
            model_response._hidden_params["custom_llm_provider"] = self.custom_llm_provider
            model_response._hidden_params["region_name"] = self.kwargs.get("aws_region_name", None)

        timeout = CompletionTimeout.resolve(
            self.kwargs.get("timeout"), self.kwargs, self.custom_llm_provider, 
            global_timeout=config.request_timeout, supports_httpx_timeout=supports_httpx_timeout,
        )
        return model_response, timeout

    def _prepare_parameters(self) -> Tuple[dict, dict]:
        base_model = self.kwargs.get("base_model") or (self.kwargs.get("model_info", {}).get("base_model"))
        optional_param_args = {
            "model": self.model,
            "custom_llm_provider": self.custom_llm_provider,
            "base_model": base_model,
            "max_retries": self.kwargs.get("max_retries", self.kwargs.get("num_retries")),
        }
        
        allowed_keys = [
            "functions", "function_call", "temperature", "top_p", "n", "stream", "stream_options", 
            "stop", "max_tokens", "max_completion_tokens", "modalities", "prediction", "audio", 
            "presence_penalty", "frequency_penalty", "logit_bias", "user", "response_format", 
            "seed", "tools", "tool_choice", "logprobs", "top_logprobs", "parallel_tool_calls", 
            "reasoning_effort", "thinking", "web_search_options", "safety_identifier", "service_tier"
        ]
        optional_param_args.update({k: v for k, v in self.kwargs.items() if k in allowed_keys})
        
        non_default_params = get_non_default_completion_params(kwargs=self.kwargs)
        optional_params = get_optional_params(**optional_param_args, **non_default_params)
        
        safe_kwargs = self.kwargs.copy()
        for key in ["acompletion", "api_key", "custom_llm_provider", "api_base"]:
            safe_kwargs.pop(key, None)

        litellm_params = get_litellm_params(
            acompletion=self.kwargs.get("acompletion", False), 
            api_key=self.api_key, 
            custom_llm_provider=self.custom_llm_provider, 
            api_base=self.api_base, 
            **safe_kwargs
        )
        return optional_params, litellm_params

    def _setup_logging(self, optional_params: dict, litellm_params: dict) -> Optional[LogDelegator]:
        logging_obj = cast(LogDelegator, self.kwargs.get("log_delegator"))
        if logging_obj:
            logging_obj.update_environment_variables(
                model=self.model, user=self.kwargs.get("user"), optional_params=optional_params, 
                litellm_params=litellm_params, custom_llm_provider=self.custom_llm_provider,
            )
        return logging_obj

def add_system_prompt_to_messages(
    messages: List[AllMessageValues],
    system_prompt: str,
    merge_with_first_system: bool = False,
) -> List[AllMessageValues]:
    if not system_prompt:
        return list(messages)

    if merge_with_first_system and messages and messages[0].get("role") == "system":
        first = dict(messages[0])
        existing_content = first.get("content", "")
        merged_content: Union[str, List[Dict[str, str]]]
        if isinstance(existing_content, str):
            merged_content = f"{system_prompt.strip()}\n\n{existing_content}"
        elif isinstance(existing_content, list):
            merged_content = [{"type": "text", "text": system_prompt.strip()}] + list(
                existing_content
            )
        else:
            merged_content = [{"type": "text", "text": system_prompt.strip()}]
        first["content"] = merged_content
        return [cast(AllMessageValues, first)] + list(messages[1:])

    system_message: AllMessageValues = {"role": "system", "content": system_prompt}
    return [system_message, *messages]

def map_system_message_pt(messages: list) -> list:
    new_messages = []
    for i, m in enumerate(messages):
        if m["role"] == "system":
            if i < len(messages) - 1:
                next_m = messages[i + 1]
                next_role = next_m["role"]
                if (next_role == "user" or next_role == "assistant"):
                    next_m["content"] = m["content"] + " " + next_m["content"]
                elif next_role == "system":
                    new_message = {"role": "user", "content": m["content"]}
                    new_messages.append(new_message)
            else:
                new_message = {"role": "user", "content": m["content"]}
                new_messages.append(new_message)
        else:
            new_messages.append(m)
    return new_messages

@dataclass
class EmbeddingContext:
    """@manifold: Normalized Embedding State Boundary"""
    model: str
    input: Union[str, List[str]]
    custom_llm_provider: str
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    timeout: Union[float, int] = 60.0
    aembedding: bool = False
    optional_params: Dict[str, Any] = field(default_factory=dict)
    model_response: EmbeddingResponse = field(default_factory=EmbeddingResponse)
    logging_obj: Any = None
    original_kwargs: Dict[str, Any] = field(default_factory=dict)

class EmbeddingPreprocessor:
    """@flow: Raw kwargs -> Normalized EmbeddingContext"""
    def __init__(self, model: str, input: Union[str, List[str]], kwargs: dict):
        self.model = model
        self.input = input
        self.kwargs = kwargs

    def build(self) -> EmbeddingContext:
        from anchor.registry.router.locator import get_llm_provider
        
        # 1. Provider 식별
        custom_llm_provider = self.kwargs.get("custom_llm_provider")
        api_base = self.kwargs.get("api_base")
        api_key = self.kwargs.get("api_key")
        
        _, resolved_provider, resolved_key, resolved_base = get_llm_provider(
            model=self.model,
            custom_llm_provider=custom_llm_provider,
            api_base=api_base,
            api_key=api_key
        )

        ## 파라미터 분리 (LiteLLM legacy params vs Model params)
        from bound.surface.bridge.param.embedding import get_optional_params_embeddings
        optional_params = get_optional_params_embeddings(
            model=self.model,
            custom_llm_provider=resolved_provider,
            **self.kwargs
        )

        return EmbeddingContext(
            model=self.model,
            input=self.input,
            custom_llm_provider=resolved_provider or "openai",
            api_key=resolved_key,
            api_base=resolved_base,
            timeout=self.kwargs.get("timeout", 60.0),
            aembedding=self.kwargs.get("aembedding", False),
            optional_params=optional_params,
            logging_obj=self.kwargs.get("log_delegator"),
            original_kwargs=self.kwargs
        )

class CompletionTimeout:
    @staticmethod
    def _fallback_when_no_explicit_timeout(
        global_timeout: Optional[Union[float, str]],
    ) -> float:
        if global_timeout is None:
            return COMPLETION_HTTP_FALLBACK_SECONDS
        if float(global_timeout) == float(DEFAULT_REQUEST_TIMEOUT_SECONDS):
            return COMPLETION_HTTP_FALLBACK_SECONDS
        return float(global_timeout)

    @staticmethod
    def resolve(
        model_timeout: Optional[Union[float, str, httpx.Timeout]],
        kwargs: dict,
        custom_llm_provider: str,
        *,
        global_timeout: Optional[Union[float, str]],
        supports_httpx_timeout: Callable[[str], bool],
    ) -> Union[float, httpx.Timeout]:
        resolved: Union[float, str, httpx.Timeout]
        if model_timeout is not None:
            resolved = model_timeout
        elif kwargs.get("timeout") is not None:
            resolved = kwargs["timeout"]
        elif kwargs.get("request_timeout") is not None:
            resolved = kwargs["request_timeout"]
        else:
            resolved = CompletionTimeout._fallback_when_no_explicit_timeout(
                global_timeout
            )

        if isinstance(resolved, httpx.Timeout) and not supports_httpx_timeout(custom_llm_provider):
            read_timeout = resolved.read
            resolved = (
                float(read_timeout)
                if read_timeout is not None
                else COMPLETION_HTTP_FALLBACK_SECONDS
            )
        elif not isinstance(resolved, httpx.Timeout):
            resolved = float(resolved)

        return resolved
