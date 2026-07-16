# bound.surface.legacy.action.completion
## @lineage: bound.adapter.surface.legacy.action.completion
import uuid
import httpx
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
from copy import deepcopy
from functools import partial
from typing import Any, Dict, List, Literal, Optional, Tuple, Type, Union, cast

from bound.adapter.switch.params import ModelResponse

from bound.adapter.mapper.exception import exception_type
from bound.surface.legacy.action.process.core import async_core_completion
from bound.surface.exception import Timeout
from bound.surface.legacy.action.client.wrapper import client
from bound.transport.adapter.asyncify import run_async_function
from bound.transport.stream.wrapper import StreamWrapper

from bound.surface.legacy.trace.dd import tracer
from bound.watcher.delegator import LogDelegator

from watcher.plane.emitter import get_emitter

log = get_emitter("action.completion")

def filter_internal_params(data: dict, additional_internal_params: Optional[set] = None) -> dict:
    if not isinstance(data, dict):
        return data

    internal_params = {
        "skip_mcp_handler",
        "mcp_handler_context",
        "_skip_mcp_handler",
    }
    if additional_internal_params:
        internal_params.update(additional_internal_params)
    return {k: v for k, v in data.items() if k not in internal_params}

def safe_deep_copy(data):
    import copy
    if config.safe_memory_mode is True:
        return data

    litellm_parent_otel_span: Optional[Any] = None
    litellm_parent_otel_span = None
    if isinstance(data, dict):
        if "metadata" in data and "litellm_parent_otel_span" in data["metadata"]:
            litellm_parent_otel_span = data["metadata"].pop("litellm_parent_otel_span")
            data["metadata"]["litellm_parent_otel_span"] = "placeholder"

        if ("litellm_metadata" in data and "litellm_parent_otel_span" in data["litellm_metadata"]):
            litellm_parent_otel_span = data["litellm_metadata"].pop("litellm_parent_otel_span")
            data["litellm_metadata"]["litellm_parent_otel_span"] = "placeholder"

    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            try:
                new_data[k] = copy.deepcopy(v)
            except Exception:
                new_data[k] = v
    else:
        try:
            new_data = copy.deepcopy(data)
        except Exception:
            new_data = data

    if isinstance(data, dict) and litellm_parent_otel_span is not None:
        if "metadata" in data and "litellm_parent_otel_span" in data["metadata"]:
            data["metadata"]["litellm_parent_otel_span"] = litellm_parent_otel_span
        if ("litellm_metadata" in data and "litellm_parent_otel_span" in data["litellm_metadata"]):
            data["litellm_metadata"]["litellm_parent_otel_span"] = litellm_parent_otel_span
    return new_data

class Completions:
    def __init__(self, params, router_obj: Optional[Any]):
        self.params = params
        self.router_obj = router_obj

    def create(self, messages, model=None, **kwargs):
        for k, v in kwargs.items():
            self.params[k] = v
        model = model or self.params.get("model")
        if self.router_obj is not None:
            return self.router_obj.completion(model=model, messages=messages, **self.params)
        return completion(model=model, messages=messages, **self.params)

@tracer.wrap()
@client
def completion(
    model: str,
    messages: List = [],
    **kwargs,
) -> Union[ModelResponse, StreamWrapper]:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        try:
            import nest_asyncio
            nest_asyncio.apply()
        except ImportError:
            log.warning("[System] nest_asyncio is required to safely run a sync wrapper inside an active event loop.")
        return loop.run_until_complete(async_core_completion(model=model, messages=messages, **kwargs))
    else:
        return asyncio.run(async_core_completion(model=model, messages=messages, **kwargs))

class AsyncCompletions:
    def __init__(self, params, router_obj: Optional[Any]):
        self.params = params
        self.router_obj = router_obj

    async def create(self, messages, model=None, **kwargs):
        for k, v in kwargs.items():
            self.params[k] = v
        model = model or self.params.get("model")
        if self.router_obj is not None:
            return await self.router_obj.acompletion(model=model, messages=messages, **self.params)
        return await acompletion(model=model, messages=messages, **self.params)

@tracer.wrap()
@client
async def acompletion(
    model: str,
    messages: List = [],
    **kwargs,
) -> Union[ModelResponse, StreamWrapper]:
    """모든 인자는 **kwargs로 위임하고, Fallback 및 Mock 처리 후 비동기 코어 엔진을 호출"""
    log.info("## acompletion")
    fallbacks = kwargs.get("fallbacks")
    if fallbacks is not None:
        response = await async_completion_with_fallbacks(model=model, messages=messages, **kwargs)
        if response is None:
            raise Exception("No response from fallbacks. Got none.")
        return response

    mock_timeout = kwargs.get("mock_timeout")
    timeout = kwargs.get("timeout")
    if mock_timeout is True:
        await _handle_mock_timeout_async(mock_timeout, timeout, model)

    log_delegator = kwargs.get("log_delegator")
    tools = kwargs.get("tools")
    if isinstance(log_delegator, LogDelegator) and log_delegator.should_run_prompt_management_hooks(
        prompt_id=kwargs.get("prompt_id"), non_default_params=kwargs, tools=tools,
    ):
        model, messages, _ = await log_delegator.async_get_chat_completion_prompt(
            model=model, messages=messages, non_default_params=kwargs,
            prompt_id=kwargs.get("prompt_id"), prompt_variables=kwargs.get("prompt_variables"),
            tools=tools, prompt_label=kwargs.get("prompt_label"), prompt_version=kwargs.get("prompt_version"),
        )
        if tools is not None and len(tools) == 0:
            kwargs["tools"] = None  # 빈 리스트 처리

    mock_delay = kwargs.get("mock_delay")
    if mock_delay and (kwargs.get("mock_response") or kwargs.get("mock_tool_calls")): 
        await asyncio.sleep(mock_delay)

    kwargs["acompletion"] = True
    try:
        response = await async_core_completion(model=model, messages=messages, **kwargs)
        log.error("========== [DEBUG: RAW API RESPONSE] ==========")
        try:
            log.error(f"Response Dump: {response.model_dump()}")
        except AttributeError:
            log.error(f"Response Dir: {dir(response)}")
        log.error("===============================================")
        
        if isinstance(response, StreamWrapper):
            response.set_logging_event_loop(loop=asyncio.get_running_loop())
            
        return response
        
    except Exception as e:
        provider = kwargs.get("custom_llm_provider", "openai")
        raise exception_type(
            model=model, custom_llm_provider=provider, original_exception=e,
            completion_kwargs={"model": model, "messages": messages, **kwargs}, extra_kwargs=kwargs,
        )

async def async_completion_with_fallbacks(**kwargs):
    """Fallback 리스트를 순회하며 acompletion을 재귀적으로 호출합니다."""
    nested_kwargs = kwargs.pop("kwargs", {}) if "kwargs" in kwargs else {}
    original_model = kwargs.pop("model")
    messages = kwargs.pop("messages", [])
    
    fallbacks = [original_model] + nested_kwargs.pop("fallbacks", [])
    kwargs.pop("acompletion", None) 
    
    base_kwargs = {**kwargs, **nested_kwargs, "call_id": str(uuid.uuid4())}
    log_delegator = base_kwargs.pop("log_delegator", None)
    most_recent_exception_str: Optional[str] = None
    
    for fallback in fallbacks:
        try:
            current_kwargs = safe_deep_copy(base_kwargs)
            if isinstance(fallback, dict):
                fallback_config = safe_deep_copy(dict(fallback))
                current_model = fallback_config.pop("model", original_model)
                current_kwargs.update(fallback_config)
            else:
                current_model = fallback

            current_kwargs = filter_internal_params(current_kwargs)

            ## 재귀 호출
            response = await acompletion(
                model=current_model, messages=messages, 
                log_delegator=log_delegator, **current_kwargs
            )
            if response is not None:
                return response

        except Exception as e:
            log.warning(f"Fallback attempt failed for model {current_model}: {str(e)}")
            most_recent_exception_str = str(e)
            continue

    raise Exception(f"{most_recent_exception_str}. All fallback attempts failed.")

async def _handle_mock_timeout_async(mock_timeout: Optional[bool], timeout: Union[float, str, httpx.Timeout, None], model: str):
    if mock_timeout is True and timeout is not None:
        if isinstance(timeout, float):
            await asyncio.sleep(timeout)
        elif isinstance(timeout, str):
            await asyncio.sleep(float(timeout))
        elif isinstance(timeout, httpx.Timeout) and timeout.connect is not None:
            await asyncio.sleep(timeout.connect)
        raise Timeout(message="This is a mock timeout error", llm_provider="openai", model=model)