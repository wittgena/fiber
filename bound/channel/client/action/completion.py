# bound.channel.client.action.completion
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

from anchor.surface.exception import Timeout
from bound.channel.switch.params import ModelResponse
from anchor.provider.mapper.exception import exception_type
from bound.channel.client.action.core import async_core_completion

from bound.transport.stream.wrapper import CustomStreamWrapper
from bound.channel.client.wrapper import client
from bound.channel.client.action.support.helpers import safe_deep_copy, filter_internal_params
from bound.channel.client.action.support.asyncify import run_async_function

from bound.watcher.plane.trace.dd import tracer
from bound.watcher.plane.delegator import Logging as LiteLLMLoggingObj

from watcher.plane.emitter import get_emitter

log = get_emitter("action.completion")

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
) -> Union[ModelResponse, CustomStreamWrapper]:
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
) -> Union[ModelResponse, CustomStreamWrapper]:
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

    litellm_logging_obj = kwargs.get("litellm_logging_obj")
    tools = kwargs.get("tools")
    if isinstance(litellm_logging_obj, LiteLLMLoggingObj) and litellm_logging_obj.should_run_prompt_management_hooks(
        prompt_id=kwargs.get("prompt_id"), non_default_params=kwargs, tools=tools,
    ):
        model, messages, _ = await litellm_logging_obj.async_get_chat_completion_prompt(
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
        
        if isinstance(response, CustomStreamWrapper):
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
    
    base_kwargs = {**kwargs, **nested_kwargs, "litellm_call_id": str(uuid.uuid4())}
    litellm_logging_obj = base_kwargs.pop("litellm_logging_obj", None)

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
                litellm_logging_obj=litellm_logging_obj, **current_kwargs
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