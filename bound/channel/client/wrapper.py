# bound.channel.client.wrapper
## @lineage: anchor.channel.client.wrapper
import asyncio
import contextvars
import copy
import datetime
import inspect
import io
import itertools
import json
import logging
import os
import random
import re
import struct
import subprocess
import sys
import textwrap
import threading
import time
import traceback
import importlib.metadata
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
from dataclasses import dataclass, field
from functools import lru_cache, wraps
from importlib import resources
from inspect import iscoroutine
from io import StringIO
from os.path import abspath, dirname, join
import httpx
import openai
import tiktoken
from httpx import Proxy
from httpx._utils import get_environment_proxies
from openai.lib import _parsing
import inspect
from weakref import WeakKeyDictionary

from anchor.surface.exception import (
    APIConnectionError,
    APIError,
    AuthenticationError,
    BadRequestError,
    BudgetExceededError,
    ContentPolicyViolationError,
    ContextWindowExceededError,
    NotFoundError,
    OpenAIError,
    PermissionDeniedError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout,
    UnprocessableEntityError,
    UnsupportedParamsError,
)

from anchor.provider.model.token.counter import get_modified_max_tokens
from anchor.provider.legacy.openai.types import (
    AllMessageValues,
    AllPromptValues,
    ChatCompletionAssistantToolCall,
    ChatCompletionNamedToolChoiceParam,
    ChatCompletionToolParam,
    ChatCompletionToolParamFunctionChunk,
    OpenAITextCompletionUserMessage,
    OpenAIWebSearchOptions,
)
from bound.router.provider.locator import get_llm_provider
from anchor.provider.legacy.types import CallTypes, Embedding, ProviderTypes

from bound.channel.config.constants import COROUTINE_CHECKER_MAX_SIZE_IN_MEMORY
from bound.channel.config.resolver import config
from bound.channel.client.action.param.format import type_to_response_format_param
from bound.channel.client.action.task.executor import executor
from bound.channel.client.action.task.logging import GLOBAL_LOGGING_WORKER
from bound.channel.client.bridge.rule import Rules
from bound.channel.client.response.metadata import update_response_metadata

from bound.transport.stream.chunk.builder import stream_chunk_builder

from xphi.xor.secret.credential import CredentialAccessor
from xphi.scope.plane.delegator import Logging as LiteLLMLoggingObject

from phase.gov.proto.gate import uuid
from watcher.plane.emitter import get_emitter

log = get_emitter("client.wrapper")

CustomLogger = Any

sentry_sdk_instance = None
capture_exception = None
add_breadcrumb = None
posthog = None
slack_app = None
alerts_channel = None
heliconeLogger = None
athinaLogger = None
promptLayerLogger = None
langsmithLogger = None
logfireLogger = None
weightsBiasesLogger = None
customLogger = None
langFuseLogger = None
openMeterLogger = None
lagoLogger = None
dataDogLogger = None
prometheusLogger = None
dynamoLogger = None
s3Logger = None
greenscaleLogger = None
lunaryLogger = None
aispendLogger = None
supabaseClient = None
callback_list: Optional[List[str]] = []
user_logger_fn = None
additional_details: Optional[Dict[str, str]] = {}
local_cache: Optional[Dict[str, str]] = {}
last_fetched_at = None
last_fetched_at_keys = None

class CoroutineChecker:
    def __init__(self):
        self._cache = WeakKeyDictionary()
        self._max_size = COROUTINE_CHECKER_MAX_SIZE_IN_MEMORY

    def is_async_callable(self, callback: Any) -> bool:
        try:
            cached = self._cache.get(callback)
            if cached is not None:
                return cached
        except Exception:
            pass

        # Determine target - optimized path for common cases
        target = callback
        if not inspect.isfunction(target) and not inspect.ismethod(target):
            try:
                call_attr = getattr(target, "__call__", None)
                if call_attr is not None:
                    target = call_attr
            except Exception:
                pass

        # Compute result
        try:
            result = inspect.iscoroutinefunction(target)
        except Exception:
            result = False

        try:
            if len(self._cache) >= self._max_size:
                self._cache.clear()
            self._cache[callback] = result
        except Exception:
            pass

        return result

coroutine_checker = CoroutineChecker()


def custom_llm_setup():
    for custom_llm in config.custom_provider_map:
        if custom_llm["provider"] not in config.provider_list:
            config.provider_list.append(custom_llm["provider"])

        if custom_llm["provider"] not in config._custom_providers:
            config._custom_providers.append(custom_llm["provider"])

def load_credentials_from_list(kwargs: dict):
    credential_name = kwargs.get("litellm_credential_name")
    if credential_name and config.credential_list:
        credential_accessor = CredentialAccessor.get_credential_values(credential_name)
        for key, value in credential_accessor.items():
            if key not in kwargs:
                kwargs[key] = value

def function_setup(original_function: str, rules_obj, start_time, *args, **kwargs):
    try:
        applied_guardrails = []
        function_id = kwargs.get("id", None)
        model = args[0] if len(args) > 0 else kwargs.get("model", None)
        call_type = original_function

        ## @removed: dynamic_callbacks를 병합하는 복잡한 로직 삭제 후 단순화
        dynamic_callbacks = kwargs.pop("callbacks", None)

        ## @simplify: 메시지 추출 로직 단순화 - 로깅 객체가 에러를 뱉지 않게만 하는 최소한의 메시지/프롬프트 추출
        messages = "default-message-value"
        if call_type in [CallTypes.completion.value, CallTypes.acompletion.value, CallTypes.anthropic_messages.value]:
            messages = args[1] if len(args) > 1 else kwargs.get("messages", messages)
            ## @removed:Gemini thought_signature 제거 로직 완벽 삭제 (가장 비효율적이었던 O(N) 순회 파트)
        elif call_type in [CallTypes.embedding.value, CallTypes.aembedding.value]:
            messages = args[1] if len(args) > 1 else kwargs.get("input", messages)
        elif call_type in [CallTypes.image_generation.value, CallTypes.aimage_generation.value, CallTypes.text_completion.value, CallTypes.atext_completion.value]:
            messages = args[0] if len(args) > 0 else kwargs.get("prompt", messages)

        stream = False
        if _is_streaming_request(kwargs=kwargs, call_type=call_type):
            stream = True

        logging_obj = LiteLLMLoggingObject(
            model=model,
            messages=messages,
            stream=stream,
            litellm_call_id=kwargs.get("litellm_call_id", str(uuid.uuid4())),
            litellm_trace_id=kwargs.get("litellm_trace_id"),
            function_id=function_id or "",
            call_type=call_type,
            start_time=start_time,
            dynamic_success_callbacks=dynamic_callbacks if isinstance(dynamic_callbacks, list) else None,
            dynamic_failure_callbacks=None,
            dynamic_async_success_callbacks=None,
            dynamic_async_failure_callbacks=None,
            kwargs=kwargs,
            applied_guardrails=applied_guardrails,
        )

        ## @temp.sustain: 환경변수 및 메타데이터 업데이트 
        litellm_params = {"api_base": ""}
        if "metadata" in kwargs:
            litellm_params["metadata"] = kwargs["metadata"]
        if "litellm_metadata" in kwargs and isinstance(kwargs["litellm_metadata"], dict):
            litellm_params["litellm_metadata"] = kwargs["litellm_metadata"].copy()
            if not litellm_params.get("metadata"):
                litellm_params["metadata"] = kwargs["litellm_metadata"].copy()

        logging_obj.update_environment_variables(
            model=model,
            user="",
            optional_params={},
            litellm_params=litellm_params,
            stream_options=kwargs.get("stream_options", None),
        )
        return logging_obj, kwargs
    except Exception as e:
        log.exception("CUSTOM function_setup() - Error in setup pipeline")
        raise e


async def _client_async_logging_helper(
    logging_obj: LiteLLMLoggingObject,
    result,
    start_time,
    end_time,
    is_completion_with_fallbacks: bool,
):
    if (is_completion_with_fallbacks is False):
        GLOBAL_LOGGING_WORKER.ensure_initialized_and_enqueue(
            async_coroutine=logging_obj.async_success_handler(
                result=result, start_time=start_time, end_time=end_time
            )
        )
        logging_obj.handle_sync_success_callbacks_for_async_calls(
            result=result,
            start_time=start_time,
            end_time=end_time,
        )

async def async_pre_call_deployment_hook(kwargs: Dict[str, Any], call_type: str):
    try:
        typed_call_type = CallTypes(call_type)
    except ValueError:
        typed_call_type = None  # unknown call type

    modified_kwargs = kwargs.copy()
    for callback in config.callbacks:
        if hasattr(callback, "async_pre_call_deployment_hook"):
            result = await callback.async_pre_call_deployment_hook(
                modified_kwargs, typed_call_type
            )
            if result is not None:
                modified_kwargs = result

    return modified_kwargs

def _is_async_request(
    kwargs: Optional[dict],
    is_pass_through: bool = False,
) -> bool:
    if kwargs is None:
        return False
    if (
        kwargs.get("acompletion", False) is True
        or kwargs.get("aembedding", False) is True
        or kwargs.get("aimg_generation", False) is True
        or kwargs.get("amoderation", False) is True
        or kwargs.get("atext_completion", False) is True
        or kwargs.get("atranscription", False) is True
        or kwargs.get("arerank", False) is True
        or kwargs.get("_arealtime", False) is True
        or kwargs.get("acreate_batch", False) is True
        or kwargs.get("acreate_fine_tuning_job", False) is True
        or is_pass_through is True
    ):
        return True
    return False


_STREAMING_CALL_TYPES = frozenset(
    {
        CallTypes.generate_content_stream,
        CallTypes.agenerate_content_stream,
        CallTypes.generate_content_stream.value,
        CallTypes.agenerate_content_stream.value,
    }
)

def _is_streaming_request(
    kwargs: Dict[str, Any],
    call_type: Union[CallTypes, str],
) -> bool:
    if "stream" in kwargs and kwargs["stream"] is True:
        return True
    return call_type in _STREAMING_CALL_TYPES

_model_cost_lowercase_map: Optional[Dict[str, str]] = None

from typing_extensions import TypedDict

def acreate(*args, **kwargs):  ## Thin client to handle the acreate langchain call
    return config.acompletion(*args, **kwargs)

def print_args_passed_to_litellm(original_function, args, kwargs):
    try:
        # we've already printed this for acompletion, don't print for completion
        if (
            "acompletion" in kwargs
            and kwargs["acompletion"] is True
            and original_function.__name__ == "completion"
        ):
            return
        elif (
            "aembedding" in kwargs
            and kwargs["aembedding"] is True
            and original_function.__name__ == "embedding"
        ):
            return
        elif (
            "aimg_generation" in kwargs
            and kwargs["aimg_generation"] is True
            and original_function.__name__ == "img_generation"
        ):
            return

        args_str = ", ".join(map(repr, args))
        kwargs_str = ", ".join(f"{key}={repr(value)}" for key, value in kwargs.items())
    except Exception:
        # This should always be non blocking
        pass

@lru_cache(maxsize=1)
def _get_bundled_model_cost_map() -> Dict[str, Any]:
    try:
        model_cost_path = resources.files("litellm").joinpath(
            "model_prices_and_context_window_backup.json"
        )
        return json.loads(model_cost_path.read_text())
    except Exception:
        return {}


def _get_model_cost_entry_for_provider_config(
    model: str,
    provider: ProviderTypes,
) -> Dict[str, Any]:
    candidate_keys = (model, f"{provider.value}/{model}")
    for model_key in candidate_keys:
        model_info = config.model_cost.get(model_key)
        if model_info is not None:
            return model_info

    bundled_model_cost = _get_bundled_model_cost_map()
    for model_key in candidate_keys:
        model_info = bundled_model_cost.get(model_key)
        if model_info is not None:
            return model_info
    return {}

def add_openai_metadata(
    metadata: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, str]]:
    if metadata is None:
        return None
    # Only include non-hidden parameters
    visible_metadata: Dict[str, str] = {
        str(k): v
        for k, v in metadata.items()
        if k != "hidden_params" and isinstance(v, str)
    }

    # max 16 keys allowed by openai - trim down to 16
    if len(visible_metadata) > 16:
        filtered_metadata = {}
        idx = 0
        for k, v in visible_metadata.items():
            if idx < 16:
                filtered_metadata[k] = v
            idx += 1
        visible_metadata = filtered_metadata
    return visible_metadata.copy()

def client(original_function):
    Rules = getattr(sys.modules.get(__name__, sys.modules["__main__"]), "Rules", None)
    rules_obj = Rules() if Rules else None

    @wraps(original_function)
    def wrapper(*args, **kwargs):
        log.warning(f"🚨 [CUSTOM_WRAPPER: SYNC] 진입! 대상 함수: {original_function.__name__}")
        
        if "litellm_call_id" not in kwargs:
            kwargs["litellm_call_id"] = str(uuid.uuid4())

        start_time = datetime.datetime.now()
        logging_obj = kwargs.get("litellm_logging_obj", None)
        model = args[0] if len(args) > 0 else kwargs.get("model", None)
        call_type = original_function.__name__

        try:
            if logging_obj is None:
                logging_obj, kwargs = function_setup(call_type, rules_obj, start_time, *args, **kwargs)
            kwargs["litellm_logging_obj"] = logging_obj
            load_credentials_from_list(kwargs)

            log.info(f"🟢 [CUSTOM_WRAPPER: SYNC] API 원본 호출 시작 (Model: {model})")
            result = original_function(*args, **kwargs)
            end_time = datetime.datetime.now()
            log.info("🟢 [CUSTOM_WRAPPER: SYNC] API 원본 호출 성공 및 응답 수신!")

            if _is_streaming_request(kwargs=kwargs, call_type=call_type):
                if kwargs.get("complete_response") is True:
                    chunks = list(result)
                    return stream_chunk_builder(chunks, messages=kwargs.get("messages", None))
                return result

            if kwargs.get("acompletion") or kwargs.get("aembedding") or asyncio.iscoroutine(result):
                return result

            ctx = contextvars.copy_context()
            executor.submit(
                ctx.run,
                logging_obj.success_handler,
                result,
                start_time,
                end_time,
            )
            return result

        except Exception as e:
            log.error(f"🔴 [CUSTOM_WRAPPER: SYNC] API 호출 실패! 예외 즉시 발생 (Fail-fast): {str(e)}")
            end_time = datetime.datetime.now()
            if logging_obj:
                logging_obj.failure_handler(e, traceback.format_exc(), start_time, end_time)
            raise e

    @wraps(original_function)
    async def wrapper_async(*args, **kwargs):
        log.warning(f"🚨 [CUSTOM_WRAPPER: ASYNC] 진입! 대상 함수: {original_function.__name__}")
        
        if "litellm_call_id" not in kwargs:
            kwargs["litellm_call_id"] = str(uuid.uuid4())

        start_time = datetime.datetime.now()
        logging_obj = kwargs.get("litellm_logging_obj", None)
        model = args[0] if len(args) > 0 else kwargs.get("model", None)
        call_type = original_function.__name__

        kwargs.pop("_is_litellm_internal_call", None)

        try:
            if logging_obj is None:
                logging_obj, kwargs = function_setup(
                    call_type, rules_obj, start_time, *args, **kwargs
                )
            
            modified_kwargs = await async_pre_call_deployment_hook(kwargs, call_type)
            if modified_kwargs is not None:
                kwargs = modified_kwargs

            kwargs["litellm_logging_obj"] = logging_obj
            load_credentials_from_list(kwargs)

            log.info(f"🟢 [CUSTOM_WRAPPER: ASYNC] API 원본 호출 시작 (Model: {model})")
            result = await original_function(*args, **kwargs)
            end_time = datetime.datetime.now()
            log.info("🟢 [CUSTOM_WRAPPER: ASYNC] API 원본 호출 성공 및 응답 수신!")

            if _is_streaming_request(kwargs=kwargs, call_type=call_type):
                if kwargs.get("complete_response") is True:
                    chunks = [chunk async for chunk in result] if hasattr(result, '__aiter__') else list(result)
                    return stream_chunk_builder(chunks, messages=kwargs.get("messages", None))
                return result

            if call_type == CallTypes.arealtime.value:
                return result

            asyncio.create_task(
                _client_async_logging_helper(
                    logging_obj=logging_obj,
                    result=result,
                    start_time=start_time,
                    end_time=end_time,
                    is_completion_with_fallbacks=False,
                )
            )
            return result
        except Exception as e:
            log.error(f"🔴 [CUSTOM_WRAPPER: ASYNC] API 호출 실패! 예외 즉시 발생 (Fail-fast): {str(e)}")
            end_time = datetime.datetime.now()
            if logging_obj:
                try:
                    logging_obj.failure_handler(e, traceback.format_exc(), start_time, end_time)
                except Exception:
                    pass
                try:
                    await logging_obj.async_failure_handler(e, traceback.format_exc(), start_time, end_time)
                except Exception:
                    pass
            raise e

    is_coroutine = coroutine_checker.is_async_callable(original_function)
    return wrapper_async if is_coroutine else wrapper

def completion_with_retries(*args, **kwargs):
    try:
        import tenacity
    except Exception as e:
        raise Exception(f"tenacity import failed please run `pip install tenacity`. Error{e}")

    num_retries = kwargs.pop("num_retries", 3)
    kwargs["max_retries"] = 0
    kwargs["num_retries"] = 0
    retry_strategy: Literal["exponential_backoff_retry", "constant_retry"] = kwargs.pop("retry_strategy", "constant_retry")
    original_function = kwargs.pop("original_function", completion)
    if retry_strategy == "exponential_backoff_retry":
        retryer = tenacity.Retrying(
            wait=tenacity.wait_exponential(multiplier=1, max=10),
            stop=tenacity.stop_after_attempt(num_retries),
            reraise=True,
        )
    else:
        retryer = tenacity.Retrying(
            stop=tenacity.stop_after_attempt(num_retries), reraise=True
        )
    return retryer(original_function, *args, **kwargs)


async def acompletion_with_retries(*args, **kwargs):
    """
    [DEPRECATED]. Use 'acompletion' or router.acompletion instead!
    Executes a litellm.completion() with 3 retries
    """
    try:
        import tenacity
    except Exception as e:
        raise Exception(
            f"tenacity import failed please run `pip install tenacity`. Error{e}"
        )

    num_retries = kwargs.pop("num_retries", 3)
    kwargs["max_retries"] = 0
    kwargs["num_retries"] = 0
    retry_strategy = kwargs.pop("retry_strategy", "constant_retry")
    original_function = kwargs.pop("original_function", completion)
    if retry_strategy == "exponential_backoff_retry":
        retryer = tenacity.AsyncRetrying(
            wait=tenacity.wait_exponential(multiplier=1, max=10),
            stop=tenacity.stop_after_attempt(num_retries),
            reraise=True,
        )
    else:
        retryer = tenacity.AsyncRetrying(stop=tenacity.stop_after_attempt(num_retries), reraise=True)
    return await retryer(original_function, *args, **kwargs)

def responses_with_retries(*args, **kwargs):
    from bound.channel.client.action.api.response import responses
    try:
        import tenacity
    except Exception as e:
        raise Exception(
            f"tenacity import failed please run `pip install tenacity`. Error{e}"
        )

    num_retries = kwargs.pop("num_retries", 3)
    kwargs["max_retries"] = 0
    kwargs["num_retries"] = 0
    retry_strategy: Literal["exponential_backoff_retry", "constant_retry"] = kwargs.pop(
        "retry_strategy", "constant_retry"
    )  # type: ignore
    original_function = kwargs.pop("original_function", responses)
    if retry_strategy == "exponential_backoff_retry":
        retryer = tenacity.Retrying(
            wait=tenacity.wait_exponential(multiplier=1, max=10),
            stop=tenacity.stop_after_attempt(num_retries),
            reraise=True,
        )
    else:
        retryer = tenacity.Retrying(stop=tenacity.stop_after_attempt(num_retries), reraise=True)
    return retryer(original_function, *args, **kwargs)


async def aresponses_with_retries(*args, **kwargs):
    from bound.channel.client.action.api.aresponse import aresponses
    try:
        import tenacity
    except Exception as e:
        raise Exception(f"tenacity import failed please run `pip install tenacity`. Error{e}")

    num_retries = kwargs.pop("num_retries", 3)
    kwargs["max_retries"] = 0
    kwargs["num_retries"] = 0
    retry_strategy = kwargs.pop("retry_strategy", "constant_retry")
    original_function = kwargs.pop("original_function", aresponses)
    if retry_strategy == "exponential_backoff_retry":
        retryer = tenacity.AsyncRetrying(
            wait=tenacity.wait_exponential(multiplier=1, max=10),
            stop=tenacity.stop_after_attempt(num_retries),
            reraise=True,
        )
    else:
        retryer = tenacity.AsyncRetrying(stop=tenacity.stop_after_attempt(num_retries), reraise=True)
    return await retryer(original_function, *args, **kwargs)