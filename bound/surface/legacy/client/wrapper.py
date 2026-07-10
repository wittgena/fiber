# bound.surface.legacy.client.wrapper
## @lineage: bound.transport.client.wrapper
## @lineage: bound.transport.channel.client
## @lineage: bound.channel.client
import asyncio
import contextvars
import datetime
import inspect
import traceback
import uuid
from typing import Any, Dict, Optional, Type, Callable
from dataclasses import dataclass
from functools import wraps
from weakref import WeakKeyDictionary

from bound.surface.legacy.types import CallTypes
from bound.surface.legacy.config.constants import COROUTINE_CHECKER_MAX_SIZE_IN_MEMORY
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.client.executor import executor
from bound.surface.legacy.client.logging import GLOBAL_LOGGING_WORKER
from bound.channel.rule import Rules
from bound.channel.stream.chunk.builder import stream_chunk_builder
from bound.watcher.delegator import LogDelegator
from xphi.xor.secret.credential import CredentialAccessor

from watcher.plane.emitter import get_emitter

log = get_emitter("channel.client")

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

        target = callback
        if not inspect.isfunction(target) and not inspect.ismethod(target):
            try:
                call_attr = getattr(target, "__call__", None)
                if call_attr is not None:
                    target = call_attr
            except Exception:
                pass

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

def load_credentials_from_list(kwargs: dict):
    credential_name = kwargs.get("litellm_credential_name")
    if credential_name and config.credential_list:
        credential_accessor = CredentialAccessor.get_credential_values(credential_name)
        for key, value in credential_accessor.items():
            if key not in kwargs:
                kwargs[key] = value

_STREAMING_CALL_TYPES = frozenset({
    CallTypes.generate_content_stream,
    CallTypes.agenerate_content_stream,
    CallTypes.generate_content_stream.value,
    CallTypes.agenerate_content_stream.value,
})

def _is_streaming_request(kwargs: Dict[str, Any], call_type: str) -> bool:
    if "stream" in kwargs and kwargs["stream"] is True:
        return True
    return call_type in _STREAMING_CALL_TYPES

async def async_pre_call_deployment_hook(kwargs: Dict[str, Any], call_type: str):
    try:
        typed_call_type = CallTypes(call_type)
    except ValueError:
        typed_call_type = None

    modified_kwargs = kwargs.copy()
    for callback in config.callbacks:
        if hasattr(callback, "async_pre_call_deployment_hook"):
            result = await callback.async_pre_call_deployment_hook(modified_kwargs, typed_call_type)
            if result is not None:
                modified_kwargs = result

    return modified_kwargs

def function_setup(original_function: str, rules_obj, start_time, log_delegator_class: Type, *args, **kwargs):
    """@desc: Prepare logging object and context before API execution."""
    try:
        applied_guardrails = []
        function_id = kwargs.get("id", None)
        model = args[0] if len(args) > 0 else kwargs.get("model", None)
        call_type = original_function

        dynamic_callbacks = kwargs.pop("callbacks", None)
        messages = "default-message-value"
        
        if call_type in [CallTypes.completion.value, CallTypes.acompletion.value, CallTypes.anthropic_messages.value]:
            messages = args[1] if len(args) > 1 else kwargs.get("messages", messages)
        elif call_type in [CallTypes.embedding.value, CallTypes.aembedding.value]:
            messages = args[1] if len(args) > 1 else kwargs.get("input", messages)
        elif call_type in [CallTypes.image_generation.value, CallTypes.aimage_generation.value, CallTypes.text_completion.value, CallTypes.atext_completion.value]:
            messages = args[0] if len(args) > 0 else kwargs.get("prompt", messages)

        stream = _is_streaming_request(kwargs=kwargs, call_type=call_type)

        logging_obj = log_delegator_class(
            model=model,
            messages=messages,
            stream=stream,
            call_id=kwargs.get("call_id", str(uuid.uuid4())),
            litellm_trace_id=kwargs.get("litellm_trace_id"),
            function_id=function_id or "",
            call_type=call_type,
            start_time=start_time,
            dynamic_success_callbacks=dynamic_callbacks if isinstance(dynamic_callbacks, list) else None,
            kwargs=kwargs,
            applied_guardrails=applied_guardrails,
        )

        return logging_obj, kwargs
    except Exception as e:
        log.exception("CUSTOM function_setup() - Error in setup pipeline")
        raise e

async def _client_async_logging_helper(logging_obj, kwargs, result, start_time, end_time, is_completion_with_fallbacks: bool):
    """Async success logging helper."""
    if not is_completion_with_fallbacks:
        GLOBAL_LOGGING_WORKER.ensure_initialized_and_enqueue(
            async_coroutine=logging_obj.async_success_handler(
                kwargs=kwargs, result=result, start_time=start_time, end_time=end_time
            )
        )
        logging_obj.handle_sync_success_callbacks_for_async_calls(
            kwargs=kwargs, result=result, start_time=start_time, end_time=end_time,
        )

@dataclass
class ClientDependencies:
    """@desc: Dependency container for mock injection and module decoupling."""
    logger: Any = log
    rules_class: Type = Rules
    log_delegator_class: Type = LogDelegator
    setup_func: Callable = function_setup
    credential_loader: Callable = load_credentials_from_list
    async_hook: Callable = async_pre_call_deployment_hook
    async_log_helper: Callable = _client_async_logging_helper

class ClientCallExecutor:
    """@desc: Executor managing the isolated lifecycle of a single API call."""

    def __init__(self, original_function, args: tuple, kwargs: dict, deps: Optional[ClientDependencies] = None):
        self.original_function = original_function
        self.call_type = original_function.__name__
        self.args = args
        self.kwargs = kwargs.copy()
        self.deps = deps or ClientDependencies()

        self.start_time = None
        self.end_time = None
        self.log_delegator = self.kwargs.get("log_delegator")
        self.model = self._extract_model()

    def _extract_model(self) -> str:
        """Extract model name via safe parameter binding."""
        try:
            sig = inspect.signature(self.original_function)
            bound_args = sig.bind(*self.args, **self.kwargs)
            bound_args.apply_defaults()
            return bound_args.arguments.get("model", self.kwargs.get("model"))
        except TypeError:
            return self.args[0] if len(self.args) > 0 else self.kwargs.get("model")

    def _prepare_context(self):
        """Setup prerequisite context (logging, ID, credentials) before API call."""
        if "call_id" not in self.kwargs:
            self.kwargs["call_id"] = str(uuid.uuid4())

        self.start_time = datetime.datetime.now()

        if self.log_delegator is None:
            rules_obj = self.deps.rules_class()
            self.log_delegator, self.kwargs = self.deps.setup_func(
                self.call_type, rules_obj, self.start_time, self.deps.log_delegator_class, *self.args, **self.kwargs
            )
            self.kwargs["log_delegator"] = self.log_delegator

        self.deps.credential_loader(self.kwargs)

    def _handle_error(self, e: Exception, is_async: bool = False):
        """Centralized error handling with fail-fast execution."""
        self.end_time = datetime.datetime.now()
        call_mode = "ASYNC" if is_async else "SYNC"
        self.deps.logger.error(f"🔴 [CLIENT_WRAPPER: {call_mode}] API call failed: {str(e)}")
        
        if self.log_delegator:
            try:
                self.log_delegator.failure_handler(
                    kwargs=self.kwargs, 
                    exception=e, 
                    traceback_exception=traceback.format_exc(), 
                    start_time=self.start_time, 
                    end_time=self.end_time
                )
            except Exception:
                pass
            
            if is_async and hasattr(self.log_delegator, "async_failure_handler"):
                try:
                    asyncio.create_task(
                        self.log_delegator.async_failure_handler(
                            kwargs=self.kwargs, 
                            exception=e, 
                            traceback_exception=traceback.format_exc(), 
                            start_time=self.start_time, 
                            end_time=self.end_time
                        )
                    )
                except Exception:
                    pass

    def run_sync(self):
        """Synchronous execution flow control."""
        self._prepare_context()
        try:
            self.deps.logger.info(f"🟢 [CLIENT_WRAPPER: SYNC] API call started (Model: {self.model})")
            result = self.original_function(*self.args, **self.kwargs)
            self.end_time = datetime.datetime.now()
            self.deps.logger.info("🟢 [CLIENT_WRAPPER: SYNC] API call succeeded")

            if _is_streaming_request(kwargs=self.kwargs, call_type=self.call_type):
                if self.kwargs.get("complete_response") is True:
                    return stream_chunk_builder(list(result), messages=self.kwargs.get("messages", None))
                return result

            if self.kwargs.get("acompletion") or self.kwargs.get("aembedding") or asyncio.iscoroutine(result):
                return result

            ctx = contextvars.copy_context()
            executor.submit(
                ctx.run, 
                self.log_delegator.success_handler, 
                kwargs=self.kwargs,
                result=result, 
                start_time=self.start_time, 
                end_time=self.end_time
            )
            return result

        except Exception as e:
            self._handle_error(e, is_async=False)
            raise e

    async def run_async(self):
        """Asynchronous execution flow control."""
        self.kwargs.pop("_is_litellm_internal_call", None)
        self._prepare_context()

        try:
            modified_kwargs = await self.deps.async_hook(self.kwargs, self.call_type)
            if modified_kwargs is not None:
                self.kwargs = modified_kwargs

            self.deps.logger.info(f"🟢 [CLIENT_WRAPPER: ASYNC] API call started (Model: {self.model})")
            result = await self.original_function(*self.args, **self.kwargs)
            self.end_time = datetime.datetime.now()
            self.deps.logger.info("🟢 [CLIENT_WRAPPER: ASYNC] API call succeeded")

            if _is_streaming_request(kwargs=self.kwargs, call_type=self.call_type):
                if self.kwargs.get("complete_response") is True:
                    chunks = [chunk async for chunk in result] if hasattr(result, '__aiter__') else list(result)
                    return stream_chunk_builder(chunks, messages=self.kwargs.get("messages", None))
                return result

            if self.call_type == CallTypes.arealtime.value:
                return result

            asyncio.create_task(
                self.deps.async_log_helper(
                    logging_obj=self.log_delegator,
                    kwargs=self.kwargs,
                    result=result,
                    start_time=self.start_time,
                    end_time=self.end_time,
                    is_completion_with_fallbacks=False,
                )
            )
            return result

        except Exception as e:
            self._handle_error(e, is_async=True)
            raise e

## @desc: Entry point decorator
def client(original_function):
    """Top-level decorator wrapping API actions. Delegates business logic to ClientCallExecutor."""
    is_coroutine = coroutine_checker.is_async_callable(original_function)

    @wraps(original_function)
    def wrapper(*args, **kwargs):
        call_executor = ClientCallExecutor(original_function, args, kwargs)
        return call_executor.run_sync()

    @wraps(original_function)
    async def wrapper_async(*args, **kwargs):
        call_executor = ClientCallExecutor(original_function, args, kwargs)
        return await call_executor.run_async()

    return wrapper_async if is_coroutine else wrapper