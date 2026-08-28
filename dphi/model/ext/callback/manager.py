# fiber.dphi.model.ext.callback.manager
## @lineage: dphi.model.ext.callback.manager
## @lineage: dphi.model.callback.manager
## @lineage: agent.llm.router.callback.manager
## @lineage: bound.agent.callback.manager
## @lineage: bound.eco.agent.adapter.callback.manager
## @lineage: eco.bound.agent.adapter.callback.manager
## @lineage: bound.agent.adapter.callback.manager
## @lineage: ext.router.adapter.callback.manager
## @lineage: router.adapter.callback.manager
import functools
from functools import wraps
import inspect
import logging
from typing import Any, Callable, cast, Dict, Generator, List, Optional, Type
from contextlib import contextmanager
from fiber.dphi.model.mapper.pydantic import CoreSchema, core_schema

logger = logging.getLogger(__name__)

class CallbackManager:
    def __init__(self, handlers: Optional[List[Any]] = None):
        self.handlers: List[Any] = handlers or []
        self.trace_map: Dict[str, List[str]] = {}

    def on_event_start(
        self,
        event_type: Any,
        payload: Optional[Dict[str, Any]] = None,
        event_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        return event_id or "dummy_event_id"

    def on_event_end(
        self,
        event_type: Any,
        payload: Optional[Dict[str, Any]] = None,
        event_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        pass

    def add_handler(self, handler: Any) -> None:
        if handler not in self.handlers:
            self.handlers.append(handler)

    def remove_handler(self, handler: Any) -> None:
        if handler in self.handlers:
            self.handlers.remove(handler)

    def set_handlers(self, handlers: List[Any]) -> None:
        self.handlers = handlers

    @contextmanager
    def event(
        self,
        event_type: Any,
        payload: Optional[Dict[str, Any]] = None,
        event_id: Optional[str] = None,
    ) -> Generator["EventContext", None, None]:
        """기존 방식의 with callback_manager.event() 구문 지원"""
        yield EventContext()

    @contextmanager
    def as_trace(self, trace_id: str) -> Generator[None, None, None]:
        """기존 방식의 with callback_manager.as_trace() 구문 지원"""
        yield

    def start_trace(self, trace_id: Optional[str] = None) -> None:
        pass

    def end_trace(
        self,
        trace_id: Optional[str] = None,
        trace_map: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        pass

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Type[Any], handler: Any
    ) -> CoreSchema:
        return core_schema.any_schema()

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: Any
    ) -> Dict[str, Any]:
        return {}


class EventContext:
    def __init__(self):
        self.started = True
        self.finished = True

    def on_start(self, payload: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        pass

    def on_end(self, payload: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        pass

def trace_method(
    trace_id: str, callback_manager_attr: str = "callback_manager"
) -> Callable[[Callable], Callable]:
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)  # preserve signature, name, etc. of func
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                callback_manager = getattr(self, callback_manager_attr)
            except AttributeError:
                logger.warning(
                    "Could not find attribute %s on %s.",
                    callback_manager_attr,
                    type(self),
                )
                return func(self, *args, **kwargs)
            callback_manager = cast(CallbackManager, callback_manager)
            with callback_manager.as_trace(trace_id):
                return func(self, *args, **kwargs)

        @functools.wraps(func)  # preserve signature, name, etc. of func
        async def async_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                callback_manager = getattr(self, callback_manager_attr)
            except AttributeError:
                logger.warning(
                    "Could not find attribute %s on %s.",
                    callback_manager_attr,
                    type(self),
                )
                return await func(self, *args, **kwargs)
            callback_manager = cast(CallbackManager, callback_manager)
            with callback_manager.as_trace(trace_id):
                return await func(self, *args, **kwargs)
        return async_wrapper if inspect.iscoroutinefunction(func) else wrapper
    return decorator

import inspect
from functools import wraps
from typing import Any, Callable

def _base_llm_callback() -> Callable:
    def wrap(f: Callable) -> Callable:
        is_wrapped = getattr(f, "__llama_wrapped__", False)
        if inspect.iscoroutinefunction(f):
            if is_wrapped:
                return f

            @wraps(f)
            async def async_wrapper(_self: Any, *args: Any, **kwargs: Any) -> Any:
                if getattr(_self, "rate_limiter", None) is not None:
                    await _self.rate_limiter.async_acquire()
                
                return await f(_self, *args, **kwargs)

            async_wrapper.__llama_wrapped__ = True
            return async_wrapper
        else:
            if is_wrapped:
                return f

            @wraps(f)
            def sync_wrapper(_self: Any, *args: Any, **kwargs: Any) -> Any:
                # LLM 속도 제한(Rate Limit) 유지
                if getattr(_self, "rate_limiter", None) is not None:
                    _self.rate_limiter.acquire()
                    
                return f(_self, *args, **kwargs)

            sync_wrapper.__llama_wrapped__ = True
            return sync_wrapper

    return wrap


def llm_chat_callback() -> Callable:
    return _base_llm_callback()

def llm_completion_callback() -> Callable:
    return _base_llm_callback()