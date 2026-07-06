# bound.watcher.plane.topos
"""
@desc: Native topological telemetry
- Preserves legacy dependency signatures while transparently collapsing 
- external observation vectors into the internal flow manifold (flow_scope).
"""
import os
import inspect
import functools
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from typing import Any
from dotenv import dotenv_values
from watcher.plane.emitter import get_emitter, flow_scope

logger = get_emitter(__name__, phase="SYSTEM")

def get_env(key: str) -> str | None:
    return os.getenv(key) or dotenv_values().get(key)

def should_enable_observability() -> bool:
    """ @compat: Retained for structural parity. Always yields False. """
    return False

class _NativeTracerBackend:
    """
    @desc: Inert pass-through boundary. Neutralizes legacy span lifecycle 
    signals (start/end) without triggering topological anomalies.
    """
    def start_active_span(self, name: str, session_id: str | None = None):
        pass

    def end_active_span(self):
        pass

_active_backend = _NativeTracerBackend()

"""Public API (Strict Signature Parity)"""
def start_active_span(name: str, session_id: str | None = None) -> None:
    """@compat: Legacy direct span initiation."""
    _active_backend.start_active_span(name, session_id)

def end_active_span() -> None:
    """@compat: Legacy direct span termination."""
    _active_backend.end_active_span()

@contextmanager
def unified_flow_span(name: str, session_id: str | None = None, auto_flush: bool = False, **flow_kwargs):
    """
    @compat: Replaces external telemetry spans with native topological scopes.
    Folds spatial context directly into the internal LogEvent ecosystem.
    """
    if "phase" not in flow_kwargs:
        flow_kwargs["phase"] = name
    if session_id:
        flow_kwargs["session_id"] = session_id

    ## @action: Bypass external projections and anchor strictly to the native flow
    try:
        with flow_scope(auto_flush=auto_flush, **flow_kwargs) as ctx:
            yield ctx
    finally:
        pass

@contextmanager
def span_context(name: str, *, attributes: Mapping[str, Any] | None = None):
    """@compat: Translates unstructured external attributes into topological flow_scope kwargs"""
    kwargs = dict(attributes or {})

    ## @action: Fold legacy OTel context as payload for the native emitter
    try:
        with flow_scope(phase=name, **kwargs) as ctx:
            yield ctx
    finally:
        pass

def observe(**kwargs):
    """@compat: Native override for external observation decorators"""
    def decorator(func: Callable) -> Callable:
        is_async = inspect.iscoroutinefunction(func)
        ## @action: Fallback to function signature if spatial phase is unmapped
        phase_name = kwargs.pop("name", func.__name__)
        if is_async:
            @functools.wraps(func)
            async def wrapper(*args, **kw):
                with flow_scope(phase=phase_name, **kwargs):
                    return await func(*args, **kw)
            return wrapper
        else:
            @functools.wraps(func)
            def wrapper(*args, **kw):
                with flow_scope(phase=phase_name, **kwargs):
                    return func(*args, **kw)
            return wrapper
            
    return decorator