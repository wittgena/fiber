# bound.watcher.plane.laminar
## @lineage: xphi.scope.plane.laminar
import os
import inspect
import functools
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, Literal
from dotenv import dotenv_values
from opentelemetry import trace

from bound.channel.config.resolver import config
from arch.proto.event.next import LogEvent
from watcher.plane.emitter import get_emitter, register_interceptor, flow_scope

logger = get_emitter(__name__)

def otel_log_interceptor(event: LogEvent):
    span = trace.get_current_span()
    if not span.is_recording():
        return

    span_ctx = span.get_span_context()
    event.context["trace_id"] = format(span_ctx.trace_id, "032x")
    event.context["span_id"] = format(span_ctx.span_id, "016x")

    if event.level not in ("DEBUG", "TRACE"):
        span.add_event(
            name=f"log.{event.level.lower()}",
            attributes={
                "message": event.message,
                "source": event.source_id,
                "phase": event.context.get("phase", "unknown")
            }
        )
        
    if event.level in ("ERROR", "CRIT", "CRITICAL"):
        span.set_status(trace.Status(trace.StatusCode.ERROR, event.message))

def get_env(key: str) -> str | None:
    return os.getenv(key) or dotenv_values().get(key)

def _get_int_env(key: str) -> int | None:
    val = get_env(key)
    if val is not None and val != "":
        try:
            return int(val)
        except ValueError:
            logger.warning("%s must be an integer, got %r", key, val)
            return None
    return None

def should_enable_observability():
    keys = [
        "LMNR_PROJECT_API_KEY",
        "OTEL_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ]
    return any(get_env(key) for key in keys)

def _is_otel_backend_laminar():
    key = get_env("LMNR_PROJECT_API_KEY")
    return key is not None and key != ""

## Backend Abstraction (내부 의존성 격리)
class _TracerBackend:
    """기본 통과(Pass-through) 백엔드: 관측망이 꺼져있을 때 사용"""
    def start_active_span(self, name: str, session_id: str | None = None): pass
    def end_active_span(self): pass

class _LaminarBackend(_TracerBackend):
    """Laminar 종속성을 캡슐화한 어댑터 클래스"""
    def __init__(self):
        from lmnr import Laminar, LaminarLiteLLMCallback, Instruments

        if _is_otel_backend_laminar():
            Laminar.initialize(
                http_port=_get_int_env("LMNR_HTTP_PORT"),
                grpc_port=_get_int_env("LMNR_GRPC_PORT"),
            )
        else:
            Laminar.initialize(
                disabled_instruments=[
                    Instruments.BROWSER_USE_SESSION,
                    Instruments.PATCHRIGHT,
                    Instruments.PLAYWRIGHT,
                ],
            )
        config.callbacks.append(LaminarLiteLLMCallback())
        
        self.Laminar = Laminar
        self._stack: list[trace.Span] = []

    def start_active_span(self, name: str, session_id: str | None = None):
        span = self.Laminar.start_active_span(name)
        if session_id:
            self.Laminar.set_trace_session_id(session_id)
        self._stack.append(span)

    def end_active_span(self):
        if not self._stack:
            logger.warning("Attempted to end active span, but stack is empty")
            return
        try:
            span = self._stack.pop()
            if span and span.is_recording():
                span.end()
        except IndexError:
            pass

_active_backend: _TracerBackend | None = None
_is_laminar_initialized = False

def _ensure_initialized() -> _TracerBackend:
    global _is_laminar_initialized, _active_backend
    if not _is_laminar_initialized:
        if should_enable_observability():
            try:
                _active_backend = _LaminarBackend()
                register_interceptor(otel_log_interceptor)
                logger.info("Observability enabled: Laminar backend initialized.")
            except ImportError as e:
                logger.warning(f"Observability enabled, but missing dependencies. Running in Null mode: {e}")
                _active_backend = _TracerBackend()
        else:
            _active_backend = _TracerBackend()
        _is_laminar_initialized = True
    return _active_backend

## Public API
def start_active_span(name: str, session_id: str | None = None) -> None:
    _ensure_initialized().start_active_span(name, session_id)

def end_active_span() -> None:
    try:
        _ensure_initialized().end_active_span()
    except Exception:
        logger.debug("Error ending active span")

@contextmanager
def unified_flow_span(name: str, session_id: str | None = None, auto_flush: bool = False, **flow_kwargs):
    backend = _ensure_initialized()
    backend.start_active_span(name, session_id)
    
    if "phase" not in flow_kwargs:
        flow_kwargs["phase"] = name

    try:
        with flow_scope(auto_flush=auto_flush, **flow_kwargs) as ctx:
            yield ctx
    finally:
        backend.end_active_span()

def observe(**kwargs):
    """데코레이터 호출 시점에 Laminar 데코레이터를 동적으로 주입"""
    def decorator(func: Callable) -> Callable:
        is_async = inspect.iscoroutinefunction(func)
        if is_async:
            @functools.wraps(func)
            async def wrapper(*args, **kw):
                _ensure_initialized()
                return await func(*args, **kw)
        else:
            @functools.wraps(func)
            def wrapper(*args, **kw):
                _ensure_initialized()
                return func(*args, **kw)

        ## 시스템 환경변수에서 관측망 활성화가 감지될 때만 외부 의존성 데코레이터 시도
        if should_enable_observability():
            try:
                from lmnr import observe as laminar_observe
                return laminar_observe(**kwargs)(wrapper)
            except ImportError:
                pass
                
        return wrapper
    return decorator