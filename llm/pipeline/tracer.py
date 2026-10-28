# fiber.llm.pipeline.tracer
from __future__ import annotations

import asyncio
import copy
import json
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from fiber.gateway.llm.context.metadata import ExecutionMetadata
from fiber.llm.pipeline.bootstrap import PipelineSlot
from fiber.gateway.llm.state.traverser import StateTraverser
from fiber.llm.response import ModelResponse
from fiber.llm.model.types.provider.core import Usage
from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.watcher.plane.emitter import get_emitter

tracer_log = get_emitter("llm.tracer")

class BaseLLMTracer(ABC):
    @abstractmethod
    async def on_llm_start(self, meta: ExecutionMetadata, kwargs: Dict[str, Any]):
        pass

    @abstractmethod
    async def on_llm_end(self, meta: ExecutionMetadata, response: Any, duration_ms: float):
        pass

    @abstractmethod
    async def on_llm_error(self, meta: ExecutionMetadata, exc: Exception, duration_ms: float):
        pass


class DebugTracer(BaseLLMTracer):
    """[Slot: PRE_OBSERVER] Practical async tracer with detailed visual logging."""
    def __init__(self):
        self.started = False
        self.ended = False
        self.error = False
        self.duration = 0.0
        self.start_time = 0.0
        
        # Added attributes to expose states for assertion tests
        self.meta: Optional[ExecutionMetadata] = None
        self.response: Optional[Any] = None

    async def on_llm_start(self, meta: ExecutionMetadata, kwargs: Dict[str, Any]):
        self.started = True
        self.start_time = time.time()
        self.meta = meta
        
        safe_kwargs = {k: v for k, v in kwargs.items() if k not in ["api_key", "headers", "interceptors", "pipeline_hooks"]}
        tracer_log.info(
            f"\n[🔍 LLM CALL INITIATED] \n"
            f" ├─ Trace ID : {meta.trace_id}\n"
            f" ├─ Model    : {meta.base_model}\n"
            f" ├─ Config   : {safe_kwargs.get('temperature', 0.7)} Temp\n"
            f" └─ Messages : {len(kwargs.get('messages', []))} items"
        )

    async def on_llm_end(self, meta: ExecutionMetadata, response: Any, duration_ms: float):
        self.ended = True
        self.duration = duration_ms
        self.meta = meta
        self.response = response
        
        is_stream = hasattr(response, "__aiter__")
        if is_stream:
            tracer_log.info(
                f"\n[🌊 LLM STREAM ESTABLISHED] \n"
                f" ├─ Trace ID : {meta.trace_id}\n"
                f" ├─ Handshake: {duration_ms:.2f} ms\n"
                f" └─ Note     : Usage will be tracked dynamically at stream end."
            )
            
            def deferred_stream_log(total_tokens: int, **kwargs):
                real_duration = (time.time() - self.start_time) * 1000
                tracer_log.info(
                    f"\n[✅ STREAM COMPLETED] \n"
                    f" ├─ Trace ID : {meta.trace_id}\n"
                    f" ├─ Latency  : {real_duration:.2f} ms\n"
                    f" └─ Usage    : {total_tokens} tokens"
                )

            hooks = meta.framework_flags.setdefault("on_stream_complete_hooks", [])
            hooks.append(deferred_stream_log)
            return

        total_tokens = StateTraverser.resolve(response, "usage.total_tokens", "N/A")
        raw_resp = getattr(response, "raw", None)
        
        if raw_resp is not None:
            raw_usage = StateTraverser.resolve(raw_resp, "usage_metadata")
            if not raw_usage:
                raw_usage = StateTraverser.resolve(raw_resp, "usage")

            if raw_usage:
                tracer_log.debug(f"[🔍 DEBUG] Original Raw API payload contains usage data: {raw_usage}")
            else:
                tracer_log.debug(f"[🔍 DEBUG] Original Raw API payload DOES NOT contain usage metadata. (Token usage is purely 0 from Provider: {meta.base_model})")
        else:
            tracer_log.debug(f"[🔍 DEBUG] 'response.raw' is missing for model {meta.base_model}. Cannot inspect original API payload.")
        
        tracer_log.info(
            f"\n[✅ LLM CALL COMPLETED] \n"
            f" ├─ Trace ID : {meta.trace_id}\n"
            f" ├─ Latency  : {duration_ms:.2f} ms\n"
            f" └─ Usage    : {total_tokens} tokens"
        )

    async def on_llm_error(self, meta: ExecutionMetadata, exc: Exception, duration_ms: float):
        self.error = True
        self.meta = meta
        
        tracer_log.error(
            f"\n[🚨 LLM CALL FAILED] \n"
            f" ├─ Trace ID : {meta.trace_id}\n"
            f" ├─ Latency  : {duration_ms:.2f} ms\n"
            f" └─ Error    : {type(exc).__name__} - {str(exc)}"
        )


class TracerInterceptorChannel(DuplexChannel):
    target_slot = PipelineSlot.PRE_OBSERVER

    def __init__(self, tracers: List[BaseLLMTracer]):
        self.tracers = tracers

    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        ctx.set_attr("tracer_start_time", time.time())
        meta = ctx.get_attr("system_meta")
        safe_kwargs = copy.deepcopy(msg)
        for tracer in self.tracers:
            asyncio.create_task(self._safe_execute(tracer.on_llm_start, meta, safe_kwargs))
            
        await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        start_time = ctx.get_attr("tracer_start_time", time.time())
        duration_ms = (time.time() - start_time) * 1000
        meta = ctx.get_attr("system_meta")
        
        for tracer in self.tracers:
            asyncio.create_task(self._safe_execute(tracer.on_llm_end, meta, msg, duration_ms))
            
        await ctx.fire_channel_read(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        start_time = ctx.get_attr("tracer_start_time", time.time())
        duration_ms = (time.time() - start_time) * 1000
        meta = ctx.get_attr("system_meta")
        
        for tracer in self.tracers:
            asyncio.create_task(self._safe_execute(tracer.on_llm_error, meta, exc, duration_ms))
            
        await ctx.fire_exception_caught(exc)

    async def _safe_execute(self, func, *args):
        try:
            await func(*args)
        except Exception as e:
            tracer_log.warning(f"[Tracer] Custom tracer '{func.__self__.__class__.__name__}' fractured: {e}")


class LlmTraceProfile:
    def __init__(self, target_model: str):
        self.target_model = target_model

    def _normalize_response(self, response: Union[Dict[str, Any], Any]) -> Dict[str, Any]:
        if isinstance(response, dict):
            return response
        if hasattr(response, "model_dump"):
            return response.model_dump(exclude_none=True)
        if hasattr(response, "__dict__"):
            return response.__dict__
        return dict(response)

    def build_mock_bypass_payload(self) -> Dict[str, Any]:
        return {
            "model": self.target_model,
            "messages": [{"role": "user", "content": "Cost me nothing!"}],
            "mock_response": "This is a bypassed mock response.",
            "mock_delay": 0.1,
            "stream": False,
            "metadata": {"node_auth": {"audit_hash": "audit_mock_shared"}}
        }

    def verify_mock_bypass(self, response: Union[Dict[str, Any], Any]) -> bool:
        res_dict = self._normalize_response(response)
        try:
            choices = res_dict.get("choices", [{}])
            if not choices:
                raise ValueError("Trace Miss: Choices array is empty.")
                
            content = choices[0].get("message", {}).get("content", "")
            if "This is a bypassed mock response." not in content:
                raise ValueError(f"Trace Miss: Expected mock string not found. Got: {content}")
            return True
        except (IndexError, AttributeError) as e:
            raise ValueError(f"Trace Miss: Invalid response structure. {e}")

    def extract_stream_finish_reason(self, chunk: Union[Dict[str, Any], Any]) -> Optional[str]:
        """스트리밍 청크에서 finish_reason을 추출합니다."""
        chunk_dict = self._normalize_response(chunk)
        choices = chunk_dict.get("choices", [])
        if choices and isinstance(choices, list) and len(choices) > 0:
            # Delta 구조 또는 Message 구조 모두 대응
            return choices[0].get("finish_reason")
        return None

    def build_fallback_payload(self, fallbacks: List[Any]) -> Dict[str, Any]:
        return {
            "model": "invalid-trigger-model", 
            "messages": [{"role": "user", "content": "Trigger fallback mechanism"}],
            "fallbacks": fallbacks,
            "stream": False,
            "metadata": {"node_auth": {"audit_hash": "audit_fallback_shared"}}
        }

    def verify_fallback(self, response: Union[Dict[str, Any], Any], expected_models: List[str]) -> bool:
        res_dict = self._normalize_response(response)
        actual_model = res_dict.get("model", "")
        
        # 모델명 검증
        model_matched = any(em in actual_model for em in expected_models)
        if not model_matched:
             raise ValueError(f"Trace Miss: Fallback model mismatch. Actual '{actual_model}' not in expected {expected_models}.")
             
        # E2E Trace 환경일 경우, 메타데이터에 Graph가 기록되었는지 검증
        provider_metadata = res_dict.get("provider_metadata") or {}
        trace_graph = provider_metadata.get("trace_graph", [])
        
        if trace_graph:
            fallback_traced = any(t.get("stage") == "FallbackHandler" for t in trace_graph)
            if not fallback_traced:
                raise ValueError("Trace Miss: Fallback occurred but was not traced in provider_metadata.trace_graph.")
                
        return True

    def build_prompt_mutation_payload(self) -> Dict[str, Any]:
        return {
            "model": self.target_model,
            "messages": [{"role": "user", "content": "Testing prompt transformer"}],
            "tools": [],
            "stream": False,
            "metadata": {"node_auth": {"audit_hash": "audit_mutation_shared"}}
        }

    def verify_prompt_mutation(self, response: Union[Dict[str, Any], Any]) -> bool:
        res_dict = self._normalize_response(response)
        provider_metadata = res_dict.get("provider_metadata") or {}
        trace_graph = provider_metadata.get("trace_graph", [])
        
        if trace_graph:
            mutation_traced = any(
                t.get("stage") == "PromptTransformer" and t.get("action") == "tools_nulled" 
                for t in trace_graph
            )
            if not mutation_traced:
                raise ValueError("Trace Miss: Prompt mutation (empty tools -> None) was not traced in breadcrumbs.")
        
        return True