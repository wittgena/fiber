from __future__ import annotations

import uuid
import time
from typing import Any, Dict, Optional

from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.gateway.llm.pipeline import PipelineSlot
from fiber.llm.response import ModelResponse
from fiber.llm.types.provider.core import Usage
from fiber.gateway.llm.context.metadata import ExecutionMetadata
from fiber.gateway.llm.state.traverser import StateTraverser

from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter

tracer_log = get_emitter("llm.tracer")

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

class DummySemanticCache(DuplexChannel):
    """[Slot: PRE_TRANSLATE] Mock cache to perform I/O short-circuits."""
    target_slot = PipelineSlot.PRE_TRANSLATE
    
    async def write(self, ctx: ChannelContext, msg: dict):
        prompt = msg.get("messages", [{}])[-1].get("content", "")
        
        if "USE_CACHE" in prompt:
            tracer_log.info("🎯 [CACHE HIT] Short-circuiting physical I/O...")
            cached_response = ModelResponse(
                id=f"cache-{uuid.uuid4()}",
                model=msg.get("model", "cached-model"),
                choices=[{"index": 0, "message": {"role": "assistant", "content": "[CACHED] Hit!"}, "finish_reason": "stop"}],
                usage=Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0)
            )
            await ctx.fire_channel_read(cached_response)
            return
            
        await ctx.fire_write(msg)