# fiber.dev.trace.llm.debugger
from __future__ import annotations

import uuid
from typing import Any, Dict

from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.llm.pipeline import PipelineSlot
from fiber.llm.param import ModelResponse
from fiber.llm.context.metadata import ExecutionMetadata
from fiber.llm.router.mapper.traverser import StateTraverser

from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter

tracer_log = get_emitter("plugin.tracer")

## [MOCK INTERCEPTORS] 파이프라인 훅(Hook) 검증용 Mock 객체들
class DebugTracer(BaseLLMTracer):
    """[Slot: PRE_OBSERVER] 실전형 비동기 방출 트레이서 (상세 시각화 로깅 지원)"""
    def __init__(self):
        self.started = False
        self.ended = False
        self.error = False
        self.duration = 0.0

    async def on_llm_start(self, meta: ExecutionMetadata, kwargs: Dict[str, Any]):
        self.started = True
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
        
        # StateTraverser를 이용한 우아한 데이터 추출
        total_tokens = StateTraverser.resolve(response, "usage.total_tokens", "N/A")
        
        tracer_log.info(
            f"\n[✅ LLM CALL COMPLETED] \n"
            f" ├─ Trace ID : {meta.trace_id}\n"
            f" ├─ Latency  : {duration_ms:.2f} ms\n"
            f" └─ Usage    : {total_tokens} tokens"
        )

    async def on_llm_error(self, meta: ExecutionMetadata, exc: Exception, duration_ms: float):
        self.error = True
        tracer_log.error(
            f"\n[🚨 LLM CALL FAILED] \n"
            f" ├─ Trace ID : {meta.trace_id}\n"
            f" ├─ Latency  : {duration_ms:.2f} ms\n"
            f" └─ Error    : {type(exc).__name__} - {str(exc)}"
        )

class DummySemanticCache(DuplexChannel):
    """[Slot: PRE_TRANSLATE] I/O 숏서킷을 수행하는 모의 캐시"""
    target_slot = PipelineSlot.PRE_TRANSLATE
    
    async def write(self, ctx: ChannelContext, msg: dict):
        prompt = msg.get("messages", [{}])[-1].get("content", "")
        if "USE_CACHE" in prompt:
            tracer_log.info("🎯 [CACHE HIT] Short-circuiting physical I/O...")
            cached_response = ModelResponse(
                id=f"cache-{uuid.uuid4()}",
                model=msg.get("model", "cached-model"),
                choices=[{"index": 0, "message": {"role": "assistant", "content": "[CACHED] Hit!"}, "finish_reason": "stop"}],
                usage={"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            )
            await ctx.fire_channel_read(cached_response)
            return
        await ctx.fire_write(msg)

class DummyPIIGuardrail(DuplexChannel):
    """[Slot: POST_TRANSLATE] 검증된 객체 상태에서 민감 정보를 차단하는 가드레일"""
    target_slot = PipelineSlot.POST_TRANSLATE
    
    async def write(self, ctx: ChannelContext, processed_msg: Any):
        original_kwargs = getattr(processed_msg, "original_kwargs", {})
        messages = original_kwargs.get("messages", [])
        for msg in messages:
            if "SECRET-SSN" in msg.get("content", ""):
                tracer_log.error("🛑 [GUARDRAIL BLOCK] Sensitive Information (PII) Detected!")
                await ctx.fire_exception_caught(PermissionError("Guardrail Triggered: PII (SSN) detected."))
                return  
        await ctx.fire_write(processed_msg)