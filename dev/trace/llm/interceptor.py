# fiber.dev.trace.llm.interceptor
## @lineage: fiber.llm.tracer
import asyncio
import copy
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from fiber.llm.execution import ExecutionMetadata
from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("llm.tracer")

# =========================================================
# 1. User Interface (트레이서 규약)
# =========================================================
class BaseLLMTracer(ABC):
    """사용자(개발자)가 상속받아 구현할 커스텀 트레이서의 규약"""
    
    @abstractmethod
    async def on_llm_start(self, meta: ExecutionMetadata, kwargs: Dict[str, Any]):
        pass

    @abstractmethod
    async def on_llm_end(self, meta: ExecutionMetadata, response: Any, duration_ms: float):
        pass

    @abstractmethod
    async def on_llm_error(self, meta: ExecutionMetadata, exc: Exception, duration_ms: float):
        pass

# =========================================================
# 2. Interceptor Channel (통제관/샌드박스)
# =========================================================
class TracerInterceptorChannel(DuplexChannel):
    """사용자 트레이서를 안전하게 격리 실행하는 파이프라인 핸들러 (Guardrail)"""
    def __init__(self, tracers: List[BaseLLMTracer]):
        self.tracers = tracers

    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        ctx.set_attr("tracer_start_time", time.time())
        meta = ctx.get_attr("system_meta")
        
        # [제약 1] Immutability: 사용자 트레이서가 원본 데이터를 조작하지 못하도록 깊은 복사본 제공
        safe_kwargs = copy.deepcopy(msg)
        
        # [제약 2] Non-blocking: 이벤트 루프 블로킹 방지를 위한 백그라운드 태스크 실행
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
        """[제약 3] Fault Isolation: 사용자 트레이서 코드가 크래시나도 메인 LLM 응답은 보호됨"""
        try:
            await func(*args)
        except Exception as e:
            log.warning(f"[Tracer] Custom tracer '{func.__self__.__class__.__name__}' fractured: {e}")