# fiber.dev.trace.llm.interceptor
import asyncio
import copy
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from fiber.llm.execution import ExecutionMetadata
from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("llm.tracer")

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

class TracerInterceptorChannel(DuplexChannel):
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
            log.warning(f"[Tracer] Custom tracer '{func.__self__.__class__.__name__}' fractured: {e}")