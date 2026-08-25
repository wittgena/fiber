# llm.stream.wrapper
import asyncio
import collections.abc
import time
import traceback
import anyio
import httpx
from typing import Any, AsyncIterator, Callable, Optional, NoReturn
from collections import deque

from fiber.dphi.model.constants import MAX_STREAMING_DURATION_SECONDS

from fiber.llm.exception.mapping import exception_type
from fiber.llm.exception.eco import OpenAIError
from fiber.llm.param import ModelResponseStream
from fiber.llm.stream.pipeline import (
    ChunkCodecHandler, 
    RuleGuardHandler, 
    StreamTelemetryHandler, 
    StreamYieldHandler
)
from fiber.llm.stream.accumulator import StreamAccumulator

from xphi.arch.model.config import config
from xphi.kernel.space.topos.network.channel.pipeline import ChannelPipeline
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("stream.wrapper")

class StreamWrapper:
    def __init__(
        self,
        completion_stream: Any,
        model: str,
        system_meta: Optional[Any] = None,
        custom_llm_provider: Optional[str] = None,
        stream_options: Optional[dict] = None,
        make_call: Optional[Callable] = None,
        _response_headers: Optional[dict] = None,
    ):
        self.completion_stream = completion_stream
        self.model = model
        self.system_meta = system_meta
        self.custom_llm_provider = custom_llm_provider
        self.make_call = make_call
        
        self._stream_created_time = time.time()
        self.cache_hit = (self.custom_llm_provider == "cached_response")

        self.pipeline = ChannelPipeline()
        self.pipeline.attributes = {
            "model": self.model,
            "provider": self.custom_llm_provider,
            "system_meta": self.system_meta,
            "accumulator": StreamAccumulator(self.model, self.custom_llm_provider),
            "outbox": deque(),
            "framework_flags": {}
        }
        self.pipeline.add_last(ChunkCodecHandler())\
                     .add_last(RuleGuardHandler())\
                     .add_last(StreamTelemetryHandler())\
                     .add_last(StreamYieldHandler())

    def _check_max_streaming_duration(self) -> None:
        """배압(Backpressure) 및 타임아웃 방어"""
        elapsed = time.time() - self._stream_created_time
        if elapsed > MAX_STREAMING_DURATION_SECONDS:
            raise config.Timeout(
                message=f"Stream exceeded max streaming duration of {MAX_STREAMING_DURATION_SECONDS}s",
                model=self.model or "", llm_provider=self.custom_llm_provider or "",
            )

    def __aiter__(self) -> AsyncIterator["ModelResponseStream"]: 
        return self

    async def aclose(self):
        """Transport 리소스 해제"""
        if self.completion_stream is not None:
            stream_to_close = self.completion_stream
            self.completion_stream = None
            with anyio.CancelScope(shield=True):
                try:
                    if hasattr(stream_to_close, "aclose"): await stream_to_close.aclose()
                    elif hasattr(stream_to_close, "close"):
                        result = stream_to_close.close()
                        if result is not None: await result
                except BaseException as e:
                    log.debug(f"StreamWrapper.aclose error: {e}")

    async def _fetch_async_stream(self):
        if self.completion_stream is None and self.make_call is not None:
            self.completion_stream = await self.make_call(client=config.module_level_aclient)

    async def __anext__(self) -> "ModelResponseStream":
        self._check_max_streaming_duration()
        try:
            await self._fetch_async_stream()
            if not isinstance(self.completion_stream, collections.abc.AsyncIterable):
                raise TypeError("StreamWrapper now exclusively supports AsyncIterable sources.")
            
            while True:
                outbox: deque = self.pipeline.attributes["outbox"]
                if outbox:
                    processed_chunk = outbox.popleft()
                    if not config.get("disable_streaming_logging", False):
                        log.trace("Stream chunk yielded", model=self.model, chunk_id=getattr(processed_chunk, "id", None))
                    return processed_chunk

                chunk = await self.completion_stream.__anext__()
                if chunk is None or chunk == "None" or (self.custom_llm_provider == "gemini" and hasattr(chunk, "parts") and len(chunk.parts) == 0):
                    continue
                
                await self.pipeline._process_read(chunk, index=0)

        except StopAsyncIteration:
            raise StopAsyncIteration
        except Exception as e:
            self._on_exception_caught(e)

    def _on_exception_caught(self, e: Exception) -> NoReturn:
        """예외 발생 시 Fallback 예외 생성 (로깅은 client.wrapper가 담당)"""
        is_timeout = isinstance(e, httpx.TimeoutException)
        if is_timeout: 
            e.args = (*e.args, f"\nRequest Timeout - {config.request_timeout}")
        self._fire_fallback_error(e)

    def _fire_fallback_error(self, e: Exception) -> NoReturn:
        from fiber.llm.exception.eco import MidStreamFallbackError
        if isinstance(e, OpenAIError): 
            mapped_exception = e
        else:
            try: 
                mapped_exception = exception_type(model=self.model, custom_llm_provider=self.custom_llm_provider, original_exception=e, completion_kwargs={}, extra_kwargs={})
            except Exception as mapping_error: 
                mapped_exception = mapping_error

        accumulator = self.pipeline.attributes["accumulator"]
        raise MidStreamFallbackError(
            message=str(mapped_exception), model=self.model, llm_provider=self.custom_llm_provider or "unknown",
            original_exception=mapped_exception, 
            generated_content=accumulator.response_uptil_now,
            is_pre_first_chunk=not accumulator.sent_first_chunk,
        )