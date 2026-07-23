# bound.gateway.stream.wrapper
## @lineage: gateway.stream.wrapper
## @lineage: bound.stream.wrapper
## @lineage: bound.transport.stream.wrapper
## @lineage: bound.surface.stream.wrapper
import asyncio
import collections.abc
import datetime
import threading
import time
import traceback
from typing import Any, AsyncIterator, Callable, Iterator, List, NoReturn, Optional
import anyio
import httpx

from atoa.executor.legacy import executor
from bound.resolver.model.config.constants import LITELLM_MAX_STREAMING_DURATION_SECONDS
from bound.resolver.model.config.resolver import config

from eco.exception import OpenAIError
from bound.gateway.adapter.mapper.exception import exception_type
from eco.legacy.types import CallTypes
from bound.gateway.adapter.switch.params import ModelResponse, ModelResponseStream
from bound.gateway.stream.bridge.rule import Rules
from bound.gateway.stream.chunk import stream_chunk_builder
from bound.gateway.stream.support import _next_sync_or_exhausted
from bound.gateway.stream.processor.chunk import StreamChunkProcessor

from bound.watcher.delegator import LogDelegator 
from watcher.plane.emitter import get_emitter

_SYNC_ITER_EXHAUSTED = object()
log = get_emitter("streaming.wrapper")

class StreamWrapper:
    """
    스트림 연결 유지, 비동기/동기 순회(Iterator), 로깅 및 예외 처리를 담당하는 메인 래퍼.
    실제 청크의 파싱 및 규격 조립은 내부의 `StreamChunkProcessor`에게 위임(Composition)합니다.
    """
    def __init__(
        self,
        completion_stream: Any,
        model: str,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str] = None,
        stream_options: Optional[dict] = None,
        make_call: Optional[Callable] = None,
        _response_headers: Optional[dict] = None,
    ):
        self.model = model
        self.make_call = make_call
        self.custom_llm_provider = custom_llm_provider
        self.logging_obj = logging_obj
        self.completion_stream = completion_stream
        
        # 1. 생명주기 및 로깅 상태
        self._stream_created_time: float = time.time()
        self.logging_loop = None
        self.rules = Rules()
        self.stream_options = stream_options or getattr(logging_obj, "stream_options", None)
        self.messages = getattr(logging_obj, "messages", None)
        self.chunks: List = []
        
        # Stream Usage 설정
        self.send_stream_usage = (self.stream_options is not None and self.stream_options.get("include_usage", False))
        self.sent_stream_usage = False

        # 2. 🚀 The Magic: 청크 데이터 처리를 전담할 프로세서 컴포지션
        # 래퍼의 chunks 리스트 참조를 주입하여 상태를 완벽히 공유(Call by Reference)
        self.processor = StreamChunkProcessor(
            model=self.model,
            custom_llm_provider=self.custom_llm_provider,
            logging_obj=self.logging_obj,
            completion_stream=self.completion_stream,
            _response_headers=_response_headers,
            chunks_ref=self.chunks  
        )

    def _check_max_streaming_duration(self) -> None:
        """스트림이 허용된 최대 지속 시간을 초과했는지 확인합니다."""
        if LITELLM_MAX_STREAMING_DURATION_SECONDS is None:
            return
        elapsed = time.time() - self._stream_created_time
        if elapsed > LITELLM_MAX_STREAMING_DURATION_SECONDS:
            raise config.Timeout(
                message=f"Stream exceeded max streaming duration of {LITELLM_MAX_STREAMING_DURATION_SECONDS}s (elapsed {elapsed:.1f}s)",
                model=self.model or "",
                llm_provider=self.custom_llm_provider or "",
            )

    def __iter__(self) -> Iterator["ModelResponseStream"]:
        return self

    def __aiter__(self) -> AsyncIterator["ModelResponseStream"]:
        return self

    async def aclose(self):
        """안전하게 스트림 리소스를 해제합니다."""
        if self.completion_stream is not None:
            stream_to_close = self.completion_stream
            self.completion_stream = None
            with anyio.CancelScope(shield=True):
                try:
                    if hasattr(stream_to_close, "aclose"):
                        await stream_to_close.aclose()
                    elif hasattr(stream_to_close, "close"):
                        result = stream_to_close.close()
                        if result is not None:
                            await result
                except BaseException as e:
                    log.debug(f"StreamWrapper.aclose: error closing completion_stream: {e}")

    def fetch_sync_stream(self):
        if self.completion_stream is None and self.make_call is not None:
            self.completion_stream = self.make_call(client=config.module_level_client)
            self._stream_iter = self.completion_stream.__iter__()
        return self.completion_stream

    async def fetch_stream(self):
        if self.completion_stream is None and self.make_call is not None:
            self.completion_stream = await self.make_call(client=config.module_level_aclient)
            self._stream_iter = self.completion_stream.__aiter__()
        return self.completion_stream

    def __next__(self) -> "ModelResponseStream":
        cache_hit = (self.custom_llm_provider == "cached_response")
        self._check_max_streaming_duration()
        
        try:
            if self.completion_stream is None:
                self.fetch_sync_stream()

            while True:
                if isinstance(self.completion_stream, (str, bytes, ModelResponse)):
                    chunk = self.completion_stream
                else:
                    chunk = next(self.completion_stream)
                    
                if chunk is not None and chunk != b"":
                    # 🚀 1. 프로세서에게 데이터 정제를 전적으로 위임
                    processed_chunk = self.processor.process_raw_chunk(chunk)
                    
                    if processed_chunk is None:
                        continue

                    # 2. 첫 응답 수신 시간 로깅
                    if self.logging_obj.completion_start_time is None:
                        self.logging_obj._update_completion_start_time(completion_start_time=datetime.datetime.now())

                    # 3. 로깅 및 캐시 비동기 전송
                    if not config.disable_streaming_logging:
                        executor.submit(self.run_success_logging_and_cache_storage, processed_chunk, cache_hit)

                    # 4. Rules 엔진 검사
                    self.rules.post_call_rules(input=self.processor.response_uptil_now, model=self.model)

                    # 5. 스트림 청크 누적 (Usage 청크 대비 딥카피 방어)
                    if getattr(processed_chunk, "usage", None) is not None:
                        self.chunks.append(processed_chunk.model_copy())
                    else:
                        self.chunks.append(processed_chunk)

                    return processed_chunk

        except StopIteration:
            if self.processor.sent_last_chunk is True:
                self._handle_stream_completion(cache_hit=cache_hit, is_async=False)
            raise StopIteration
        except Exception as e:
            self._handle_stream_error(e)

    async def __anext__(self) -> "ModelResponseStream":
        cache_hit = (self.custom_llm_provider == "cached_response")
        self._check_max_streaming_duration()
        
        try:
            if self.completion_stream is None:
                await self.fetch_stream()

            if isinstance(self.completion_stream, collections.abc.AsyncIterable):
                async for chunk in self.completion_stream:
                    if chunk is None or chunk == "None": 
                        continue
                    if self.custom_llm_provider == "gemini" and hasattr(chunk, "parts") and len(chunk.parts) == 0: 
                        continue
                    
                    # 🚀 1. 프로세서에게 데이터 정제를 전적으로 위임
                    processed_chunk = self.processor.process_raw_chunk(chunk)
                    if processed_chunk is None: 
                        continue

                    if self.logging_obj.completion_start_time is None:
                        self.logging_obj._update_completion_start_time(completion_start_time=datetime.datetime.now())

                    self.rules.post_call_rules(input=self.processor.response_uptil_now, model=self.model)

                    # 2. 누적 및 Usage 포맷팅 제어
                    if hasattr(processed_chunk, "usage") and getattr(processed_chunk, "usage", None) is not None:
                        self.chunks.append(processed_chunk.model_copy())
                    else:
                        self.chunks.append(processed_chunk)

                    # 3. 마지막 청크 Hook 호출 (프로세서의 상태를 참조)
                    if self.processor.sent_last_chunk:
                        processed_chunk = await self._call_post_streaming_deployment_hook(processed_chunk)

                    return processed_chunk
                
                raise StopAsyncIteration

            else:
                # 비동기를 지원하지 않는 동기 스트림(boto3 등)을 위한 Fallback
                while True:
                    if isinstance(self.completion_stream, (str, bytes)):
                        chunk = self.completion_stream
                    else:
                        chunk = await asyncio.to_thread(_next_sync_or_exhausted, self.completion_stream)
                        if chunk is _SYNC_ITER_EXHAUSTED: 
                            raise StopAsyncIteration
                    
                    if chunk is not None and chunk != b"":
                        processed_chunk = self.processor.process_raw_chunk(chunk)
                        if processed_chunk is None: 
                            continue
                        
                        self.rules.post_call_rules(input=self.processor.response_uptil_now, model=self.model)
                        self.chunks.append(processed_chunk)
                        return processed_chunk

        except (StopAsyncIteration, StopIteration):
            if self.processor.sent_last_chunk:
                self._handle_stream_completion(cache_hit=cache_hit, is_async=True)
            raise StopAsyncIteration
            
        except httpx.TimeoutException as e:
            self._handle_stream_error(e, is_timeout=True)
        except Exception as e:
            self._handle_stream_error(e)

    def _handle_stream_completion(self, cache_hit: bool, is_async: bool = False):
        """스트림이 정상 종료되었을 때, 전체 데이터를 재조립하여 캐싱 및 성공 로그를 남깁니다."""
        complete_streaming_response = stream_chunk_builder(
            chunks=self.chunks,
            messages=self.messages,
            logging_obj=self.logging_obj,
        )

        if complete_streaming_response is not None:
            try:
                _copy = complete_streaming_response.model_copy(deep=True)
            except RuntimeError:
                _copy = complete_streaming_response.model_copy()

            if is_async:
                asyncio.create_task(self.async_cache_streaming_response(processed_chunk=_copy, cache_hit=cache_hit))
                _deferred_cb = getattr(self.logging_obj, "_on_deferred_stream_complete", None)
                if _deferred_cb is not None:
                    self.logging_obj._deferred_stream_complete_args = (complete_streaming_response, cache_hit)
                else:
                    asyncio.create_task(
                        self.logging_obj.dispatch_success_handlers(
                            complete_streaming_response, cache_hit=cache_hit, prefer_async_handlers=True
                        )
                    )
            else:
                self.cache_streaming_response(processed_chunk=_copy, cache_hit=cache_hit)
                executor.submit(self.logging_obj.success_handler, _copy, None, None, cache_hit)

    def _handle_stream_error(self, e: Exception, is_timeout: bool = False) -> NoReturn:
        """스트림 중단 및 에러 발생 시 로그를 남기고, 통합된 에러 객체로 변환하여 던집니다."""
        traceback_exception = traceback.format_exc()
        
        if is_timeout:
            traceback_exception += f"\nLiteLLM Default Request Timeout - {config.request_timeout}"

        if self.logging_obj is not None:
            threading.Thread(target=self.logging_obj.failure_handler, args=(e, traceback_exception)).start()
            try:
                asyncio.create_task(self.logging_obj.async_failure_handler(e, traceback_exception))
            except RuntimeError:
                pass # Event loop is closed or not running
                
        self._handle_stream_fallback_error(e)

    def _handle_stream_fallback_error(self, e: Exception) -> NoReturn:
        from eco.exception import MidStreamFallbackError

        if isinstance(e, OpenAIError):
            mapped_exception: Exception = e
        else:
            try:
                mapped_exception = exception_type(
                    model=self.model,
                    custom_llm_provider=self.custom_llm_provider,
                    original_exception=e,
                    completion_kwargs={},
                    extra_kwargs={},
                )
            except Exception as mapping_error:
                mapped_exception = mapping_error

        raise MidStreamFallbackError(
            message=str(mapped_exception),
            model=self.model,
            llm_provider=self.custom_llm_provider or "anthropic",
            original_exception=mapped_exception,
            generated_content=self.processor.response_uptil_now, # Processor의 상태 참조
            is_pre_first_chunk=not self.processor.sent_first_chunk,
        )

    def set_logging_event_loop(self, loop):
        self.logging_loop = loop

    def cache_streaming_response(self, processed_chunk, cache_hit: bool):
        if not cache_hit and self.logging_obj._llm_caching_handler is not None:
            self.logging_obj._llm_caching_handler._sync_add_streaming_response_to_cache(processed_chunk)

    async def async_cache_streaming_response(self, processed_chunk, cache_hit: bool):
        if not cache_hit and self.logging_obj._llm_caching_handler is not None:
            await self.logging_obj._llm_caching_handler._add_streaming_response_to_cache(processed_chunk)

    def run_success_logging_and_cache_storage(self, processed_chunk, cache_hit: bool):
        if config.disable_streaming_logging is True:
            return
            
        if self.logging_loop is not None:
            future = asyncio.run_coroutine_threadsafe(
                self.logging_obj.async_success_handler(processed_chunk, None, None, cache_hit),
                loop=self.logging_loop,
            )
            future.result()
        else:
            try:
                asyncio.run(self.logging_obj.async_success_handler(processed_chunk, None, None, cache_hit))
            except RuntimeError:
                pass # Event loop conflicts 방어

        litellm_params = self.logging_obj.model_call_details.get("litellm_params", {})
        if self.logging_obj._is_sync_litellm_request(litellm_params):
            self.logging_obj.success_handler(processed_chunk, None, None, cache_hit)

    async def _call_post_streaming_deployment_hook(self, chunk):
        try:
            if getattr(self, "_post_streaming_hooks", None) is None:
                self._post_streaming_hooks = []
            if not self._post_streaming_hooks:
                return chunk

            request_data = self.logging_obj.model_call_details
            try:
                typed_call_type = CallTypes(self.logging_obj.call_type)
            except ValueError:
                typed_call_type = None

            for callback in self._post_streaming_hooks:
                result = await callback.async_post_call_streaming_deployment_hook(
                    request_data=request_data,
                    response_chunk=chunk,
                    call_type=typed_call_type,
                )
                if result is not None:
                    chunk = result
            return chunk
        except Exception as e:
            log.exception(f"Error in post-call streaming deployment hook: {str(e)}")
            return chunk