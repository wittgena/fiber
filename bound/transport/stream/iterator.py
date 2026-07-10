# bound.transport.stream.iterator
## @lineage: bound.surface.response.stream.iterator
from __future__ import annotations
import asyncio
import json
import time
import traceback
import datetime
from datetime import datetime
from typing import Any, Dict, Optional, Union
import httpx
from openai._streaming import SSEDecoder

import datetime
from typing import Any, Optional, Union

from bound.adapter.mapper.key import adapt_payload_for_external_litellm, get_legacy_key
from bound.surface.legacy.types import EmbeddingResponse, HiddenParams, ModelResponse, TranscriptionResponse
from anchor.registry.model.config.constants import LITELLM_DETAILED_TIMING
from bound.surface.bridge.tosync import SyncStreamAdapter

import bound.surface.legacy.openai.types as openai_types

from anchor.registry.model.config.resolver import config
from anchor.registry.model.config.constants import LITELLM_MAX_STREAMING_DURATION_SECONDS, STREAM_SSE_DONE_STRING
from bound.surface.legacy.openai.types import ResponsesAPIStreamEvents
from bound.surface.legacy.types import CallTypes
from anchor.registry.model.config.response import BaseResponsesAPIConfig
from anchor.executor.legacy import executor
from bound.transport.convert.header import process_response_headers
from anchor.registry.model.api.base import get_api_base
from anchor.registry.model.api.base import APIBridge
from bound.transport.stream.api.identity import IdentityRouter

from bound.watcher.delegator import LogDelegator
from watcher.plane.emitter import get_emitter

log = get_emitter("stream.iterator")

def _log_background_task_failure(task: "asyncio.Task[Any]", *, task_name: str) -> None:
    if task.cancelled():
        return
    exception = task.exception()
    if exception is not None:
        log.error("%s failed: %s", task_name, exception)

class ResponseStreamIterator:
    def __init__(
        self,
        response: httpx.Response,
        model: str,
        responses_api_provider_config: Optional[BaseResponsesAPIConfig],
        logging_obj: LogDelegator,
        litellm_metadata: Optional[Dict[str, Any]] = None,
        custom_llm_provider: Optional[str] = None,
        request_data: Optional[Dict[str, Any]] = None,
        call_type: Optional[str] = None,
    ):
        self.response = response
        self.model = model
        self.logging_obj = logging_obj
        self.finished = False
        self.responses_api_provider_config = responses_api_provider_config
        self.completed_response: Optional[Any] = None
        self.start_time = getattr(logging_obj, "start_time", datetime.now())
        self._failure_handled = False
        self._completed_response_cached = False
        self._completed_response_logged = False
        self._completed_response_cache_hit: Optional[bool] = None
        self._persist_completed_response_before_logging = True
        self._stream_created_time: float = time.time()

        # track request context for hooks
        self.litellm_metadata = litellm_metadata
        self.custom_llm_provider = custom_llm_provider
        self.request_data: Dict[str, Any] = request_data or {}
        self.call_type: Optional[str] = call_type

        # HTTPX Response 비동기 바이트 스트림 파서 초기화
        self.stream_iterator = SSEDecoder().aiter_bytes(response.aiter_bytes())

        _api_base = get_api_base(
            model=model or "",
            optional_params=self.logging_obj.model_call_details.get("litellm_params", {}),
        )
        _model_info: Dict = litellm_metadata.get("model_info", {}) if litellm_metadata else {}
        
        self._hidden_params = {
            "model_id": _model_info.get("id", None),
            "api_base": _api_base,
            "custom_llm_provider": custom_llm_provider,
        }
        self._hidden_params["additional_headers"] = process_response_headers(self.response.headers or {})

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        try:
            self._check_max_streaming_duration()
            while True:
                try:
                    sse = await self.stream_iterator.__anext__()
                except StopAsyncIteration:
                    self.finished = True
                    raise StopAsyncIteration

                self._check_max_streaming_duration()
                result = self._process_chunk(sse.data)

                if self.finished:
                    raise StopAsyncIteration
                elif result is not None:
                    # Post-processing hook 실행 후 즉시 반환
                    return await self._call_post_streaming_deployment_hook(chunk=result)

        except StopAsyncIteration:
            raise
        except Exception as e:
            self.finished = True
            self._handle_failure(e)
            raise e

    def _check_max_streaming_duration(self) -> None:
        if LITELLM_MAX_STREAMING_DURATION_SECONDS is None:
            return
        elapsed = time.time() - self._stream_created_time
        if elapsed > LITELLM_MAX_STREAMING_DURATION_SECONDS:
            raise config.Timeout(
                message=f"Stream exceeded max streaming duration of {LITELLM_MAX_STREAMING_DURATION_SECONDS}s (elapsed {elapsed:.1f}s)",
                model=self.model or "",
                llm_provider=self.custom_llm_provider or "",
            )

    def _process_chunk(self, chunk) -> Optional[Any]:
        if not chunk:
            return None

        if chunk == STREAM_SSE_DONE_STRING:
            self.finished = True
            return None

        try:
            parsed_chunk = json.loads(chunk)

            if isinstance(parsed_chunk, dict):
                if self.responses_api_provider_config is None:
                    raise ValueError("responses_api_provider_config is required to process live streaming chunks")
                
                openai_responses_api_chunk = self.responses_api_provider_config.transform_streaming_response(
                    model=self.model,
                    parsed_chunk=parsed_chunk,
                    logging_obj=self.logging_obj,
                )

                if "response" in parsed_chunk:
                    response_object = getattr(openai_responses_api_chunk, "response", None)
                    if response_object is not None:
                        response = APIBridge._update_responses_api_response_id_with_model_id(
                            responses_api_response=response_object,
                            litellm_metadata=self.litellm_metadata,
                            custom_llm_provider=self.custom_llm_provider,
                        )
                        setattr(openai_responses_api_chunk, "response", response)

                _event_type = getattr(openai_responses_api_chunk, "type", None)
                _stream_model_id = self.litellm_metadata.get("model_info", {}).get("id") if self.litellm_metadata else None
                
                if _event_type in (ResponsesAPIStreamEvents.OUTPUT_ITEM_ADDED, ResponsesAPIStreamEvents.OUTPUT_ITEM_DONE):
                    _item = getattr(openai_responses_api_chunk, "item", None)
                    if _item is not None:
                        APIBridge._encode_container_id_on_output_item(
                            item=_item, custom_llm_provider=self.custom_llm_provider, model_id=_stream_model_id
                        )
                elif _event_type == ResponsesAPIStreamEvents.OUTPUT_TEXT_ANNOTATION_ADDED:
                    _annotation = getattr(openai_responses_api_chunk, "annotation", None)
                    if _annotation is not None:
                        APIBridge._encode_container_id_on_output_item(
                            item=_annotation, custom_llm_provider=self.custom_llm_provider, model_id=_stream_model_id
                        )
                elif _event_type == ResponsesAPIStreamEvents.CONTENT_PART_DONE:
                    _part = getattr(openai_responses_api_chunk, "part", None)
                    if _part is not None:
                        annotations = _part.get("annotations") if isinstance(_part, dict) else getattr(_part, "annotations", None)
                        APIBridge._encode_container_ids_in_annotations(annotations, self.custom_llm_provider, _stream_model_id)

                if self.litellm_metadata and self.litellm_metadata.get("encrypted_content_affinity_enabled"):
                    if _event_type in (openai_types.ResponsesAPIStreamEvents.OUTPUT_ITEM_ADDED, openai_types.ResponsesAPIStreamEvents.OUTPUT_ITEM_DONE):
                        item = getattr(openai_responses_api_chunk, "item", None)
                        if item:
                            encrypted_content = getattr(item, "encrypted_content", None)
                            if encrypted_content and isinstance(encrypted_content, str) and _stream_model_id:
                                wrapped_content = IdentityRouter._wrap_encrypted_content_with_model_id(encrypted_content, _stream_model_id)
                                setattr(item, "encrypted_content", wrapped_content)

                _chunk_type = getattr(openai_responses_api_chunk, "type", None)
                if openai_responses_api_chunk and _chunk_type in (
                    openai_types.ResponsesAPIStreamEvents.RESPONSE_COMPLETED,
                    openai_types.ResponsesAPIStreamEvents.RESPONSE_INCOMPLETE,
                    openai_types.ResponsesAPIStreamEvents.RESPONSE_FAILED,
                ):
                    self.completed_response = openai_responses_api_chunk
                    
                    if config.include_cost_in_streaming_usage and self.logging_obj is not None:
                        response_obj: Optional[Any] = getattr(openai_responses_api_chunk, "response", None)
                        if response_obj:
                            usage_obj: Optional[Any] = getattr(response_obj, "usage", None)
                            if usage_obj is not None:
                                try:
                                    cost: Optional[float] = self.logging_obj._response_cost_calculator(result=response_obj)
                                    if cost is not None:
                                        setattr(usage_obj, "cost", cost)
                                except Exception:
                                    pass

                    if _chunk_type == openai_types.ResponsesAPIStreamEvents.RESPONSE_FAILED:
                        self._handle_logging_failed_response()
                    else:
                        self._log_completed_response()

                return openai_responses_api_chunk
            return None

        except json.JSONDecodeError:
            return None
        except Exception as e:
            self._handle_failure(e)
            raise

    def _log_completed_response(self) -> None:
        if self._completed_response_logged:
            return
        self._completed_response_logged = True

        if self._persist_completed_response_before_logging:
            self._persist_completed_response_to_cache()

        logging_response = self.completed_response
        if self.completed_response is not None and hasattr(self.completed_response, "model_dump"):
            try:
                logging_response = type(self.completed_response).model_validate(self.completed_response.model_dump())
            except Exception:
                pass

        end_time = datetime.now()
        
        # Async 로깅 태스크 백그라운드 실행
        asyncio.create_task(
            self.logging_obj.async_success_handler(
                result=logging_response, start_time=self.start_time, end_time=end_time, cache_hit=self._completed_response_cache_hit
            )
        )
        
        # 레거시 Sync 로깅 지원 (Executor 활용)
        executor.submit(
            self.logging_obj.success_handler,
            result=logging_response, cache_hit=self._completed_response_cache_hit, start_time=self.start_time, end_time=end_time
        )
        self._run_post_success_hooks(end_time=end_time)

    def _handle_logging_failed_response(self):
        response_obj = getattr(self.completed_response, "response", None) if self.completed_response else None
        error_info = getattr(response_obj, "error", None) if response_obj else None
        error_message = error_info.get("message", str(error_info)) if isinstance(error_info, dict) else "Response failed"
        
        exception = config.APIError(
            status_code=500, message=error_message, llm_provider=self.custom_llm_provider or "", model=self.model or ""
        )
        self._handle_failure(exception)

    def _get_completed_response_object(self) -> Optional[Any]:
        completed_response = self.completed_response
        if isinstance(completed_response, openai_types.ResponsesAPIResponse):
            return completed_response
        response_obj = getattr(completed_response, "response", None)
        if isinstance(response_obj, openai_types.ResponsesAPIResponse):
            return response_obj
        return None

    def _persist_completed_response_to_cache(self) -> None:
        if self._completed_response_cached or config.cache is None:
            return

        completed_response = self.completed_response
        if getattr(completed_response, "type", None) != openai_types.ResponsesAPIStreamEvents.RESPONSE_COMPLETED:
            return

        response_obj = self._get_completed_response_object()
        caching_handler = getattr(self.logging_obj, "_llm_caching_handler", None)
        if response_obj is None or caching_handler is None:
            return

        request_kwargs = getattr(caching_handler, "request_kwargs", None)
        if not isinstance(request_kwargs, dict) or request_kwargs.get("stream") is not True:
            return
            
        request_kwargs = request_kwargs.copy()
        preset_cache_key = getattr(caching_handler, "preset_cache_key", None)
        request_cache_key = request_kwargs.pop("cache_key", None)
        if preset_cache_key is None:
            preset_cache_key = request_cache_key
        request_kwargs.pop("metadata", None)
        request_kwargs.pop("custom_llm_provider", None)
        if preset_cache_key is not None:
            request_kwargs["cache_key"] = preset_cache_key

        if not caching_handler._should_store_result_in_cache(original_function=caching_handler.original_function, kwargs=request_kwargs):
            return

        cached_response = response_obj.model_dump_json()
        cache_write_task = asyncio.create_task(
            config.cache.async_add_cache(
                cached_response, dynamic_cache_object=getattr(caching_handler, "dual_cache", None), **request_kwargs
            )
        )
        cache_write_task.add_done_callback(
            lambda task: _log_background_task_failure(task, task_name="Responses stream cache write")
        )
        self._completed_response_cached = True

    async def _call_post_streaming_deployment_hook(self, chunk):
        try:
            typed_call_type: Optional[CallTypes] = None
            if self.call_type is not None:
                try: typed_call_type = CallTypes(self.call_type)
                except ValueError: pass
                
            if typed_call_type is None:
                try: typed_call_type = CallTypes(getattr(self.logging_obj, "call_type", None))
                except Exception: pass

            request_data = self.request_data or getattr(self.logging_obj, "model_call_details", {})
            hooks_ran = False
            
            for callback in config.callbacks:
                if hasattr(callback, "async_post_call_streaming_deployment_hook"):
                    hooks_ran = True
                    result = await callback.async_post_call_streaming_deployment_hook(
                        request_data=request_data, response_chunk=chunk, call_type=typed_call_type
                    )
                    if result is not None:
                        chunk = result
                        
            if hooks_ran:
                setattr(chunk, "_post_streaming_hooks_ran", True)
            return chunk
        except Exception:
            return chunk

    def _run_post_success_hooks(self, end_time: datetime):
        if self.completed_response is None:
            return

        request_payload: Dict[str, Any] = {}
        if isinstance(self.request_data, dict): request_payload.update(self.request_data)
        
        try:
            if hasattr(self.logging_obj, "model_call_details"):
                request_payload.update(self.logging_obj.model_call_details)
        except Exception: pass
        
        if "litellm_params" not in request_payload:
            try: request_payload["litellm_params"] = getattr(self.logging_obj, "model_call_details", {}).get("litellm_params", {})
            except Exception: request_payload["litellm_params"] = {}

        try:
            update_response_metadata(
                result=self.completed_response, logging_obj=self.logging_obj, model=self.model,
                kwargs=request_payload, start_time=self.start_time, end_time=end_time
            )
        except Exception: pass

    def _handle_failure(self, exception: Exception):
        if self._failure_handled:
            return
        self._failure_handled = True

        traceback_exception = traceback.format_exc()
        
        # Async 훅 비동기 실행 (이전의 run_async_function 제거)
        try:
            asyncio.create_task(
                self.logging_obj.async_failure_handler(
                    exception=exception, traceback_exception=traceback_exception, 
                    start_time=self.start_time, end_time=datetime.now()
                )
            )
        except Exception: pass

        # 레거시 Sync 지원 훅
        try:
            executor.submit(
                self.logging_obj.failure_handler,
                exception, traceback_exception, self.start_time, datetime.now()
            )
        except Exception: pass


async def call_post_streaming_hooks_for_testing(iterator, chunk):
    hook_fn = getattr(iterator, "_call_post_streaming_deployment_hook", None)
    if hook_fn is None:
        return chunk
    return await hook_fn(chunk)

class ResponseMetadata:
    def __init__(self, result: Any):
        self.result = result
        self._hidden_params: Union[HiddenParams, dict] = getattr(result, "_hidden_params", {}) or {}

    @property
    def supports_response_time(self) -> bool:
        """Check if response type supports timing metrics"""
        return (
            isinstance(self.result, ModelResponse)
            or isinstance(self.result, EmbeddingResponse)
            or isinstance(self.result, TranscriptionResponse)
        )

    def set_hidden_params(self, logging_obj: LogDelegator, model: Optional[str], kwargs: dict) -> None:
        """Set hidden parameters on the response"""

        model_info = kwargs.get("model_info", {}) or {}
        model_id = model_info.get("id", None)
        new_params = {
            "call_id": getattr(logging_obj, "call_id", None),
            "api_base": get_api_base(model=model or "", optional_params=kwargs),
            "model_id": model_id,
            "response_cost": logging_obj._response_cost_calculator(result=self.result, model_name=model, router_model_id=model_id),
            "additional_headers": process_response_headers(self._get_value_from_hidden_params("additional_headers") or {}),
            "model_name": model,
        }
        self._update_hidden_params(new_params)

    def _update_hidden_params(self, new_params: dict) -> None:
        """Update hidden params - dynamically maps clean keys to legacy keys"""
        adapted_params = adapt_payload_for_external_litellm(new_params)
        
        # Handle both dict and HiddenParams cases (using adapted_params instead of new_params)
        if isinstance(self._hidden_params, dict):
            self._hidden_params.update(adapted_params)
        elif isinstance(self._hidden_params, HiddenParams):
            # For HiddenParams object, set attributes individually
            for key, value in adapted_params.items():
                setattr(self._hidden_params, key, value)

    def _get_value_from_hidden_params(self, key: str) -> Optional[Any]:
        """Get value from hidden params - dynamically resolves legacy keys for reads"""
        search_key = get_legacy_key(key)
        
        if isinstance(self._hidden_params, dict):
            return self._hidden_params.get(search_key, None)
        elif isinstance(self._hidden_params, HiddenParams):
            return getattr(self._hidden_params, search_key, None)

    def set_timing_metrics(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        logging_obj: LogDelegator,
    ) -> None:
        """Set response timing metrics"""
        total_response_time_ms = (end_time - start_time).total_seconds() * 1000

        # Set total response time if supported
        if self.supports_response_time:
            self.result._response_ms = total_response_time_ms

        self._update_hidden_params({"_response_ms": total_response_time_ms,})
        llm_api_duration_ms = logging_obj.model_call_details.get("llm_api_duration_ms")
        if llm_api_duration_ms is not None:
            overhead_ms = round(total_response_time_ms - llm_api_duration_ms, 4)
            self._update_hidden_params({"overhead_time_ms": overhead_ms})

        callback_duration_ms = getattr(logging_obj, "callback_duration_ms", None)
        if callback_duration_ms is not None:
            self._update_hidden_params({"callback_duration_ms": round(callback_duration_ms, 4)})

        if (
            logging_obj.caching_details is not None
            and logging_obj.caching_details.get("cache_hit") is True
            and (
                cache_duration_ms := logging_obj.caching_details.get(
                    "cache_duration_ms"
                )
            )
            is not None
        ):
            overhead_ms = total_response_time_ms - cache_duration_ms
            self._update_hidden_params({"overhead_time_ms": overhead_ms})

        if LITELLM_DETAILED_TIMING and llm_api_duration_ms is not None:
            detailed: dict = {"timing_llm_api_ms": round(llm_api_duration_ms, 4)}

            ## message copy time from Logging.__init__()
            msg_copy_ms = getattr(logging_obj, "message_copy_duration_ms", None)
            if msg_copy_ms is not None:
                detailed["timing_message_copy_ms"] = round(msg_copy_ms, 4)

            ## pre-processing = time from request start to LLM API call start
            api_call_start = logging_obj.model_call_details.get("api_call_start_time")
            if api_call_start is not None and start_time is not None:
                pre_ms = (api_call_start - start_time).total_seconds() * 1000
                detailed["timing_pre_processing_ms"] = round(pre_ms, 4)

                ## post-processing = total - pre - llm_api
                post_ms = total_response_time_ms - pre_ms - llm_api_duration_ms
                detailed["timing_post_processing_ms"] = round(max(post_ms, 0), 4)

            self._update_hidden_params(detailed)

    def apply(self) -> None:
        """Apply metadata to the response object"""
        if hasattr(self.result, "_hidden_params"):
            self.result._hidden_params = self._hidden_params


def update_response_metadata(
    result: Any,
    logging_obj: LogDelegator,
    model: Optional[str],
    kwargs: dict,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
) -> None:
    if result is None:
        return

    metadata = ResponseMetadata(result)
    metadata.set_hidden_params(logging_obj, model, kwargs)
    metadata.set_timing_metrics(start_time, end_time, logging_obj)
    metadata.apply()


"""Legacy Import Mapping & Async-to-Sync Bridge"""
ResponsesAPIStreamingIterator = ResponseStreamIterator
MockResponsesAPIStreamingIterator = ResponseStreamIterator
CachedResponsesAPIStreamingIterator = ResponseStreamIterator

class SyncResponsesAPIStreamingIterator(SyncStreamAdapter):
    def __init__(self, *args, **kwargs):
        async_iterator = ResponseStreamIterator(*args, **kwargs)
        super().__init__(async_gen=async_iterator)
        
    def __getattr__(self, name):
        return getattr(self.async_gen, name)