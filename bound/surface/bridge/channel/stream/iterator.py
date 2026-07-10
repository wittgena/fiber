# bound.surface.bridge.channel.stream.iterator
## @lineage: bound.channel.stream.iterator
## @lineage: bound.transport.stream.iterator
from __future__ import annotations
import asyncio
import json
import time
import traceback
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Literal, Optional
import httpx
from openai._streaming import SSEDecoder
import bound.surface.legacy.openai.types as openai_types
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.config.constants import LITELLM_MAX_STREAMING_DURATION_SECONDS, STREAM_SSE_DONE_STRING
from bound.surface.legacy.openai.types import ResponsesAPIStreamEvents
from bound.surface.legacy.types import CallTypes
from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from bound.surface.legacy.client.executor import executor
from bound.surface.bridge.channel.convert.asyncify import run_async_function
from bound.surface.bridge.channel.convert.header import process_response_headers
from bound.surface.bridge.channel.api import get_api_base
from bound.surface.legacy.response.metadata import update_response_metadata
from bound.surface.bridge.channel.api import APIBridge
from bound.surface.legacy.response.identity import ResponseIdentityManager

from bound.watcher.delegator import LogDelegator
from watcher.plane.emitter import get_emitter

log = get_emitter("streaming.iterator")

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
        self._failure_handled = False  # Track if failure handler has been called
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

        # set hidden params for response headers (e.g., x-litellm-model-id)
        _api_base = get_api_base(
            model=model or "",
            optional_params=self.logging_obj.model_call_details.get(
                "litellm_params", {}
            ),
        )
        _model_info: Dict = (
            litellm_metadata.get("model_info", {}) if litellm_metadata else {}
        )
        self._hidden_params = {
            "model_id": _model_info.get("id", None),
            "api_base": _api_base,
            "custom_llm_provider": custom_llm_provider,
        }
        self._hidden_params["additional_headers"] = process_response_headers(
            self.response.headers or {}
        )  # GUARANTEE OPENAI HEADERS IN RESPONSE

    def _check_max_streaming_duration(self) -> None:
        """Raise litellm.Timeout if the stream has exceeded LITELLM_MAX_STREAMING_DURATION_SECONDS."""
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
        """Process a single chunk of data from the stream"""
        if not chunk:
            return None

        # NOTE: ``SSEDecoder`` already strips the SSE ``data:`` field prefix, so
        # the value passed in here is the raw field content. Do not re-run
        # ``_strip_sse_data_from_chunk`` on it — doing so would incorrectly mangle
        # payloads whose actual JSON value happens to start with ``data:``.

        # Handle "[DONE]" marker
        if chunk == STREAM_SSE_DONE_STRING:
            self.finished = True
            return None

        try:
            # Parse the JSON chunk
            parsed_chunk = json.loads(chunk)

            # Format as ResponsesAPIStreamingResponse
            if isinstance(parsed_chunk, dict):
                if self.responses_api_provider_config is None:
                    raise ValueError(
                        "responses_api_provider_config is required to process live streaming chunks"
                    )
                openai_responses_api_chunk = (
                    self.responses_api_provider_config.transform_streaming_response(
                        model=self.model,
                        parsed_chunk=parsed_chunk,
                        logging_obj=self.logging_obj,
                    )
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

                # Encode container_id on streaming events so proxy/UI follow-ups route correctly
                _event_type = getattr(openai_responses_api_chunk, "type", None)
                _stream_model_id = (
                    self.litellm_metadata.get("model_info", {}).get("id")
                    if self.litellm_metadata
                    else None
                )
                if _event_type in (
                    ResponsesAPIStreamEvents.OUTPUT_ITEM_ADDED,
                    ResponsesAPIStreamEvents.OUTPUT_ITEM_DONE,
                ):
                    _item = getattr(openai_responses_api_chunk, "item", None)
                    if _item is not None:
                        APIBridge._encode_container_id_on_output_item(
                            item=_item,
                            custom_llm_provider=self.custom_llm_provider,
                            model_id=_stream_model_id,
                        )
                elif (
                    _event_type == ResponsesAPIStreamEvents.OUTPUT_TEXT_ANNOTATION_ADDED
                ):
                    _annotation = getattr(
                        openai_responses_api_chunk, "annotation", None
                    )
                    if _annotation is not None:
                        APIBridge._encode_container_id_on_output_item(
                            item=_annotation,
                            custom_llm_provider=self.custom_llm_provider,
                            model_id=_stream_model_id,
                        )
                elif _event_type == ResponsesAPIStreamEvents.CONTENT_PART_DONE:
                    _part = getattr(openai_responses_api_chunk, "part", None)
                    if _part is not None:
                        if isinstance(_part, dict):
                            APIBridge._encode_container_ids_in_annotations(
                                _part.get("annotations"),
                                self.custom_llm_provider,
                                _stream_model_id,
                            )
                        else:
                            APIBridge._encode_container_ids_in_annotations(
                                getattr(_part, "annotations", None),
                                self.custom_llm_provider,
                                _stream_model_id,
                            )

                # Wrap encrypted_content in streaming events (output_item.added, output_item.done)
                if self.litellm_metadata and self.litellm_metadata.get(
                    "encrypted_content_affinity_enabled"
                ):
                    event_type = getattr(openai_responses_api_chunk, "type", None)
                    if event_type in (
                        openai_types.ResponsesAPIStreamEvents.OUTPUT_ITEM_ADDED,
                        openai_types.ResponsesAPIStreamEvents.OUTPUT_ITEM_DONE,
                    ):
                        item = getattr(openai_responses_api_chunk, "item", None)
                        if item:
                            encrypted_content = getattr(item, "encrypted_content", None)
                            if encrypted_content and isinstance(encrypted_content, str):
                                model_id = (
                                    self.litellm_metadata.get("model_info", {}).get(
                                        "id"
                                    )
                                    if self.litellm_metadata
                                    else None
                                )
                                if model_id:
                                    wrapped_content = ResponseIdentityManager._wrap_encrypted_content_with_model_id(encrypted_content, model_id)
                                    setattr(item, "encrypted_content", wrapped_content)

                # Store the completed response (also for incomplete/failed so logging still fires)
                _chunk_type = getattr(openai_responses_api_chunk, "type", None)
                if openai_responses_api_chunk and _chunk_type in (
                    openai_types.ResponsesAPIStreamEvents.RESPONSE_COMPLETED,
                    openai_types.ResponsesAPIStreamEvents.RESPONSE_INCOMPLETE,
                    openai_types.ResponsesAPIStreamEvents.RESPONSE_FAILED,
                ):
                    self.completed_response = openai_responses_api_chunk
                    # Add cost to usage object if include_cost_in_streaming_usage is True
                    if (config.include_cost_in_streaming_usage and self.logging_obj is not None):
                        response_obj: Optional[Any] = getattr(
                            openai_responses_api_chunk, "response", None
                        )
                        if response_obj:
                            usage_obj: Optional[Any] = getattr(
                                response_obj, "usage", None
                            )
                            if usage_obj is not None:
                                try:
                                    cost: Optional[float] = (
                                        self.logging_obj._response_cost_calculator(
                                            result=response_obj
                                        )
                                    )
                                    if cost is not None:
                                        setattr(usage_obj, "cost", cost)
                                except Exception:
                                    # Best-effort usage cost annotation should not break stream replay.
                                    pass

                    if (
                        _chunk_type
                        == openai_types.ResponsesAPIStreamEvents.RESPONSE_FAILED
                    ):
                        self._handle_logging_failed_response()
                    else:
                        self._handle_logging_completed_response()

                return openai_responses_api_chunk

            return None
        except json.JSONDecodeError:
            # If we can't parse the chunk, continue
            return None
        except Exception as e:
            # Trigger failure hooks before re-raising
            # This ensures failures are logged even when _process_chunk is called directly
            self._handle_failure(e)
            raise

    def _log_completed_response(self, *, is_async: bool) -> None:
        if self._completed_response_logged:
            return
        self._completed_response_logged = True

        if self._persist_completed_response_before_logging:
            self._persist_completed_response_to_cache(is_async=is_async)

        # Create a copy for logging to avoid modifying the response object that will be returned to the user
        # The logging handlers may transform usage from Responses API format (input_tokens/output_tokens)
        # to chat completion format (prompt_tokens/completion_tokens) for internal logging
        # Use model_dump + model_validate instead of deepcopy to avoid pickle errors with
        # Pydantic ValidatorIterator when response contains tool_choice with allowed_tools (fixes #17192)
        logging_response = self.completed_response
        if self.completed_response is not None and hasattr(
            self.completed_response, "model_dump"
        ):
            try:
                logging_response = type(self.completed_response).model_validate(
                    self.completed_response.model_dump()
                )
            except Exception:
                # Fallback to original if serialization fails
                pass

        end_time = datetime.now()
        if is_async:
            asyncio.create_task(
                self.logging_obj.async_success_handler(
                    result=logging_response,
                    start_time=self.start_time,
                    end_time=end_time,
                    cache_hit=self._completed_response_cache_hit,
                )
            )
        else:
            run_async_function(
                async_function=self.logging_obj.async_success_handler,
                result=logging_response,
                start_time=self.start_time,
                end_time=end_time,
                cache_hit=self._completed_response_cache_hit,
            )

        executor.submit(
            self.logging_obj.success_handler,
            result=logging_response,
            cache_hit=self._completed_response_cache_hit,
            start_time=self.start_time,
            end_time=end_time,
        )
        self._run_post_success_hooks(end_time=end_time)

    def _handle_logging_completed_response(self):
        """Base implementation - should be overridden by subclasses"""
        pass

    def _handle_logging_failed_response(self):
        """
        Handle logging for RESPONSE_FAILED events by routing to failure handlers.

        Unlike _handle_logging_completed_response (which calls success handlers),
        this constructs an exception from the response error and routes to
        async_failure_handler / failure_handler so logging integrations correctly
        record the call as failed.
        """
        response_obj = (
            getattr(self.completed_response, "response", None)
            if self.completed_response
            else None
        )
        error_info = getattr(response_obj, "error", None) if response_obj else None
        error_message = "Response failed"
        if isinstance(error_info, dict):
            error_message = error_info.get("message", str(error_info))
        exception = config.APIError(
            status_code=500,
            message=error_message,
            llm_provider=self.custom_llm_provider or "",
            model=self.model or "",
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

    def _persist_completed_response_to_cache(self, *, is_async: bool) -> None:
        if self._completed_response_cached:
            return

        completed_response = self.completed_response
        if (getattr(completed_response, "type", None) != openai_types.ResponsesAPIStreamEvents.RESPONSE_COMPLETED):
            return

        response_obj = self._get_completed_response_object()
        if response_obj is None:
            return

        caching_handler = getattr(self.logging_obj, "_llm_caching_handler", None)
        if caching_handler is None:
            return

        request_kwargs = getattr(caching_handler, "request_kwargs", None)
        if (
            not isinstance(request_kwargs, dict)
            or request_kwargs.get("stream") is not True
        ):
            return
        request_kwargs = request_kwargs.copy()
        preset_cache_key = getattr(caching_handler, "preset_cache_key", None)
        request_cache_key = request_kwargs.pop("cache_key", None)
        if preset_cache_key is None:
            preset_cache_key = request_cache_key
        if request_kwargs.get("metadata") is None:
            request_kwargs.pop("metadata", None)
        request_kwargs.pop("custom_llm_provider", None)
        if preset_cache_key is not None:
            request_kwargs["cache_key"] = preset_cache_key

        if not caching_handler._should_store_result_in_cache(
            original_function=caching_handler.original_function,
            kwargs=request_kwargs,
        ):
            return

        if config.cache is None:
            return

        cached_response = response_obj.model_dump_json()
        if is_async:
            cache_write_task = asyncio.create_task(
                config.cache.async_add_cache(
                    cached_response,
                    dynamic_cache_object=getattr(caching_handler, "dual_cache", None),
                    **request_kwargs,
                )
            )
            cache_write_task.add_done_callback(
                lambda task: _log_background_task_failure(
                    task,
                    task_name="Responses stream cache write",
                )
            )
        else:
            config.cache.add_cache(
                cached_response,
                dynamic_cache_object=getattr(caching_handler, "dual_cache", None),
                **request_kwargs,
            )

        self._completed_response_cached = True

    async def _call_post_streaming_deployment_hook(self, chunk):
        """
        Allow callbacks to modify streaming chunks before returning (parity with chat).
        """
        try:
            # Align with chat pipeline: use logging_obj model_call_details + call_type
            typed_call_type: Optional[CallTypes] = None
            if self.call_type is not None:
                try:
                    typed_call_type = CallTypes(self.call_type)
                except ValueError:
                    typed_call_type = None
            if typed_call_type is None:
                try:
                    typed_call_type = CallTypes(
                        getattr(self.logging_obj, "call_type", None)
                    )
                except Exception:
                    typed_call_type = None

            request_data = self.request_data or getattr(
                self.logging_obj, "model_call_details", {}
            )
            callbacks = config.callbacks
            hooks_ran = False
            for callback in callbacks:
                if hasattr(callback, "async_post_call_streaming_deployment_hook"):
                    hooks_ran = True
                    result = await callback.async_post_call_streaming_deployment_hook(
                        request_data=request_data,
                        response_chunk=chunk,
                        call_type=typed_call_type,
                    )
                    if result is not None:
                        chunk = result
            if hooks_ran:
                setattr(chunk, "_post_streaming_hooks_ran", True)
            return chunk
        except Exception:
            return chunk

    async def call_post_streaming_hooks_for_testing(self, chunk):
        """
        Helper to invoke streaming deployment hooks explicitly (used in tests).
        """
        return await self._call_post_streaming_deployment_hook(chunk)

    def _run_post_success_hooks(self, end_time: datetime):
        """
        Run post-call deployment hooks and update metadata similar to chat pipeline.
        """
        if self.completed_response is None:
            return

        request_payload: Dict[str, Any] = {}
        if isinstance(self.request_data, dict):
            request_payload.update(self.request_data)
        try:
            if hasattr(self.logging_obj, "model_call_details"):
                request_payload.update(self.logging_obj.model_call_details)
        except Exception:
            pass
        if "litellm_params" not in request_payload:
            try:
                request_payload["litellm_params"] = getattr(
                    self.logging_obj, "model_call_details", {}
                ).get("litellm_params", {})
            except Exception:
                request_payload["litellm_params"] = {}

        try:
            update_response_metadata(
                result=self.completed_response,
                logging_obj=self.logging_obj,
                model=self.model,
                kwargs=request_payload,
                start_time=self.start_time,
                end_time=end_time,
            )
        except Exception:
            # Non-blocking
            pass

        try:
            typed_call_type: Optional[CallTypes] = None
            if self.call_type is not None:
                try:
                    typed_call_type = CallTypes(self.call_type)
                except ValueError:
                    typed_call_type = None
        except Exception:
            typed_call_type = None
        if typed_call_type is None:
            try:
                typed_call_type = CallTypes.responses
            except Exception:
                typed_call_type = None

    def _handle_failure(self, exception: Exception):
        """
        Trigger failure handlers before bubbling the exception.
        Only calls handlers once even if called multiple times.
        """
        # Prevent double-calling failure handlers
        if self._failure_handled:
            return
        self._failure_handled = True

        traceback_exception = traceback.format_exc()
        try:
            run_async_function(
                async_function=self.logging_obj.async_failure_handler,
                exception=exception,
                traceback_exception=traceback_exception,
                start_time=self.start_time,
                end_time=datetime.now(),
            )
        except Exception:
            pass

        try:
            executor.submit(
                self.logging_obj.failure_handler,
                exception,
                traceback_exception,
                self.start_time,
                datetime.now(),
            )
        except Exception:
            pass


async def call_post_streaming_hooks_for_testing(iterator, chunk):
    """
    Module-level helper for tests to ensure hooks can be invoked even if the iterator is wrapped.
    """
    hook_fn = getattr(iterator, "_call_post_streaming_deployment_hook", None)
    if hook_fn is None:
        return chunk
    return await hook_fn(chunk)


class ResponsesAPIStreamingIterator(ResponseStreamIterator):
    """
    Async iterator for processing streaming responses from the Responses API.
    """

    def __init__(
        self,
        response: httpx.Response,
        model: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        logging_obj: LogDelegator,
        litellm_metadata: Optional[Dict[str, Any]] = None,
        custom_llm_provider: Optional[str] = None,
        request_data: Optional[Dict[str, Any]] = None,
        call_type: Optional[str] = None,
    ):
        super().__init__(
            response,
            model,
            responses_api_provider_config,
            logging_obj,
            litellm_metadata,
            custom_llm_provider,
            request_data,
            call_type,
        )
        self.stream_iterator = SSEDecoder().aiter_bytes(response.aiter_bytes())

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        try:
            self._check_max_streaming_duration()
            while True:
                # Get the next chunk from the stream
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
                    # Await hook directly instead of run_async_function
                    # (which spawns a thread + event loop per call)
                    result = await self._call_post_streaming_deployment_hook(
                        chunk=result,
                    )
                    return result
                # If result is None, continue the loop to get the next chunk

        except StopAsyncIteration:
            # Normal end of stream - don't log as failure
            raise
        except httpx.HTTPError as e:
            # Handle HTTP errors
            self.finished = True
            self._handle_failure(e)
            raise e
        except Exception as e:
            self.finished = True
            self._handle_failure(e)
            raise e

    def _handle_logging_completed_response(self):
        """Handle logging for completed responses in async context"""
        self._log_completed_response(is_async=True)


class SyncResponsesAPIStreamingIterator(ResponseStreamIterator):
    """
    Synchronous iterator for processing streaming responses from the Responses API.
    """

    def __init__(
        self,
        response: httpx.Response,
        model: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        logging_obj: LogDelegator,
        litellm_metadata: Optional[Dict[str, Any]] = None,
        custom_llm_provider: Optional[str] = None,
        request_data: Optional[Dict[str, Any]] = None,
        call_type: Optional[str] = None,
    ):
        super().__init__(
            response,
            model,
            responses_api_provider_config,
            logging_obj,
            litellm_metadata,
            custom_llm_provider,
            request_data,
            call_type,
        )
        self.stream_iterator = SSEDecoder().iter_bytes(response.iter_bytes())

    def __iter__(self):
        return self

    def __next__(self):
        try:
            self._check_max_streaming_duration()
            while True:
                # Get the next chunk from the stream
                try:
                    sse = next(self.stream_iterator)
                except StopIteration:
                    self.finished = True
                    raise StopIteration

                self._check_max_streaming_duration()
                result = self._process_chunk(sse.data)

                if self.finished:
                    raise StopIteration
                elif result is not None:
                    # Sync path: use run_async_function for the hook
                    result = run_async_function(
                        async_function=self._call_post_streaming_deployment_hook,
                        chunk=result,
                    )
                    return result
                # If result is None, continue the loop to get the next chunk

        except StopIteration:
            # Normal end of stream - don't log as failure
            raise
        except httpx.HTTPError as e:
            # Handle HTTP errors
            self.finished = True
            self._handle_failure(e)
            raise e
        except Exception as e:
            self.finished = True
            self._handle_failure(e)
            raise e

    def _handle_logging_completed_response(self):
        """Handle logging for completed responses in sync context"""
        self._log_completed_response(is_async=False)


class MockResponsesAPIStreamingIterator(ResponseStreamIterator):
    """
    Mock iterator—fake a stream by slicing the full response text into
    5 char deltas, then emit a completed event.

    Models like o1-pro don't support streaming, so we fake it.
    """

    CHUNK_SIZE = 5

    def __init__(
        self,
        response: httpx.Response,
        model: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        logging_obj: LogDelegator,
        litellm_metadata: Optional[Dict[str, Any]] = None,
        custom_llm_provider: Optional[str] = None,
        request_data: Optional[Dict[str, Any]] = None,
        call_type: Optional[str] = None,
    ):
        transformed = responses_api_provider_config.transform_response_api_response(
            model=model,
            raw_response=response,
            logging_obj=logging_obj,
        )
        super().__init__(
            response=httpx.Response(200),
            model=model,
            responses_api_provider_config=None,
            logging_obj=logging_obj,
            litellm_metadata=litellm_metadata,
            custom_llm_provider=custom_llm_provider,
            request_data=request_data,
            call_type=call_type,
        )
        self._set_events_from_response(transformed=transformed, logging_obj=logging_obj)

    def _set_events_from_response(
        self,
        transformed: Any,
        logging_obj: LogDelegator,
    ) -> None:
        self._events = _build_synthetic_response_events(
            transformed=transformed,
            logging_obj=logging_obj,
            chunk_size=self.CHUNK_SIZE,
        )
        self._idx = 0
        self.completed_response = self._events[-1]

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        if self._idx >= len(self._events):
            raise StopAsyncIteration
        evt = self._events[self._idx]
        self._idx += 1
        if (getattr(evt, "type", None) == openai_types.ResponsesAPIStreamEvents.RESPONSE_COMPLETED):
            self.completed_response = evt
            self._log_completed_response(is_async=True)
        return evt

    def __iter__(self):
        return self

    def __next__(self) -> Any:
        if self._idx >= len(self._events):
            raise StopIteration
        evt = self._events[self._idx]
        self._idx += 1
        if (getattr(evt, "type", None) == openai_types.ResponsesAPIStreamEvents.RESPONSE_COMPLETED):
            self.completed_response = evt
            self._log_completed_response(is_async=False)
        return evt


class CachedResponsesAPIStreamingIterator(ResponseStreamIterator):
    def __init__(
        self,
        response: Any,
        logging_obj: LogDelegator,
        request_data: Optional[Dict[str, Any]] = None,
        call_type: Optional[str] = None,
    ):
        ResponseStreamIterator.__init__(
            self,
            response=httpx.Response(200),
            model=getattr(response, "model", ""),
            responses_api_provider_config=None,
            logging_obj=logging_obj,
            litellm_metadata=None,
            custom_llm_provider="cached_response",
            request_data=request_data,
            call_type=call_type,
        )
        self._completed_response_cache_hit = True
        self._persist_completed_response_before_logging = False
        self._events: List[Any] = []
        self._idx = 0
        self._set_events_from_response(transformed=response, logging_obj=logging_obj)

    def _set_events_from_response(
        self,
        transformed: Any,
        logging_obj: LogDelegator,
    ) -> None:
        self._events = _build_synthetic_response_events(
            transformed=transformed,
            logging_obj=logging_obj,
            chunk_size=MockResponsesAPIStreamingIterator.CHUNK_SIZE,
        )
        self._idx = 0
        self.completed_response = self._events[-1]

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        if self._idx >= len(self._events):
            raise StopAsyncIteration
        evt = self._events[self._idx]
        self._idx += 1
        if (
            getattr(evt, "type", None)
            == openai_types.ResponsesAPIStreamEvents.RESPONSE_COMPLETED
        ):
            self.completed_response = evt
            self._log_completed_response(is_async=True)
        return evt

    def __iter__(self):
        return self

    def __next__(self) -> Any:
        if self._idx >= len(self._events):
            raise StopIteration
        evt = self._events[self._idx]
        self._idx += 1
        if (
            getattr(evt, "type", None)
            == openai_types.ResponsesAPIStreamEvents.RESPONSE_COMPLETED
        ):
            self.completed_response = evt
            self._log_completed_response(is_async=False)
        return evt


def _dump_response_object(obj: Any) -> Dict[str, Any]:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, dict):
        return obj
    return {}


def _build_response_status_event(
    event_type: Literal[
        "response.created",
        "response.in_progress",
    ],
    transformed: Any,
) -> Any:
    in_progress_response = transformed.model_copy(
        deep=True,
        update={"status": "in_progress", "output": []},
    )
    if event_type == openai_types.ResponsesAPIStreamEvents.RESPONSE_CREATED:
        return openai_types.ResponseCreatedEvent(
            type=event_type, response=in_progress_response
        )
    return openai_types.ResponseInProgressEvent(
        type=event_type, response=in_progress_response
    )


def _build_content_part_done_event(
    *,
    item_id: str,
    output_index: int,
    content_index: int,
    part_payload: Dict[str, Any],
) -> Optional[Any]:
    part_type = part_payload.get("type")
    part: Any
    if part_type == "output_text":
        annotations = [
            openai_types.BaseOpenAIResponse(**annotation)
            for annotation in part_payload.get("annotations", []) or []
        ]
        part = openai_types.ContentPartDonePartOutputText(
            type="output_text",
            text=str(part_payload.get("text") or ""),
            annotations=annotations,
            logprobs=part_payload.get("logprobs"),
        )
    elif part_type == "refusal":
        part = openai_types.ContentPartDonePartRefusal(
            type="refusal",
            refusal=str(part_payload.get("refusal") or ""),
        )
    elif part_type == "reasoning_text":
        part = openai_types.ContentPartDonePartReasoningText(
            type="reasoning_text",
            reasoning=str(part_payload.get("reasoning") or ""),
        )
    else:
        return None

    return openai_types.ContentPartDoneEvent(
        type=openai_types.ResponsesAPIStreamEvents.CONTENT_PART_DONE,
        item_id=item_id,
        output_index=output_index,
        content_index=content_index,
        part=part,
    )


def _add_text_like_part_events(
    *,
    events: List[Any],
    item_id: str,
    output_index: int,
    content_index: int,
    part_payload: Dict[str, Any],
    chunk_size: int,
) -> None:
    part_type = part_payload.get("type")
    if part_type == "output_text":
        text = str(part_payload.get("text") or "")
        for i in range(0, len(text), chunk_size):
            events.append(
                openai_types.OutputTextDeltaEvent(
                    type=openai_types.ResponsesAPIStreamEvents.OUTPUT_TEXT_DELTA,
                    item_id=item_id,
                    output_index=output_index,
                    content_index=content_index,
                    delta=text[i : i + chunk_size],
                )
            )
        for annotation_index, annotation in enumerate(
            part_payload.get("annotations", []) or []
        ):
            events.append(
                openai_types.OutputTextAnnotationAddedEvent(
                    type=openai_types.ResponsesAPIStreamEvents.OUTPUT_TEXT_ANNOTATION_ADDED,
                    item_id=item_id,
                    output_index=output_index,
                    content_index=content_index,
                    annotation_index=annotation_index,
                    annotation=annotation,
                )
            )
        events.append(
            openai_types.OutputTextDoneEvent(
                type=openai_types.ResponsesAPIStreamEvents.OUTPUT_TEXT_DONE,
                item_id=item_id,
                output_index=output_index,
                content_index=content_index,
                text=text,
            )
        )
    elif part_type == "refusal":
        refusal = str(part_payload.get("refusal") or "")
        for i in range(0, len(refusal), chunk_size):
            events.append(
                openai_types.RefusalDeltaEvent(
                    type=openai_types.ResponsesAPIStreamEvents.REFUSAL_DELTA,
                    item_id=item_id,
                    output_index=output_index,
                    content_index=content_index,
                    delta=refusal[i : i + chunk_size],
                )
            )
        events.append(
            openai_types.RefusalDoneEvent(
                type=openai_types.ResponsesAPIStreamEvents.REFUSAL_DONE,
                item_id=item_id,
                output_index=output_index,
                content_index=content_index,
                refusal=refusal,
            )
        )


def _build_synthetic_response_events(
    *,
    transformed: Any,
    logging_obj: LogDelegator,
    chunk_size: int,
) -> List[Any]:
    if config.include_cost_in_streaming_usage and logging_obj is not None:
        usage_obj: Optional[Any] = getattr(transformed, "usage", None)
        if usage_obj is not None:
            try:
                cost: Optional[float] = logging_obj._response_cost_calculator(
                    result=transformed
                )
                if cost is not None:
                    setattr(usage_obj, "cost", cost)
            except Exception:
                pass

    events: List[Any] = [
        _build_response_status_event(
            openai_types.ResponsesAPIStreamEvents.RESPONSE_CREATED, transformed
        ),
        _build_response_status_event(
            openai_types.ResponsesAPIStreamEvents.RESPONSE_IN_PROGRESS, transformed
        ),
    ]

    sequence_number = 0
    for output_index, output_item in enumerate(
        getattr(transformed, "output", []) or []
    ):
        output_item_payload = _dump_response_object(output_item)
        item_id = str(output_item_payload.get("id") or transformed.id)
        item_type = output_item_payload.get("type")

        events.append(
            openai_types.OutputItemAddedEvent(
                type=openai_types.ResponsesAPIStreamEvents.OUTPUT_ITEM_ADDED,
                output_index=output_index,
                item=openai_types.BaseOpenAIResponse(
                    **output_item_payload
                ),
            )
        )

        if item_type == "message":
            for content_index, part in enumerate(
                output_item_payload.get("content", []) or []
            ):
                part_payload = _dump_response_object(part)
                events.append(
                    openai_types.ContentPartAddedEvent(
                        type=openai_types.ResponsesAPIStreamEvents.CONTENT_PART_ADDED,
                        item_id=item_id,
                        output_index=output_index,
                        content_index=content_index,
                        part=openai_types.BaseOpenAIResponse(
                            **part_payload
                        ),
                    )
                )
                _add_text_like_part_events(
                    events=events,
                    item_id=item_id,
                    output_index=output_index,
                    content_index=content_index,
                    part_payload=part_payload,
                    chunk_size=chunk_size,
                )
                done_event = _build_content_part_done_event(
                    item_id=item_id,
                    output_index=output_index,
                    content_index=content_index,
                    part_payload=part_payload,
                )
                if done_event is not None:
                    events.append(done_event)
        elif item_type == "function_call":
            arguments = str(output_item_payload.get("arguments") or "")
            for i in range(0, len(arguments), chunk_size):
                events.append(
                    openai_types.FunctionCallArgumentsDeltaEvent(
                        type=openai_types.ResponsesAPIStreamEvents.FUNCTION_CALL_ARGUMENTS_DELTA,
                        item_id=item_id,
                        output_index=output_index,
                        delta=arguments[i : i + chunk_size],
                    )
                )
            events.append(
                openai_types.FunctionCallArgumentsDoneEvent(
                    type=openai_types.ResponsesAPIStreamEvents.FUNCTION_CALL_ARGUMENTS_DONE,
                    item_id=item_id,
                    output_index=output_index,
                    arguments=arguments,
                )
            )
        elif item_type == "reasoning":
            for summary_index, summary in enumerate(
                output_item_payload.get("summary", []) or []
            ):
                summary_payload = _dump_response_object(summary)
                summary_text = str(summary_payload.get("text") or "")
                for i in range(0, len(summary_text), chunk_size):
                    events.append(
                        openai_types.ReasoningSummaryTextDeltaEvent(
                            type=openai_types.ResponsesAPIStreamEvents.REASONING_SUMMARY_TEXT_DELTA,
                            item_id=item_id,
                            output_index=output_index,
                            summary_index=summary_index,
                            delta=summary_text[i : i + chunk_size],
                        )
                    )
                sequence_number += 1
                events.append(
                    openai_types.ReasoningSummaryTextDoneEvent(
                        type=openai_types.ResponsesAPIStreamEvents.REASONING_SUMMARY_TEXT_DONE,
                        item_id=item_id,
                        output_index=output_index,
                        sequence_number=sequence_number,
                        summary_index=summary_index,
                        text=summary_text,
                    )
                )
                sequence_number += 1
                events.append(
                    openai_types.ReasoningSummaryPartDoneEvent(
                        type=openai_types.ResponsesAPIStreamEvents.REASONING_SUMMARY_PART_DONE,
                        item_id=item_id,
                        output_index=output_index,
                        sequence_number=sequence_number,
                        summary_index=summary_index,
                        part=openai_types.BaseOpenAIResponse(
                            **summary_payload
                        ),
                    )
                )

        sequence_number += 1
        events.append(
            openai_types.OutputItemDoneEvent(
                type=openai_types.ResponsesAPIStreamEvents.OUTPUT_ITEM_DONE,
                output_index=output_index,
                sequence_number=sequence_number,
                item=openai_types.BaseOpenAIResponse(
                    **output_item_payload
                ),
            )
        )

    events.append(
        openai_types.ResponseCompletedEvent(
            type=openai_types.ResponsesAPIStreamEvents.RESPONSE_COMPLETED,
            response=transformed,
        )
    )
    return events