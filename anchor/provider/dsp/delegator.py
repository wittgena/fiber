# anchor.provider.dsp.delegator
import os
import pydantic
from typing import Any, cast
from anyio.streams.memory import MemoryObjectSendStream
from asyncer import syncify

from bound.channel.client.action.completion import completion, acompletion
from bound.channel.client.action.api.response import responses
from bound.channel.client.action.api.aresponse import aresponses
from xphi.scope.dsp.context import runtime
from bound.transport.stream.chunk.builder import stream_chunk_builder

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class DSPDelegator:
    """Delegates completion and response requests to the appropriate bound channels."""

    def _header_identifier(self, headers: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = headers or {}
        return {
            "User-Agent": "surgent/1.5.1",
            **headers,
        }

    def _get_stream_completion_fn(
        self,
        request: dict[str, Any],
        cache_kwargs: dict[str, Any],
        sync: bool = True,
        headers: dict[str, Any] | None = None,
    ):
        stream = runtime.send_stream
        caller_predict = runtime.caller_predict

        if stream is None:
            return None

        # The stream is already opened, and will be closed by the caller.
        stream = cast(MemoryObjectSendStream, stream)
        caller_predict_id = id(caller_predict) if caller_predict else None

        if runtime.track_usage:
            request["stream_options"] = {"include_usage": True}

        async def stream_completion(request: dict[str, Any], cache_kwargs: dict[str, Any]):
            response = await acompletion(
                cache=cache_kwargs,
                stream=True,
                headers=headers,
                **request,
            )
            chunks = []
            async for chunk in response:
                if caller_predict_id:
                    # Add the predict id to the chunk so that the stream listener can identify which predict produces it.
                    chunk.predict_id = caller_predict_id
                chunks.append(chunk)
                await stream.send(chunk)
            return stream_chunk_builder(chunks)

        def sync_stream_completion():
            syncified_stream_completion = syncify(stream_completion)
            return syncified_stream_completion(request, cache_kwargs)

        async def async_stream_completion():
            return await stream_completion(request, cache_kwargs)

        return sync_stream_completion if sync else async_stream_completion

    def delegate_completion(self, request: dict[str, Any], num_retries: int, cache: dict[str, Any] | None = None):
        cache = cache or {"no-cache": True, "no-store": True}
        request = dict(request)
        request.pop("rollout_id", None)
        headers = self._header_identifier(request.pop("headers", None))
        stream_completion = self._get_stream_completion_fn(request, cache, sync=True, headers=headers)
        
        if stream_completion is None:
            return completion(
                cache=cache,
                num_retries=num_retries,
                retry_strategy="exponential_backoff_retry",
                headers=headers,
                **request,
            )
        return stream_completion()

    async def delegate_acompletion(self, request: dict[str, Any], num_retries: int, cache: dict[str, Any] | None = None):
        cache = cache or {"no-cache": True, "no-store": True}
        request = dict(request)
        request.pop("rollout_id", None)
        headers = request.pop("headers", None)
        stream_completion = self._get_stream_completion_fn(request, cache, sync=False)
        
        if stream_completion is None:
            return await acompletion(
                cache=cache,
                num_retries=num_retries,
                retry_strategy="exponential_backoff_retry",
                headers=self._header_identifier(headers),
                **request,
            )
        return await stream_completion()

    def delegate_responses(self, request: dict[str, Any], num_retries: int, cache: dict[str, Any] | None = None):
        cache = cache or {"no-cache": True, "no-store": True}
        request = dict(request)
        request.pop("rollout_id", None)
        headers = request.pop("headers", None)
        request = self._convert_chat_request_to_responses_request(request)

        return responses(
            cache=cache,
            num_retries=num_retries,
            retry_strategy="exponential_backoff_retry",
            headers=self._header_identifier(headers),
            **request,
        )

    async def delegate_aresponses(self, request: dict[str, Any], num_retries: int, cache: dict[str, Any] | None = None):
        cache = cache or {"no-cache": True, "no-store": True}
        request = dict(request)
        request.pop("rollout_id", None)
        headers = request.pop("headers", None)
        request = self._convert_chat_request_to_responses_request(request)

        return await aresponses(
            cache=cache,
            num_retries=num_retries,
            retry_strategy="exponential_backoff_retry",
            headers=self._header_identifier(headers),
            **request,
        )

    def _convert_chat_request_to_responses_request(self, request: dict[str, Any]):
        """Convert a chat request to a responses request."""
        request = dict(request)
        if "messages" in request:
            content_blocks = []
            for msg in request.pop("messages"):
                c = msg.get("content")
                if isinstance(c, str):
                    content_blocks.append({"type": "input_text", "text": c})
                elif isinstance(c, list):
                    for item in c:
                        content_blocks.append(self._convert_content_item_to_responses_format(item))
            request["input"] = [{"role": msg.get("role", "user"), "content": content_blocks}]
            
        if "reasoning_effort" in request:
            effort = request.pop("reasoning_effort")
            request["reasoning"] = {"effort": effort, "summary": "auto"}

        if "response_format" in request:
            response_format = request.pop("response_format")
            if isinstance(response_format, type) and issubclass(response_format, pydantic.BaseModel):
                response_format = {
                    "name": response_format.__name__,
                    "type": "json_schema",
                    "schema": response_format.model_json_schema(),
                }
            text = request.pop("text", {})
            request["text"] = {**text, "format": response_format}

        return request

    def _convert_content_item_to_responses_format(self, item: dict[str, Any]) -> dict[str, Any]:
        if item.get("type") == "image_url":
            image_url = item.get("image_url", {}).get("url", "")
            return {
                "type": "input_image",
                "image_url": image_url,
            }
        elif item.get("type") == "text":
            return {
                "type": "input_text",
                "text": item.get("text", ""),
            }
        elif item.get("type") == "file":
            file = item.get("file", {})
            return {
                "type": "input_file",
                "file_data": file.get("file_data"),
                "filename": file.get("filename"),
                "file_id": file.get("file_id"),
            }
        return item