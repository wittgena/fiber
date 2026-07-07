# bound.transport.stream.ws
## @lineage: bound.transport.stream.iterator
"""deprecated"""
from __future__ import annotations
import asyncio
import json
import time
import traceback
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Literal, Optional
import httpx
import bound.surface.legacy.openai.types as openai_types
from bound.surface.legacy.config.resolver import config
from bound.transport.client.executor import executor
from bound.transport.response.identity import ResponseIdentityManager
from bound.watcher.delegator import LogDelegator

from watcher.plane.emitter import get_emitter

log = get_emitter("stream.ws")

RESPONSES_WS_LOGGED_EVENT_TYPES = [
    "response.created",
    "response.completed",
    "response.failed",
    "response.incomplete",
    "error",
]

class ResponseWSStreaming:
    def __init__(
        self,
        websocket: Any,
        backend_ws: Any,
        logging_obj: LogDelegator,
        user_api_key_dict: Optional[Any] = None,
        request_data: Optional[Dict] = None,
        first_message: Optional[str] = None,
    ):
        self.websocket = websocket
        self.backend_ws = backend_ws
        self.logging_obj = logging_obj
        self.user_api_key_dict = user_api_key_dict
        self.request_data: Dict = request_data or {}
        self.messages: list[Dict] = []
        self.input_messages: list[Dict[str, str]] = []
        self.first_message = first_message

    def _should_store_event(self, event_obj: dict) -> bool:
        return event_obj.get("type") in RESPONSES_WS_LOGGED_EVENT_TYPES

    def _store_event(self, event: Any) -> None:
        if isinstance(event, bytes):
            event = event.decode("utf-8")
        if isinstance(event, str):
            try:
                event_obj = json.loads(event)
            except (json.JSONDecodeError, TypeError):
                return
        else:
            event_obj = event

        if self._should_store_event(event_obj):
            self.messages.append(event_obj)

    def _collect_input_from_client_event(self, message: Any) -> None:
        """Extract user input content from response.create for logging."""
        try:
            if isinstance(message, str):
                msg_obj = json.loads(message)
            elif isinstance(message, dict):
                msg_obj = message
            else:
                return

            if msg_obj.get("type") != "response.create":
                return

            input_items = msg_obj.get("input", [])
            if isinstance(input_items, str):
                self.input_messages.append({"role": "user", "content": input_items})
                return

            if isinstance(input_items, list):
                for item in input_items:
                    if not isinstance(item, dict):
                        continue
                    if item.get("type") == "message" and item.get("role") == "user":
                        content = item.get("content", [])
                        if isinstance(content, str):
                            self.input_messages.append(
                                {"role": "user", "content": content}
                            )
                        elif isinstance(content, list):
                            for c in content:
                                if (
                                    isinstance(c, dict)
                                    and c.get("type") == "input_text"
                                ):
                                    text = c.get("text", "")
                                    if text:
                                        self.input_messages.append(
                                            {"role": "user", "content": text}
                                        )
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass

    def _store_input(self, message: Any) -> None:
        self._collect_input_from_client_event(message)
        if self.logging_obj:
            self.logging_obj.pre_call(input=message, api_key="")

    async def _log_messages(self) -> None:
        if not self.logging_obj:
            return
        if self.input_messages:
            self.logging_obj.model_call_details["messages"] = self.input_messages
        if self.messages:
            asyncio.create_task(self.logging_obj.async_success_handler(self.messages))
            executor.submit(self.logging_obj.success_handler, self.messages)

    async def backend_to_client(self) -> None:
        """Forward events from backend WebSocket to the client."""
        import websockets

        try:
            while True:
                try:
                    raw_response = await self.backend_ws.recv(decode=False)  # type: ignore[union-attr]
                except TypeError:
                    raw_response = await self.backend_ws.recv()  # type: ignore[union-attr, assignment]

                if isinstance(raw_response, bytes):
                    response_str = raw_response.decode("utf-8")
                else:
                    response_str = raw_response

                self._store_event(response_str)
                await self.websocket.send_text(response_str)

        except websockets.exceptions.ConnectionClosed as e:  # type: ignore
            log.debug("Responses WS backend connection closed: %s", e)
        except Exception as e:
            log.exception("Error in responses WS backend_to_client: %s", e)
        finally:
            await self._log_messages()

    async def client_to_backend(self) -> None:
        """Forward response.create events from client to backend."""
        try:
            if self.first_message is not None:
                self._store_input(self.first_message)
                self._store_event(self.first_message)
                await self.backend_ws.send(self.first_message)  # type: ignore[union-attr]

            while True:
                message = await self.websocket.receive_text()

                self._store_input(message)
                self._store_event(message)
                await self.backend_ws.send(message)  # type: ignore[union-attr]

        except Exception as e:
            log.debug("Responses WS client_to_backend ended: %s", e)

    async def bidirectional_forward(self) -> None:
        """Run both forwarding directions concurrently."""
        forward_task = asyncio.create_task(self.backend_to_client())
        try:
            await self.client_to_backend()
        except Exception:
            pass
        finally:
            if not forward_task.done():
                forward_task.cancel()
                try:
                    await forward_task
                except asyncio.CancelledError:
                    pass
            try:
                await self.backend_ws.close()
            except Exception:
                pass

_RESPONSE_CREATE_PARAMS: frozenset = (
    openai_types.ResponsesAPIRequestParams.__required_keys__
    | openai_types.ResponsesAPIRequestParams.__optional_keys__
)

_MANAGED_WS_SKIP_KWARGS: frozenset = frozenset(
    {
        "log_delegator",
        "call_id",
        "aresponses",
        "_aresponses_websocket",
        "user_api_key_dict",
    }
)

class ResponseWSHandler:
    def __init__(
        self,
        websocket: Any,
        model: str,
        logging_obj: LogDelegator,
        user_api_key_dict: Optional[Any] = None,
        litellm_metadata: Optional[Dict[str, Any]] = None,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        timeout: Optional[float] = None,
        custom_llm_provider: Optional[str] = None,
        first_message: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.websocket = websocket
        self.model = model
        self.logging_obj = logging_obj
        self.user_api_key_dict = user_api_key_dict
        self.litellm_metadata: Dict[str, Any] = litellm_metadata or {}
        self.api_key = api_key
        self.api_base = api_base
        self.timeout = timeout
        self.custom_llm_provider = custom_llm_provider
        self._connection_provider = self._resolve_provider(model) or custom_llm_provider
        self.first_message = first_message
        self.extra_kwargs: Dict[str, Any] = {k: v for k, v in kwargs.items() if k not in _MANAGED_WS_SKIP_KWARGS}
        self._session_history: Dict[str, List[Dict[str, Any]]] = {}

    @staticmethod
    def _serialize_chunk(chunk: Any) -> Optional[str]:
        """Serialize a streaming chunk to a JSON string for WebSocket transmission."""
        try:
            if hasattr(chunk, "model_dump_json"):
                return chunk.model_dump_json(exclude_none=True)
            if hasattr(chunk, "model_dump"):
                return json.dumps(chunk.model_dump(exclude_none=True), default=str)
            if isinstance(chunk, dict):
                return json.dumps(chunk, default=str)
            return json.dumps(str(chunk))
        except Exception as exc:
            log.debug(
                "ManagedResponsesWS: failed to serialize chunk: %s", exc
            )
            return None

    async def _send_error(self, message: str, error_type: str = "server_error") -> None:
        try:
            await self.websocket.send_text(
                json.dumps(
                    {"type": "error", "error": {"type": error_type, "message": message}}
                )
            )
        except Exception:
            pass

    def _get_history_messages(self, previous_response_id: str) -> List[Dict[str, Any]]:
        decoded = ResponseIdentityManager._decode_responses_api_response_id(previous_response_id)
        raw_id = decoded.get("response_id", previous_response_id)
        return list(self._session_history.get(raw_id, []))

    def _store_history(self, response_id: str, messages: List[Dict[str, Any]]) -> None:
        self._session_history[response_id] = messages

    @staticmethod
    def _extract_response_id(completed_event: Dict[str, Any]) -> Optional[str]:
        resp_obj = completed_event.get("response", {})
        encoded_id: Optional[str] = (
            resp_obj.get("id") if isinstance(resp_obj, dict) else None
        )
        if not encoded_id:
            return None
        decoded = ResponseIdentityManager._decode_responses_api_response_id(encoded_id)
        return decoded.get("response_id", encoded_id)

    @staticmethod
    def _extract_output_messages(
        completed_event: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Convert the output items in a ``response.completed`` event into
        Responses API message dicts suitable for the next turn's ``input``.
        """
        resp_obj = completed_event.get("response", {})
        if not isinstance(resp_obj, dict):
            return []
        messages: List[Dict[str, Any]] = []
        for item in resp_obj.get("output", []) or []:
            if not isinstance(item, dict):
                continue
            item_type = item.get("type")
            role = item.get("role", "assistant")
            if item_type == "message":
                content_parts = item.get("content") or []
                text_parts = [
                    p.get("text", "")
                    for p in content_parts
                    if isinstance(p, dict) and p.get("type") in ("output_text", "text")
                ]
                text = "".join(text_parts)
                if text:
                    messages.append(
                        {
                            "type": "message",
                            "role": role,
                            "content": [{"type": "output_text", "text": text}],
                        }
                    )
            elif item_type == "function_call":
                messages.append(item)
        return messages

    @staticmethod
    def _input_to_messages(input_val: Any) -> List[Dict[str, Any]]:
        """
        Normalise the ``input`` field of a ``response.create`` event to a list
        of Responses API message dicts.
        """
        if isinstance(input_val, str):
            return [
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": input_val}],
                }
            ]
        if isinstance(input_val, list):
            return [item for item in input_val if isinstance(item, dict)]
        return []

    # ------------------------------------------------------------------
    # _process_response_create sub-methods
    # ------------------------------------------------------------------

    async def _parse_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        """Parse raw WS text; return the message dict or None (JSON error / ignored type)."""
        try:
            msg_obj = json.loads(raw_message)
        except json.JSONDecodeError:
            await self._send_error(
                "Invalid JSON in response.create event", "invalid_request_error"
            )
            return None
        if msg_obj.get("type") != "response.create":
            # Silently ignore non-response.create messages (e.g. warmup pings)
            return None
        return msg_obj

    @staticmethod
    def _build_base_call_kwargs(msg_obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract Responses API params from the event, handling both wire formats:
          Nested: {"type": "response.create", "response": {"input": [...], ...}}
          Flat:   {"type": "response.create", "input": [...], "model": "...", ...}
        """
        nested = msg_obj.get("response")
        response_params: Dict[str, Any] = (
            nested
            if isinstance(nested, dict) and nested
            else {k: v for k, v in msg_obj.items() if k != "type"}
        )
        return {
            param: response_params[param]
            for param in _RESPONSE_CREATE_PARAMS
            if param in response_params and response_params[param] is not None
        }

    def _apply_history(
        self,
        call_kwargs: Dict[str, Any],
        previous_response_id: Optional[str],
        current_messages: List[Dict[str, Any]],
        prior_history: List[Dict[str, Any]],
    ) -> None:
        """Prepend in-memory turn history, or fall back to DB-based reconstruction."""
        if not previous_response_id:
            return
        if prior_history:
            call_kwargs["input"] = prior_history + current_messages
            log.debug(
                "ManagedResponsesWS: prepended %d history messages for previous_response_id=%s",
                len(prior_history),
                previous_response_id,
            )
        else:
            log.debug(
                "ManagedResponsesWS: no in-memory history for previous_response_id=%s; "
                "falling back to DB-based session reconstruction",
                previous_response_id,
            )
            # Fall back to DB-based session reconstruction (may work for
            # cross-connection multi-turn when spend logs are committed)
            call_kwargs["previous_response_id"] = previous_response_id

    @staticmethod
    def _resolve_provider(model: Optional[str]) -> Optional[str]:
        """Resolve the LLM provider for a model string, or None if unresolvable."""
        if not model:
            return None
        try:
            from anchor.registry.router.locator import get_llm_provider
            _, provider, _, _ = get_llm_provider(model=model)
            return provider
        except Exception:
            return None

    def _same_provider(self, model: Optional[str]) -> bool:
        """Return True if model uses the same LLM provider as the connection model."""
        if model is None or model == self.model:
            return True
        event_provider = self._resolve_provider(model)
        if event_provider is None:
            return False
        return event_provider == self._connection_provider

    def _inject_credentials(
        self, call_kwargs: Dict[str, Any], model: Optional[str] = None
    ) -> None:
        """Inject connection-level credentials and metadata into call_kwargs."""
        if self.api_key is not None:
            call_kwargs["api_key"] = self.api_key
        if self.api_base is not None:
            call_kwargs["api_base"] = self.api_base
        if self.timeout is not None:
            call_kwargs["timeout"] = self.timeout
        if self.custom_llm_provider is not None and self._same_provider(model):
            call_kwargs["custom_llm_provider"] = self.custom_llm_provider
        if self.litellm_metadata:
            call_kwargs["litellm_metadata"] = dict(self.litellm_metadata)

    @staticmethod
    def _update_proxy_request(call_kwargs: Dict[str, Any], model: str) -> None:
        """Update proxy_server_request body so spend logs record the full request."""
        proxy_server_request = (call_kwargs.get("litellm_metadata") or {}).get(
            "proxy_server_request"
        ) or {}
        if not isinstance(proxy_server_request, dict):
            return
        body = dict(proxy_server_request.get("body") or {})
        body["input"] = call_kwargs.get("input")
        body["store"] = call_kwargs.get("store")
        body["model"] = model
        for k in ("tools", "tool_choice", "instructions", "metadata"):
            if k in call_kwargs and call_kwargs[k] is not None:
                body[k] = call_kwargs[k]
        proxy_server_request = {**proxy_server_request, "body": body}
        if "litellm_metadata" not in call_kwargs:
            call_kwargs["litellm_metadata"] = {}
        call_kwargs["litellm_metadata"]["proxy_server_request"] = proxy_server_request
        call_kwargs.setdefault("litellm_params", {})
        call_kwargs["litellm_params"]["proxy_server_request"] = proxy_server_request

    async def _stream_and_forward(self, model: str, call_kwargs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        completed_event: Optional[Dict[str, Any]] = None
        stream_response = await config.aresponses(model=model, **call_kwargs)
        async for chunk in stream_response:  # type: ignore[union-attr]
            if chunk is None:
                continue
            # Read type from the object before serializing to avoid double JSON parse
            chunk_type = getattr(chunk, "type", None) or (
                chunk.get("type") if isinstance(chunk, dict) else None
            )
            serialized = self._serialize_chunk(chunk)
            if serialized is None:
                continue
            if chunk_type == "response.completed" and completed_event is None:
                try:
                    completed_event = json.loads(serialized)
                except Exception:
                    pass
            try:
                await self.websocket.send_text(serialized)
            except Exception as send_exc:
                log.debug(
                    "ManagedResponsesWS: error sending chunk to client: %s", send_exc
                )
                return completed_event  # Client disconnected
        return completed_event

    def _save_turn_history(
        self,
        completed_event: Optional[Dict[str, Any]],
        prior_history: List[Dict[str, Any]],
        current_messages: List[Dict[str, Any]],
    ) -> None:
        """Store this turn in in-memory history for future previous_response_id lookups."""
        if completed_event is None:
            return
        new_response_id = self._extract_response_id(completed_event)
        if not new_response_id:
            return
        output_msgs = self._extract_output_messages(completed_event)
        all_messages = prior_history + current_messages + output_msgs
        self._store_history(new_response_id, all_messages)
        log.debug(
            "ManagedResponsesWS: stored %d messages for response_id=%s",
            len(all_messages),
            new_response_id,
        )

    async def _process_response_create(self, raw_message: str) -> None:
        msg_obj = await self._parse_message(raw_message)
        if msg_obj is None:
            return

        call_kwargs = self._build_base_call_kwargs(msg_obj)
        call_kwargs["stream"] = True

        model = call_kwargs.pop("model", None) or self.model

        previous_response_id: Optional[str] = call_kwargs.pop(
            "previous_response_id", None
        )
        current_messages = self._input_to_messages(call_kwargs.get("input"))

        # Fetch history once; reused in both _apply_history and _save_turn_history
        prior_history = (
            self._get_history_messages(previous_response_id)
            if previous_response_id
            else []
        )

        self._apply_history(
            call_kwargs, previous_response_id, current_messages, prior_history
        )
        self._inject_credentials(call_kwargs, model=model)
        self._update_proxy_request(call_kwargs, model)
        call_kwargs.update(self.extra_kwargs)

        try:
            completed_event = await self._stream_and_forward(model, call_kwargs)
        except Exception as exc:
            log.exception(
                "ManagedResponsesWS: error processing response.create: %s", exc
            )
            await self._send_error(str(exc))
            return

        self._save_turn_history(completed_event, prior_history, current_messages)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Main loop: accept ``response.create`` events sequentially and handle
        each one before waiting for the next message.
        """
        try:
            if self.first_message is not None:
                await self._process_response_create(self.first_message)

            while True:
                try:
                    message = await self.websocket.receive_text()
                except Exception as exc:
                    log.debug(
                        "ManagedResponsesWS: client disconnected: %s", exc
                    )
                    break

                await self._process_response_create(message)

        except Exception as exc:
            log.exception("ManagedResponsesWS: unexpected error: %s", exc)
            await self._send_error(f"Internal server error: {exc}")
