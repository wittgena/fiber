# bound.transport.http.ws
from __future__ import annotations
import json
import ssl
import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, cast
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import websockets
from websockets.asyncio.client import ClientConnection

from anchor.provider.param.legacy import GenericLiteLLMParams
import bound.surface.legacy.openai.types as openai_types
from bound.surface.legacy.config.resolver import config
from bound.transport.client.executor import executor
from bound.transport.response.identity import ResponseIdentityManager
from bound.watcher.delegator import LogDelegator
from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from bound.surface.legacy.config.constants import REALTIME_WEBSOCKET_MAX_MESSAGE_SIZE_BYTES
from bound.transport.http.security import get_ssl_configuration
from xphi.xor.secret.redact import redact_string

from watcher.plane.emitter import get_emitter

log = get_emitter("channel.ws")

_shared_realtime_ssl_context: Optional[Union[bool, str, ssl.SSLContext]] = None

def get_shared_realtime_ssl_context() -> Union[bool, str, ssl.SSLContext]:
    global _shared_realtime_ssl_context
    if _shared_realtime_ssl_context is None:
        _shared_realtime_ssl_context = get_ssl_configuration()
    return _shared_realtime_ssl_context

@dataclass
class RealtimeSessionContext:
    """@desc: Encapsulates previously scattered connection details, credentials, and metadata into a single domain context"""
    websocket: Any
    model: str
    logging_obj: LogDelegator
    provider_config: Optional[BaseResponsesAPIConfig]
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    timeout: Optional[float] = None
    user_api_key_dict: Optional[Any] = None
    litellm_metadata: Dict[str, Any] = field(default_factory=dict)
    first_message: Optional[str] = None
    extra_kwargs: Dict[str, Any] = field(default_factory=dict)

    @property
    def supports_native(self) -> bool:
        return self.provider_config is not None and self.provider_config.supports_native_websocket()

    def build_native_connection_params(self) -> tuple[str, Dict[str, str], ssl.SSLContext | bool]:
        """@desc: Autonomously generates the URL, Headers, and SSL configurations required for a native connection"""
        if not self.provider_config:
            raise ValueError("Provider config is missing for native connection.")
            
        litellm_params = GenericLiteLLMParams()
        headers = self.provider_config.validate_environment(
            headers={}, model=self.model, litellm_params=litellm_params
        )
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        http_url = self.provider_config.get_complete_url(api_base=self.api_base, litellm_params={})
        ws_url = http_url.replace("https://", "wss://").replace("http://", "ws://")
        
        _parsed = urlparse(ws_url)
        _qs = parse_qs(_parsed.query)
        if "model" not in _qs:
            _qs["model"] = [self.model]
            ws_url = urlunparse(_parsed._replace(query=urlencode({k: v[0] for k, v in _qs.items()})))

        ssl_context = get_shared_realtime_ssl_context()
        if ws_url.startswith("wss://") and ssl_context is False:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        return ws_url, headers, ssl_context

class OpenAIRealtimeProtocol:
    """@desc: Completely isolates JSON serialization/deserialization and OpenAI Realtime Event specification parsing from the I/O communication logic"""
    LOGGABLE_EVENTS = frozenset(["response.created", "response.completed", "response.failed", "response.incomplete", "error"])
    _RESPONSE_CREATE_PARAMS = frozenset(
        openai_types.ResponsesAPIRequestParams.__required_keys__
        | openai_types.ResponsesAPIRequestParams.__optional_keys__
    )

    @classmethod
    def parse_event(cls, raw_data: Any) -> Optional[Dict[str, Any]]:
        """@desc: Safely parses raw WebSocket messages and returns them as Event objects"""
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode("utf-8")
        if not isinstance(raw_data, str):
            return raw_data if isinstance(raw_data, dict) else None
            
        try:
            return json.loads(raw_data)
        except json.JSONDecodeError:
            return None

    @classmethod
    def is_loggable(cls, event: Dict[str, Any]) -> bool:
        return event.get("type") in cls.LOGGABLE_EVENTS

    @classmethod
    def extract_user_input(cls, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """@desc: Normalizes and extracts only the user's input content from the 'response.create' event"""
        if event.get("type") != "response.create":
            return []

        input_items = event.get("input", [])
        if isinstance(input_items, str):
            return [{"role": "user", "content": input_items}]

        extracted = []
        if isinstance(input_items, list):
            for item in input_items:
                if isinstance(item, dict) and item.get("type") == "message" and item.get("role") == "user":
                    content = item.get("content", [])
                    if isinstance(content, str):
                        extracted.append({"role": "user", "content": content})
                    elif isinstance(content, list):
                        for c in content:
                            if isinstance(c, dict) and c.get("type") == "input_text" and c.get("text"):
                                extracted.append({"role": "user", "content": c.get("text")})
        return extracted


class ConversationMemory:
    """@desc: Exclusively manages the conversation session history (message accumulation) and logging state"""
    def __init__(self, delegator: LogDelegator):
        self.delegator = delegator
        self._session_history: Dict[str, List[Dict[str, Any]]] = {}
        self.input_messages: List[Dict[str, Any]] = []
        self.server_events: List[Dict[str, Any]] = []

    def record_input(self, event: Dict[str, Any], raw_message: Any):
        """@desc: Records client inputs and sends a pre-call to the logging Delegator"""
        inputs = OpenAIRealtimeProtocol.extract_user_input(event)
        self.input_messages.extend(inputs)
        if self.delegator:
            self.delegator.pre_call(input=raw_message, api_key="")

    def record_server_event(self, event: Dict[str, Any]):
        if OpenAIRealtimeProtocol.is_loggable(event):
            self.server_events.append(event)

    async def flush_logs(self):
        """@phase: Post-Call Cleanup"""
        if not self.delegator:
            return
        if self.input_messages:
            self.delegator.model_call_details["messages"] = self.input_messages
        if self.server_events:
            asyncio.create_task(self.delegator.async_success_handler(self.server_events))
            executor.submit(self.delegator.success_handler, self.server_events)

class ResponseWebsocketHandler:
    """@desc: The entry point for WebSocket requests and the sole application service. Orchestrates the Domain Models"""
    async def async_responses_websocket(
        self,
        model: str,
        websocket: Any,
        logging_obj: LogDelegator,
        responses_api_provider_config: Optional[BaseResponsesAPIConfig],
        **kwargs: Any,
    ) -> None:
        """@flow: Context Initialization -> Environment Validation -> Strategy Selection"""
        # @phase: Context Initialization
        context = RealtimeSessionContext(
            websocket=websocket,
            model=model,
            logging_obj=logging_obj,
            provider_config=responses_api_provider_config,
            api_key=kwargs.pop("api_key", None),
            api_base=kwargs.pop("api_base", None),
            timeout=kwargs.pop("timeout", None),
            user_api_key_dict=kwargs.pop("user_api_key_dict", None),
            litellm_metadata=kwargs.pop("litellm_metadata", {}),
            first_message=kwargs.pop("first_message", None),
            extra_kwargs=kwargs
        )

        # @phase: Strategy Execution
        if context.supports_native:
            await self._execute_native_stream(context)
        else:
            await self._execute_managed_stream(context)

    async def _execute_native_stream(self, context: RealtimeSessionContext) -> None:
        """@desc: Establishes a native backend WebSocket connection and builds a bidirectional bridge with the client"""
        ws_url, headers, ssl_context = context.build_native_connection_params()
        memory = ConversationMemory(context.logging_obj)
        context.logging_obj.pre_call(
            input=None, api_key=context.api_key or "",
            additional_args={"api_base": ws_url, "headers": headers, "complete_input_dict": {"mode": "responses_websocket"}}
        )

        try:
            async with websockets.connect(
                ws_url, additional_headers=headers, max_size=REALTIME_WEBSOCKET_MAX_MESSAGE_SIZE_BYTES, ssl=ssl_context,
            ) as backend_ws:
                
                ## @phase: Bi-directional Communication Tasks
                async def backend_to_client():
                    try:
                        while True:
                            raw_response = await backend_ws.recv()
                            event = OpenAIRealtimeProtocol.parse_event(raw_response)
                            if event:
                                memory.record_server_event(event)
                            await context.websocket.send_text(raw_response if isinstance(raw_response, str) else raw_response.decode('utf-8'))
                    except websockets.exceptions.ConnectionClosed:
                        pass
                    finally:
                        await memory.flush_logs()

                async def client_to_backend():
                    try:
                        if context.first_message:
                            event = OpenAIRealtimeProtocol.parse_event(context.first_message)
                            if event:
                                memory.record_input(event, context.first_message)
                            await backend_ws.send(context.first_message)

                        while True:
                            message = await context.websocket.receive_text()
                            event = OpenAIRealtimeProtocol.parse_event(message)
                            if event:
                                memory.record_input(event, message)
                            await backend_ws.send(message)
                    except websockets.exceptions.ConnectionClosed:
                        pass

                ## @phase: Concurrent Forwarding
                forward_task = asyncio.create_task(backend_to_client())
                try:
                    await client_to_backend()
                finally:
                    if not forward_task.done():
                        forward_task.cancel()
        except websockets.exceptions.InvalidStatusCode as e:
            await context.websocket.close(code=e.status_code, reason=redact_string(str(e)))
        except Exception as e:
            try:
                await context.websocket.close(code=1011, reason=redact_string(f"Internal error: {str(e)}"))
            except RuntimeError:
                pass

    async def _execute_managed_stream(self, context: RealtimeSessionContext) -> None:
        """@desc: Emulates real-time communication using an internal polling/streaming engine for providers lacking native support"""
        memory = ConversationMemory(context.logging_obj)
        async def process_single_message(raw_message: str):
            event = OpenAIRealtimeProtocol.parse_event(raw_message)
            if not event or event.get("type") != "response.create":
                return
            
            ## @desc: Parameter assembly (Utilizing the Protocol Adapter to execute kwargs generation logic).
            call_kwargs = {k: v for k, v in event.items() if k in OpenAIRealtimeProtocol._RESPONSE_CREATE_PARAMS}
            call_kwargs["stream"] = True
            call_kwargs["api_key"] = context.api_key
            
            try:
                stream_response = await config.aresponses(model=context.model, **call_kwargs)
                async for chunk in stream_response:
                    if not chunk: continue
                    serialized = json.dumps(chunk) if isinstance(chunk, dict) else chunk.model_dump_json(exclude_none=True)
                    await context.websocket.send_text(serialized)
            except Exception as exc:
                await context.websocket.send_text(json.dumps({"type": "error", "error": {"message": str(exc)}}))

        try:
            if context.first_message:
                await process_single_message(context.first_message)

            while True:
                message = await context.websocket.receive_text()
                await process_single_message(message)
        except Exception as exc:
            log.exception("ManagedResponsesWS: error", exc_info=exc)