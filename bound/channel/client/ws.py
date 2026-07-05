# bound.channel.client.ws
## @lineage: anchor.channel.client.ws
import json
import ssl
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Coroutine,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)

import httpx
import websockets
from websockets.asyncio.client import ClientConnection

# from gate.litellm.voider import Logging as LiteLLMLoggingObj
LiteLLMLoggingObj = Any

from xphi.xor.secret.redact import redact_string
from bound.channel.config.response import BaseResponsesAPIConfig
from bound.channel.config.constants import REALTIME_WEBSOCKET_MAX_MESSAGE_SIZE_BYTES
from bound.transport.stream.iterator import ResponseWSStreaming, ResponseWSHandler
from anchor.provider.model.param.legacy import GenericLiteLLMParams
from bound.channel.client.http import get_shared_realtime_ssl_context

from watcher.plane.emitter import get_emitter

log = get_emitter("llms.handler.websocket")

class ResponseWebsocketHandler:
    async def async_responses_websocket(
        self,
        model: str,
        websocket: Any,
        logging_obj: LiteLLMLoggingObj,
        responses_api_provider_config: Optional[BaseResponsesAPIConfig],
        api_base: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: Optional[float] = None,
        user_api_key_dict: Optional[Any] = None,
        litellm_metadata: Optional[Dict[str, Any]] = None,
        custom_llm_provider: Optional[str] = None,
        first_message: Optional[str] = None,
        **kwargs: Any,
    ):
        if (
            responses_api_provider_config is None
            or not responses_api_provider_config.supports_native_websocket()
        ):
            handler = ResponseWSHandler(
                websocket=websocket,
                model=model,
                logging_obj=logging_obj,
                user_api_key_dict=user_api_key_dict,
                litellm_metadata=litellm_metadata,
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                custom_llm_provider=custom_llm_provider,
                first_message=first_message,
                **kwargs,
            )
            await handler.run()
            return

        litellm_params = GenericLiteLLMParams()
        headers = responses_api_provider_config.validate_environment(
            headers={},
            model=model,
            litellm_params=litellm_params,
        )
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        http_url = responses_api_provider_config.get_complete_url(
            api_base=api_base,
            litellm_params={},
        )
        ws_url = http_url.replace("https://", "wss://").replace("http://", "ws://")
        _parsed = urlparse(ws_url)
        _qs = parse_qs(_parsed.query)
        if "model" not in _qs:
            _qs["model"] = [model]
            ws_url = urlunparse(
                _parsed._replace(query=urlencode({k: v[0] for k, v in _qs.items()}))
            )

        try:
            ssl_context = get_shared_realtime_ssl_context()
            if ws_url.startswith("wss://") and ssl_context is False:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

            logging_obj.pre_call(
                input=None,
                api_key=api_key or "",
                additional_args={
                    "api_base": ws_url,
                    "headers": headers,
                    "complete_input_dict": {"mode": "responses_websocket"},
                },
            )

            async with websockets.connect(  # type: ignore
                ws_url,
                additional_headers=headers,
                max_size=REALTIME_WEBSOCKET_MAX_MESSAGE_SIZE_BYTES,
                ssl=ssl_context,
            ) as backend_ws:
                _request_data: Dict[str, Any] = {}
                if litellm_metadata:
                    _request_data["litellm_metadata"] = litellm_metadata
                streaming = ResponseWSStreaming(
                    websocket=websocket,
                    backend_ws=cast(ClientConnection, backend_ws),
                    logging_obj=logging_obj,
                    user_api_key_dict=user_api_key_dict,
                    request_data=_request_data,
                    first_message=first_message,
                )
                await streaming.bidirectional_forward()
        except websockets.exceptions.InvalidStatusCode as e:  # type: ignore
            log.exception(f"Error connecting to responses WS backend: {e}")
            await websocket.close(code=e.status_code, reason=redact_string(str(e)))
        except Exception as e:
            log.exception(f"Error in responses WS: {e}")
            try:
                await websocket.close(code=1011, reason=redact_string(f"Internal server error: {str(e)}"))
            except RuntimeError as close_error:
                if "already completed" in str(close_error) or "websocket.close" in str(close_error):
                    pass
                else:
                    raise Exception(f"Unexpected error while closing WebSocket: {close_error}")