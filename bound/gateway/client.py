# bound.gateway.client
import os
import ssl
import time
import asyncio
import concurrent.futures
from enum import Enum
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union

import httpx
from httpx import USE_CLIENT_DEFAULT, AsyncHTTPTransport, HTTPTransport
from httpx._types import RequestFiles
import certifi

from bound.resolver.model.config.resolver import config
from bound.resolver.model.config.constants import (
    AIOHTTP_CONNECTOR_LIMIT,
    AIOHTTP_KEEPALIVE_TIMEOUT,
    COMPLETION_HTTP_FALLBACK_SECONDS,
    HTTP_HANDLER_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_SSL_CIPHERS,
)
from eco.exception import Timeout
from bound.resolver.model.protype import ProviderTypes
from watcher.plane.emitter import get_emitter

log = get_emitter("transport.client")

VerifyTypes = Union[str, bool, ssl.SSLContext]

class httpxSpecialProvider(str, Enum):
    LoggingCallback = "logging_callback"
    GuardrailCallback = "guardrail_callback"
    Caching = "caching"
    Oauth2Check = "oauth2_check"
    Oauth2Register = "oauth2_register"
    SecretManager = "secret_manager"
    PassThroughEndpoint = "pass_through_endpoint"
    PromptFactory = "prompt_factory"
    SSO_HANDLER = "sso_handler"
    Search = "search"
    MCP = "mcp"
    RAG = "rag"
    A2AProvider = "a2a_provider"
    AgentHealthCheck = "agent_health_check"
    A2A = "a2a"
    PromptManagement = "prompt_management"
    UI = "ui"

_DEFAULT_TIMEOUT = httpx.Timeout(
    timeout=COMPLETION_HTTP_FALLBACK_SECONDS,
    connect=HTTP_HANDLER_CONNECT_TIMEOUT_SECONDS,
)
_STREAMING_ERROR_BODY_READ_TIMEOUT_SECONDS = 5.0
_HTTPX_CLIENT_CACHE: Dict[str, Any] = {}
_ssl_context_cache: Dict[Tuple[Optional[str], Optional[str], Optional[str]], ssl.SSLContext] = {}
_STREAMING_ERROR_BODY_READ_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=50, thread_name_prefix="gate-streaming-error-body-read")


# =====================================================================
# 2. Utility & Helper Functions
# =====================================================================
def _str_to_bool(val: Union[str, bool]) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("true", "1", "yes", "t", "y")

def get_default_headers() -> dict:
    user_agent = os.environ.get("LITELLM_USER_AGENT")
    if user_agent is not None:
        return {"User-Agent": user_agent}
    return {"User-Agent": "gate/1.0"}

def _prepare_request_data_and_content(data: Optional[Union[dict, str, bytes]] = None, content: Any = None) -> Tuple[Optional[Union[dict, Mapping]], Any]:
    request_data = None
    request_content = content
    if data is not None:
        if isinstance(data, (bytes, str)):
            if content is None:
                request_content = data
        else:
            request_data = data
    return request_data, request_content

def _safe_get_response_text(response: httpx.Response) -> str:
    try:
        return response.text
    except Exception:
        return ""


# =====================================================================
# 3. SSL Configuration & Security
# =====================================================================
def _create_ssl_context(cafile: Optional[str], ssl_security_level: Optional[str], ssl_ecdh_curve: Optional[str]) -> ssl.SSLContext:
    custom_ssl_context = ssl.create_default_context(cafile=cafile)
    custom_ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

    if ssl_security_level and isinstance(ssl_security_level, str):
        custom_ssl_context.set_ciphers(ssl_security_level)
    else:
        custom_ssl_context.set_ciphers(DEFAULT_SSL_CIPHERS)

    if ssl_ecdh_curve and isinstance(ssl_ecdh_curve, str):
        try:
            custom_ssl_context.set_ecdh_curve(ssl_ecdh_curve)
        except Exception as e:
            log.warning(f"Failed to set SSL ECDH curve '{ssl_ecdh_curve}': {e}")

    return custom_ssl_context

def get_ssl_verify(ssl_verify: Optional[Union[bool, str]] = None) -> Union[bool, str]:
    if ssl_verify is None:
        ssl_verify = os.getenv("SSL_VERIFY", getattr(config, "ssl_verify", True))

    if isinstance(ssl_verify, str):
        if os.path.exists(ssl_verify):
            return ssl_verify
        ssl_verify = _str_to_bool(ssl_verify)

    if ssl_verify is True:
        ssl_cert_file = os.getenv("SSL_CERT_FILE")
        if ssl_cert_file and os.path.exists(ssl_cert_file):
            return ssl_cert_file

    return ssl_verify if ssl_verify is not None else True

def get_ssl_configuration(ssl_verify: Optional[VerifyTypes] = None) -> Union[bool, str, ssl.SSLContext]:
    if isinstance(ssl_verify, ssl.SSLContext):
        return ssl_verify

    ssl_verify = get_ssl_verify(ssl_verify=ssl_verify)
    ssl_security_level = os.getenv("SSL_SECURITY_LEVEL", getattr(config, "ssl_security_level", None))
    ssl_ecdh_curve = os.getenv("SSL_ECDH_CURVE", getattr(config, "ssl_ecdh_curve", None))

    cafile = None
    if isinstance(ssl_verify, str) and os.path.exists(ssl_verify):
        cafile = ssl_verify
    if not cafile:
        ssl_cert_file = os.getenv("SSL_CERT_FILE")
        if ssl_cert_file and os.path.exists(ssl_cert_file):
            cafile = ssl_cert_file
        else:
            cafile = certifi.where()

    if ssl_verify is not False:
        cache_key = (cafile, ssl_security_level, ssl_ecdh_curve)
        if cache_key not in _ssl_context_cache:
            _ssl_context_cache[cache_key] = _create_ssl_context(cafile, ssl_security_level, ssl_ecdh_curve)
        return _ssl_context_cache[cache_key]

    return ssl_verify


# =====================================================================
# 4. Error Masking & Response Reading
# =====================================================================
def mask_sensitive_info(error_message):
    if isinstance(error_message, str):
        key_index = error_message.find("key=")
    else:
        return error_message

    if key_index != -1:
        next_param = error_message.find("&", key_index)
        if next_param == -1:
            masked_message = error_message[: key_index + 4] + "[REDACTED_API_KEY]"
        else:
            masked_message = error_message[: key_index + 4] + "[REDACTED_API_KEY]" + error_message[next_param:]
        return masked_message
    return error_message

async def _safe_aread_response(response: httpx.Response, timeout: Optional[float] = None) -> bytes:
    try:
        if timeout is not None:
            return await asyncio.wait_for(response.aread(), timeout=timeout)
        return await response.aread()
    except Exception:
        return b""

def _safe_read_response(response: httpx.Response, timeout: Optional[float] = None) -> bytes:
    try:
        if timeout is not None:
            future = _STREAMING_ERROR_BODY_READ_EXECUTOR.submit(response.read)
            try:
                return future.result(timeout=timeout)
            except Exception:
                response.close()
                return b""
        return response.read()
    except Exception:
        return b""

class MaskedHTTPStatusError(httpx.HTTPStatusError):
    def __init__(self, original_error: httpx.HTTPStatusError, message: Optional[str] = None, text: Optional[str] = None):
        masked_url = mask_sensitive_info(str(original_error.request.url))
        masked_original_message = mask_sensitive_info(str(original_error))

        try:
            response_content = original_error.response.content
        except Exception:
            response_content = b""

        response_headers = {
            k: v for k, v in original_error.response.headers.items()
            if k.lower() not in ("content-encoding", "content-length")
        }

        try:
            request_content = original_error.request.content
        except httpx.RequestNotRead:
            request_content = b""

        masked_request = httpx.Request(
            method=original_error.request.method,
            url=masked_url,
            headers=original_error.request.headers,
            content=request_content,
        )

        super().__init__(
            message=masked_original_message,
            request=masked_request,
            response=httpx.Response(
                status_code=original_error.response.status_code,
                content=response_content,
                headers=response_headers,
                request=masked_request,
            ),
        )
        self.message = message
        self.text = text
        self.status_code = original_error.response.status_code

async def _raise_masked_async_error(e: httpx.HTTPStatusError, stream: bool) -> None:
    if stream:
        try:
            _body = mask_sensitive_info(
                await _safe_aread_response(e.response, timeout=_STREAMING_ERROR_BODY_READ_TIMEOUT_SECONDS)
            )
            raise MaskedHTTPStatusError(e, message=_body, text=_body) from None
        finally:
            try:
                await e.response.aclose()
            except Exception:
                pass
    _text = mask_sensitive_info(_safe_get_response_text(e.response))
    raise MaskedHTTPStatusError(e, message=_text, text=_text) from None

def _raise_masked_sync_error(e: httpx.HTTPStatusError, stream: bool) -> None:
    if stream:
        try:
            _body = mask_sensitive_info(
                _safe_read_response(e.response, timeout=_STREAMING_ERROR_BODY_READ_TIMEOUT_SECONDS)
            )
            raise MaskedHTTPStatusError(e, message=_body, text=_body) from None
        finally:
            try:
                e.response.close()
            except Exception:
                pass
    _text = mask_sensitive_info(_safe_get_response_text(e.response))
    raise MaskedHTTPStatusError(e, message=_text, text=_text) from None


# =====================================================================
# 5. Core HTTP Clients
# =====================================================================
def _get_httpx_limits() -> httpx.Limits:
    return httpx.Limits(
        max_keepalive_connections=AIOHTTP_CONNECTOR_LIMIT if AIOHTTP_CONNECTOR_LIMIT > 0 else 20,
        max_connections=AIOHTTP_CONNECTOR_LIMIT if AIOHTTP_CONNECTOR_LIMIT > 0 else 100,
        keepalive_expiry=AIOHTTP_KEEPALIVE_TIMEOUT if AIOHTTP_KEEPALIVE_TIMEOUT > 0 else 5.0,
    )

class AsyncHTTPClient:
    def __init__(
        self,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        event_hooks: Optional[Mapping[str, List[Callable[..., Any]]]] = None,
        ssl_verify: Optional[VerifyTypes] = None,
        **kwargs
    ):
        self.timeout = timeout
        self.event_hooks = event_hooks
        self.client = self.create_client(timeout, event_hooks, ssl_verify)

    def create_client(self, timeout, event_hooks, ssl_verify) -> httpx.AsyncClient:
        ssl_config = get_ssl_configuration(ssl_verify)
        cert = os.getenv("SSL_CERTIFICATE", getattr(config, "ssl_certificate", None))
        timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT

        transport = AsyncHTTPTransport(
            verify=ssl_config if ssl_config is not None else True,
            cert=cert,
            limits=_get_httpx_limits(),
            local_address="0.0.0.0" if getattr(config, "force_ipv4", False) else None,
            retries=1
        )

        return httpx.AsyncClient(
            transport=transport, event_hooks=event_hooks, timeout=timeout,
            headers=get_default_headers(), follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, *args):
        await self.client.aclose()

    async def get(self, url: str, params=None, headers=None, follow_redirects=None, timeout=None):
        _follow_redirects = follow_redirects if follow_redirects is not None else USE_CLIENT_DEFAULT
        from urllib.parse import parse_qsl, urlsplit
        params = params or {}
        params.update(dict(parse_qsl(urlsplit(url).query)))

        return await self.client.get(
            url, params=params, headers=headers, follow_redirects=_follow_redirects,
            timeout=timeout if timeout is not None else USE_CLIENT_DEFAULT,
        )

    async def _asend_request(
        self, method: str, url: str, data=None, json=None, params=None, 
        headers=None, stream=False, timeout=None, files=None, content=None
    ):
        start_time = time.time()
        try:
            timeout = timeout if timeout is not None else getattr(self, "timeout", _DEFAULT_TIMEOUT)
            request_data, request_content = _prepare_request_data_and_content(data, content)
            req = self.client.build_request(
                method, url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, files=files, content=request_content,
            )
            response = await self.client.send(req, stream=stream)
            response.raise_for_status()
            return response

        except httpx.TimeoutException:
            time_delta = round(time.time() - start_time, 3)
            raise Timeout(
                message=f"Connection timed out. Timeout passed={timeout}, time taken={time_delta} seconds",
                llm_provider="gate-httpx-handler"
            )
        except httpx.HTTPStatusError as e:
            await _raise_masked_async_error(e, stream)
            raise e
        except (httpx.RemoteProtocolError, httpx.ConnectError) as e:
            raise e

    async def post(self, url: str, **kwargs):
        kwargs.pop('logging_obj', None)
        return await self._asend_request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs):
        return await self._asend_request("PUT", url, **kwargs)

    async def patch(self, url: str, **kwargs):
        return await self._asend_request("PATCH", url, **kwargs)

    async def delete(self, url: str, **kwargs):
        return await self._asend_request("DELETE", url, **kwargs)

    def __del__(self) -> None:
        try:
            asyncio.get_running_loop().create_task(self.close())
        except Exception:
            pass


class HTTPClient:
    def __init__(
        self,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[httpx.Client] = None,
        ssl_verify: Optional[Union[bool, str]] = None,
        disable_default_headers: Optional[bool] = False,
        **kwargs
    ):
        timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT
        ssl_config = get_ssl_configuration(ssl_verify)
        cert = os.getenv("SSL_CERTIFICATE", getattr(config, "ssl_certificate", None))
        default_headers = get_default_headers() if not disable_default_headers else None

        if client is None:
            transport = HTTPTransport(
                verify=ssl_config if ssl_config is not None else True,
                cert=cert,
                limits=_get_httpx_limits(),
                local_address="0.0.0.0" if getattr(config, "force_ipv4", False) else None,
                retries=1
            )
            self.client = httpx.Client(
                transport=transport, timeout=timeout,
                headers=default_headers, follow_redirects=True,
            )
        else:
            self.client = client

    def close(self):
        self.client.close()

    def get(self, url: str, params=None, headers=None, follow_redirects=None, timeout=None):
        _follow_redirects = follow_redirects if follow_redirects is not None else USE_CLIENT_DEFAULT
        from urllib.parse import parse_qsl, urlsplit
        params = params or {}
        params.update(dict(parse_qsl(urlsplit(url).query)))

        return self.client.get(
            url, params=params, headers=headers, follow_redirects=_follow_redirects,
            timeout=timeout if timeout is not None else USE_CLIENT_DEFAULT,
        )

    def _send_request(
        self, method: str, url: str, data=None, json=None, params=None, 
        headers=None, stream=False, timeout=None, files=None, content=None
    ):
        try:
            timeout = timeout if timeout is not None else getattr(self, "timeout", _DEFAULT_TIMEOUT)
            request_data, request_content = _prepare_request_data_and_content(data, content)
            req = self.client.build_request(
                method, url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, files=files, content=request_content
            )
            response = self.client.send(req, stream=stream)
            response.raise_for_status() 
            return response
            
        except httpx.TimeoutException:
            raise Timeout(message=f"Connection timed out after {timeout} seconds.", llm_provider="gate-httpx-handler")
        except httpx.HTTPStatusError as e:
            _raise_masked_sync_error(e, stream)
            raise e

    def post(self, url: str, **kwargs):
        return self._send_request("POST", url, **kwargs)

    def patch(self, url: str, **kwargs):
        return self._send_request("PATCH", url, **kwargs)

    def put(self, url: str, **kwargs):
        return self._send_request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs):
        return self._send_request("DELETE", url, **kwargs)

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


def get_client(
    is_async: bool,
    llm_provider: Optional[Union[ProviderTypes, httpxSpecialProvider]] = None,
    params: Optional[dict] = None,
    **kwargs
) -> Union[AsyncHTTPClient, HTTPClient]:
    
    params_key = ""
    handler_params = {}

    if params is not None:
        ignore_keys = {"shared_session", "disable_aiohttp_transport"}
        handler_params = {k: v for k, v in params.items() if k not in ignore_keys}
        for key, value in sorted(handler_params.items()):
            params_key += f"{key}_{value}"
    else:
        handler_params = {"timeout": _DEFAULT_TIMEOUT}

    prefix = "async_httpx_client" if is_async else "httpx_client"
    suffix = f"_{llm_provider}" if (is_async and llm_provider) else ""
    cache_key = f"{prefix}_{params_key}{suffix}"

    if cache_key in _HTTPX_CLIENT_CACHE:
        return _HTTPX_CLIENT_CACHE[cache_key]

    if is_async:
        client = AsyncHTTPClient(**handler_params)
    else:
        client = HTTPClient(**handler_params)

    _HTTPX_CLIENT_CACHE[cache_key] = client
    return client