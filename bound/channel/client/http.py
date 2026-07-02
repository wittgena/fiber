# bound.channel.client.http
## @lineage: anchor.channel.client.http
## @lineage: bound.channel.handler.http
## @lineage: bound.channel.action.handler.http
import ssl
from enum import Enum
import asyncio
import concurrent.futures
import inspect
import os
import socket
import sys
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import certifi
import httpx
from aiohttp import ClientSession, TCPConnector
from httpx import USE_CLIENT_DEFAULT, AsyncHTTPTransport, HTTPTransport
from httpx._types import RequestFiles
from watcher.plane.emitter import get_emitter
from bound.channel.config.resolver import config
from anchor.surface.exception import Timeout

from bound.channel.config.constants import (
    _DEFAULT_TTL_FOR_HTTPX_CLIENTS,
    AIOHTTP_CONNECTOR_LIMIT,
    AIOHTTP_CONNECTOR_LIMIT_PER_HOST,
    AIOHTTP_KEEPALIVE_TIMEOUT,
    AIOHTTP_NEEDS_CLEANUP_CLOSED,
    AIOHTTP_SO_KEEPALIVE,
    AIOHTTP_TCP_KEEPCNT,
    AIOHTTP_TCP_KEEPIDLE,
    AIOHTTP_TCP_KEEPINTVL,
    AIOHTTP_TTL_DNS_CACHE,
    COMPLETION_HTTP_FALLBACK_SECONDS,
    DEFAULT_SSL_CIPHERS,
    HTTP_HANDLER_CONNECT_TIMEOUT_SECONDS,
)

from anchor.provider.types import ProviderTypes
from bound.transport.aiohttp import AiohttpTransport

log = get_emitter("handler.http")

version = getattr(config, "version", "1.0.0")
_AIOHTTP_SUPPORTS_SOCKET_FACTORY = (
    "socket_factory" in inspect.signature(TCPConnector.__init__).parameters
)

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

VerifyTypes = Union[str, bool, ssl.SSLContext]

def _str_to_bool(val: Union[str, bool]) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("true", "1", "yes", "t", "y")


def _build_aiohttp_keepalive_socket_factory() -> (
    Optional[Callable[[Tuple[Any, ...]], socket.socket]]
):
    if not AIOHTTP_SO_KEEPALIVE or not _AIOHTTP_SUPPORTS_SOCKET_FACTORY:
        return None

    def factory(addr_info: Tuple[Any, ...]) -> socket.socket:
        family, type_, proto = addr_info[0], addr_info[1], addr_info[2]
        sock = socket.socket(family=family, type=type_, proto=proto)
        sock.setblocking(False)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, AIOHTTP_TCP_KEEPIDLE)
        elif hasattr(socket, "TCP_KEEPALIVE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, AIOHTTP_TCP_KEEPIDLE)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, AIOHTTP_TCP_KEEPINTVL)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, AIOHTTP_TCP_KEEPCNT)
        return sock

    return factory


def get_default_headers() -> dict:
    user_agent = os.environ.get("LITELLM_USER_AGENT")
    if user_agent is not None:
        return {"User-Agent": user_agent}
    return {"User-Agent": f"gate/{version}"}


headers = get_default_headers()

_DEFAULT_TIMEOUT = httpx.Timeout(
    timeout=COMPLETION_HTTP_FALLBACK_SECONDS,
    connect=HTTP_HANDLER_CONNECT_TIMEOUT_SECONDS,
)
_STREAMING_ERROR_BODY_READ_TIMEOUT_SECONDS = 5.0
_STREAMING_ERROR_BODY_READ_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=50,
    thread_name_prefix="gate-streaming-error-body-read",
)


def _prepare_request_data_and_content(
    data: Optional[Union[dict, str, bytes]] = None,
    content: Any = None,
) -> Tuple[Optional[Union[dict, Mapping]], Any]:
    request_data = None
    request_content = content

    if data is not None:
        if isinstance(data, (bytes, str)):
            if content is None:
                request_content = data
        else:
            request_data = data

    return request_data, request_content


_ssl_context_cache: Dict[Tuple[Optional[str], Optional[str], Optional[str]], ssl.SSLContext] = {}


def _create_ssl_context(
    cafile: Optional[str],
    ssl_security_level: Optional[str],
    ssl_ecdh_curve: Optional[str],
) -> ssl.SSLContext:
    custom_ssl_context = ssl.create_default_context(cafile=cafile)
    custom_ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

    if ssl_security_level and isinstance(ssl_security_level, str):
        custom_ssl_context.set_ciphers(ssl_security_level)
    else:
        custom_ssl_context.set_ciphers(DEFAULT_SSL_CIPHERS)

    if ssl_ecdh_curve and isinstance(ssl_ecdh_curve, str):
        try:
            custom_ssl_context.set_ecdh_curve(ssl_ecdh_curve)
            log.debug(f"SSL ECDH curve set to: {ssl_ecdh_curve}")
        except AttributeError:
            log.warning(f"SSL ECDH curve configuration not supported. Requested curve: {ssl_ecdh_curve}.")
        except ValueError as e:
            log.warning(f"Invalid SSL ECDH curve name: '{ssl_ecdh_curve}'. {e}.")

    return custom_ssl_context


def get_ssl_verify(ssl_verify: Optional[Union[bool, str]] = None) -> Union[bool, str]:
    # [개선됨] config 프록시를 통한 폴백 참조
    if ssl_verify is None:
        ssl_verify = os.getenv("SSL_VERIFY", config.ssl_verify)

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
    
    # [개선됨] config 프록시를 통한 폴백 참조
    ssl_security_level = os.getenv("SSL_SECURITY_LEVEL", config.ssl_security_level)
    ssl_ecdh_curve = os.getenv("SSL_ECDH_CURVE", config.ssl_ecdh_curve)

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
            _ssl_context_cache[cache_key] = _create_ssl_context(
                cafile=cafile,
                ssl_security_level=ssl_security_level,
                ssl_ecdh_curve=ssl_ecdh_curve,
            )
        return _ssl_context_cache[cache_key]

    return ssl_verify


_shared_realtime_ssl_context: Optional[Union[bool, str, ssl.SSLContext]] = None


def get_shared_realtime_ssl_context() -> Union[bool, str, ssl.SSLContext]:
    global _shared_realtime_ssl_context
    if _shared_realtime_ssl_context is None:
        _shared_realtime_ssl_context = get_ssl_configuration()
    return _shared_realtime_ssl_context


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


def _safe_get_response_text(response: httpx.Response) -> str:
    try:
        return response.text
    except Exception:
        return ""


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


class MaskedHTTPStatusError(httpx.HTTPStatusError):
    def __init__(self, original_error, message: Optional[str] = None, text: Optional[str] = None):
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


class AsyncHTTPHandler:
    def __init__(
        self,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        event_hooks: Optional[Mapping[str, List[Callable[..., Any]]]] = None,
        concurrent_limit=None,
        client_alias: Optional[str] = None,
        ssl_verify: Optional[VerifyTypes] = None,
        shared_session: Optional["ClientSession"] = None,
    ):
        self.timeout = timeout
        self.event_hooks = event_hooks
        self.client = self.create_client(
            timeout=timeout,
            event_hooks=event_hooks,
            ssl_verify=ssl_verify,
            shared_session=shared_session,
        )
        self.client_alias = client_alias

    def create_client(
        self,
        timeout: Optional[Union[float, httpx.Timeout]],
        event_hooks: Optional[Mapping[str, List[Callable[..., Any]]]],
        ssl_verify: Optional[VerifyTypes] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> httpx.AsyncClient:
        ssl_config = get_ssl_configuration(ssl_verify)
        # [개선됨] config 프록시를 통한 폴백 참조
        cert = os.getenv("SSL_CERTIFICATE", config.ssl_certificate)

        if timeout is None:
            timeout = _DEFAULT_TIMEOUT

        transport = AsyncHTTPHandler._create_async_transport(
            ssl_context=ssl_config if isinstance(ssl_config, ssl.SSLContext) else None,
            ssl_verify=ssl_config if isinstance(ssl_config, bool) else None,
            shared_session=shared_session,
        )

        default_headers = get_default_headers()

        return httpx.AsyncClient(
            transport=transport,
            event_hooks=event_hooks,
            timeout=timeout,
            verify=ssl_config,
            cert=cert,
            headers=default_headers,
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, *args):
        await self.client.aclose()

    async def get(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        follow_redirects: Optional[bool] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
    ):
        _follow_redirects = follow_redirects if follow_redirects is not None else USE_CLIENT_DEFAULT
        params = params or {}
        params.update(HTTPHandler.extract_query_params(url))

        response = await self.client.get(
            url,
            params=params,
            headers=headers,
            follow_redirects=_follow_redirects,
            timeout=timeout if timeout is not None else USE_CLIENT_DEFAULT,
        )
        return response

    # [개선됨] @track_llm_api_timing 데코레이터 완전 삭제
    async def post(
        self,
        url: str,
        data: Optional[Union[dict, str, bytes]] = None,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        stream: bool = False,
        logging_obj: Any = None,  # 의존성 제거
        files: Optional[RequestFiles] = None,
        content: Any = None,
    ):
        start_time = time.time()
        try:
            if timeout is None:
                timeout = self.timeout

            request_data, request_content = _prepare_request_data_and_content(data, content)

            req = self.client.build_request(
                "POST", url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, files=files, content=request_content,
            )
            response = await self.client.send(req, stream=stream)
            response.raise_for_status()
            return response
        except (httpx.RemoteProtocolError, httpx.ConnectError):
            new_client = self.create_client(timeout=timeout, event_hooks=self.event_hooks)
            try:
                return await self.single_connection_post_request(
                    url=url, client=new_client, data=data, json=json,
                    params=params, headers=headers, stream=stream,
                )
            finally:
                await new_client.aclose()
        except httpx.TimeoutException as e:
            end_time = time.time()
            time_delta = round(end_time - start_time, 3)
            raise Timeout(
                message=f"Connection timed out. Timeout passed={timeout}, time taken={time_delta} seconds",
                llm_provider="gate-httpx-handler"
            )
        except httpx.HTTPStatusError as e:
            await _raise_masked_async_error(e, stream)
            raise e
        except Exception as e:
            raise e

    async def put(self, url: str, data=None, json=None, params=None, headers=None, timeout=None, stream=False, content=None):
        try:
            if timeout is None:
                timeout = self.timeout
            request_data, request_content = _prepare_request_data_and_content(data, content)

            req = self.client.build_request(
                "PUT", url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, content=request_content
            )
            response = await self.client.send(req)
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            raise Timeout(message=f"Connection timed out after {timeout} seconds.", llm_provider="gate-httpx-handler")
        except httpx.HTTPStatusError as e:
            await _raise_masked_async_error(e, stream)
            raise e

    async def patch(self, url: str, data=None, json=None, params=None, headers=None, timeout=None, stream=False, content=None):
        try:
            if timeout is None:
                timeout = self.timeout
            request_data, request_content = _prepare_request_data_and_content(data, content)

            req = self.client.build_request(
                "PATCH", url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, content=request_content
            )
            response = await self.client.send(req)
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            raise Timeout(message=f"Connection timed out after {timeout} seconds.", llm_provider="gate-httpx-handler")
        except httpx.HTTPStatusError as e:
            await _raise_masked_async_error(e, stream)
            raise e

    async def delete(self, url: str, data=None, json=None, params=None, headers=None, timeout=None, stream=False, content=None):
        try:
            if timeout is None:
                timeout = self.timeout
            request_data, request_content = _prepare_request_data_and_content(data, content)

            req = self.client.build_request(
                "DELETE", url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, content=request_content
            )
            response = await self.client.send(req, stream=stream)
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            raise Timeout(message=f"Connection timed out after {timeout} seconds.", llm_provider="gate-httpx-handler")
        except httpx.HTTPStatusError as e:
            await _raise_masked_async_error(e, stream)
            raise e

    async def single_connection_post_request(self, url: str, client: httpx.AsyncClient, data=None, json=None, params=None, headers=None, stream=False, content=None):
        request_data, request_content = _prepare_request_data_and_content(data, content)
        req = client.build_request(
            "POST", url, data=request_data, json=json, params=params, headers=headers, content=request_content
        )
        response = await client.send(req, stream=stream)
        response.raise_for_status()
        return response

    def __del__(self) -> None:
        try:
            asyncio.get_running_loop().create_task(self.close())
        except Exception:
            pass

    @staticmethod
    def _create_async_transport(
        ssl_context: Optional[ssl.SSLContext] = None,
        ssl_verify: Optional[bool] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> Optional[Union[AiohttpTransport, AsyncHTTPTransport]]:
        if AsyncHTTPHandler._should_use_aiohttp_transport():
            return AsyncHTTPHandler._create_aiohttp_transport(
                ssl_context=ssl_context, ssl_verify=ssl_verify, shared_session=shared_session
            )
        return AsyncHTTPHandler._create_httpx_transport()

    @staticmethod
    def _should_use_aiohttp_transport() -> bool:
        # [개선됨] config를 통해 비활성화 여부 체크
        if config.disable_aiohttp_transport is True or _str_to_bool(os.getenv("DISABLE_AIOHTTP_TRANSPORT", "False")):
            return False
        log.debug("Using AiohttpTransport...")
        return True

    @staticmethod
    def _get_ssl_connector_kwargs(ssl_verify: Optional[bool] = None, ssl_context: Optional[ssl.SSLContext] = None) -> Dict[str, Any]:
        connector_kwargs: Dict[str, Any] = {
            "local_addr": ("0.0.0.0", 0) if config.force_ipv4 else None,
        }
        if ssl_context is not None:
            connector_kwargs["ssl"] = ssl_context
        elif ssl_verify is False:
            connector_kwargs["ssl"] = False
        return connector_kwargs

    @staticmethod
    def _create_aiohttp_transport(
        ssl_verify: Optional[bool] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> AiohttpTransport:
        # [개선됨] 내부 모듈 복사 가정. 복사되지 않았다면 aiohttp_transport 구현체 필요.
        from xphi.custom_httpx.aiohttp_transport import LiteLLMAiohttpTransport

        connector_kwargs = AsyncHTTPHandler._get_ssl_connector_kwargs(ssl_verify=ssl_verify, ssl_context=ssl_context)
        trust_env: bool = config.aiohttp_trust_env or _str_to_bool(os.getenv("AIOHTTP_TRUST_ENV", "False"))

        ssl_for_transport = ssl_context if ssl_context is not None else (False if ssl_verify is False else None)

        if shared_session is not None and not shared_session.closed:
            log.debug(f"SHARED SESSION: Reusing existing ClientSession (ID: {id(shared_session)})")
            return LiteLLMAiohttpTransport(client=shared_session, ssl_verify=ssl_for_transport, owns_session=False)

        log.debug("NEW SESSION: Creating new ClientSession (no shared session provided)")
        transport_connector_kwargs = {
            "keepalive_timeout": AIOHTTP_KEEPALIVE_TIMEOUT,
            "ttl_dns_cache": AIOHTTP_TTL_DNS_CACHE,
            **connector_kwargs,
        }
        if AIOHTTP_NEEDS_CLEANUP_CLOSED:
            transport_connector_kwargs["enable_cleanup_closed"] = True
        if AIOHTTP_CONNECTOR_LIMIT > 0:
            transport_connector_kwargs["limit"] = AIOHTTP_CONNECTOR_LIMIT
        if AIOHTTP_CONNECTOR_LIMIT_PER_HOST > 0:
            transport_connector_kwargs["limit_per_host"] = AIOHTTP_CONNECTOR_LIMIT_PER_HOST

        socket_factory = _build_aiohttp_keepalive_socket_factory()
        if socket_factory is not None:
            transport_connector_kwargs["socket_factory"] = socket_factory

        return LiteLLMAiohttpTransport(
            client=lambda: ClientSession(connector=TCPConnector(**transport_connector_kwargs), trust_env=trust_env),
            ssl_verify=ssl_for_transport,
        )

    @staticmethod
    def _create_httpx_transport() -> Optional[AsyncHTTPTransport]:
        if config.force_ipv4:
            return AsyncHTTPTransport(local_address="0.0.0.0")
        return None


class HTTPHandler:
    def __init__(
        self,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        concurrent_limit=None,
        client: Optional[httpx.Client] = None,
        ssl_verify: Optional[Union[bool, str]] = None,
        disable_default_headers: Optional[bool] = False,
    ):
        if timeout is None:
            timeout = _DEFAULT_TIMEOUT

        ssl_config = get_ssl_configuration(ssl_verify)
        cert = os.getenv("SSL_CERTIFICATE", config.ssl_certificate)
        default_headers = get_default_headers() if not disable_default_headers else None

        if client is None:
            transport = self._create_sync_transport()
            self.client = httpx.Client(
                transport=transport,
                timeout=timeout,
                verify=ssl_config,
                cert=cert,
                headers=default_headers,
                follow_redirects=True,
            )
        else:
            self.client = client

    def close(self):
        self.client.close()

    def get(self, url: str, params=None, headers=None, follow_redirects=None, timeout=None):
        _follow_redirects = follow_redirects if follow_redirects is not None else USE_CLIENT_DEFAULT
        params = params or {}
        params.update(self.extract_query_params(url))
        return self.client.get(
            url, params=params, headers=headers, follow_redirects=_follow_redirects,
            timeout=timeout if timeout is not None else USE_CLIENT_DEFAULT,
        )

    @staticmethod
    def extract_query_params(url: str) -> Dict[str, str]:
        from urllib.parse import parse_qsl, urlsplit
        return dict(parse_qsl(urlsplit(url).query))

    def post(self, url: str, data=None, json=None, params=None, headers=None, stream=False, timeout=None, files=None, content=None, logging_obj=None):
        try:
            request_data, request_content = _prepare_request_data_and_content(data, content)
            req = self.client.build_request(
                "POST", url, data=request_data, json=json, params=params,
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

    def patch(self, url: str, data=None, json=None, params=None, headers=None, stream=False, timeout=None, content=None):
        try:
            request_data, request_content = _prepare_request_data_and_content(data, content)
            req = self.client.build_request(
                "PATCH", url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, content=request_content
            )
            response = self.client.send(req, stream=stream)
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            raise Timeout(message=f"Connection timed out after {timeout} seconds.", llm_provider="gate-httpx-handler")
        except httpx.HTTPStatusError as e:
            _raise_masked_sync_error(e, stream)
            raise e

    def put(self, url: str, data=None, json=None, params=None, headers=None, stream=False, timeout=None, content=None):
        try:
            request_data, request_content = _prepare_request_data_and_content(data, content)
            req = self.client.build_request(
                "PUT", url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, content=request_content
            )
            response = self.client.send(req, stream=stream)
            return response
        except httpx.TimeoutException:
            raise Timeout(message=f"Connection timed out after {timeout} seconds.", llm_provider="gate-httpx-handler")
        except httpx.HTTPStatusError as e:
            _raise_masked_sync_error(e, stream)
            raise e

    def delete(self, url: str, data=None, json=None, params=None, headers=None, timeout=None, stream=False, content=None):
        try:
            request_data, request_content = _prepare_request_data_and_content(data, content)
            req = self.client.build_request(
                "DELETE", url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, content=request_content
            )
            response = self.client.send(req, stream=stream)
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            raise Timeout(message=f"Connection timed out after {timeout} seconds.", llm_provider="gate-httpx-handler")
        except httpx.HTTPStatusError as e:
            _raise_masked_sync_error(e, stream)
            raise e

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _create_sync_transport(self) -> Optional[HTTPTransport]:
        if config.force_ipv4:
            return HTTPTransport(local_address="0.0.0.0")
        return getattr(config, "sync_transport", None)

_HTTPX_CLIENT_CACHE: Dict[str, Any] = {}

def get_async_httpx_client(
    llm_provider: Union[ProviderTypes, httpxSpecialProvider],
    params: Optional[dict] = None,
    shared_session: Optional["ClientSession"] = None,
) -> AsyncHTTPHandler:
    
    _params_key_name = ""
    if params is not None:
        for key, value in params.items():
            _params_key_name += f"{key}_{value}"

    _cache_key_name = f"async_httpx_client_{_params_key_name}_{llm_provider}"

    if _cache_key_name in _HTTPX_CLIENT_CACHE:
        return _HTTPX_CLIENT_CACHE[_cache_key_name]

    if params is not None:
        handler_params = {k: v for k, v in params.items() if k != "disable_aiohttp_transport"}
        handler_params["shared_session"] = shared_session
        _new_client = AsyncHTTPHandler(**handler_params)
    else:
        _new_client = AsyncHTTPHandler(timeout=_DEFAULT_TIMEOUT, shared_session=shared_session)

    _HTTPX_CLIENT_CACHE[_cache_key_name] = _new_client
    return _new_client


def _get_httpx_client(params: Optional[dict] = None) -> HTTPHandler:
    _params_key_name = ""
    if params is not None:
        for key, value in params.items():
            _params_key_name += f"{key}_{value}"

    _cache_key_name = f"httpx_client_{_params_key_name}"

    if _cache_key_name in _HTTPX_CLIENT_CACHE:
        return _HTTPX_CLIENT_CACHE[_cache_key_name]

    if params is not None:
        handler_params = {k: v for k, v in params.items() if k != "disable_aiohttp_transport"}
        _new_client = HTTPHandler(**handler_params)
    else:
        _new_client = HTTPHandler(timeout=_DEFAULT_TIMEOUT)

    _HTTPX_CLIENT_CACHE[_cache_key_name] = _new_client
    return _new_client