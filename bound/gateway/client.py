# bound.gateway.client
import os
import ssl
import time
import asyncio
import warnings
from enum import Enum
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union
import httpx
from aiohttp import ClientSession, TCPConnector
from httpx import USE_CLIENT_DEFAULT, AsyncHTTPTransport, HTTPTransport
from httpx._types import RequestFiles
import certifi

from bound.resolver.model.config.resolver import config
from bound.resolver.model.config.constants import (
    AIOHTTP_CONNECTOR_LIMIT,
    AIOHTTP_CONNECTOR_LIMIT_PER_HOST,
    AIOHTTP_KEEPALIVE_TIMEOUT,
    AIOHTTP_NEEDS_CLEANUP_CLOSED,
    AIOHTTP_TTL_DNS_CACHE,
)

from eco.exception import Timeout
from eco.legacy.info import ProviderTypes
from bound.gateway.base import (
    VerifyTypes,
    _DEFAULT_TIMEOUT,
    get_default_headers,
    _prepare_request_data_and_content,
    _safe_get_response_text,
    _STREAMING_ERROR_BODY_READ_TIMEOUT_SECONDS,
    _HTTPX_CLIENT_CACHE,
    httpxSpecialProvider,
)
from bound.gateway.stream.secure import (
    _safe_read_response,
    MaskedHTTPStatusError,
    _build_aiohttp_keepalive_socket_factory,
    _safe_aread_response, 
    mask_sensitive_info,
    get_ssl_configuration,
)
from bound.gateway.aio import AiohttpTransport
from watcher.plane.emitter import get_emitter

log = get_emitter("transport.client")

def _str_to_bool(val: Union[str, bool]) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("true", "1", "yes", "t", "y")

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

class AsyncHTTPClient:
    def __init__(
        self,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        event_hooks: Optional[Mapping[str, List[Callable[..., Any]]]] = None,
        concurrent_limit=None,
        client_alias: Optional[str] = None,
        ssl_verify: Optional[VerifyTypes] = None,
        shared_session: Optional["ClientSession"] = None,
    ):
        warnings.warn(
            "AsyncHTTPHandler Wrapper is deprecated. In future versions, "
            "this will return a pure httpx.AsyncClient.",
            DeprecationWarning,
            stacklevel=2
        )
        self.timeout = timeout
        self.event_hooks = event_hooks
        self.client_alias = client_alias
        self.client = self.create_client(
            timeout=timeout,
            event_hooks=event_hooks,
            ssl_verify=ssl_verify,
            shared_session=shared_session,
        )

    def create_client(
        self,
        timeout: Optional[Union[float, httpx.Timeout]],
        event_hooks: Optional[Mapping[str, List[Callable[..., Any]]]],
        ssl_verify: Optional[VerifyTypes] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> httpx.AsyncClient:
        ssl_config = get_ssl_configuration(ssl_verify)
        cert = os.getenv("SSL_CERTIFICATE", config.ssl_certificate)

        if timeout is None:
            timeout = _DEFAULT_TIMEOUT

        transport = AsyncHTTPClient._create_async_transport(
            ssl_context=ssl_config if isinstance(ssl_config, ssl.SSLContext) else None,
            ssl_verify=ssl_config if isinstance(ssl_config, bool) else None,
            shared_session=shared_session,
        )
        return httpx.AsyncClient(
            transport=transport,
            event_hooks=event_hooks,
            timeout=timeout,
            verify=ssl_config,
            cert=cert,
            headers=get_default_headers(),
            follow_redirects=True,
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
            url,
            params=params,
            headers=headers,
            follow_redirects=_follow_redirects,
            timeout=timeout if timeout is not None else USE_CLIENT_DEFAULT,
        )

    async def _asend_request(
        self, method: str, url: str, data=None, json=None, params=None, 
        headers=None, stream=False, timeout=None, files=None, content=None
    ):
        start_time = time.time()
        try:
            if timeout is None:
                timeout = getattr(self, "timeout", _DEFAULT_TIMEOUT)

            request_data, request_content = _prepare_request_data_and_content(data, content)
            req = self.client.build_request(
                method, url, data=request_data, json=json, params=params,
                headers=headers, timeout=timeout, files=files, content=request_content,
            )
            response = await self.client.send(req, stream=stream)
            response.raise_for_status()
            return response

        except (httpx.RemoteProtocolError, httpx.ConnectError) as e:
            new_client = self.create_client(timeout=timeout, event_hooks=self.event_hooks)
            try:
                req = new_client.build_request(
                    method, url, data=request_data, json=json, params=params, 
                    headers=headers, content=request_content
                )
                response = await new_client.send(req, stream=stream)
                response.raise_for_status()
                return response
            finally:
                await new_client.aclose()

        except httpx.TimeoutException:
            time_delta = round(time.time() - start_time, 3)
            raise Timeout(
                message=f"Connection timed out. Timeout passed={timeout}, time taken={time_delta} seconds",
                llm_provider="gate-httpx-handler"
            )
        except httpx.HTTPStatusError as e:
            await _raise_masked_async_error(e, stream)
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

    @staticmethod
    def _create_async_transport(
        ssl_context: Optional[ssl.SSLContext] = None,
        ssl_verify: Optional[bool] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> Optional[Union[AiohttpTransport, AsyncHTTPTransport]]:
        if AsyncHTTPClient._should_use_aiohttp_transport():
            return AsyncHTTPClient._create_aiohttp_transport(
                ssl_context=ssl_context, ssl_verify=ssl_verify, shared_session=shared_session
            )
        if getattr(config, "force_ipv4", False):
            return AsyncHTTPTransport(local_address="0.0.0.0")
        return None

    @staticmethod
    def _should_use_aiohttp_transport() -> bool:
        if getattr(config, "disable_aiohttp_transport", False) or _str_to_bool(os.getenv("DISABLE_AIOHTTP_TRANSPORT", "False")):
            return False
        return True

    @staticmethod
    def _create_aiohttp_transport(
        ssl_verify: Optional[bool] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> AiohttpTransport:
        from xphi.custom_httpx.aiohttp_transport import LiteLLMAiohttpTransport
        
        connector_kwargs = {"local_addr": ("0.0.0.0", 0) if getattr(config, "force_ipv4", False) else None}
        if ssl_context is not None:
            connector_kwargs["ssl"] = ssl_context
        elif ssl_verify is False:
            connector_kwargs["ssl"] = False

        trust_env = getattr(config, "aiohttp_trust_env", False) or _str_to_bool(os.getenv("AIOHTTP_TRUST_ENV", "False"))
        ssl_for_transport = ssl_context if ssl_context is not None else (False if ssl_verify is False else None)

        if shared_session is not None and not shared_session.closed:
            return LiteLLMAiohttpTransport(client=shared_session, ssl_verify=ssl_for_transport, owns_session=False)

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

class HTTPClient:
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

    def _create_sync_transport(self) -> Optional[HTTPTransport]:
        if getattr(config, "force_ipv4", False):
            return HTTPTransport(local_address="0.0.0.0")
        return getattr(config, "sync_transport", None)

    @staticmethod
    def extract_query_params(url: str) -> Dict[str, str]:
        from urllib.parse import parse_qsl, urlsplit
        return dict(parse_qsl(urlsplit(url).query))

    def get(self, url: str, params=None, headers=None, follow_redirects=None, timeout=None):
        _follow_redirects = follow_redirects if follow_redirects is not None else USE_CLIENT_DEFAULT
        params = params or {}
        params.update(self.extract_query_params(url))
        return self.client.get(
            url, params=params, headers=headers, follow_redirects=_follow_redirects,
            timeout=timeout if timeout is not None else USE_CLIENT_DEFAULT,
        )

    def _send_request(
        self, method: str, url: str, data=None, json=None, params=None, 
        headers=None, stream=False, timeout=None, files=None, content=None
    ):
        try:
            if timeout is None:
                timeout = getattr(self, "timeout", _DEFAULT_TIMEOUT)
                
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
    shared_session: Optional["ClientSession"] = None,
) -> Union[AsyncHTTPClient, HTTPClient]:
    params_key = ""
    handler_params = {}

    ## Parameter refinement and cache key generation
    if params is not None:
        # Dynamically determine which parameter keys to ignore based on the execution context
        ignore_keys = {"shared_session"}
        if not is_async:
            ignore_keys.add("disable_aiohttp_transport")

        handler_params = {k: v for k, v in params.items() if k not in ignore_keys}

        # Ensure deterministic cache key generation by sorting the parameters alphanumerically
        for key, value in sorted(handler_params.items()):
            params_key += f"{key}_{value}"
    else:
        # Apply default configuration when no explicit parameters are provided
        handler_params = {"timeout": _DEFAULT_TIMEOUT}

    ## Cache key assembly
    # Append appropriate prefixes and provider-specific suffixes based on the client type
    prefix = "async_httpx_client" if is_async else "httpx_client"
    suffix = f"_{llm_provider}" if (is_async and llm_provider) else ""
    cache_key = f"{prefix}_{params_key}{suffix}"

    ## Cache hit resolution
    if cache_key in _HTTPX_CLIENT_CACHE:
        return _HTTPX_CLIENT_CACHE[cache_key]

    ## Cache miss resolution (Instance creation)
    if is_async:
        # Inject the shared aiohttp session exclusively for asynchronous clients
        if shared_session is not None:
            handler_params["shared_session"] = shared_session
        client = AsyncHTTPClient(**handler_params)
    else:
        client = HTTPClient(**handler_params)

    ## State persistence
    _HTTPX_CLIENT_CACHE[cache_key] = client
    return client