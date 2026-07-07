# bound.transport.channel.aiohttp
## @lineage: bound.channel.aiohttp
import asyncio
import contextlib
import os
import ssl
import typing
import urllib.request
from typing import Any, Callable, Dict, Optional, Union
import aiohttp
import aiohttp.client_exceptions
import aiohttp.http_exceptions
import httpx
from aiohttp.client import ClientResponse, ClientSession
from watcher.plane.emitter import get_emitter

log = get_emitter("aiohttp.transport")

def _str_to_bool(val: Union[str, bool]) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("true", "1", "yes", "t", "y")

AIOHTTP_EXC_MAP: Dict = {
    # Order matters here, most specific exception first Timeout related exceptions
    asyncio.TimeoutError: httpx.TimeoutException,
    aiohttp.ServerTimeoutError: httpx.TimeoutException,
    aiohttp.ConnectionTimeoutError: httpx.ConnectTimeout,
    aiohttp.SocketTimeoutError: httpx.ReadTimeout,
    # Proxy related exceptions
    aiohttp.ClientProxyConnectionError: httpx.ProxyError,
    # SSL related exceptions
    aiohttp.ClientConnectorCertificateError: httpx.ProtocolError,
    aiohttp.ClientSSLError: httpx.ProtocolError,
    aiohttp.ServerFingerprintMismatch: httpx.ProtocolError,
    # Network related exceptions
    aiohttp.ClientConnectorError: httpx.ConnectError,
    aiohttp.ClientOSError: httpx.ConnectError,
    aiohttp.ClientPayloadError: httpx.ReadError,
    # Connection disconnection exceptions
    aiohttp.ServerDisconnectedError: httpx.ReadError,
    # Response related exceptions
    aiohttp.ClientConnectionError: httpx.NetworkError,
    aiohttp.ClientPayloadError: httpx.ReadError,
    aiohttp.ContentTypeError: httpx.ReadError,
    aiohttp.TooManyRedirects: httpx.TooManyRedirects,
    # URL related exceptions
    aiohttp.InvalidURL: httpx.InvalidURL,
    # Base exceptions
    aiohttp.ClientError: httpx.RequestError,
}

# Add client_exceptions module exceptions
try:
    import aiohttp.client_exceptions
    AIOHTTP_EXC_MAP[aiohttp.client_exceptions.ClientPayloadError] = httpx.ReadError
except ImportError:
    pass


@contextlib.contextmanager
def map_aiohttp_exceptions() -> typing.Iterator[None]:
    try:
        yield
    except Exception as exc:
        mapped_exc = None

        for from_exc, to_exc in AIOHTTP_EXC_MAP.items():
            if not isinstance(exc, from_exc):  # type: ignore
                continue
            if mapped_exc is None or issubclass(to_exc, mapped_exc):
                mapped_exc = to_exc

        if mapped_exc is None:  # pragma: no cover
            raise

        message = str(exc)
        raise mapped_exc(message) from exc


class AiohttpResponseStream(httpx.AsyncByteStream):
    CHUNK_SIZE = 1024 * 16

    def __init__(self, aiohttp_response: ClientResponse) -> None:
        self._aiohttp_response = aiohttp_response

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        try:
            async for chunk in self._aiohttp_response.content.iter_chunked(self.CHUNK_SIZE):
                yield chunk
        except (
            aiohttp.ClientPayloadError,
            aiohttp.client_exceptions.ClientPayloadError,
        ) as e:
            log.debug(f"Transfer incomplete, but continuing: {e}")
            return
        except RuntimeError as e:
            if "Connection closed" in str(e):
                log.debug("Upstream closed streaming connection; ending iterator gracefully")
                return
            raise
        except aiohttp.http_exceptions.TransferEncodingError as e:
            log.debug(f"Transfer encoding error, but continuing: {e}")
            return
        except Exception:
            with map_aiohttp_exceptions():
                raise

    async def aclose(self) -> None:
        with map_aiohttp_exceptions():
            await self._aiohttp_response.__aexit__(None, None, None)


class AiohttpTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        client: Union[ClientSession, Callable[[], ClientSession]],
        owns_session: bool = True,
    ) -> None:
        self.client = client
        self._owns_session = owns_session
        self.proxy_cache: Dict[str, Optional[str]] = {}

    async def aclose(self) -> None:
        if self._owns_session and isinstance(self.client, ClientSession):
            await self.client.close()


class AiohttpTransport(AiohttpTransport):
    """
    Pure AiohttpTransport wrapper to handle %-encodings in URLs
    and event loop lifecycle issues in CI/CD environments
    """

    def __init__(
        self,
        client: Union[ClientSession, Callable[[], ClientSession]],
        ssl_verify: Optional[Union[bool, ssl.SSLContext]] = None,
        owns_session: bool = True,
        # [개선됨] 전역 상태 참조를 없애고, 파라미터로 의존성을 주입받도록 변경
        disable_trust_env: Optional[bool] = None,
    ):
        self.client = client
        self._ssl_verify = ssl_verify
        
        # [개선됨] litellm 전역 객체 대신 인자 또는 환경변수로 로컬 상태 초기화
        if disable_trust_env is not None:
            self._disable_trust_env = disable_trust_env
        else:
            self._disable_trust_env = _str_to_bool(os.getenv("DISABLE_AIOHTTP_TRUST_ENV", "False"))
            
        super().__init__(client=client, owns_session=owns_session)
        if callable(client):
            self._client_factory = client

    def _get_valid_client_session(self) -> ClientSession:
        from aiohttp.client import ClientSession

        if not isinstance(self.client, ClientSession):
            if hasattr(self, "_client_factory") and callable(self._client_factory):
                self.client = self._client_factory()
            else:
                self.client = ClientSession()

        if self.client.closed:
            log.debug("Session is closed, creating new session")
            if hasattr(self, "_client_factory") and callable(self._client_factory):
                self.client = self._client_factory()
            else:
                self.client = ClientSession()
            return self.client

        try:
            session_loop = getattr(self.client, "_loop", None)
            current_loop = asyncio.get_running_loop()

            if (
                session_loop is None
                or session_loop != current_loop
                or session_loop.is_closed()
            ):
                old_session = self.client
                try:
                    if not old_session.closed:
                        try:
                            asyncio.create_task(old_session.close())
                        except RuntimeError:
                            log.debug("Old session from different loop, relying on GC")
                except Exception as e:
                    log.debug(f"Error closing old session: {e}")

                if hasattr(self, "_client_factory") and callable(self._client_factory):
                    self.client = self._client_factory()
                else:
                    self.client = ClientSession()

        except (RuntimeError, AttributeError):
            if hasattr(self, "_client_factory") and callable(self._client_factory):
                self.client = self._client_factory()
            else:
                self.client = ClientSession()

        return self.client

    async def _make_aiohttp_request(
        self,
        client_session: ClientSession,
        request: httpx.Request,
        timeout: dict,
        proxy: Optional[str],
        sni_hostname: Optional[str],
        ssl_verify: Optional[Union[bool, ssl.SSLContext]] = None,
    ) -> ClientResponse:
        from aiohttp import ClientTimeout
        from yarl import URL as YarlURL

        try:
            data = request.content or None
        except httpx.RequestNotRead:
            data = request.stream  # type: ignore
            request.headers.pop("transfer-encoding", None)

        request_kwargs: Dict[str, Any] = {
            "method": request.method,
            "url": YarlURL(str(request.url), encoded=True),
            "headers": request.headers,
            "data": data,
            "allow_redirects": False,
            "auto_decompress": False,
            "timeout": ClientTimeout(
                sock_connect=timeout.get("connect"),
                sock_read=timeout.get("read"),
                connect=timeout.get("pool"),
            ),
            "proxy": proxy,
            "server_hostname": sni_hostname,
        }
        if ssl_verify is not None:
            request_kwargs["ssl"] = ssl_verify

        response = await client_session.request(**request_kwargs).__aenter__()
        return response

    async def handle_async_request(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        timeout = request.extensions.get("timeout", {})
        sni_hostname = request.extensions.get("sni_hostname")

        client_session = self._get_valid_client_session()
        proxy = await self._get_proxy_settings(request)
        ssl_config = self._ssl_verify

        try:
            with map_aiohttp_exceptions():
                response = await self._make_aiohttp_request(
                    client_session=client_session,
                    request=request,
                    timeout=timeout,
                    proxy=proxy,
                    sni_hostname=sni_hostname,
                    ssl_verify=ssl_config,
                )
        except RuntimeError as e:
            if "Session is closed" in str(e):
                log.debug(f"Session closed during request, retrying with new session: {e}")
                if hasattr(self, "_client_factory") and callable(self._client_factory):
                    self.client = self._client_factory()
                else:
                    self.client = ClientSession()
                client_session = self.client

                with map_aiohttp_exceptions():
                    response = await self._make_aiohttp_request(
                        client_session=client_session,
                        request=request,
                        timeout=timeout,
                        proxy=proxy,
                        sni_hostname=sni_hostname,
                        ssl_verify=ssl_config,
                    )
            else:
                raise

        return httpx.Response(
            status_code=response.status,
            headers=response.headers,
            stream=AiohttpResponseStream(response),
            request=request,
        )

    async def _get_proxy_settings(self, request: httpx.Request):
        proxy = None
        # [개선됨] litellm.disable_aiohttp_trust_env 전역 변수 대신 주입받은 로컬 상태 사용
        if not self._disable_trust_env:
            try:
                proxy = self._proxy_from_env(request.url)
            except Exception as e:
                log.debug(f"Error reading proxy env: {e}")

        return proxy

    def _proxy_from_env(self, url: httpx.URL) -> typing.Optional[str]:
        proxy_cache_key = url.host

        if proxy_cache_key in self.proxy_cache:
            return self.proxy_cache[proxy_cache_key]

        proxies = urllib.request.getproxies()
        if urllib.request.proxy_bypass(url.host):
            proxy_url = None
        else:
            proxy = proxies.get(url.scheme) or proxies.get("all")
            if proxy and "://" not in proxy:
                proxy = f"http://{proxy}"
            proxy_url = proxy

        self.proxy_cache[proxy_cache_key] = proxy_url
        return proxy_url