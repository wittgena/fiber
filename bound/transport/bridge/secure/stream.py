# bound.transport.bridge.secure.stream
## @lineage: bound.bridge.transport.secure.stream
## @lineage: anchor.phase.ingress.stream.security
## @lineage: bound.ingress.stream.security
## @lineage: bound.transport.http.security
import ssl
from enum import Enum
import asyncio
import concurrent.futures
import inspect
import os
import socket
import sys
import time
from typing import Any, Callable, Dict, Optional, Tuple, Union
import certifi
import httpx
from aiohttp import TCPConnector

from bound.registry.model.config.resolver import config
from bound.exception import Timeout
from bound.registry.model.config.constants import AIOHTTP_SO_KEEPALIVE, AIOHTTP_TCP_KEEPCNT, AIOHTTP_TCP_KEEPIDLE, AIOHTTP_TCP_KEEPINTVL, DEFAULT_SSL_CIPHERS
from bound.transport.bridge.base import VerifyTypes
from watcher.plane.emitter import get_emitter

log = get_emitter("http.security")

_AIOHTTP_SUPPORTS_SOCKET_FACTORY = "socket_factory" in inspect.signature(TCPConnector.__init__).parameters
_STREAMING_ERROR_BODY_READ_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=50, thread_name_prefix="gate-streaming-error-body-read")
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

def _build_aiohttp_keepalive_socket_factory() -> (Optional[Callable[[Tuple[Any, ...]], socket.socket]]):
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