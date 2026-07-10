# bound.transport.sync
## @lineage: bound.surface.bridge.transport.sync
## @lineage: bound.surface.legacy.transport.sync
## @lineage: bound.transport.http.sync
import os
import certifi
import httpx
from typing import Any, Dict, Optional, Union

from httpx import USE_CLIENT_DEFAULT, HTTPTransport
from httpx._types import RequestFiles

from anchor.registry.model.config.resolver import config
from bound.surface.exception import Timeout
from bound.ingress.stream.security import (
    MaskedHTTPStatusError,
    _safe_read_response,
    mask_sensitive_info,
    get_ssl_configuration,
)
from bound.transport.base import (
    _DEFAULT_TIMEOUT,
    get_default_headers,
    _prepare_request_data_and_content,
    _safe_get_response_text,
    _STREAMING_ERROR_BODY_READ_TIMEOUT_SECONDS,
    _HTTPX_CLIENT_CACHE,
)
from watcher.plane.emitter import get_emitter

log = get_emitter("http.sync")

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