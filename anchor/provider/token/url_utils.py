# anchor.provider.token.url_utils
## @lineage: anchor.provider.model.token.url_utils
## @lineage: anchor.model.token.url_utils
## @lineage: anchor.surface.model.token.url_utils
import socket
from ipaddress import ip_address, ip_network
from typing import Any, List, Set, Tuple
from urllib.parse import urlparse, urlunparse
import httpx

from bound.surface.legacy.config.resolver import config

class SSRFError(ValueError):
    """Raised when a URL targets a blocked network."""
    pass

class SafeHttpClient:
    """SSRF 방지 및 URL 유효성 검사를 자동으로 수행하는 HTTP 클라이언트 래퍼(Wrapper)"""

    _CLOUD_METADATA_EXCEPTIONS = [ip_network("168.63.129.16/32")]
    _ALLOWED_SCHEMES = {"http", "https"}
    _MAX_REDIRECTS = 10

    def __init__(self, client: Any):
        """
        초기화 시 httpx.Client 등 실제 HTTP 클라이언트 객체를 주입받습니다.
        """
        self._client = client

    def _is_blocked_ip(self, addr: str) -> bool:
        try:
            ip = ip_address(addr)
        except ValueError:
            return True
        
        if ip.version == 6 and hasattr(ip, "ipv4_mapped") and ip.ipv4_mapped:
            ip = ip.ipv4_mapped
            
        if not ip.is_global or ip.is_multicast:
            return True
            
        return any(ip in net for net in self._CLOUD_METADATA_EXCEPTIONS)

    def _is_host_allowlisted(self, hostname: str, effective_port: int) -> bool:
        configured: List[str] = config.user_url_allowed_hosts or []
        if not configured:
            return False
            
        normalized_host = hostname.lower().rstrip(".")
        host_repr = f"[{normalized_host}]" if ":" in normalized_host else normalized_host
        candidates: Set[str] = {host_repr, f"{host_repr}:{effective_port}"}
        allowlist: Set[str] = {entry.lower().rstrip(".") for entry in configured if entry}
        
        return bool(candidates & allowlist)

    def _validate_url(self, url: str) -> Tuple[str, str]:
        parsed = urlparse(url)
        if parsed.scheme not in self._ALLOWED_SCHEMES:
            raise SSRFError(f"URL scheme '{parsed.scheme}' is not allowed")

        hostname = parsed.hostname
        if not hostname:
            raise SSRFError("URL has no hostname")

        default_port = 443 if parsed.scheme == "https" else 80
        effective_port = parsed.port if parsed.port is not None else default_port
        
        bracketed_host = f"[{hostname}]" if ":" in hostname else hostname
        host_header = bracketed_host if effective_port == default_port else f"{bracketed_host}:{effective_port}"

        is_allowlisted = self._is_host_allowlisted(hostname, effective_port)

        try:
            addrinfo = socket.getaddrinfo(hostname, effective_port, proto=socket.IPPROTO_TCP)
        except socket.gaierror as e:
            raise SSRFError(f"DNS resolution failed for '{hostname}': {e}")

        if not addrinfo:
            raise SSRFError(f"No addresses found for '{hostname}'")

        if not is_allowlisted:
            for _, _, _, _, sockaddr in addrinfo:
                resolved_ip = sockaddr[0]
                if not isinstance(resolved_ip, str):
                    raise SSRFError(f"getaddrinfo returned non-string host: {resolved_ip!r}")
                if self._is_blocked_ip(resolved_ip):
                    raise SSRFError(f"URL targets a blocked address ({resolved_ip}).")

        if parsed.scheme == "https" and getattr(config, "ssl_verify", True) is not False:
            return url, host_header

        validated_ip = addrinfo[0][4][0]
        is_ipv6 = addrinfo[0][0] == socket.AF_INET6
        ip_host = f"[{validated_ip}]" if is_ipv6 else validated_ip

        new_netloc = f"{ip_host}:{parsed.port}" if parsed.port is not None else ip_host
        rewritten = urlunparse((parsed.scheme, new_netloc, parsed.path, parsed.params, parsed.query, ""))
        
        return rewritten, host_header

    def _extract_redirect_url(self, response: Any, request_url: str) -> str:
        location = response.headers.get("location")
        if not isinstance(location, str) or not location:
            raise SSRFError("Redirect response has no Location header")
        return str(httpx.URL(request_url).join(location))

    def get(self, url: str, **kwargs: Any) -> Any:
        """안전한 동기식 GET 요청을 수행합니다."""
        if not getattr(config, "user_url_validation", True):
            kwargs.setdefault("follow_redirects", True)
            return self._client.get(url, **kwargs)
            
        kwargs.pop("follow_redirects", None)
        caller_headers = kwargs.pop("headers", {})
        
        for _ in range(self._MAX_REDIRECTS):
            validated_url, original_host = self._validate_url(url)
            response = self._client.get(
                validated_url,
                headers={**caller_headers, "Host": original_host},
                follow_redirects=False,
                **kwargs,
            )
            if not response.is_redirect:
                return response
            url = self._extract_redirect_url(response, url)
            
        raise SSRFError("Too many redirects")

    async def async_get(self, url: str, **kwargs: Any) -> Any:
        """안전한 비동기식 GET 요청을 수행합니다."""
        if not getattr(config, "user_url_validation", True):
            kwargs.setdefault("follow_redirects", True)
            return await self._client.get(url, **kwargs)
            
        kwargs.pop("follow_redirects", None)
        caller_headers = kwargs.pop("headers", {})
        
        for _ in range(self._MAX_REDIRECTS):
            validated_url, original_host = self._validate_url(url)
            response = await self._client.get(
                validated_url,
                headers={**caller_headers, "Host": original_host},
                follow_redirects=False,
                **kwargs,
            )
            if not response.is_redirect:
                return response
            url = self._extract_redirect_url(response, url)
            
        raise SSRFError("Too many redirects")