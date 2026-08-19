# bound.client.http
import hashlib
import json
import os
import re
import socket
import ssl
import time
from ipaddress import ip_address, ip_network
from typing import Any, Dict, Optional, Tuple, Union
from urllib.parse import urlparse, urlunparse

import certifi
import httpx

from bound.eco.agent.adapter.constants import (
    AIOHTTP_CONNECTOR_LIMIT,
    AIOHTTP_KEEPALIVE_TIMEOUT,
    COMPLETION_HTTP_FALLBACK_SECONDS,
    HTTP_HANDLER_CONNECT_TIMEOUT_SECONDS,
)
from arch.model.config import config
from kernel.dphi.adapter.sign import NodeSigner
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("transport.client")

_DEFAULT_TIMEOUT = httpx.Timeout(
    timeout=COMPLETION_HTTP_FALLBACK_SECONDS,
    connect=HTTP_HANDLER_CONNECT_TIMEOUT_SECONDS,
)
_HTTPX_CLIENT_CACHE: Dict[str, Union[httpx.Client, httpx.AsyncClient]] = {}


def _get_ssl_context() -> Union[bool, str, ssl.SSLContext]:
    """환경변수 및 설정에 기반하여 SSL 컨텍스트(또는 Verify 설정)를 반환합니다."""
    ssl_verify = os.getenv("SSL_VERIFY", getattr(config, "ssl_verify", True))
    if str(ssl_verify).lower() in ("false", "0", "no"):
        return False
        
    cert_file = os.getenv("SSL_CERT_FILE")
    if cert_file and os.path.exists(cert_file):
        return cert_file
        
    return certifi.where()


def _get_httpx_limits() -> httpx.Limits:
    return httpx.Limits(
        max_keepalive_connections=AIOHTTP_CONNECTOR_LIMIT if AIOHTTP_CONNECTOR_LIMIT > 0 else 20,
        max_connections=AIOHTTP_CONNECTOR_LIMIT if AIOHTTP_CONNECTOR_LIMIT > 0 else 100,
        keepalive_expiry=AIOHTTP_KEEPALIVE_TIMEOUT if AIOHTTP_KEEPALIVE_TIMEOUT > 0 else 5.0,
    )


# ==========================================
# 2. Client Factory (Native Client 반환)
# ==========================================

def get_client(
    is_async: bool, params: Optional[dict] = None, **kwargs
) -> Union[httpx.Client, httpx.AsyncClient]:
    params_key = str(sorted((params or {}).items()))
    prefix = "async" if is_async else "sync"
    cache_key = f"{prefix}_{params_key}"

    if cache_key in _HTTPX_CLIENT_CACHE:
        return _HTTPX_CLIENT_CACHE[cache_key]

    headers = {"User-Agent": os.environ.get("DPHI_USER_AGENT", "gate/1.0")}
    verify = _get_ssl_context()
    limits = _get_httpx_limits()
    
    if is_async:
        client = httpx.AsyncClient(
            timeout=_DEFAULT_TIMEOUT, headers=headers, verify=verify, limits=limits, follow_redirects=True
        )
    else:
        client = httpx.Client(
            timeout=_DEFAULT_TIMEOUT, headers=headers, verify=verify, limits=limits, follow_redirects=True
        )

    _HTTPX_CLIENT_CACHE[cache_key] = client
    return client


def mask_sensitive_info(text: str) -> str:
    """에러 메시지나 URL에 포함된 API 키를 마스킹합니다."""
    if not isinstance(text, str):
        return text
    return re.sub(r"([?&](?:key|api_key)=)[^&]+", r"\1[REDACTED_API_KEY]", text)


# ==========================================
# 3. SSRF Protection & URL Validation
# ==========================================

class SSRFError(ValueError):
    """Raised when a URL targets a blocked network."""
    pass

class SafeHttpClient:
    """SSRF 방지 및 URL 유효성 검사를 수행하는 래퍼. ext.vision 등에서 사용됩니다."""

    _CLOUD_METADATA_EXCEPTIONS = [ip_network("168.63.129.16/32")]
    _ALLOWED_SCHEMES = {"http", "https"}

    def __init__(self, client: Union[httpx.Client, httpx.AsyncClient]):
        self._client = client

    def _is_blocked_ip(self, addr: str) -> bool:
        try:
            ip = ip_address(addr)
        except ValueError:
            return True
            
        if ip.version == 6 and getattr(ip, "ipv4_mapped", None):
            ip = ip.ipv4_mapped
            
        if not ip.is_global or ip.is_multicast:
            return True
            
        return any(ip in net for net in self._CLOUD_METADATA_EXCEPTIONS)

    def _validate_url(self, url: str) -> Tuple[str, str]:
        parsed = urlparse(url)
        if parsed.scheme not in self._ALLOWED_SCHEMES:
            raise SSRFError(f"URL scheme '{parsed.scheme}' is not allowed")

        hostname = parsed.hostname
        if not hostname:
            raise SSRFError("URL has no hostname")

        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        host_header = f"[{hostname}]:{port}" if ":" in hostname else f"{hostname}:{port}"

        try:
            addrinfo = socket.getaddrinfo(hostname, port, proto=socket.IPPROTO_TCP)
        except socket.gaierror as e:
            raise SSRFError(f"DNS resolution failed for '{hostname}': {e}")

        allowed_hosts = set(getattr(config, "user_url_allowed_hosts", []) or [])
        if hostname.lower() not in allowed_hosts:
            resolved_ip = addrinfo[0][4][0]
            if self._is_blocked_ip(resolved_ip):
                raise SSRFError(f"URL targets a blocked address ({resolved_ip}).")

            # DNS Rebinding 방지를 위해 도메인 대신 실제 IP로 요청 URL 재작성
            is_ipv6 = addrinfo[0][0] == socket.AF_INET6
            ip_host = f"[{resolved_ip}]" if is_ipv6 else resolved_ip
            netloc = f"{ip_host}:{port}" if parsed.port else ip_host
            url = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, ""))

        return url, host_header

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        if not getattr(config, "user_url_validation", True):
            return self._client.get(url, **kwargs)

        validated_url, host_header = self._validate_url(url)
        headers = kwargs.pop("headers", {})
        headers["Host"] = host_header

        return self._client.get(validated_url, headers=headers, **kwargs)

    async def async_get(self, url: str, **kwargs: Any) -> httpx.Response:
        if not getattr(config, "user_url_validation", True):
            return await self._client.get(url, **kwargs)

        validated_url, host_header = self._validate_url(url)
        headers = kwargs.pop("headers", {})
        headers["Host"] = host_header
        return await self._client.get(validated_url, headers=headers, **kwargs)


# ==========================================
# 4. First-Party Attestation Client
# ==========================================

class ProofVerificationError(ValueError):
    pass

class ReplayAttackError(ValueError):
    pass

class VerifiedHttpClient:
    """
    내가 구축한 서버(또는 신뢰할 수 있는 노드)에서 
    AttestationMiddleware를 통해 주입한 X-Dphi-* 헤더의 서명을 검증하는 클라이언트입니다.
    """
    def __init__(self, client: Union[httpx.Client, httpx.AsyncClient], max_age_seconds: int = 60):
        self._client = client
        self._max_age_seconds = max_age_seconds
        # 검증에 사용할 싱글톤 Signer (외부 시스템인 경우 PyNaCl의 VerifyKey를 직접 주입받을 수 있도록 수정 가능)
        self._signer = NodeSigner.get_instance()

    def _verify_header_proof(self, response: httpx.Response, request_url: str) -> None:
        """응답 헤더의 암호학적 서명을 검증합니다."""
        signature = response.headers.get("X-Dphi-Signature")
        timestamp = response.headers.get("X-Dphi-Timestamp")
        signer_pubkey = response.headers.get("X-Dphi-Signer")
        
        if not signature or not timestamp:
            raise ProofVerificationError("응답 헤더에 증명서(X-Dphi-Signature/Timestamp)가 누락되었습니다.")

        # 1. Replay Attack 검증
        try:
            ts_int = int(timestamp)
        except ValueError:
            raise ProofVerificationError("유효하지 않은 타임스탬프 포맷입니다.")
            
        if time.time() - ts_int > self._max_age_seconds:
            raise ReplayAttackError(f"응답이 만료되었습니다. (Age: {time.time() - ts_int:.1f}s)")
            
        # 2. Payload 해싱 (서버 미들웨어와 동일한 방식)
        body_hash = hashlib.sha256(response.content).hexdigest()
        parsed_url = urlparse(request_url)
        
        signature_payload = {
            "path": parsed_url.path,
            "timestamp": ts_int,
            "body_hash": body_hash
        }
        
        # 3. Canonicalization
        canonical_bytes = StateAdapter.to_canonical_bytes(signature_payload)
        
        # 4. 서명 검증 (NodeSigner.verify_signature 활용)
        is_valid = self._signer.verify_signature(
            canonical_bytes=canonical_bytes,
            signature_hex=signature,
            pubkey_hex=signer_pubkey
        )
        
        if not is_valid:
            log.critical(f"[VerifiedHttpClient] 🚨 Signature mismatch! URL: {request_url}")
            raise ProofVerificationError("서명 검증 실패: 데이터가 변조되었거나 잘못된 서명자입니다.")
            
        log.debug(f"[VerifiedHttpClient] ✅ Proof verified for {parsed_url.path}")

    def get_verified(self, url: str, **kwargs: Any) -> httpx.Response:
        """동기식 검증 GET 요청"""
        response = self._client.get(url, **kwargs)
        response.raise_for_status()
        self._verify_header_proof(response, url)
        return response

    async def async_get_verified(self, url: str, **kwargs: Any) -> httpx.Response:
        """비동기식 검증 GET 요청"""
        response = await self._client.get(url, **kwargs)
        response.raise_for_status()
        self._verify_header_proof(response, url)
        return response