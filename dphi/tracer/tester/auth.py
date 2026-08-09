# dphi.tracer.tester.auth
## @lineage: phase.tracer.tester.auth
## @lineage: entry.tracer.tester.auth
## @lineage: receptor.surface.tracer.tester.auth
## @lineage: surface.tracer.tester.auth
## @lineage: surface.tester.auth
## @lineage: dphi.wasm.auth
## @lineage: dphi.tester.auth
from __future__ import annotations

import asyncio
import platform
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any

from urllib.parse import urlencode
from aiohttp import web
from authlib.common.security import generate_token
from authlib.jose import JsonWebKey, jwt
from authlib.jose.errors import JoseError
from authlib.oauth2.rfc7636 import create_s256_code_challenge
from httpx import AsyncClient, Client
from watcher.plane.emitter import get_emitter

log = get_emitter("tester.auth")

CLIENT_ID = "your_generic_client_id"
ISSUER = "https://auth.your-service.com"
JWKS_URL = f"{ISSUER}/.well-known/jwks.json"
API_ENDPOINT = "https://api.your-service.com/v1/resource"

DEFAULT_OAUTH_PORT = 8080
OAUTH_TIMEOUT_SECONDS = 300  # 5분
JWKS_CACHE_TTL_SECONDS = 3600  # 1시간

CONSENT_BANNER = """\
[Notice] This application requires access to your account.
By continuing, you agree to our Terms of Service and Privacy Policy.
https://your-service.com/terms/
"""
CONSENT_MARKER_FILENAME = ".app_consent_acknowledged"

def _get_consent_marker_path() -> Path:
    return Path.home() / ".your_app" / CONSENT_MARKER_FILENAME

def _has_acknowledged_consent() -> bool:
    return _get_consent_marker_path().exists()

def _mark_consent_acknowledged() -> None:
    marker_path = _get_consent_marker_path()
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.touch()

def require_consent_in_cli() -> bool:
    """CLI 환경에서 최초 1회 사용자 동의를 받습니다."""
    if _has_acknowledged_consent():
        return True

    log.info("\n" + "=" * 70)
    log.info(CONSENT_BANNER)
    log.info("=" * 70 + "\n")

    if not sys.stdin.isatty():
        raise RuntimeError("Non-interactive mode requires prior consent acknowledgment.")

    try:
        response = input("Do you want to continue? [y/N]: ").strip().lower()
        if response in ("y", "yes"):
            _mark_consent_acknowledged()
            return True
        return False
    except (EOFError, KeyboardInterrupt):
        log.info()
        return False

# -------------------------------------------------------------------------
# 3. JWKS 캐싱 및 JWT 서명 검증 (Stateless Auth Validation)
# -------------------------------------------------------------------------
class JWKSCache:
    """OAuth 제공자의 공개키(JWKS)를 스레드 안전하게 캐싱합니다."""
    def __init__(self) -> None:
        self._keys: dict[str, Any] = {}
        self._fetched_at: float = 0
        self._lock = threading.Lock()

    def get_key_set(self) -> Any:
        with self._lock:
            now = time.time()
            if not self._keys or (now - self._fetched_at) > JWKS_CACHE_TTL_SECONDS:
                self._fetch_jwks()
            return JsonWebKey.import_key_set(self._keys)

    def _fetch_jwks(self) -> None:
        try:
            with Client(timeout=10) as client:
                response = client.get(JWKS_URL)
                response.raise_for_status()
                self._keys = response.json()
                self._fetched_at = time.time()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch JWKS: {e}") from e

_jwks_cache = JWKSCache()

def extract_user_id_from_token(access_token: str) -> str | None:
    """JWT 토큰의 서명을 로컬에서 검증하고 유저 ID를 추출합니다."""
    try:
        key_set = _jwks_cache.get_key_set()
        claims = jwt.decode(access_token, key_set)
        claims.validate()
        
        # 제공자마다 claim 구조가 다름. 아래는 예시 ("sub"는 표준 식별자)
        return claims.get("sub") 
    except JoseError as e:
        log.warning(f"JWT signature verification failed: {e}")
        return None
    except Exception as e:
        log.warning(f"Failed to decode JWT: {e}")
        return None

# -------------------------------------------------------------------------
# 4. PKCE OAuth 2.0 로그인 및 로컬 콜백 서버 (Local Auth Flow)
# -------------------------------------------------------------------------
_HTML_SUCCESS = "<html><body><h1>Authorization Successful</h1><p>You can close this window</p><script>setTimeout(() => window.close(), 2000);</script></body></html>"
_HTML_ERROR = "<html><body><h1>Authorization Failed</h1><div class='error'>{error}</div></body></html>"

class CLIOAuthManager:
    def __init__(self, port: int = DEFAULT_OAUTH_PORT):
        self._oauth_port = port

    def _generate_pkce(self) -> tuple[str, str]:
        verifier = generate_token(43)
        challenge = create_s256_code_challenge(verifier)
        return verifier, challenge

    async def _exchange_code(self, code: str, redirect_uri: str, verifier: str) -> dict[str, Any]:
        async with AsyncClient() as client:
            response = await client.post(
                f"{ISSUER}/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "client_id": CLIENT_ID,
                    "code_verifier": verifier,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
            return response.json()

    async def login(self, open_browser: bool = True) -> dict[str, Any]:
        """로컬 웹서버를 띄우고 브라우저 로그인을 수행하여 토큰을 받아옵니다."""
        code_verifier, code_challenge = self._generate_pkce()
        state = generate_token(32)
        redirect_uri = f"http://localhost:{self._oauth_port}/auth/callback"
        
        params = {
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": redirect_uri,
            "scope": "openid profile email offline_access",
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "state": state,
        }
        auth_url = f"{ISSUER}/oauth/authorize?{urlencode(params)}"

        callback_future: asyncio.Future[dict[str, Any]] = asyncio.Future()
        app = web.Application()

        async def handle_callback(request: web.Request) -> web.Response:
            params = request.query
            if "error" in params:
                error_msg = params.get("error_description", params["error"])
                callback_future.set_exception(RuntimeError(error_msg))
                return web.Response(text=_HTML_ERROR.format(error=error_msg), content_type="text/html")

            if params.get("state") != state:
                callback_future.set_exception(RuntimeError("Invalid state - CSRF suspected"))
                return web.Response(text=_HTML_ERROR.format(error="Invalid state"), content_type="text/html")

            try:
                tokens = await self._exchange_code(params["code"], redirect_uri, code_verifier)
                callback_future.set_result(tokens)
                return web.Response(text=_HTML_SUCCESS, content_type="text/html")
            except Exception as e:
                callback_future.set_exception(e)
                return web.Response(text=_HTML_ERROR.format(error=str(e)), content_type="text/html", status=500)

        app.router.add_get("/auth/callback", handle_callback)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", self._oauth_port)

        try:
            await site.start()
            if open_browser:
                webbrowser.open(auth_url)
            else:
                log.info(f"Please open this URL in your browser:\n{auth_url}")

            tokens = await asyncio.wait_for(callback_future, timeout=OAUTH_TIMEOUT_SECONDS)
            return tokens
        finally:
            await runner.cleanup()

def transform_prompt_for_legacy_api(system_prompt: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not system_prompt:
        return messages

    prefix_content = f"Context (system prompt):\n{system_prompt}\n\n"
    normalized = list(messages)
    for item in normalized:
        if item.get("role") == "user":
            original_content = item.get("content", "")
            if isinstance(original_content, str):
                item["content"] = prefix_content + original_content
            elif isinstance(original_content, list):
                item["content"].insert(0, {"type": "text", "text": prefix_content})
            return normalized
            
    normalized.insert(0, {"role": "user", "content": prefix_content})
    return normalized

async def create_authenticated_client() -> AsyncClient:
    if not require_consent_in_cli():
        raise SystemExit("Consent required to proceed.")

    auth_manager = CLIOAuthManager()
    tokens = await auth_manager.login()
    access_token = tokens["access_token"]
    
    user_id = extract_user_id_from_token(access_token)
    log.info(f"Authenticated successfully as user: {user_id}")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "User-Agent": f"MyApp-CLI ({platform.system()}; {platform.machine()})",
    }
    return AsyncClient(headers=headers, base_url=API_ENDPOINT)