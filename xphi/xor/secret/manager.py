# xphi.xor.secret.manager
import ast
import os
import time
import traceback
import httpx
from typing import Optional, Union, Dict, Tuple

from bound.bridge.transport.client import HTTPClient
from anchor.registry.model.config.resolver import config
from xphi.xor.secret.handler.client import get_secret_from_vendor

from watcher.plane.emitter import get_emitter

log = get_emitter("secret.manager")

class OIDCTTLCache:
    def __init__(self):
        self._cache: Dict[str, Tuple[str, float]] = {}

    def get(self, key: str) -> Optional[str]:
        if key in self._cache:
            value, expiry = self._cache[key]
            if time.time() < expiry:
                return value
            else:
                del self._cache[key]
        return None

    def set(self, key: str, value: str, ttl_seconds: int):
        self._cache[key] = (value, time.time() + ttl_seconds)

class SecretManager:
    def __init__(self):
        self.oidc_cache = OIDCTTLCache()
        self.http_timeout = httpx.Timeout(timeout=600.0, connect=5.0)

    def get_secret(
        self, 
        secret_name: str, 
        default_value: Optional[Union[str, bool]] = None
    ) -> Optional[Union[str, bool]]:
        """메인 엔트리 포인트: 요청에 따라 OIDC, Manager, Env로 분기"""
        if secret_name.startswith("os.environ/"):
            secret_name = secret_name.replace("os.environ/", "")

        try:
            if secret_name.startswith("oidc/"):
                return self._resolve_oidc(secret_name)

            if self._should_read_secret_from_secret_manager():
                return self._fetch_from_manager(secret_name)
            else:
                return self._fetch_from_env(secret_name)
        except Exception as e:
            if default_value is not None:
                return default_value
            raise e

    def _resolve_oidc(self, secret_name: str) -> str:
        cached_token = self.oidc_cache.get(secret_name)
        if cached_token:
            return cached_token

        secret_name_split = secret_name.replace("oidc/", "")
        oidc_provider, oidc_aud = secret_name_split.split("/", 1)
        oidc_aud = "/".join(secret_name_split.split("/")[1:])

        token = None
        ttl = 3600

        # OIDC Provider 분기 처리 (기존 로직 유지, 의존성만 제거)
        if oidc_provider == "google":
            with HTTPClient(timeout=self.http_timeout) as client:
                resp = client.get(
                    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity",
                    params={"audience": oidc_aud},
                    headers={"Metadata-Flavor": "Google"},
                )
                if resp.status_code != 200:
                    raise ValueError("Google OIDC provider failed")
                token = resp.text
                ttl = 3540 # 3600 - 60

        elif oidc_provider in ("circleci", "circleci_v2"):
            env_key = "CIRCLE_OIDC_TOKEN" if oidc_provider == "circleci" else "CIRCLE_OIDC_TOKEN_V2"
            token = os.getenv(env_key)
            if not token:
                raise ValueError(f"{env_key} not found in environment")

        elif oidc_provider == "github":
            req_url = os.getenv("ACTIONS_ID_TOKEN_REQUEST_URL")
            req_token = os.getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
            if not req_url or not req_token:
                raise ValueError("ACTIONS_ID_TOKEN_REQUEST_URL or TOKEN not found")
            with HTTPClient(timeout=self.http_timeout) as client:
                resp = client.get(
                    req_url,
                    params={"audience": oidc_aud},
                    headers={
                        "Authorization": f"Bearer {req_token}",
                        "Accept": "application/json; api-version=2.0",
                    },
                )
                if resp.status_code != 200:
                    raise ValueError("Github OIDC provider failed")
                token = resp.json().get("value")
                ttl = 295 # 300 - 5

        elif oidc_provider == "azure":
            azure_federated_token_file = os.getenv("AZURE_FEDERATED_TOKEN_FILE")
            if azure_federated_token_file:
                with open(azure_federated_token_file, "r") as f:
                    token = f.read()
            else:
                # litellm 의존성 제거: azure-identity 직접 사용
                try:
                    from azure.identity import DefaultAzureCredential
                    token = DefaultAzureCredential().get_token(oidc_aud).token
                except Exception as e:
                    raise ValueError(f"Azure OIDC provider failed: {str(e)}")

        elif oidc_provider == "file":
            safe_path = self._resolve_oidc_file_path(oidc_aud)
            with open(safe_path, "r") as f:
                token = f.read()

        elif oidc_provider == "env":
            token = os.getenv(oidc_aud)
            if not token:
                raise ValueError(f"Environment variable {oidc_aud} not found")

        elif oidc_provider == "env_path":
            path = os.getenv(oidc_aud)
            if not path:
                raise ValueError(f"Environment variable {oidc_aud} not found")
            with open(path, "r") as f:
                token = f.read()
        else:
            raise ValueError("Unsupported OIDC provider")

        self.oidc_cache.set(secret_name, token, ttl)
        return token

    def _fetch_from_manager(self, secret_name: str):
        try:
            client = config.secret_manager_client
            key_manager = "local"
            
            if config._key_management_system:
                key_manager = config._key_management_system.value

            settings = config._key_management_settings
            if settings and settings.hosted_keys is not None:
                if secret_name not in settings.hosted_keys:
                    key_manager = "local"

            return get_secret_from_manager(
                client=client,
                key_manager=key_manager,
                secret_name=secret_name,
                key_management_settings=settings,
            )
        except Exception as e:
            log.error(f"Defaulting to os.environ value for key={secret_name}. Error - {str(e)}.\n{traceback.format_exc()}")
            return self._fetch_from_env(secret_name)

    def _fetch_from_env(self, secret_name: str):
        secret = os.environ.get(secret_name)
        if secret is None:
            return None
            
        lower_val = secret.strip().lower()
        if lower_val in {"true", "false"}:
            return lower_val == "true"
            
        try:
            parsed = ast.literal_eval(secret)
            return parsed if isinstance(parsed, bool) else secret
        except Exception:
            return secret

    def _should_read_secret_from_secret_manager(self) -> bool:
        if config.secret_manager_client is not None and config._key_management_settings is not None:
            mode = config._key_management_settings.access_mode
            return mode in ("read_only", "read_and_write")
        return False

    def _resolve_oidc_file_path(self, requested_path: str) -> str:
        override = os.getenv("GATE_OIDC_ALLOWED_CREDENTIAL_DIRS")
        raw_dirs = [d.strip() for d in override.split(",") if d.strip()] if override else ["/var/run/secrets", "/run/secrets"]
        allowed_dirs = [os.path.realpath(d) for d in raw_dirs]

        if not os.path.isabs(requested_path):
            raise ValueError("oidc/file path must be absolute.")
            
        resolved = os.path.realpath(requested_path)
        for allowed in allowed_dirs:
            try:
                if os.path.commonpath([resolved, allowed]) == allowed:
                    return resolved
            except ValueError:
                continue
        raise ValueError("oidc/file path is outside the allowed credential directories.")

# 3. 외부 시그니처 유지를 위한 싱글톤 및 래퍼 함수
_manager = SecretManager()

def get_secret(secret_name: str, default_value: Optional[Union[str, bool]] = None) -> Optional[Union[str, bool]]:
    return _manager.get_secret(secret_name, default_value)

def get_secret_str(secret_name: str, default_value: Optional[Union[str, bool]] = None) -> Optional[str]:
    val = get_secret(secret_name, default_value)
    return val if isinstance(val, str) else None

def get_secret_bool(secret_name: str, default_value: Optional[bool] = None) -> Optional[bool]:
    val = get_secret(secret_name, default_value)
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        lower_val = val.strip().lower()
        if lower_val == "true": return True
        if lower_val == "false": return False
    return default_value