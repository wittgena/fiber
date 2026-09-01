# fiber.llm.model.provider.secret
from __future__ import annotations

import ast
import os
import time
import traceback
import hashlib
from base64 import b64encode
from typing import Optional, Union, Dict, Tuple, List, Any

import httpx
from pydantic import BaseModel, SecretStr
from cryptography.fernet import Fernet

from xphi.arch.model.config import config
from xphi.xor.secret.client import get_secret_from_vendor
from xphi.xor.secret.cipher import Cipher
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("provider.secret")


# =====================================================================
# 1. Credential Models & Accessor (Merged from Validator)
# =====================================================================
class CredentialBase(BaseModel):
    credential_name: str
    credential_info: dict

class CredentialItem(CredentialBase):
    credential_values: dict

class CredentialAccessor:
    """Safe accessor and manager for dynamically loaded credentials."""
    
    @staticmethod
    def get_credential_values(credential_name: str) -> dict:
        if not getattr(config, "credential_list", None):
            return {}
        for credential in config.credential_list:
            if credential.credential_name == credential_name:
                return credential.credential_values.copy()
        return {}

    @staticmethod
    def upsert_credentials(credentials: List[CredentialItem]):
        if not hasattr(config, "credential_list"):
            config.credential_list = []
            
        credential_names = [cred.credential_name for cred in config.credential_list]
        for credential in credentials:
            if credential.credential_name in credential_names:
                for i, existing_cred in enumerate(config.credential_list):
                    if existing_cred.credential_name == credential.credential_name:
                        config.credential_list[i] = credential
                        break
            else:
                config.credential_list.append(credential)


# =====================================================================
# 2. Pydantic Secret Validators (Merged from Validator)
# =====================================================================
def serialize_secret(v: SecretStr | None, info: Any):
    """Pydantic field serializer for secret values."""
    if v is None:
        return None

    if info.context and info.context.get("cipher"):
        cipher: Cipher = info.context.get("cipher")
        return cipher.encrypt(v)

    if info.context and info.context.get("expose_secrets"):
        return v.get_secret_value()

    return v


def validate_secret(v: str | SecretStr | None, info: Any) -> SecretStr | None:
    """Pydantic field validator & decryptor for secret values."""
    if v is None:
        return None

    if isinstance(v, SecretStr):
        secret_value = v.get_secret_value()
    else:
        secret_value = v

    if not secret_value or not secret_value.strip() or secret_value == "**********":
        return None

    if info.context and info.context.get("cipher"):
        cipher: Cipher = info.context.get("cipher")
        return cipher.decrypt(secret_value)

    if isinstance(v, SecretStr):
        return v
    else:
        return SecretStr(secret_value)


# =====================================================================
# 3. OIDC Cache & Secret Manager
# =====================================================================
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
        self, secret_name: str, default_value: Optional[Union[str, bool]] = None
    ) -> Optional[Union[str, bool]]:
        """메인 엔트리 포인트: 요청에 따라 OIDC, Manager, Env로 분기"""
        if secret_name.startswith("os.environ/"):
            secret_name = secret_name.replace("os.environ/", "", 1)

        try:
            if secret_name.startswith("oidc/"):
                return self._resolve_oidc(secret_name)

            if self._should_read_secret_from_secret_manager():
                return self._fetch_from_manager(secret_name)
            
            return self._fetch_from_env(secret_name)
        except Exception as e:
            if default_value is not None:
                return default_value
            raise e

    def _resolve_oidc(self, secret_name: str) -> str:
        cached_token = self.oidc_cache.get(secret_name)
        if cached_token:
            return cached_token

        secret_name_split = secret_name.replace("oidc/", "", 1)
        parts = secret_name_split.split("/", 1)
        oidc_provider = parts[0]
        oidc_aud = parts[1] if len(parts) > 1 else ""

        token = None
        ttl = 3600

        if oidc_provider == "google":
            with httpx.Client(timeout=self.http_timeout) as client:
                resp = client.get(
                    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity",
                    params={"audience": oidc_aud},
                    headers={"Metadata-Flavor": "Google"},
                )
                resp.raise_for_status()
                token = resp.text
                ttl = 3540  # 3600 - 60

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
            
            with httpx.Client(timeout=self.http_timeout) as client:
                resp = client.get(
                    req_url,
                    params={"audience": oidc_aud},
                    headers={
                        "Authorization": f"Bearer {req_token}",
                        "Accept": "application/json; api-version=2.0",
                    },
                )
                resp.raise_for_status()
                token = resp.json().get("value")
                ttl = 295  # 300 - 5

        elif oidc_provider == "azure":
            azure_federated_token_file = os.getenv("AZURE_FEDERATED_TOKEN_FILE")
            if azure_federated_token_file:
                with open(azure_federated_token_file, "r") as f:
                    token = f.read()
            else:
                try:
                    from azure.identity import DefaultAzureCredential
                    token = DefaultAzureCredential().get_token(oidc_aud).token
                except Exception as e:
                    raise ValueError(f"Azure OIDC provider failed: {str(e)}")

        elif oidc_provider == "file":
            safe_path = self._resolve_oidc_file_path(oidc_aud)
            with open(safe_path, "r") as f:
                token = f.read()

        elif oidc_provider in ("env", "env_path"):
            val = os.getenv(oidc_aud)
            if not val:
                raise ValueError(f"Environment variable {oidc_aud} not found")
            if oidc_provider == "env":
                token = val
            else:
                with open(val, "r") as f:
                    token = f.read()
        else:
            raise ValueError(f"Unsupported OIDC provider: {oidc_provider}")

        self.oidc_cache.set(secret_name, token, ttl)
        return token

    def _fetch_from_manager(self, secret_name: str):
        try:
            client = config.secret_manager_client
            key_manager = "local"
            
            if hasattr(config, "_key_management_system") and config._key_management_system:
                key_manager = config._key_management_system.value

            settings = getattr(config, "_key_management_settings", None)
            if settings and hasattr(settings, "hosted_keys") and settings.hosted_keys is not None:
                if secret_name not in settings.hosted_keys:
                    key_manager = "local"

            return get_secret_from_vendor(
                client=client,
                key_manager=key_manager,
                secret_name=secret_name,
                key_management_settings=settings,
                google_kms_resource_name=getattr(config, "_google_kms_resource_name", None)
            )
        except Exception as e:
            log.error(f"Defaulting to os.environ value for key={secret_name}. Error - {e}\n{traceback.format_exc()}")
            return self._fetch_from_env(secret_name)

    def _fetch_from_env(self, secret_name: str) -> Optional[Union[str, bool]]:
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
        if getattr(config, "secret_manager_client", None) is not None and getattr(config, "_key_management_settings", None) is not None:
            mode = getattr(config._key_management_settings, "access_mode", "")
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


# =====================================================================
# 4. Global Accessor Functions
# =====================================================================
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