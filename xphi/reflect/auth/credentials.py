# xphi.reflect.auth.credentials
## @lineage: xphi.proxy.auth.credentials
"""Credential storage and retrieval for OAuth-based LLM authentication."""
from __future__ import annotations
import json
import os
import time
import warnings
from pathlib import Path
from typing import Literal, Optional
import anyio
from pydantic import BaseModel, Field
from mcp.shared.auth import OAuthToken, OAuthClientInformationFull
from mcp.client.auth.oauth2 import TokenStorage

from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

AUTH_ROOT = resolve_path("xor") / "auth"

logger = get_emitter("auth.credentials")

def get_credentials_dir() -> Path:
    return AUTH_ROOT

class OAuthCredentials(BaseModel):
    type: Literal["oauth"] = "oauth"
    vendor: str = Field(description="The vendor/provider (e.g., 'openai')")
    access_token: str = Field(description="The OAuth access token")
    refresh_token: str = Field(description="The OAuth refresh token")
    expires_at: int = Field(
        description="Unix timestamp (ms) when the access token expires"
    )
    
    ## MCP Provider와의 호환성을 위해 기존 형태를 깨지 않고 선택적 필드 추가
    raw_token: Optional[dict] = Field(default=None, description="Serialized full OAuthToken")
    client_info: Optional[dict] = Field(default=None, description="Serialized OAuthClientInformationFull")

    def is_expired(self) -> bool:
        return self.expires_at < (int(time.time() * 1000) + 60_000)


class CredentialStore:
    def __init__(self, credentials_dir: Path | None = None):
        self._credentials_dir = credentials_dir or get_credentials_dir()
        logger.info(f"Using credentials directory: {self._credentials_dir}")

    @property
    def credentials_dir(self) -> Path:
        """Get the credentials directory, creating it if necessary."""
        self._credentials_dir.mkdir(parents=True, exist_ok=True)
        # Set directory permissions to owner-only (rwx------)
        if os.name != "nt":
            self._credentials_dir.chmod(0o700)
        return self._credentials_dir

    def _get_credentials_file(self, vendor: str) -> Path:
        """Get the path to the credentials file for a vendor."""
        return self.credentials_dir / f"{vendor}_oauth.json"

    def get(self, vendor: str) -> OAuthCredentials | None:
        creds_file = self._get_credentials_file(vendor)
        if not creds_file.exists():
            return None

        try:
            with open(creds_file) as f:
                data = json.load(f)
            return OAuthCredentials.model_validate(data)
        except (json.JSONDecodeError, ValueError):
            # Invalid credentials file, remove it
            creds_file.unlink(missing_ok=True)
            return None

    def save(self, credentials: OAuthCredentials) -> None:
        creds_file = self._get_credentials_file(credentials.vendor)
        with open(creds_file, "w") as f:
            json.dump(credentials.model_dump(exclude_none=True), f, indent=2)
        if os.name != "nt":  # Not Windows
            creds_file.chmod(0o600)
        else:
            warnings.warn(
                "File permissions on Windows should be manually restricted",
                stacklevel=2,
            )

    def delete(self, vendor: str) -> bool:
        creds_file = self._get_credentials_file(vendor)
        if creds_file.exists():
            creds_file.unlink()
            return True
        return False

    def update_tokens(
        self,
        vendor: str,
        access_token: str,
        refresh_token: str | None,
        expires_in: int,
    ) -> OAuthCredentials | None:
        existing = self.get(vendor)
        if existing is None:
            return None

        updated = OAuthCredentials(
            vendor=vendor,
            access_token=access_token,
            refresh_token=refresh_token or existing.refresh_token,
            expires_at=int(time.time() * 1000) + (expires_in * 1000),
            raw_token=existing.raw_token,
            client_info=existing.client_info
        )
        self.save(updated)
        return updated

    def as_mcp_storage(self, vendor: str) -> "MCPTokenStorageAdapter":
        """현재 CredentialStore를 MCP의 TokenStorage 프로토콜에 맞게 감싸는 어댑터를 반환"""
        return MCPTokenStorageAdapter(self, vendor)

class MCPTokenStorageAdapter(TokenStorage):
    """Adapts CredentialStore to the TokenStorage Protocol required by MCP Provider."""
    
    def __init__(self, store: CredentialStore, vendor: str):
        self.store = store
        self.vendor = vendor

    async def get_tokens(self) -> OAuthToken | None:
        # 파일 I/O를 anyio 비동기 스레드로 위임하여 이벤트 루프 블로킹 방지
        creds = await anyio.to_thread.run_sync(self.store.get, self.vendor)
        if not creds:
            return None
            
        if creds.raw_token:
            return OAuthToken.model_validate(creds.raw_token)
            
        # 기존에 생성된 레거시 자격 증명 파일에 대한 하위 호환성 지원
        if creds.access_token:
            expires_in_sec = max(0, int((creds.expires_at - time.time() * 1000) / 1000))
            return OAuthToken(
                access_token=creds.access_token,
                refresh_token=creds.refresh_token,
                token_type="Bearer",
                expires_in=expires_in_sec
            )
        return None

    async def set_tokens(self, tokens: OAuthToken) -> None:
        creds = await anyio.to_thread.run_sync(self.store.get, self.vendor)
        expires_at = int(time.time() * 1000) + ((tokens.expires_in or 3600) * 1000)
        
        if creds:
            creds.access_token = tokens.access_token
            creds.refresh_token = tokens.refresh_token or creds.refresh_token
            creds.expires_at = expires_at
            creds.raw_token = tokens.model_dump(mode='json')
        else:
            creds = OAuthCredentials(
                vendor=self.vendor,
                access_token=tokens.access_token,
                refresh_token=tokens.refresh_token or "",
                expires_at=expires_at,
                raw_token=tokens.model_dump(mode='json')
            )
        await anyio.to_thread.run_sync(self.store.save, creds)

    async def get_client_info(self) -> OAuthClientInformationFull | None:
        creds = await anyio.to_thread.run_sync(self.store.get, self.vendor)
        if not creds or not creds.client_info:
            return None
        return OAuthClientInformationFull.model_validate(creds.client_info)

    async def set_client_info(self, client_info: OAuthClientInformationFull) -> None:
        creds = await anyio.to_thread.run_sync(self.store.get, self.vendor)
        if creds:
            creds.client_info = client_info.model_dump(mode='json')
        else:
            creds = OAuthCredentials(
                vendor=self.vendor,
                access_token="",
                refresh_token="",
                expires_at=0,
                client_info=client_info.model_dump(mode='json')
            )
        await anyio.to_thread.run_sync(self.store.save, creds)