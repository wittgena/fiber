# bound.adapter.bridge.mcp.auth
## @lineage: bound.channel.mcp.bridge.auth
## @lineage: anchor.channel.bridge.auth
"""
@phase: MCP Auth Bridge
@desc: 
- Abstracts OAuth flows and transport creation for automated agents.
- Provides a factory to generate authenticated Transport objects without blocking or spawning local servers.
"""
from __future__ import annotations

import httpx
from typing import Any, Callable, Awaitable

from mcp.shared.auth import OAuthClientInformationFull, OAuthClientMetadata, OAuthToken
from mcp.client._transport import Transport
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamable_http_client
from xphi.auth.oauth.provider import OAuthClientProvider, TokenStorage

from watcher.plane.emitter import get_emitter

log = get_emitter("bridge.auth")

# Type aliases for dependency injection
RedirectHandlerT = Callable[[str], Awaitable[None]]
CallbackHandlerT = Callable[[], Awaitable[tuple[str, str | None]]]


class InMemoryTokenStorage(TokenStorage):
    """
    Fallback in-memory token storage.
    In production, agents should inject a secure Vault implementation here.
    """
    def __init__(self):
        self._tokens: OAuthToken | None = None
        self._client_info: OAuthClientInformationFull | None = None

    async def get_tokens(self) -> OAuthToken | None:
        return self._tokens

    async def set_tokens(self, tokens: OAuthToken) -> None:
        self._tokens = tokens

    async def get_client_info(self) -> OAuthClientInformationFull | None:
        return self._client_info

    async def set_client_info(self, client_info: OAuthClientInformationFull) -> None:
        self._client_info = client_info


class AuthenticatedTransportFactory:
    """
    A neutral factory that builds authenticated MCP Transports.
    Delegates the actual OAuth interaction to the calling agent.
    """

    def __init__(
        self,
        server_url: str,
        redirect_handler: RedirectHandlerT,
        callback_handler: CallbackHandlerT,
        storage: TokenStorage | None = None,
        client_metadata: dict[str, Any] | None = None,
        client_metadata_url: str | None = None,
    ):
        """
        Initialize the transport factory with dependency injection.

        Args:
            server_url: The base URL of the MCP server.
            redirect_handler: Async func to handle directing the user/system to the auth URL.
            callback_handler: Async func that resolves to (auth_code, state) when auth is complete.
            storage: Secure token storage. Defaults to InMemoryTokenStorage if None.
            client_metadata: OAuth client metadata dict.
            client_metadata_url: Optional URL for dynamic client registration.
        """
        self.server_url = server_url
        self.redirect_handler = redirect_handler
        self.callback_handler = callback_handler
        self.storage = storage or InMemoryTokenStorage()
        self.client_metadata_url = client_metadata_url
        
        default_metadata = {
            "client_name": "Automated MCP Agent",
            "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob"], # Out-of-band by default
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
        }
        self.client_metadata = client_metadata or default_metadata

    async def create_transport(self, transport_type: str = "streamable-http") -> Transport:
        """
        Builds and returns the configured, authenticated Transport object.
        Does NOT open a session or start an interactive loop.
        """
        log.info(f"Configuring OAuth provider for {self.server_url}")

        oauth_provider = OAuthClientProvider(
            server_url=self.server_url.replace("/mcp", ""),
            client_metadata=OAuthClientMetadata.model_validate(self.client_metadata),
            storage=self.storage,
            redirect_handler=self.redirect_handler,
            callback_handler=self.callback_handler,
            client_metadata_url=self.client_metadata_url,
        )

        log.info(f"Establishing authenticated {transport_type} transport...")

        if transport_type == "sse":
            return sse_client(
                url=self.server_url,
                auth=oauth_provider,
                timeout=60.0,
            )
        else:
            custom_client = httpx.AsyncClient(auth=oauth_provider, follow_redirects=True)
            return streamable_http_client(
                url=self.server_url, 
                http_client=custom_client
            )