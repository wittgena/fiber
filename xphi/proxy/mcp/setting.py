# xphi.proxy.mcp.setting
## @lineage: xphi.reflect.server.setting
import datetime
from typing import TypedDict, Any, Literal
from pydantic import AnyHttpUrl, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from bound.transport.session.server.http import EventStore

class ServerRunConfig(TypedDict, total=False):
    transport: Literal["stdio", "sse", "streamable-http"]
    port: int
    event_store: EventStore | None
    retry_interval: int
    uvicorn_kwargs: dict[str, Any]

class SimpleAuthSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MCP_")
    demo_username: str = "demo_user"
    demo_password: str = "demo_password"
    mcp_scope: str = "user"

class AuthServerSettings(BaseModel):
    """Authorization Server(브로커)를 위한 설정"""
    host: str = "localhost"
    port: int = 9000
    server_url: AnyHttpUrl = AnyHttpUrl("http://localhost:9000")
    auth_callback_path: str = "http://localhost:9000/login/callback"

class ResourceServerSettings(BaseSettings):
    """Resource Server(MCP 도구)를 위한 설정"""
    model_config = SettingsConfigDict(env_prefix="MCP_RESOURCE_")
    host: str = "localhost"
    port: int = 8001
    server_url: AnyHttpUrl = AnyHttpUrl("http://localhost:8001/mcp")
    auth_server_url: AnyHttpUrl = AnyHttpUrl("http://localhost:9000")
    auth_server_introspection_endpoint: str = "http://localhost:9000/introspect"
    mcp_scope: str = "user"
    oauth_strict: bool = False

async def tool_get_time() -> dict[str, str | float]:
    """Get the current server time (보호된 자원 예시)."""
    now = datetime.datetime.now()
    return {
        "current_time": now.isoformat(),
        "timezone": "UTC",
        "timestamp": now.timestamp(),
        "formatted": now.strftime("%Y-%m-%d %H:%M:%S"),
    }

default_auth_settings = SimpleAuthSettings()