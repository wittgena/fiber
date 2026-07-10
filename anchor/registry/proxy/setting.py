# anchor.registry.proxy.setting
## @lineage: xphi.proxy.setting
## @lineage: xphi.proxy.mcp.setting
import datetime
from typing import TypedDict, Any, Literal
from pydantic import AnyHttpUrl, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from mcp.server.streamable_http import EventStore

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

## [추가됨] Gateway 전용 설정
class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GATEWAY_")
    host: str = "0.0.0.0"
    port: int = 8000
    secret_token: str = "brane-super-secret-token"

## [추가됨] PyPI Proxy 전용 설정
class PyPIProxySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PYPI_")
    host: str = "127.0.0.1"
    port: int = 8083
    upstream_url: str = "https://pypi.org"
    transport_mode: Literal["stdio", "sse"] = "sse"  # 실행 모드 제어

async def tool_get_time() -> dict[str, str | float]:
    """Get the current server time (보호된 자원 예시)."""
    now = datetime.datetime.now()
    return {
        "current_time": now.isoformat(),
        "timezone": "KST",
        "timestamp": now.timestamp(),
        "formatted": now.strftime("%Y-%m-%d %H:%M:%S"),
    }

default_auth_settings = SimpleAuthSettings()
gateway_settings = GatewaySettings()
pypi_settings = PyPIProxySettings()