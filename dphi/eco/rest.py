# dphi.eco.rest
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from arch.gov.server.mcp import SecureMCPServer, SentinelFirewallMiddleware
from arch.gov.server.middleware import LocalMiddleware, WasTelemetry
from arch.topos.tunnel.factory import UniversalFacade
from arch.topos.tunnel.subs import DistributedPubSub
from kernel.dphi.broker import WasmBroker
from kernel.phase.stream.store import LogStreamStore
from watcher.plane.emitter import get_emitter

from watcher.receptor.edge.eco import eco_router
from watcher.receptor.edge.core import core_edge

log = get_emitter(__name__)

API_KEY_NAME = "X-Dphi-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

class Config(BaseModel):
    web_url: str = ""
    allow_cors_origins: list[str] = ["http://localhost:3000"] 
    session_api_keys: list[str] = []
    pubsub_channel: str = "xelog_audit_channel"
    wasm_timeout: float = 10.0

def get_default_config() -> Config:
    return Config()

async def verify_api_key(api_key: str = Security(api_key_header), config: Config = Depends(get_default_config)):
    if not config.session_api_keys:
        log.warning("No session_api_keys configured. Running in insecure mode!")
        return None
    if api_key not in config.session_api_keys:
        raise HTTPException(status_code=403, detail="Invalid or missing API Key.")
    return api_key

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting REST Edge & Services...")
    config: Config = app.state.config
    
    app.state.store = LogStreamStore()
    
    tunnel = UniversalFacade() 
    pubsub = DistributedPubSub(channel=config.pubsub_channel, tunnel=tunnel)
    await pubsub.start_listening()
    app.state.pubsub = pubsub
    
    app.state.broker = WasmBroker(timeout=config.wasm_timeout)
    log.info(f"WasmBroker initialized (timeout: {config.wasm_timeout}s).")

    yield

    log.info("Shutting down XeLog Hub safely...")
    if hasattr(app.state, "pubsub"):
        await pubsub.close()
    if hasattr(app.state, "store"):
        await app.state.store.close()
    log.info("Teardown complete. Goodbye.")

def _get_root_path(config: Config) -> str:
    if config.web_url:
        return urlparse(config.web_url).path.rstrip("/")
    return ""

def create_app(config: Optional[Config] = None) -> FastAPI:
    config = config or get_default_config()
    app = FastAPI(
        title="Edge Router",
        description="Immutable Ledger Interface",
        lifespan=lifespan,
        root_path=_get_root_path(config),
        dependencies=[Depends(verify_api_key)]
    )
    
    app.state.config = config
    app.include_router(eco_router)
    app.include_router(core_edge, tags=["mcp-exposed"])

    app.add_middleware(SentinelFirewallMiddleware)
    app.add_middleware(LocalMiddleware, allow_origins=config.allow_cors_origins)
    app.add_middleware(WasTelemetry)

    log.info("Initializing Secure MCP Server...")
    mcp = SecureMCPServer(name="MCP-Server", version="1.0.0")
    mcp.bind_fastapi(app, allowed_tags=["mcp-exposed"])
    mcp_asgi_app = mcp.sse_app()
    app.mount("/mcp", mcp_asgi_app)
    
    return app

api = create_app()