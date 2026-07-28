# topos.xelog.restapi
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from topos.xelog.edge.a2a.router import a2a_router

from topos.xelog.edge.audit import audit_edge 
from topos.xelog.edge.otlp import otlp_edge
from topos.xelog.edge.anchor import anchor_edge
from topos.xelog.edge.ledger import ledger_edge

from topos.xelog.middleware import WasTelemetry, LocalMiddleware
from topos.xelog.topos.log.store import LogStreamStore

from arch.topos.tunnel.subs import DistributedPubSub
from arch.topos.tunnel.factory import UniversalFacade
from watcher.dphi.broker import WasmBroker

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class Config(BaseModel):
    web_url: str = ""
    allow_cors_origins: list[str] = ["*"]
    session_api_keys: list[str] = []
    pubsub_channel: str = "xelog_audit_channel"
    wasm_timeout: float = 10.0

def get_default_config() -> Config:
    return Config()

@asynccontextmanager
async def xelog_lifespan(app: FastAPI):
    log.info("[XeLog] Starting XeLog Hub REST API...")
    config: Config = app.state.config
    
    # HTTP 클라이언트를 포함한 Store를 앱 생명주기에 바인딩
    app.state.store = LogStreamStore()
    
    tunnel = UniversalFacade() 
    pubsub = DistributedPubSub(channel=config.pubsub_channel, tunnel=tunnel)
    await pubsub.start_listening()
    app.state.pubsub = pubsub
    
    app.state.broker = WasmBroker(timeout=config.wasm_timeout)
    log.info(f"[XeLog] WasmBroker initialized (timeout: {config.wasm_timeout}s).")

    yield

    # Teardown
    log.info("[XeLog] Shutting down XeLog Hub safely...")
    if hasattr(app.state, "pubsub"):
        await app.state.pubsub.close()
    if hasattr(app.state, "store"):
        await app.state.store.close()
    log.info("[XeLog] Teardown complete. Goodbye.")

def _get_root_path(config: Config) -> str:
    if config.web_url:
        return urlparse(config.web_url).path.rstrip("/")
    return ""

def add_api_routes(app: FastAPI, config: Config) -> None:
    app.include_router(a2a_router)

    app.include_router(otlp_edge)
    app.include_router(ledger_edge)
    app.include_router(anchor_edge)

    hub = APIRouter(prefix="/hub")
    hub.include_router(audit_edge)
    app.include_router(hub)

def create_app(config: Optional[Config] = None) -> FastAPI:
    config = config or get_default_config()

    app = FastAPI(
        title="XeLog Hub & Edge Router",
        description="Agentic Web & Immutable Ledger Interface",
        lifespan=xelog_lifespan,
        root_path=_get_root_path(config),
    )
    
    app.state.config = config
    add_api_routes(app, config)
    
    app.add_middleware(LocalMiddleware, allow_origins=config.allow_cors_origins)
    app.add_middleware(WasTelemetry)
    return app

api = create_app()