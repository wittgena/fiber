# ops.xelog.restapi
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from ops.xelog.edge.audit import audit_edge 
from ops.xelog.edge.otlp import otlp_edge
from ops.xelog.edge.anchor import anchor_edge
from ops.xelog.edge.a2a import a2a_edge
from ops.xelog.middleware import WasTelemetry, LocalMiddleware
from ops.xelog.store import get_logstream_store

from arch.topos.bound.interface.subs import DistributedPubSub
from arch.topos.bound.tunnel import UniversalFacade
from phase.wasm.broker import WasmBroker

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
    
    app.state.store = get_logstream_store()
    
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
    app.include_router(otlp_edge)
    
    hub = APIRouter(prefix="/hub")
    hub.include_router(audit_edge)
    app.include_router(hub)
    app.include_router(anchor_edge)
    app.include_router(a2a_edge)

def create_app(config: Optional[Config] = None) -> FastAPI:
    config = config or get_default_config()

    app = FastAPI(
        title="XeLog Hub",
        description="XeLog Hub - Dedicated REST Interface",
        lifespan=xelog_lifespan,
        root_path=_get_root_path(config),
    )
    
    app.state.config = config
    add_api_routes(app, config)
    
    app.add_middleware(LocalMiddleware, allow_origins=config.allow_cors_origins)
    app.add_middleware(WasTelemetry)
    return app

api = create_app()