# fiber.receptor.dphi.rest
## @lineage: fiber.dphi.receptor.rest
import os
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Security, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from fiber.receptor.dphi.edge.public import public_edge
from fiber.receptor.dphi.edge.internal import internal_router
from fiber.receptor.dphi.edge.ext import ext_router
from fiber.receptor.dphi.edge.llm import llm_edge

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.space.topos.tunnel.subs import DistributedPubSub
from xphi.arch.xor.parser.otlp import StrictOtlpRulesetParser
from xphi.arch.xor.stream.edge import LogStreamStore
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.adapter.utxo import UtxoAdapter

from xphi.watcher.mcp.server import SecureMCPServer, SentinelFirewallMiddleware
from xphi.watcher.ingress.middleware import (
    AttestationMiddleware,
    LocalMiddleware,
    WasTelemetry,
)
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.mcp.adapter.state import RedisAppendOnlyCache, MCPStateAdapter

log = get_emitter(__name__)

API_KEY_NAME = "X-Dphi-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

class Config(BaseModel):
    web_url: str = ""
    allow_cors_origins: list[str] = ["http://localhost:3000"] 
    session_api_keys: list[str] = []
    pubsub_channel: str = "audit_channel"
    wasm_timeout: float = 10.0
    committee_pubs: list[str] = []
    internal_edge_url: str = Field(default_factory=lambda: os.getenv("INTERNAL_EDGE_URL", "http://internal-edge-cluster.local:8080"))
    
    redis_url: str = Field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379"))
    max_payload_size: int = Field(default_factory=lambda: int(os.getenv("MAX_PAYLOAD_SIZE", 1024 * 1024 * 10)))

def get_default_config() -> Config:
    return Config()

async def verify_access_credential(
    request: Request,
    api_key: str = Security(api_key_header)
):
    path = request.url.path
    public_whitelist = {
        "/v1/public/agent/quote",
        "/v1/public/agent/handshake",
        "/v1/public/billing/invoice",
        "/v1/public/billing/balance",
        "/v1/public/audit/verify",
        "/v1/public/keys",
        "/openapi.json",
        "/docs",
        "/redoc"
    }
    
    if path in public_whitelist:
        return None

    if path.startswith("/v1/eco/") or path.startswith("/v1/core/") or path.startswith("/v1/ext/"):
        return None

    if path.startswith("/v1/mcp-gateway"):
        return None

    config: Config = request.app.state.config
    if config.session_api_keys and api_key in config.session_api_keys:
        return api_key

    l402_header = request.headers.get("X-X402-Receipt") or request.headers.get("Authorization")
    if l402_header:
        return l402_header

    raise HTTPException(
        status_code=status.HTTP_402_PAYMENT_REQUIRED, 
        detail="Zero-Trust Enforced: Payment Required. Please provide an L402 receipt.",
        headers={"WWW-Authenticate": 'L402 macaroon=""'}
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting REST Edge & Services...")
    config: Config = getattr(app.state, "config", get_default_config())
    
    try:
        # 1. State Store & PubSub Init
        app.state.store = LogStreamStore()
        tunnel = await TunnelFactory.get_default() 
        pubsub = DistributedPubSub(channel=config.pubsub_channel, tunnel=tunnel)
        await pubsub.start_listening()
        app.state.pubsub = pubsub
        
        ## WASM Kernel Broker Init
        app.state.broker = DphiBroker(timeout=config.wasm_timeout)
        log.info(f"WasmBroker initialized (timeout: {config.wasm_timeout}s).")

        ## UTXO Adapter Init
        app.state.utxo_adapter = UtxoAdapter(broker=app.state.broker)
        log.info("UtxoAdapter initialized and mounted to app state.")

        ## OTLP Parser & Extraction Engine Init
        default_otlp_ruleset = {
            "global_config": {"required_root_keys": ["resourceLogs"]},
            "targets": [
                {"tag": "tenant_id", "path": "resourceLogs.0.resource.attributes.tenant.id"},
                {"tag": "model", "path": "resourceLogs.0.scopeLogs.0.logRecords.0.attributes.llm.model"},
                {"tag": "prompt_tokens", "path": "resourceLogs.0.scopeLogs.0.logRecords.0.attributes.prompt_tokens"},
                {"tag": "completion_tokens", "path": "resourceLogs.0.scopeLogs.0.logRecords.0.attributes.completion_tokens"}
            ]
        }
        otlp_parser = StrictOtlpRulesetParser()
        app.state.otlp_engine = otlp_parser.parse_ruleset(default_otlp_ruleset)
        log.info("StrictOtlpExtractionEngine initialized.")

        app.state.mcp_cache = RedisAppendOnlyCache(redis_url=config.redis_url)
        app.state.mcp_state_adapter = MCPStateAdapter(cache=app.state.mcp_cache)
        log.info(f"Lock-Free MCP State Adapter initialized (Redis: {config.redis_url}).")
        log.info("REST Edge & Services successfully started.")
        yield
    except Exception as e:
        log.error(f"Failed to initialize REST Edge services: {e}", exc_info=True)
        raise
    finally:
        log.info("Shutting down receptor.rest safely...")
        if hasattr(app.state, "pubsub"):
            await app.state.pubsub.close()
            
        if hasattr(app.state, "store"):
            if hasattr(app.state.store, "close"):
                await app.state.store.close()

        if hasattr(app.state, "mcp_cache"):
            if hasattr(app.state.mcp_cache.redis, "close"):
                await app.state.mcp_cache.redis.close()
                log.info("MCP Cache Redis connection closed.")
                
        await TunnelFactory.close_all()
        log.info("Teardown complete. Goodbye.")

def _get_root_path(config: Config) -> str:
    if config.web_url:
        return urlparse(config.web_url).path.rstrip("/")
    return ""

def create_app(config: Optional[Config] = None) -> FastAPI:
    config = config or get_default_config()
    app = FastAPI(
        title="DPHI Edge Gateway",
        description="Immutable Ledger Interface, Proof of Compute, and First-Party Oracle",
        lifespan=lifespan,
        root_path=_get_root_path(config),
        dependencies=[Depends(verify_access_credential)]
    )
    
    app.state.config = config
    app.include_router(public_edge, tags=["mcp-exposed"]) 
    app.include_router(llm_edge)
    app.include_router(internal_router) 
    app.include_router(ext_router) 

    # [핵심 패치] 바이너리 스머글링(Smuggling) 및 디코딩 에러(500) 자멸 방어
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        # 로깅을 추가하여 공격 시도를 추적 가능하게 함
        log.warning(f"[Security] Rejected malformed payload from {request.client.host if request.client else 'unknown'}")
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": "Payload validation failed (Invalid encoding or format)"}
        )

    app.add_middleware(SentinelFirewallMiddleware, max_body_size=config.max_payload_size)
    app.add_middleware(LocalMiddleware, allow_origins=config.allow_cors_origins)
    app.add_middleware(AttestationMiddleware)
    app.add_middleware(WasTelemetry)

    log.info("Initializing Secure MCP Server (Native 2.0 Internal)...")
    mcp = SecureMCPServer(name="MCP-Server", version="1.0.0")
    mcp.bind_fastapi(app, allowed_tags=["mcp-exposed"])
    
    mcp_asgi_app = mcp.sse_app()
    app.mount("/mcp", mcp_asgi_app)
    return app