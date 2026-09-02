# fiber.dphi.edge.payload
from contextlib import asynccontextmanager
from typing import Optional, Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, HTTPException, Security, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from fiber.dphi.edge.transition.bridge import NonceReplayProtector, TransitionBridge
from fiber.dphi.edge.serv.public import public_edge
from fiber.dphi.edge.serv.ext import ext_router
from fiber.dphi.edge.serv.llm import llm_edge

from xphi.kernel.space.topos.tunnel.subs import DistributedPubSub
from xphi.kernel.dphi.broker import DphiBroker
from xphi.xor.parser.ruleset.otlp import StrictOtlpRulesetParser

from xphi.watcher.server.mcp import SecureMCPServer, SentinelFirewallMiddleware
from xphi.watcher.server.middleware import (
    AttestationMiddleware,
    LocalMiddleware,
    WasTelemetry,
)
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

API_KEY_NAME = "X-Dphi-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

class Config(BaseModel):
    web_url: str = ""
    internal_edge_url: str = ""
    allow_cors_origins: list[str] = ["http://localhost:3000"] 
    session_api_keys: list[str] = []
    pubsub_channel: str = "audit_channel"
    wasm_timeout: float = 10.0
    committee_pubs: list[str] = []
    
    redis_url: str = "redis://localhost:6379"
    max_payload_size: int = 10485760  # 10MB

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
        "/redoc",
        "/_health"
    }
    
    if path in public_whitelist:
        return None

    if path.startswith("/v1/ext/"):
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
    log.info("Starting DPHI REST Edge API Payload (Stateless Mode)...")
    config: Config = getattr(app.state, "config", get_default_config())
    
    try:
        tunnel = app.state.tunnel
        redis_client = app.state.redis_client
        ledger = app.state.ledger

        if not all([tunnel, redis_client, ledger]):
            log.warning("Some infrastructure dependencies (tunnel, redis, ledger) are missing from injection.")

        pubsub = DistributedPubSub(channel=config.pubsub_channel, tunnel=tunnel)
        await pubsub.start_listening()
        app.state.pubsub = pubsub
        
        # WASM Broker 초기화 (비즈니스 도메인 바인딩)
        app.state.broker = DphiBroker(timeout=config.wasm_timeout)
        log.info(f"WasmBroker initialized (timeout: {config.wasm_timeout}s).")

        # 2. OTLP Parser & Extraction Engine Init (순수 로직)
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

        # 3. 주입받은 인프라 자원을 활용한 Transition Bridge 생성
        nonce_protector = NonceReplayProtector(redis_client=redis_client)
        app.state.mcp_transition_adapter = TransitionBridge(
            ledger=ledger,
            nonce_protector=nonce_protector
        )
        log.info(f"MCP 1.0 <-> 2.0 Transition Bridge initialized (Using injected Redis Client).")
        
        app.state.is_ready = True
        log.info("REST Edge API Payload is fully READY.")
        yield
        
    except Exception as e:
        log.error(f"Failed to initialize REST Edge services: {e}", exc_info=True)
        raise
        
    finally:
        # 상태를 즉시 False로 전환하여 추가 인입 방지
        app.state.is_ready = False
        log.info("Shutting down receptor.rest payload safely...")
        
        # API가 스스로 생성(소유)한 어플리케이션 레벨 자원만 정리
        if hasattr(app.state, "pubsub"):
            try:
                await app.state.pubsub.close()
            except Exception as e:
                log.error(f"Error closing PubSub: {e}")
        
        log.info("API Payload Teardown complete. Goodbye.")

def _get_root_path(config: Config) -> str:
    if config.web_url:
        return urlparse(config.web_url).path.rstrip("/")
    return ""

def create_app(
    config: Optional[Config] = None,
    tunnel: Optional[Any] = None,
    redis_client: Optional[Any] = None,
    ledger: Optional[Any] = None
) -> FastAPI:
    config = config or get_default_config()
    app = FastAPI(
        title="DPHI Edge Gateway",
        description="Stateless Immutable Gateway, Proof of Compute, and First-Party Oracle",
        lifespan=lifespan,
        root_path=_get_root_path(config),
        dependencies=[Depends(verify_access_credential)]
    )
    
    # 전달받은 글로벌 리소스를 어플리케이션 상태에 바인딩
    app.state.config = config
    app.state.tunnel = tunnel
    app.state.redis_client = redis_client
    app.state.ledger = ledger
    app.state.is_ready = False  # 초기 상태
    
    app.include_router(public_edge, tags=["mcp-exposed"]) 
    app.include_router(llm_edge)
    app.include_router(ext_router) 

    @app.get("/_health", tags=["system"], include_in_schema=False)
    async def readiness_probe():
        if getattr(app.state, "is_ready", False):
            return {"status": "ok", "message": "API Payload is ready"}
        raise HTTPException(status_code=503, detail="Service Not Ready")

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        log.warning(f"[Security] Rejected malformed payload from {request.client.host if request.client else 'unknown'}")
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": "Payload validation failed (Invalid encoding or format)"}
        )

    app.add_middleware(SentinelFirewallMiddleware, max_body_size=config.max_payload_size)
    app.add_middleware(LocalMiddleware, allow_origins=config.allow_cors_origins)
    app.add_middleware(AttestationMiddleware)
    app.add_middleware(WasTelemetry)

    log.info("Initializing Secure MCP Server (Native 2.0 Gateway)...")
    mcp = SecureMCPServer(name="MCP-Server", version="1.0.0")
    mcp.bind_fastapi(app, allowed_tags=["mcp-exposed"])
    
    mcp_asgi_app = mcp.sse_app()
    app.mount("/mcp", mcp_asgi_app)
    
    return app