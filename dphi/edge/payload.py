# fiber.dphi.edge.payload
from contextlib import asynccontextmanager
from typing import Optional, Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, HTTPException, Security, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from fiber.dphi.edge.serv.gateway import IdempotencyMapper, NonceReplayProtector, TransitionBridge, mcp_bridge
from fiber.dphi.edge.serv.public import public_edge
from fiber.dphi.edge.serv.ext import ext_router
from fiber.dphi.edge.serv.llm import llm_edge
from fiber.dphi.eco.config.origin import OriginRegistry

from xphi.kernel.space.topos.tunnel.subs import DistributedPubSub
from xphi.kernel.wasm.broker import DphiBroker
from xphi.bound.xor.parser.ruleset.otlp import StrictOtlpRulesetParser
from xphi.watcher.server.mcp import SecureMCPServer, SentinelFirewallMiddleware
from xphi.watcher.server.middleware import (
    AttestationMiddleware,
    LocalMiddleware,
    WasTelemetry,
)
from xphi.watcher.plane.emitter import get_emitter

from xphi.bound.xor.secret.cipher import Cipher
from xphi.bound.xor.secret.client import get_secret_from_vendor, KMSVendor
from xphi.watcher.receptor.audit.secret import SecretAuditor

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
        "/v1/public/sandbox/quote",
        "/v1/public/sandbox/handshake",
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

    # MCP Gateway 라우터 우회: 내부 브릿지가 DPoP / x402(Track A/B)를 직접 자체 검증함
    if path.startswith("/v1/mcp-gateway/"):
        return None

    config: Config = request.app.state.config
    if config.session_api_keys and api_key in config.session_api_keys:
        return api_key

    # 글로벌 LLM Gateway 결제 검증
    x402_header = request.headers.get("X-X402-Receipt") or request.headers.get("Authorization")
    if x402_header:
        return x402_header

    raise HTTPException(
        status_code=status.HTTP_402_PAYMENT_REQUIRED, 
        detail="Zero-Trust Enforced: Payment Required. Please provide a stablecoin/x402 receipt.",
        headers={"WWW-Authenticate": 'x402_signed_proof=""'}
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting DPHI REST Edge API Payload (Stateless Mode)...")
    config: Config = getattr(app.state, "config", get_default_config())
    
    try:
        # -------------------------------------------------------------------
        # [CRITICAL SECURITY INITIALIZATION]
        # 0. Origin Registry 초기화 및 암호학적 자가 검증 (Fail-Fast)
        # -------------------------------------------------------------------
        log.info("Initializing Origin Registry and verifying cryptographic state...")
        registry = OriginRegistry()
        trusted_state = registry.load_and_verify()
        
        app.state.origin_registry = registry
        log.info(f"Origin Registry integrated successfully. Active signers: {len(trusted_state.active_signers)}")

        # -------------------------------------------------------------------
        # [NEW] KMS 기반 SecretAuditor 싱글톤 초기화 (Fail-Fast)
        # -------------------------------------------------------------------
        log.info("Provisioning Cryptographic Secret Auditor via KMS...")
        secret_key = get_secret_from_vendor(
            client=None,
            key_manager=KMSVendor.LOCAL,  # 추후 AWS_KMS 등으로 변경 가능
            secret_name="LEDGER_CIPHER_KEY"
        )
        if not secret_key:
            log.warning("KMS returned no key. Using ephemeral testing key.")
            secret_key = "dphi-ephemeral-test-key-32bytes!"
            
        app.state.secret_auditor = SecretAuditor(cipher=Cipher(secret_key=secret_key))
        log.info("SecretAuditor mounted successfully to app.state.")
        # -------------------------------------------------------------------

        tunnel = app.state.tunnel
        ledger = app.state.ledger

        if not all([tunnel, ledger]):
            log.warning("Some infrastructure dependencies (tunnel, ledger) are missing from injection.")

        pubsub = DistributedPubSub(channel=config.pubsub_channel, tunnel=tunnel)
        await pubsub.start_listening()
        app.state.pubsub = pubsub
        
        # 1. WASM Broker 초기화
        app.state.broker = DphiBroker(timeout=config.wasm_timeout)
        log.info(f"WasmBroker initialized (timeout: {config.wasm_timeout}s).")

        # 2. OTLP Parser & Extraction Engine Init
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

        # 3. Stateless Transition Bridge 인스턴스 마운트 (2026-07-28 규격)
        nonce_protector = NonceReplayProtector(tunnel=tunnel)
        mapper = IdempotencyMapper(tunnel=tunnel)

        app.state.mcp_transition_adapter = TransitionBridge(
            mapper=mapper,                           
            nonce_protector=nonce_protector
        )
        log.info("Stateless MCP Transition Bridge (2026-07-28) initialized and mounted to app.state.")
        
        app.state.is_ready = True
        log.info("REST Edge API Payload is fully READY.")
        yield
        
    except Exception as e:
        log.error(f"Failed to initialize REST Edge services: {e}", exc_info=True)
        # 보안 검증 등 크리티컬한 초기화가 실패하면 애플리케이션 기동 자체를 중지시킴
        raise
        
    finally:
        app.state.is_ready = False
        log.info("Shutting down receptor.rest payload safely...")
        
        if hasattr(app.state, "pubsub"):
            try:
                await app.state.pubsub.close()
            except Exception as e:
                log.error(f"Error closing PubSub: {e}")
        
        log.info("API Payload Teardown complete. Goodbye.")


# =====================================================================
# App Factory
# =====================================================================
def _get_root_path(config: Config) -> str:
    if config.web_url:
        return urlparse(config.web_url).path.rstrip("/")
    return ""

def create_app(
    config: Optional[Config] = None,
    tunnel: Optional[Any] = None,
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
    
    # State Injection
    app.state.config = config
    app.state.tunnel = tunnel
    app.state.ledger = ledger  
    app.state.is_ready = False  
    
    # Routers Binding
    app.include_router(public_edge, tags=["mcp-exposed"]) 
    app.include_router(llm_edge)
    app.include_router(mcp_bridge)  
    app.include_router(ext_router) 

    # Readiness Probe
    @app.get("/_health", tags=["system"], include_in_schema=False)
    async def readiness_probe():
        if getattr(app.state, "is_ready", False):
            return {"status": "ok", "message": "API Payload is ready"}
        raise HTTPException(status_code=503, detail="Service Not Ready")

    # [보안 개선] 통제된 정보 누출 (Controlled Information Leakage) 적용
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        client_host = request.client.host if request.client else 'unknown'
        
        # 내부 구조(Tree)는 숨기고, 문제가 된 '마지막 필드명(Leaf Node)'만 추출
        safe_hints = []
        for err in exc.errors():
            loc = err.get("loc", [])
            if len(loc) > 0:
                field_name = str(loc[-1])
                # 인덱스 번호(리스트 위치)는 제거하고 실제 키 이름만 수집
                if not field_name.isdigit(): 
                    safe_hints.append(field_name)
        
        hint_msg = ""
        if safe_hints:
            unique_hints = sorted(list(set(safe_hints)))
            hint_msg = f" Missing or invalid field(s) detected: {unique_hints}."

        log.warning(f"[Security] Rejected malformed payload from {client_host}.{hint_msg}")
        
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, 
            content={
                "detail": f"Payload validation failed.{hint_msg}",
                "resolution": "Ensure the payload complies with the strictly required JSON schema."
            }
        )

    # Middlewares
    app.add_middleware(SentinelFirewallMiddleware, max_body_size=config.max_payload_size)
    app.add_middleware(LocalMiddleware, allow_origins=config.allow_cors_origins)
    app.add_middleware(AttestationMiddleware)
    app.add_middleware(WasTelemetry)

    # Legacy Secure MCP Server Mount
    log.info("Initializing Secure MCP Server (Native Gateway)...")
    mcp = SecureMCPServer(name="MCP-Server", version="1.0.0")
    mcp.bind_fastapi(app, allowed_tags=["mcp-exposed"])
    
    mcp_asgi_app = mcp.sse_app()
    app.mount("/mcp", mcp_asgi_app)
    
    return app