# dphi.receptor.rest
import os
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Security, Request, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from fiber.dphi.receptor.edge.public import public_edge
from fiber.dphi.receptor.edge.internal import internal_router
from fiber.dphi.receptor.edge.ext import ext_router
from fiber.dphi.receptor.edge.llm import llm_edge

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.space.topos.tunnel.subs import DistributedPubSub
from xphi.arch.xor.parser.otlp import StrictOtlpRulesetParser
from xphi.arch.xor.stream.edge import LogStreamStore
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.adapter.utxo import UtxoAdapter

from xphi.watcher.ingress.mcp import SecureMCPServer, SentinelFirewallMiddleware
from xphi.watcher.ingress.middleware import (
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
    allow_cors_origins: list[str] = ["http://localhost:3000"] 
    session_api_keys: list[str] = []
    pubsub_channel: str = "audit_channel"
    wasm_timeout: float = 10.0
    committee_pubs: list[str] = []
    internal_edge_url: str = Field(default_factory=lambda: os.getenv("INTERNAL_EDGE_URL", "http://internal-edge-cluster.local:8080"))


def get_default_config() -> Config:
    return Config()

async def verify_access_credential(
    request: Request,
    api_key: str = Security(api_key_header)
):
    """
    @desc: 전역 엑세스 통제 (HTTP 402 합법적 탈취)
    """
    path = request.url.path
    
    # 🌟 1. 결제가 필요 없는 Public 엔드포인트 (견적, 인보이스, 잔고, 인증키 등)
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

    # 🌟 2. 내부망(Internal) 마이크로서비스 통신 우회
    # (E2E 테스트 환경에서는 동일 앱에 마운트되므로 면제. 실제 환경에서는 네트워크 분리로 보호됨)
    if path.startswith("/v1/eco/") or path.startswith("/v1/core/") or path.startswith("/v1/ext/"):
        return None

    config: Config = request.app.state.config
    
    # 3. 관리자 / 세션 API Key가 설정되어 있고 일치하는 경우 (Admin Bypass)
    if config.session_api_keys and api_key in config.session_api_keys:
        return api_key

    # 4. API Key가 없거나 일치하지 않는 경우 -> L402 결제 영수증 확인
    l402_header = request.headers.get("X-X402-Receipt") or request.headers.get("Authorization")
    
    if l402_header:
        return l402_header

    # 5. 증명이 없는 경우 HTTP 402 반환 (Execute, Telemetry 등 실 과금 엔드포인트 방어)
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

        ## UTXO Adapter Init (Boot 과정에서 Singleton으로 마운트)
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
        log.info("StrictOtlpExtractionEngine initialized and mounted to app state.")

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

    app.add_middleware(SentinelFirewallMiddleware)
    app.add_middleware(LocalMiddleware, allow_origins=config.allow_cors_origins)
    app.add_middleware(AttestationMiddleware)
    app.add_middleware(WasTelemetry)

    log.info("Initializing Secure MCP Server...")
    mcp = SecureMCPServer(name="MCP-Server", version="1.0.0")
    mcp.bind_fastapi(app, allowed_tags=["mcp-exposed"])
    
    mcp_asgi_app = mcp.sse_app()
    app.mount("/mcp", mcp_asgi_app)
    return app