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

from xphi.arch.topos.tunnel.factory import TunnelFactory
from xphi.arch.topos.tunnel.subs import DistributedPubSub
from xphi.arch.xor.parser.otlp import StrictOtlpRulesetParser
from xphi.arch.xor.stream.edge import LogStreamStore
from xphi.kernel.dphi.broker import DphiBroker
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
    - 관리자 API Key가 있다면 통과시킵니다.
    - 없다면 403 에러가 아닌, L402 결제를 요구하는 HTTP 402 상태 코드를 반환합니다.
    """
    config: Config = request.app.state.config
    
    # 1. 관리자 / 세션 API Key가 설정되어 있고 일치하는 경우 (Admin Bypass)
    if config.session_api_keys and api_key in config.session_api_keys:
        return api_key

    # 2. API Key가 없거나 일치하지 않는 경우 -> L402 결제 영수증 확인
    # (에이전트가 헤더에 X-X402-Receipt 또는 Authorization: L402... 형태로 제출)
    l402_header = request.headers.get("X-X402-Receipt") or request.headers.get("Authorization")
    
    if l402_header:
        # 영수증이 존재하면 하위 라우터(edge.llm) 및 WASM 커널에게 구체적인 검증(잔고, 위변조) 위임
        return l402_header

    # 3. 어떠한 증명도 없는 경우 -> HTTP 403(Forbidden) 대신 HTTP 402(Payment Required) 반환
    # 이 반환값(WWW-Authenticate)을 통해 외부 에이전트들은 시스템을 표준 OpenAI API로 착각하면서도
    # 백그라운드에서 자연스럽게 DPHI의 L402 결제 핸드셰이크를 시작하게 됩니다.
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
        
        # 2. WASM Kernel Broker Init
        app.state.broker = DphiBroker(timeout=config.wasm_timeout)
        log.info(f"WasmBroker initialized (timeout: {config.wasm_timeout}s).")

        # 3. OTLP Parser & Extraction Engine Init
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
        dependencies=[Depends(verify_access_credential)]  # 🌟 전역 L402 결제 유도 미들웨어 적용
    )
    
    app.state.config = config
    
    # 🌟 라우터 등록 
    app.include_router(public_edge, tags=["mcp-exposed"]) 
    app.include_router(llm_edge) # 신규 LLM Edge Gateway 등록 (WASM 종속형)
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