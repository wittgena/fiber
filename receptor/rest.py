# receptor.rest
## @lineage: surface.rest
## @lineage: dphi.eco.rest
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from receptor.ingress.server.mcp import SecureMCPServer, SentinelFirewallMiddleware
from receptor.ingress.server.middleware import LocalMiddleware, WasTelemetry
from arch.topos.tunnel.factory import TunnelFactory
from arch.topos.tunnel.subs import DistributedPubSub
from arch.xor.parser.otlp import StrictOtlpRulesetParser

from kernel.dphi.broker import DphiBroker
from receptor.stream.store import LogStreamStore
from watcher.plane.emitter import get_emitter

from receptor.edge.eco import eco_router
from receptor.edge.core import core_edge

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

def get_default_config() -> Config:
    return Config()

async def verify_api_key(api_key: str = Security(api_key_header), config: Config = Depends(get_default_config)):
    if not config.session_api_keys:
        return None
    if api_key not in config.session_api_keys:
        raise HTTPException(status_code=403, detail="Invalid or missing API Key.")
    return api_key

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting REST Edge & Services...")
    config: Config = getattr(app.state, "config", get_default_config())
    
    try:
        app.state.store = LogStreamStore()
        tunnel = await TunnelFactory.get_default() 
        pubsub = DistributedPubSub(channel=config.pubsub_channel, tunnel=tunnel)
        await pubsub.start_listening()
        app.state.pubsub = pubsub
        
        app.state.broker = DphiBroker(timeout=config.wasm_timeout)
        log.info(f"WasmBroker initialized (timeout: {config.wasm_timeout}s).")

        # 🌟 신규: OTLP 규격 파서 및 결정론적 데이터 추출 엔진 초기화
        # 실제 운영 환경에서는 데이터베이스나 별도 JSON 설정 파일에서 규칙을 동적으로 불러올 수 있습니다.
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
        log.info("Shutting down XeLog Hub safely...")
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