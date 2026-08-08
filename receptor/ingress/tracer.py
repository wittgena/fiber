# receptor.ingress.tracer
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict
import httpx
from fastapi.routing import APIRoute

from watcher.plane.emitter import flow_scope, get_emitter

log = get_emitter("net.tracer")

class TargetOp:
    OTLP_INGRESS = "core.otlp_logs_export"            
    TRADE_INGRESS = "eco.exchange.submit_trade_intent" 
    LEDGER_APPEND = "core.append_to_stream"            
    ANCHOR_SEAL = "core.seal_state"                    

DEFAULT_FALLBACK_ROUTES: Dict[str, str] = {
    TargetOp.OTLP_INGRESS: "/v1/logs",
    TargetOp.TRADE_INGRESS: "/v1/eco/exchange/order/ingress",
    TargetOp.LEDGER_APPEND: "/v1/ledger/stream/append",
    TargetOp.ANCHOR_SEAL: "/v1/anchor/seal"
}

@dataclass
class E2EConfig:
    host: str = "localhost"
    port: int = 8000
    protocol: str = "http"
    fallback_routes: Dict[str, str] = field(default_factory=lambda: DEFAULT_FALLBACK_ROUTES)
    
    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"


@dataclass
class SceneConfig:
    """DI container for payload builders."""
    otlp_builder: Callable[[bool], dict]
    trade_builder: Callable[[bool], dict]
    ledger_builder: Callable[[str, bool], dict]


class RouteRegistry:
    """Dynamically scans FastAPI routes with fallback support."""
    def __init__(self, app, fallbacks: Dict[str, str] = None):
        self.app = app
        self.fallbacks = fallbacks or {}

    def url_for(self, target_name: str) -> str:
        # Dynamic scan to prevent early-binding cache misses
        for route in self.app.routes:
            if isinstance(route, APIRoute) and route.name == target_name:
                return route.path

        if target_name in self.fallbacks:
            log.warning(f"Route '{target_name}' not found natively. Using fallback: {self.fallbacks[target_name]}")
            return self.fallbacks[target_name]
            
        available_routes = [r.name for r in self.app.routes if isinstance(r, APIRoute)]
        log.error(f"Target Route '{target_name}' not found! Available routes: {available_routes}")
        raise ValueError(f"Route '{target_name}' not found and no fallback provided.")


class HttpFlowTracer:
    """HTTP interceptor for flow tracing and logging."""
    async def trace_request(self, request: httpx.Request):
        flow_id = f"http_{uuid.uuid4().hex[:8]}"
        request.headers["x-flow-id"] = flow_id
        
        with flow_scope(flow_id=flow_id, phase="HTTP_TX", bound="tester"):
            log.info(f"[Trace:TX] {request.method} {request.url}")
            log.debug(f"  └─ Headers: {dict(request.headers)}")

    async def trace_response(self, response: httpx.Response):
        flow_id = response.request.headers.get("x-flow-id", "unknown_flow")
        
        with flow_scope(flow_id=flow_id, phase="HTTP_RX", bound="tester"):
            await response.aread()
            elapsed_str = f" in {response.elapsed.total_seconds():.3f}s" if hasattr(response, "elapsed") else ""
            status_log = f"[Trace:RX] {response.status_code} {response.reason_phrase}{elapsed_str}"
            
            if response.status_code >= 400:
                safe_text = "<Binary/Unreadable Body>"
                try:
                    # Safely log text/json bodies to prevent crash on binary data
                    content_type = response.headers.get("content-type", "")
                    if "text" in content_type or "json" in content_type:
                        safe_text = response.text[:200] if hasattr(response, 'text') else "<Empty>"
                except Exception:
                    pass
                log.warning(f"{status_log}\n  └─ Body: {safe_text}")
            else:
                log.info(status_log)