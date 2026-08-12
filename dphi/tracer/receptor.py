# dphi.tracer.receptor
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict
import httpx
from fastapi.routing import APIRoute

from watcher.plane.emitter import flow_scope, get_emitter

log = get_emitter("tracer.receptor")

# =====================================================================
# [변경됨] Public Gateway와 Internal API의 명확한 분리 반영
# =====================================================================
class TargetOp:
    # --- Public Gateway Endpoints (외부 클라이언트 시뮬레이션용) ---
    PUBLIC_AGENT_EXECUTE = "public.public_agent_execute"       # 단일 인텐트 실행 및 영수증 발급
    PUBLIC_OTLP_INGRESS  = "public.public_otlp_logs_export"    # OTLP 텔레메트리 수집
    PUBLIC_AUDIT_EVENT   = "public.public_audit_log"           # 감사 로그 ZK 증명 발급

    # --- Internal Endpoints (내부망/백엔드 통합 테스트용) ---
    INTERNAL_TRADE_INGRESS = "eco.exchange.submit_trade_intent"
    INTERNAL_EXECUTE_BILLED= "eco.profile.execute_billed_workload"
    INTERNAL_LEDGER_APPEND = "core.internal.append_to_stream"
    INTERNAL_ANCHOR_SEAL   = "core.internal.seal_state"


DEFAULT_FALLBACK_ROUTES: Dict[str, str] = {
    # Public Fallbacks
    TargetOp.PUBLIC_AGENT_EXECUTE: "/v1/public/agent/execute",
    TargetOp.PUBLIC_OTLP_INGRESS: "/v1/public/telemetry/logs",
    TargetOp.PUBLIC_AUDIT_EVENT: "/v1/public/audit/event",
    
    # Internal Fallbacks
    TargetOp.INTERNAL_TRADE_INGRESS: "/internal/v1/eco/exchange/order/ingress",
    TargetOp.INTERNAL_EXECUTE_BILLED: "/internal/v1/eco/profile/execute/billed",
    TargetOp.INTERNAL_LEDGER_APPEND: "/internal/v1/core/ledger/stream/append",
    TargetOp.INTERNAL_ANCHOR_SEAL: "/internal/v1/core/anchor/seal"
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
    # 테스트 시나리오에 맞게 빌더 이름들도 직관적으로 변경 가능
    otlp_builder: Callable[[bool], dict]
    agent_intent_builder: Callable[[bool], dict]   # 기존 trade_builder 대체 (Public Agent)
    ledger_builder: Callable[[str, bool], dict]    # Internal Ledger Append


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