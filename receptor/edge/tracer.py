# receptor.edge.tracer
## @lineage: phase.edge.tracer
import uuid
import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Any

import httpx

from watcher.plane.emitter import flow_scope, get_emitter

log = get_emitter("edge.tracer")


@dataclass
class E2EConfig:
    host: str = "localhost"
    port: int = 8000
    protocol: str = "http"
    
    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"


@dataclass
class SceneConfig:
    otlp_builder: Callable[[bool], dict]
    agent_intent_builder: Callable[[bool], dict]
    ledger_builder: Callable[[str, bool], dict]
    trade_builder: Optional[Callable[[bool], dict]] = None 

class RouteRegistry:
    def __init__(self, app):
        def _flatten(routes):
            for r in routes:
                if hasattr(r, "routes"):
                    yield from _flatten(r.routes)
                else:
                    yield r
        
        self._routes = {r.name: r.path for r in _flatten(app.routes) if getattr(r, "name", None)}

    def url_for(self, target_name: str) -> str:
        return self._routes[target_name]

class HttpFlowTracer:
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
                    content_type = response.headers.get("content-type", "")
                    if "text" in content_type or "json" in content_type:
                        safe_text = response.text[:200] if hasattr(response, 'text') else "<Empty>"
                except Exception:
                    pass
                log.warning(f"{status_log}\n  └─ Body: {safe_text}")
            else:
                log.info(status_log)


@dataclass
class InjectPolicy:
    target_path: str
    latency_sec: float = 0.0                                    
    mock_status: Optional[int] = None                           
    mock_body: Optional[Any] = None                             
    request_mutator: Optional[Callable[[httpx.Request], None]] = None 


class ActiveFlowTracer(httpx.AsyncBaseTransport):
    def __init__(
        self, 
        underlying_transport: httpx.AsyncBaseTransport, 
        policies: Dict[str, InjectPolicy] = None
    ):
        self.underlying = underlying_transport
        self.policies = policies or {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        flow_id = f"transport_{uuid.uuid4().hex[:8]}"
        request.headers["x-active-flow-id"] = flow_id
        path_str = request.url.path

        policy = next((p for p in self.policies.values() if p.target_path in path_str), None)

        with flow_scope(flow_id=flow_id, phase="TRANSPORT_TX", bound="injector"):
            log.info(f"[Transport:TX] {request.method} {request.url}")
            
            if policy and policy.request_mutator:
                policy.request_mutator(request)
                log.warning(f"  [!] Active Mutation applied by policy for: {policy.target_path}")

            log.debug(f"  └─ Headers: {dict(request.headers)}")

        if policy and policy.latency_sec > 0:
            log.warning(f"[Transport:Inject] Halting stream for {policy.latency_sec}s (Artificial Latency)")
            await asyncio.sleep(policy.latency_sec)

        start_time = time.perf_counter()

        if policy and policy.mock_status:
            log.warning(f"[Transport:Inject] Short-circuiting! Returning mock status {policy.mock_status}")
            response = httpx.Response(
                status_code=policy.mock_status,
                json=policy.mock_body if isinstance(policy.mock_body, (dict, list)) else None,
                content=policy.mock_body if isinstance(policy.mock_body, (str, bytes)) else None,
                request=request
            )
        else:
            response = await self.underlying.handle_async_request(request)

        elapsed_sec = time.perf_counter() - start_time

        with flow_scope(flow_id=flow_id, phase="TRANSPORT_RX", bound="injector"):
            status_log = f"[Transport:RX] {response.status_code} {response.reason_phrase} (in {elapsed_sec:.3f}s)"
            if response.status_code >= 400 or policy is not None:
                log.warning(f"{status_log} - Stream kept intact for upstream layer")
            else:
                log.info(status_log)

        return response

    async def aclose(self):
        await self.underlying.aclose()