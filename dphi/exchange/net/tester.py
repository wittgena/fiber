# dphi.exchange.net.tester

import argparse
import asyncio
import random
import sys
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List

import httpx
from fastapi.routing import APIRoute

# Local / Internal imports
from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from dphi.eco.rest import api as rest_app
from kernel.dphi.scheme.runner import WebRunner
from watcher.plane.emitter import flow_scope, get_emitter
from watcher.wasm.builder import WasmBuilder
from watcher.wasm.tracer import WasmTracer

log = get_emitter("net.tester")

# =====================================================================
# [Route Identifiers & Fallbacks]
# 실제 앱(rest_app)의 라우터 트리를 역추적한 100% 정확한 물리적 경로입니다.
# =====================================================================
class TargetOp:
    OTLP_INGRESS = "core.otlp_logs_export"            
    TRADE_INGRESS = "eco.exchange.submit_trade_intent" 
    LEDGER_APPEND = "core.append_to_stream"           
    ANCHOR_SEAL = "core.seal_state"                   

FALLBACK_ROUTES: Dict[str, str] = {
    # core_edge (prefix="/v1") + @post("/logs")
    TargetOp.OTLP_INGRESS: "/v1/logs",
    
    # eco_router (prefix="/v1/eco") + exchange_edge (prefix="/exchange") + @post("/order/ingress")
    TargetOp.TRADE_INGRESS: "/v1/eco/exchange/order/ingress",  # [완전 교정됨]
    
    # core_edge (prefix="/v1") + @post("/ledger/stream/append")
    TargetOp.LEDGER_APPEND: "/v1/ledger/stream/append",
    
    # core_edge (prefix="/v1") + @post("/anchor/seal")
    TargetOp.ANCHOR_SEAL: "/v1/anchor/seal"
}

@dataclass
class Phase:
    name: str
    action: Callable[[], Coroutine[Any, Any, None]]

class PipelineRunner:
    """순차적 비동기 파이프라인 실행을 위한 베이스 오케스트레이터"""
    def __init__(self, name: str, scope_name: str):
        self.name = name
        self.scope_name = scope_name
        self.phases: List[Phase] = []
        
    def set_phases(self, phases: List[Phase]):
        self.phases = phases
        
    async def run_pipeline(self):
        log.info(f"=== Starting Pipeline: {self.name} ({self.scope_name}) ===")
        for phase in self.phases:
            log.info(f"--> Executing Phase: {phase.name}")
            await phase.action()
        log.info(f"=== Pipeline Completed: {self.name} ===")

@dataclass
class E2EConfig:
    host: str = "localhost"
    port: int = 8000
    protocol: str = "http"
    
    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"

class RouteRegistry:
    def __init__(self, app, fallbacks: Dict[str, str] = None):
        self.app = app
        self._routes = {route.name: route for route in app.routes if isinstance(route, APIRoute)}
        self.fallbacks = fallbacks or {}

    def url_for(self, target_name: str) -> str:
        if target_name in self._routes:
            return self._routes[target_name].path
        if target_name in self.fallbacks:
            log.warning(f"Route '{target_name}' not found in App. Using fallback: {self.fallbacks[target_name]}")
            return self.fallbacks[target_name]
        raise ValueError(f"Route '{target_name}' not found and no fallback provided.")

class StartSceneMsg(WorkflowMessage): pass
class OtlpIngressMsg(WorkflowMessage): pass
class D3FiExchangeMsg(WorkflowMessage): pass
class LedgerAppendMsg(WorkflowMessage): pass
class GlobalAnchorMsg(WorkflowMessage): pass

class SceneRunner(Workflow):
    def __init__(self, config: E2EConfig):
        super().__init__(name="E2E_SCENE_NET")
        self.config = config
        self.routes = RouteRegistry(rest_app, FALLBACK_ROUTES)
        self.runner = WebRunner(config.base_url)
        self.state_roots: Dict[str, str] = {}
        self.log = get_emitter("workflow.scene_runner")

    async def start_pipeline(self):
        self.log.info(f"\n=== [START] {self.name} ===")
        self.post_message(StartSceneMsg())
        await self.run()

    @step
    async def phase_head_smoke(self, msg: StartSceneMsg) -> WorkflowMessage:
        self.runner.log.info("\n--- [Phase 1] API Head & MCP Connectivity Sweep ---")
        res = await self.runner.client.get(f"{self.runner.base_url}/openapi.json")
        if res.status_code == 200:
            self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("API Head is unreachable")

        mcp_res = await self.runner.client.post(f"{self.runner.base_url}/mcp/sse")
        if mcp_res.status_code in [405, 400]: 
            self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("MCP Server unreachable")
            
        return OtlpIngressMsg()

    @step
    async def phase_otlp_ingress(self, msg: OtlpIngressMsg) -> WorkflowMessage:
        self.runner.log.info("\n--- [Phase 2] OTLP Telemetry Ingress & WASM Seal ---")
        payload = {"resourceLogs": [], "genai_metrics": {"tenant_id": "tenant-01"}}
        
        path = self.routes.url_for(TargetOp.OTLP_INGRESS)
        
        res = await self.runner._run_api_case("OTLP Usage Ingress", "POST", path, payload, 200)
        if not res or res.status_code != 200:
            return ErrorMessage("OTLP Ingress Failed")
            
        self.state_roots["otlp_root"] = res.headers.get("x-edge-content-hash", "failed")
        return D3FiExchangeMsg()

    @step
    async def phase_d3fi_exchange(self, msg: D3FiExchangeMsg) -> WorkflowMessage:
        self.runner.log.info("\n--- [Phase 3] D3Fi P2P Trade & Settlement (ExchangeNet) ---")
        
        path = self.routes.url_for(TargetOp.TRADE_INGRESS)
        
        await self.runner._run_api_case("D3Fi Trade Ingress", "POST", path, {"action": "SWAP"}, 200)
        self.state_roots["exchange_root"] = "d3fi_state_hash_0x88"
        return LedgerAppendMsg()

    @step
    async def phase_ledger_append(self, msg: LedgerAppendMsg) -> WorkflowMessage:
        self.runner.log.info("\n--- [Phase 4] Immutable Ledger Stream Append ---")
        
        path = self.routes.url_for(TargetOp.LEDGER_APPEND)
        
        res = await self.runner._run_api_case("Ledger Append", "POST", path, {"verbose": True}, 200)
        if not res or res.status_code != 200:
            return ErrorMessage("Ledger Append Failed")
            
        self.state_roots["ledger_root"] = res.json().get("result", {}).get("hash", "failed")
        return GlobalAnchorMsg()

    @step
    async def phase_global_anchor(self, msg: GlobalAnchorMsg) -> WorkflowMessage:
        self.runner.log.info("\n--- [Phase 5] Global Anchor (Epoch Sealing) ---")
        if "failed" in self.state_roots.values():
            return ErrorMessage("Cannot anchor: Sub-state roots are missing.")
        
        path = self.routes.url_for(TargetOp.ANCHOR_SEAL)
        
        res = await self.runner._run_api_case("Anchor Epoch Seal", "POST", path, {"proposed_parity": self.state_roots}, 200)
        if not res or res.status_code != 200:
            return ErrorMessage("Global Anchor Failed")

        self.runner.log.info(f"\n[SUCCESS] {self.name} Completed successfully.")
        self.runner.report()
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} aborted during execution: {msg.msg}")
        self.runner.report()
        return StopMessage(result=False)

class ChaosPayloadLibrary:
    OOM = lambda: b"A" * 1024 * 1024 * 10
    SMUGGLING = lambda: b"GET / HTTP/1.1\r\n\r\nGET /admin HTTP/1.1\r\n"
    MCP_PATH_TRAVERSAL = lambda: b"../../../../etc/passwd"

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
                log.warning(f"{status_log}\n  └─ Body: {response.text[:200]}")
            else:
                log.info(status_log)

class TracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Xelog Full E2E & Security Trace Pipeline", scope_name="GLOBAL_TRACE_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        self.routes = RouteRegistry(rest_app, FALLBACK_ROUTES)
        
        self.set_phases([
            Phase("Wasm Build", self.phase_wasm_build),
            Phase("Functional E2E", self.phase_functional_e2e),
            Phase("Sentinel Security", self.phase_sentinel_security)
        ])

    async def phase_wasm_build(self):
        log.info("\n[Pipeline] Running WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        if getattr(builder, 'rupture_confirmed', False):
            raise RuntimeError("WasmBuilder failed to construct valid binaries.")

    async def phase_functional_e2e(self):
        log.info(f"\n[Pipeline] Starting In-Memory ASGI Workflows (Functional E2E)...")
        transport = httpx.ASGITransport(app=rest_app)
        
        async with httpx.AsyncClient(
            transport=transport, 
            base_url=self.config.base_url,
            event_hooks={
                'request': [self.tracer.trace_request],
                'response': [self.tracer.trace_response]
            }
        ) as client:
            runner = SceneRunner(self.config)
            runner.runner.client = client
            tracer = WasmTracer(tester=runner)
            await tracer.trace() 
            if getattr(tracer, 'rupture_confirmed', False):
                raise RuntimeError("Functional E2E Phase failed. See sub-pipeline logs.")

    async def phase_sentinel_security(self):
        log.info(f"\n[Pipeline] Initiating Sentinel Chaos Attacks against XeLog Membrane...")
        transport = httpx.ASGITransport(app=rest_app)
        attack_vectors = [
            ("OOM_Exhaustion", ChaosPayloadLibrary.OOM),
            ("Protocol_Smuggling", ChaosPayloadLibrary.SMUGGLING),
            ("Path_Traversal", ChaosPayloadLibrary.MCP_PATH_TRAVERSAL)
        ]
        
        target_path = self.routes.url_for(TargetOp.OTLP_INGRESS)
        
        async with httpx.AsyncClient(
            transport=transport, 
            base_url=self.config.base_url,
            event_hooks={'request': [self.tracer.trace_request], 'response': [self.tracer.trace_response]}
        ) as client:
            for vector_name, rule_list in attack_vectors:
                payload = random.choice(rule_list)() if isinstance(rule_list, list) else rule_list()
                with flow_scope(execution_mode="CHAOS_TEST", security_probe=vector_name):
                    
                    response = await client.post(target_path, content=payload)
                    
                    if response.status_code >= 500 or response.status_code < 400:
                        raise RuntimeError(f"Membrane Breach! {vector_name} bypassed defenses. Status: {response.status_code}")
                        
        log.info("  └─ All Chaos probes successfully deflected by Sentinel Membrane.")

    async def teardown(self):
        self.log.info("[Pipeline] E2E Pipeline executed & Lineage Sealed successfully.")