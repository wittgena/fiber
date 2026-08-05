# dphi.exchange.net.tracer
import argparse
import asyncio
import random
import sys
import uuid
import time
import hashlib
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional
import httpx
from fastapi.routing import APIRoute

from dphi.eco.rest import api as rest_app, lifespan 
from dphi.exchange.mock.net import MockNetBuilder, MockNotarySwarm
from dphi.exchange.mock.config import mock_env
from dphi.exchange.chaos.injector import HttpChaosLibrary 

from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.dphi.scheme.runner import WebRunner
from kernel.dphi.adapter.state import StateAdapter
from watcher.wasm.builder import WasmBuilder
from watcher.wasm.tracer import WasmTracer
from watcher.plane.emitter import flow_scope, get_emitter

log = get_emitter("net.tracer")

class TargetOp:
    OTLP_INGRESS = "core.otlp_logs_export"            
    TRADE_INGRESS = "eco.exchange.submit_trade_intent" 
    LEDGER_APPEND = "core.append_to_stream"            
    ANCHOR_SEAL = "core.seal_state"                    

FALLBACK_ROUTES: Dict[str, str] = {
    TargetOp.OTLP_INGRESS: "/v1/logs",
    TargetOp.TRADE_INGRESS: "/v1/eco/exchange/order/ingress",
    TargetOp.LEDGER_APPEND: "/v1/ledger/stream/append",
    TargetOp.ANCHOR_SEAL: "/v1/anchor/seal"
}

@dataclass
class Phase:
    name: str
    action: Callable[[], Coroutine[Any, Any, None]]

class PipelineRunner:
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
            log.warning(f"Route '{target_name}' not found. Using fallback: {self.fallbacks[target_name]}")
            return self.fallbacks[target_name]
        raise ValueError(f"Route '{target_name}' not found and no fallback provided.")

class StartSceneMsg(WorkflowMessage): pass
class OtlpIngressMsg(WorkflowMessage): pass
class D3FiExchangeMsg(WorkflowMessage): pass
class LedgerAppendMsg(WorkflowMessage): pass
class GlobalAnchorMsg(WorkflowMessage): pass

class SceneRunner(Workflow):
    def __init__(self, config: E2EConfig, client: httpx.AsyncClient, inject_faults: bool = False):
        super().__init__(name="E2E_SCENE_NET")
        self.config = config
        self.inject_faults = inject_faults
        self.routes = RouteRegistry(rest_app, FALLBACK_ROUTES)
        self.runner = WebRunner(config.base_url, client=client)
        self.state_roots: Dict[str, str] = {}
        self.log = get_emitter("workflow.scene_runner")
        self.notary_swarm = MockNotarySwarm(size=3)

    async def execute(self):
        mode_str = "Negative/Faults" if self.inject_faults else "Golden Path"
        self.log.info(f"\n=== [START] {self.name} ({mode_str}) ===")
        
        if not self.inject_faults:
            self.log.info(f"  └─ Settlement Sink: Chain {mock_env.settlement_target.chain_id} (Receptor: {mock_env.settlement_target.nexus_contract_address})")
            self.log.info(f"  └─ Exchange Agents: {mock_env.agents.alpha.did} ⟷ {mock_env.agents.beta.did}")
            self.log.info(f"  └─ Export Notaries: {len(self.notary_swarm.public_keys)} Notary Nodes Loaded")

        self.post_message(StartSceneMsg())
        await self.run()

    @step
    async def phase_head_smoke(self, msg: StartSceneMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1] API Head & MCP Connectivity Sweep ---")
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
        self.log.info("\n--- [Phase 2] OTLP Telemetry Ingress & WASM Seal ---")
        path = self.routes.url_for(TargetOp.OTLP_INGRESS)
        
        if self.inject_faults:
            payload = {"garbage_field_missing_required_keys": True}
            expected_status = 422
            test_desc = "OTLP Ingress (Membrane Strict Block Test)"
        else:
            payload = MockNetBuilder.otlp_payload(is_malformed=False)
            expected_status = 200
            test_desc = "OTLP Ingress (Golden Path)"
        
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
            return ErrorMessage(f"OTLP Ingress Check Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
            
        if not self.inject_faults:
            self.state_roots["otlp_root"] = res.headers.get("x-edge-content-hash", "0x_default_otlp_hash")
        return D3FiExchangeMsg()

    @step
    async def phase_d3fi_exchange(self, msg: D3FiExchangeMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 3] D3Fi P2P Trade & Settlement (ExchangeNet) ---")
        path = self.routes.url_for(TargetOp.TRADE_INGRESS)
        
        if self.inject_faults:
            payload = {"invalid_trade": "missing_all_required_data"}
            expected_status = 422
            test_desc = "D3Fi Trade Ingress (Membrane Strict Block Test)"
        else:
            payload = MockNetBuilder.trade_intent(should_fail_policy=False)
            expected_status = 200
            test_desc = "D3Fi Trade Ingress (Golden Path)"
        
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
             return ErrorMessage(f"D3Fi Ingress Check Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
             
        if not self.inject_faults:
            self.state_roots["exchange_root"] = f"d3fi_state_hash_{uuid.uuid4().hex[:8]}"
        return LedgerAppendMsg()

    @step
    async def phase_ledger_append(self, msg: LedgerAppendMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 4] Immutable Ledger Stream Append ---")
        path = self.routes.url_for(TargetOp.LEDGER_APPEND)
        
        if self.inject_faults: 
            payload = {"stream_name": "missing_events_field_test"}
            expected_status = 422
            test_desc = "Ledger Append (Membrane Strict Block Test)"
        else:
            exchange_root = self.state_roots.get("exchange_root", "0x00")
            payload = MockNetBuilder.ledger_append("A2A_TRADE_SETTLEMENT", exchange_root)
            expected_status = 200
            test_desc = "Ledger Append (Golden Path)"
            
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
            return ErrorMessage(f"Ledger Append Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
            
        if self.inject_faults:
            return GlobalAnchorMsg()

        self.state_roots["ledger_root"] = res.json().get("result", {}).get("hash", "0x_default_ledger_hash")
        return GlobalAnchorMsg()

    @step
    async def phase_global_anchor(self, msg: GlobalAnchorMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 5] Export Plug: Attest & Submit Anchor ---")
        if self.inject_faults:
            self.log.info(f"\n[SUCCESS] {self.name} Fault-Injection Scenario Completed.")
            self.runner.report()
            return StopMessage(result=True)
            
        required_keys = ["otlp_root", "exchange_root", "ledger_root"]
        if not all(k in self.state_roots for k in required_keys):
            return ErrorMessage("Cannot anchor: Sub-state roots are missing.")
        
        parity_triplet = {"topos_id": f"epoch_{time.strftime('%Y%m%d')}_batch_01", "nexus_id": 1, "phase_id": 1}
        receptor_id = mock_env.settlement_target.nexus_contract_address
        parent_state_hash = "0x0000000000000000000000000000000000000000000000000000000000000000"
        
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, 
            parent_nexus_id=0, 
            parent_commit_id=parent_state_hash, 
            repos=self.state_roots, 
            cached_states={}
        )
        
        # 🌟 정렬: 외부 제출용으로 가짜 공증인(Notary)들의 서명을 겉면에 포장(Wrap)합니다.
        commit_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(anchor_commit)).hexdigest().encode('utf-8')
        signatures = self.notary_swarm.attest_payload(commit_hash)
        
        payload = {
            "receptor_id": receptor_id,  
            "proposed_parity": parity_triplet,
            "parent_nexus_id": 0,
            "self_parent_state": parent_state_hash,
            "repos": self.state_roots,
            "signers": self.notary_swarm.public_keys, 
            "signatures": signatures,
            "timestamp": int(time.time() * 1000)
        }
        
        path = self.routes.url_for(TargetOp.ANCHOR_SEAL)
        res = await self.runner._run_api_case("Export Attested Anchor", "POST", path, payload, 200)
        
        if not res or res.status_code != 200:
            return ErrorMessage("Global Anchor Export Failed")

        self.log.info(f"\n[SUCCESS] {self.name} Completed successfully.")
        self.runner.report()
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} aborted during execution: {msg.msg}")
        self.runner.report()
        return StopMessage(result=False)

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
                safe_text = response.text[:200] if hasattr(response, 'text') else "<Binary/Unreadable Body>"
                log.warning(f"{status_log}\n  └─ Body: {safe_text}")
            else:
                log.info(status_log)

class TracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Full E2E & Security Trace Pipeline", scope_name="GLOBAL_TRACE_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        self.routes = RouteRegistry(rest_app, FALLBACK_ROUTES)
        
        self.set_phases([
            Phase("Wasm Build", self.phase_wasm_build),
            Phase("Functional E2E (Golden Path)", self.phase_functional_e2e_golden),
            Phase("Functional E2E (Negative Path)", self.phase_functional_e2e_negative),
            Phase("Sentinel Security (Chaos Membrane)", self.phase_sentinel_security)
        ])

    async def phase_wasm_build(self):
        log.info("\n[Pipeline] Running WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        if getattr(builder, 'rupture_confirmed', False):
            raise RuntimeError("WasmBuilder failed to construct valid binaries.")

    async def _run_scene(self, inject_faults: bool):
        transport = httpx.ASGITransport(app=rest_app)
        
        async with lifespan(rest_app):
            async with httpx.AsyncClient(
                transport=transport, 
                base_url=self.config.base_url,
                event_hooks={'request': [self.tracer.trace_request], 'response': [self.tracer.trace_response]}
            ) as client:
                runner = SceneRunner(self.config, client=client, inject_faults=inject_faults)
                if hasattr(rest_app.state, 'config'):
                    rest_app.state.config.committee_pubs = runner.notary_swarm.public_keys
                    
                tracer = WasmTracer(tester=runner)
                await tracer.trace() 
                
                has_rupture = getattr(tracer, 'rupture_confirmed', False)
                if has_rupture or runner.runner.fail_count > 0:
                    raise RuntimeError(f"Functional E2E Phase failed (Fault Inject: {inject_faults}). See logs for details.")

    async def phase_functional_e2e_golden(self):
        log.info(f"\n[Pipeline] Starting In-Memory ASGI Workflows (Golden Path)...")
        await self._run_scene(inject_faults=False)

    async def phase_functional_e2e_negative(self):
        log.info(f"\n[Pipeline] Starting In-Memory ASGI Workflows (Negative Faults Injection)...")
        await self._run_scene(inject_faults=True)

    async def phase_sentinel_security(self):
        log.info(f"\n[Pipeline] Initiating Sentinel Chaos Attacks against XeLog Membrane...")
        transport = httpx.ASGITransport(app=rest_app)
        attack_vectors = HttpChaosLibrary.get_all_vectors()
        target_path = self.routes.url_for(TargetOp.OTLP_INGRESS)
        
        async with lifespan(rest_app):
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
                            raise RuntimeError(f"Membrane Breach! '{vector_name}' bypassed defenses. Received Status: {response.status_code}")
                            
            log.info("  └─ All Chaos probes successfully deflected by Sentinel Membrane.")