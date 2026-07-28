# phi.ops.xelog.tester
import sys
import argparse
import asyncio
import time
from typing import Tuple, Dict
from dataclasses import dataclass

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from fastapi.routing import APIRoute

from topos.xelog.restapi import api as rest_app  

from phase.wasm.builder import WasmBuilder
from phase.wasm.tracer import WasmTracer
from watcher.dphi.scheme.runner import TrustlessWebRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("xelog.tester")

@dataclass
class E2EConfig:
    host: str = "localhost"
    port: int = 8000
    protocol: str = "http"

    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"

class RouteRegistry:
    def __init__(self, app):
        """
        FastAPI app 객체를 주입받아, 내부 라우팅 테이블을 함수명(name) 기준으로 매핑합니다.
        """
        self.app = app
        self._route_map = {
            route.name: route.path 
            for route in app.routes if isinstance(route, APIRoute)
        }

    def get_path(self, target_name: str, fallback: str) -> str:
        """라우터의 핸들러(함수) 이름으로 실제 Path를 찾아 반환합니다."""
        path = self._route_map.get(target_name)
        if not path:
            log.warning(f"[RouteRegistry] '{target_name}' handler not found in REST API. Using fallback: {fallback}")
            return fallback
        return path

    # 백엔드(edge)에 선언된 실제 함수명(예: def ingest_otlp(...))을 타겟팅합니다.
    # 만약 함수명을 정확히 모를 경우를 대비해 기존의 Path를 fallback으로 둡니다.
    @property
    def otlp_ingress(self): 
        return self.get_path("ingest_otlp_logs", "/v1/logs")
        
    @property
    def ledger_stream(self): 
        return self.get_path("append_ledger_stream", "/v1/ledger/stream/append")
        
    @property
    def anchor_seal(self): 
        return self.get_path("seal_epoch_anchor", "/anchor/v1/seal")
        
    @property
    def d3fi_ingress(self): 
        return self.get_path("ingress_d3fi_order", "/d3fi/v1/order/ingress")
        
    @property
    def d3fi_clearing(self): 
        return self.get_path("generate_d3fi_receipt", "/d3fi/v1/clearing/receipt/generate")


class ScenarioTester(TrustlessWebRunner):
    """구체적인 E2E 테스트 케이스 모음"""
    def __init__(self, config: E2EConfig):
        super().__init__(config.base_url)
        # restapi.py에서 가져온 FastAPI 애플리케이션(rest_app)을 주입
        self.routes = RouteRegistry(rest_app)

    async def run_genai_otlp(self) -> str:
        payload = {
            "resourceLogs": [{"resource": {"attributes": [{"key": "tenant_id", "value": {"stringValue": "tenant-01"}}]}}],
            "genai_metrics": {"tenant_id": "tenant-01", "model": "gpt-4", "usage": {"tokens": 2048}}
        }
        res = await self._run_api_case("OTLP Billing Ingress", "POST", self.routes.otlp_ingress, payload, 200)
        return res.headers.get("x-edge-content-hash", "failed") if res else "failed"

    async def run_d3fi_trade(self) -> str:
        ingress_req = {"agent_id": "agent-x", "action": "SWAP", "amount": "5000", "slippage": "0.005"}
        await self._run_api_case("D3Fi Trade Ingress", "POST", self.routes.d3fi_ingress, ingress_req, 200)
        
        entangled_state = "d3fi_state_hash_0x88"
        dummy_agent_key = ed25519.Ed25519PrivateKey.generate()
        signatures = self._sign_payload([dummy_agent_key], {"state": entangled_state})
        
        clearing_req = {"entangled_state": entangled_state, "signatures": signatures, "cost_metrics": {"gas": 21000}}
        res = await self._run_api_case("D3Fi Receipt Generation", "POST", self.routes.d3fi_clearing, clearing_req, 200)
        return entangled_state if res and res.status_code == 200 else "failed"

    async def run_ledger_stream_append(self) -> str:
        payload = {
            "stream_name": "stream_core_infrastructure",
            "events": [
                {"action": "SYSTEM_WARNING", "user_id": "autonomous_agent_1", "details": "node_lock_timeout"},
                {"action": "MEMORY_MONITOR", "user_id": "autonomous_agent_1", "details": "token_leak detected"}
            ],
            "verbose": True
        }
        res = await self._run_api_case("Ledger Stream Append", "POST", self.routes.ledger_stream, payload, 200)
        return res.json().get("result", {}).get("hash", "failed") if res and res.status_code == 200 else "failed"

    async def run_global_anchor(self, state_roots: Dict[str, str]):
        proposed_parity = {"state_roots": state_roots}
        signatures = self._sign_payload(self.committee_keys, proposed_parity)
        
        anchor_payload = {
            "receptor_id": "e2e-validator-node",
            "proposed_parity": proposed_parity,
            "parent_nexus_id": 1000, 
            "repos": {},
            "signers": self.committee_pubs,
            "signatures": signatures,
            "timestamp": int(time.time() * 1000)
        }
        await self._run_api_case("Anchor Epoch Seal", "POST", self.routes.anchor_seal, anchor_payload, 200)

class XelogTester:
    def __init__(self, config: E2EConfig):
        self.orchestrator = ScenarioTester(config)

    async def execute(self) -> Tuple[bool, str]:
        try:
            log.info(f"\n[WebTesterAdapter] Starting E2E HTTP Workflows on {self.orchestrator.base_url}...")
            
            state_roots = {
                "otlp_root": await self.orchestrator.run_genai_otlp(),
                "d3fi_root": await self.orchestrator.run_d3fi_trade(),
                "ledger_root": await self.orchestrator.run_ledger_stream_append()
            }
            
            if "failed" in state_roots.values():
                self.orchestrator.report()
                return False, "E2E Validation Failed: Roots failed to generate."

            await self.orchestrator.run_global_anchor(state_roots)
            self.orchestrator.report()
            
            if self.orchestrator.fail_count > 0:
                return False, "E2E Errors occurred during Global Anchor Execution."
                
            return True, ""
            
        except Exception as e:
            return False, f"Critical Exception: {str(e)}"
        finally:
            await self.orchestrator.client.aclose()

class XelogPipelineCLI:
    def __init__(self, config: E2EConfig):
        self.config = config
        self.log = get_emitter("xelog.cli")

    async def run_pipeline(self):
        self.log.info("[CLI] Starting Xelog Full E2E Pipeline (Build ➔ HTTP E2E ➔ Trace/Seal)...")
        
        self.log.info("[CLI] [Step 1] Running WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        if builder.rupture_confirmed:
            self.log.error("[CLI] Builder encountered a fatal rupture.")
            sys.exit(1)

        self.log.info("[CLI] [Step 2] Running HTTP E2E Tester via WasmTracer...")
        web_tester = XelogTester(self.config)
        tracer = WasmTracer(tester=web_tester)
        
        await tracer.trace() 

        if getattr(tracer, 'rupture_confirmed', False):
            self.log.error("[CLI] E2E Pipeline ended in a Rupture/Collapse state.")
            sys.exit(1)
            
        self.log.info("[CLI] E2E Pipeline executed & Lineage Sealed successfully.")

    @classmethod
    def run_cli(cls):
        parser = argparse.ArgumentParser(description="Xelog E2E HTTP Pipeline Tester")
        parser.add_argument("--host", type=str, default="localhost", help="Target API Host")
        parser.add_argument("--port", type=int, default=8000, help="Target API Port")
        
        args = parser.parse_args()
        config = E2EConfig(host=args.host, port=args.port)
        
        app = cls(config)
        try:
            asyncio.run(app.run_pipeline())
        except KeyboardInterrupt:
            app.log.warning("\n[CLI] Process interrupted by user. Shutting down gracefully...")
            sys.exit(0)

if __name__ == "__main__":
    XelogPipelineCLI.run_cli()