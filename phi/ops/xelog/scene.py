# phi.ops.xelog.scene
import abc
import time
from dataclasses import dataclass, field
from typing import Dict, Any, List

from cryptography.hazmat.primitives.asymmetric import ed25519

from phi.ops.xelog.router import E2EConfig, RouteRegistry
from watcher.xelog.rest import api as rest_app  

from watcher.dphi.scheme.runner import TrustlessWebRunner
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("xelog.scene")


# =====================================================================
# 1. State Context: 파이프라인을 관통하며 공유되는 상태 객체
# =====================================================================
@dataclass
class SceneContext:
    config: E2EConfig
    routes: RouteRegistry
    runner: TrustlessWebRunner
    
    # 워크플로우 진행 중 채워질 State Roots
    state_roots: Dict[str, str] = field(default_factory=dict)


# =====================================================================
# 2. Phases: 블록체인 트랜잭션 처리 단계처럼 각 시나리오를 캡슐화
# =====================================================================
class ScenePhase(abc.ABC):
    @abc.abstractmethod
    async def execute(self, ctx: SceneContext):
        pass


class HeadSmokePhase(ScenePhase):
    """[Step 1] API Head & MCP Connectivity Sweep"""
    async def execute(self, ctx: SceneContext):
        log.info("\n--- [Phase 1] API Head & MCP Connectivity Sweep ---")
        runner = ctx.runner
        
        # 1-1. OpenAPI Schema
        res = await runner.client.get(f"{runner.base_url}/openapi.json")
        if res.status_code == 200:
            runner.success_count += 1
            route_count = len(ctx.routes.get_all_routes())
            log.info(f"  [PASS] OpenAPI Schema generated. ({route_count} routes)")
        else:
            runner.fail_count += 1
            log.error("  [FAIL] OpenAPI Schema validation failed.")
            raise RuntimeError("API Head is unreachable")

        # 1-2. MCP Server (SSE)
        mcp_res = await runner.client.post(f"{runner.base_url}/mcp/sse")
        if mcp_res.status_code in [405, 400]: 
            runner.success_count += 1
            log.info(f"  [PASS] SecureMCPServer active at /mcp/sse (Status: {mcp_res.status_code}).")
        else:
            runner.fail_count += 1
            log.error(f"  [FAIL] MCP Server connectivity error (Status: {mcp_res.status_code}).")
            raise RuntimeError("MCP Server unreachable")


class OtlpIngressPhase(ScenePhase):
    """[Step 2] OTLP Telemetry Ingress & Seal"""
    async def execute(self, ctx: SceneContext):
        log.info("\n--- [Phase 2] OTLP Telemetry Ingress & WASM Seal ---")
        payload = {
            "resourceLogs": [{"resource": {"attributes": [{"key": "tenant_id", "value": {"stringValue": "tenant-01"}}]}}],
            "genai_metrics": {"tenant_id": "tenant-01", "model": "gpt-4", "usage": {"tokens": 2048}}
        }
        path = ctx.routes.url_for("otlp_logs_export", fallback="/hub/v1/logs")
        res = await ctx.runner._run_api_case("OTLP Usage Ingress", "POST", path, payload, 200)
        
        if not res or res.status_code != 200:
            raise RuntimeError("OTLP Ingress Failed")
            
        ctx.state_roots["otlp_root"] = res.headers.get("x-edge-content-hash", "failed")


class D3FiExchangePhase(ScenePhase):
    """[Step 3] D3Fi P2P Trade Intent & Clearing Settlement"""
    async def execute(self, ctx: SceneContext):
        log.info("\n--- [Phase 3] D3Fi P2P Trade & Settlement (ExchangeNet) ---")
        
        # 3-1. Trade Ingress
        ingress_req = {"agent_id": "agent-x", "action": "SWAP", "amount": "5000", "slippage": "0.005"}
        ingress_path = ctx.routes.url_for("submit_trade_intent", fallback="/v1/a2a/exchange/order/ingress")
        await ctx.runner._run_api_case("D3Fi Trade Ingress", "POST", ingress_path, ingress_req, 200)
        
        # 3-2. Receipt Generation
        entangled_state = "d3fi_state_hash_0x88"
        dummy_agent_key = ed25519.Ed25519PrivateKey.generate()
        signatures = ctx.runner._sign_payload([dummy_agent_key], {"state": entangled_state})
        
        clearing_req = {"entangled_state": entangled_state, "signatures": signatures, "cost_metrics": {"gas": 21000}}
        clearing_path = ctx.routes.url_for("generate_external_receipt", fallback="/v1/a2a/exchange/clearing/receipt/generate")
        res = await ctx.runner._run_api_case("D3Fi Receipt Generation", "POST", clearing_path, clearing_req, 200)
        
        if not res or res.status_code != 200:
            raise RuntimeError("Exchange Settlement Failed")
            
        ctx.state_roots["exchange_root"] = entangled_state


class LedgerAppendPhase(ScenePhase):
    """[Step 4] Immutable Ledger Stream Bulk Append"""
    async def execute(self, ctx: SceneContext):
        log.info("\n--- [Phase 4] Immutable Ledger Stream Append ---")
        payload = {
            "stream_name": "stream_core_infrastructure",
            "events": [
                {"action": "SYSTEM_WARNING", "user_id": "agent_1", "details": "node_lock_timeout"},
                {"action": "MEMORY_MONITOR", "user_id": "agent_1", "details": "token_leak detected"}
            ],
            "verbose": True
        }
        path = ctx.routes.url_for("append_to_stream", fallback="/v1/ledger/stream/append")
        res = await ctx.runner._run_api_case("Ledger Stream Append", "POST", path, payload, 200)
        
        if not res or res.status_code != 200:
            raise RuntimeError("Ledger Append Failed")
            
        ctx.state_roots["ledger_root"] = res.json().get("result", {}).get("hash", "failed")


class GlobalAnchorPhase(ScenePhase):
    """[Step 5] Global Anchor: 모든 상태 루트를 모아 Epoch Sealing"""
    async def execute(self, ctx: SceneContext):
        log.info("\n--- [Phase 5] Global Anchor (Epoch Sealing) ---")
        
        if "failed" in ctx.state_roots.values():
            raise RuntimeError("Cannot anchor: Sub-state roots are missing or failed.")
            
        proposed_parity = {"state_roots": ctx.state_roots}
        signatures = ctx.runner._sign_payload(ctx.runner.committee_keys, proposed_parity)
        
        anchor_payload = {
            "receptor_id": "e2e-validator-node",
            "proposed_parity": proposed_parity,
            "parent_nexus_id": 1000, 
            "repos": {},
            "signers": ctx.runner.committee_pubs,
            "signatures": signatures,
            "timestamp": int(time.time() * 1000)
        }
        path = ctx.routes.url_for("seal_epoch_anchor", fallback="/v1/anchor/seal")
        res = await ctx.runner._run_api_case("Anchor Epoch Seal", "POST", path, anchor_payload, 200)
        
        if not res or res.status_code != 200:
            raise RuntimeError("Global Anchor Failed")

class SceneRunner:
    """Xelog Immutable Network E2E 통합 오케스트레이터"""
    def __init__(self, config: E2EConfig):
        self.config = config
        self.routes = RouteRegistry(rest_app)
        
        # 내부 상태 및 통신을 전담하는 핵심 러너
        self.web_runner = TrustlessWebRunner(config.base_url)
        
        # 블록체인/트랜잭션 라이프사이클과 유사하게 워크플로우 정의
        self.workflow = [
            HeadSmokePhase(),
            OtlpIngressPhase(),
            D3FiExchangePhase(),
            LedgerAppendPhase(),
            GlobalAnchorPhase()
        ]

    # 외부(tester.py)에서 httpx.AsyncClient(ASGITransport)를 주입할 수 있도록 브릿지 제공
    @property
    def client(self):
        return self.web_runner.client
        
    @client.setter
    def client(self, client_instance):
        self.web_runner.client = client_instance

    async def execute(self) -> bool:
        """단일 세션 안에서 구성된 E2E 워크플로우(Phase)를 순차적으로 관통합니다."""
        ctx = SceneContext(
            config=self.config,
            routes=self.routes,
            runner=self.web_runner
        )
        
        with flow_scope(phase="E2E_SCENE_NET"):
            log.info("\n=== [START] Xelog Immutable Network E2E Scenarios ===")
            try:
                for phase in self.workflow:
                    await phase.execute(ctx)
                
                log.info("\n[SUCCESS] Entire E2E Network Scenario Completed without Ruptures.")
                return True
                
            except Exception as e:
                log.exception(f"\n[FAIL] E2E Pipeline aborted during execution: {e}")
                return False
                
            finally:
                # 결과 리포팅
                self.web_runner.report()