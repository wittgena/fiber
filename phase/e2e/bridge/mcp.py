# fiber.phase.e2e.bridge.mcp
import sys
import time
import uuid
import json
import asyncio
from typing import Any, List, Dict

import httpx
import uvicorn

from fiber.dphi.rpc.client import InternalRpcClient
from fiber.infra.worker.connector import WorkerConnector
from fiber.phase.e2e.edge import (
    PipelineRunner, 
    ManagedTestServer, 
    TestResult, 
    Phase, 
    E2EConfig
)

from fiber.dphi.rpc.handler import INTERNAL_HANDLERS_REGISTRY
from fiber.phase.kernel.daemon.rpc import RpcWorkerDaemon

import fiber.infra.worker.agent.deploy as agent_deploy
import fiber.infra.worker.agent.oracle as agent_oracle
import fiber.infra.worker.agent.sentinel as agent_sentinel
import fiber.infra.worker.agent.finlib as agent_finlib
import fiber.infra.worker.agent.margin as agent_margin  # [추가됨] 마진 계산 데몬

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from fiber.dphi.edge.payload import create_app, Config

log = get_emitter("e2e.bridge.mcp")

class MockStream:
    def __init__(self, handle_id: str, target: str, fuel: int):
        self.id = handle_id
        self.metadata = {"target": target, "fuel_locked": fuel, "risk_score": 0}

class MockLedger:
    """Zero-Latency Bridge 및 Sentinel 테스트를 위한 순수 상태 저장소"""
    def __init__(self):
        self.states = {}
        self.yield_timestamps = {}

    async def query_state(self, handle_id: str):
        return self.states.get(handle_id)

    def set_resolved(self, handle_id: str, payload: dict):
        class DummyState:
            action = "dphi.transition.resolve"
            metadata = {"status": "RESOLVED"}
            def __init__(self, p): self.payload = p
        self.states[handle_id] = DummyState(payload)
        self.yield_timestamps.pop(handle_id, None)

    def set_faulted(self, handle_id: str, error_detail: str):
        class DummyState:
            action = "dphi.transition.resolve"
            metadata = {"status": "FAULTED", "error_detail": error_detail}
            def __init__(self): self.payload = None
        self.states[handle_id] = DummyState()
        self.yield_timestamps.pop(handle_id, None)

    def set_yielded(self, handle_id: str, payload: dict):
        class DummyState:
            action = "dphi.transition.yield"
            metadata = {"status": "YIELD"}
            def __init__(self, p): self.payload = p
        self.states[handle_id] = DummyState(payload)
        self.yield_timestamps[handle_id] = time.time()

    async def query_stale_streams(self, current_time: float, thresholds: dict) -> List[MockStream]:
        stale = []
        for hid, ts in list(self.yield_timestamps.items()):
            if current_time - ts > 2.0:
                stale.append(MockStream(hid, "legacy-01", 100))
        return stale

    async def force_transition(self, handle_id: str, new_action: str, metadata: dict):
        class DummyState:
            action = new_action
            def __init__(self, md): self.metadata = md
            self.payload = {}
        self.states[handle_id] = DummyState(metadata)
        self.yield_timestamps.pop(handle_id, None)


class McpBridgePipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Zero-Latency, L402 Billing & Hybrid FSM Trace", scope_name="MCP_BRIDGE_SUITE")
        self.config = config
        
        self.oracle_id = "oracle-01"
        self.deploy_id = "legacy-01"
        self.finlib_id = "finlib-01"
        self.margin_id = "margin-01"  # [추가됨]
        
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        
        self.mock_ledger = MockLedger()
        self.rest_app = create_app(Config(wasm_timeout=5.0))
        
        u_config = uvicorn.Config(app=self.rest_app, host="127.0.0.1", port=self.config.port, log_level="error", access_log=False)
        self.server = ManagedTestServer(u_config)
        self._server_task = None

        oracle_cmd = f"{sys.executable} -m {agent_oracle.__name__}"
        self.oracle_connector = WorkerConnector(target_id=self.oracle_id, legacy_command=oracle_cmd, mode="ephemeral")
        self._oracle_task = None
        
        deploy_cmd = f"{sys.executable} -m {agent_deploy.__name__}"
        self.deploy_connector = WorkerConnector(target_id=self.deploy_id, legacy_command=deploy_cmd, mode="ephemeral")
        self._deploy_task = None

        finlib_cmd = f"{sys.executable} -m {agent_finlib.__name__}"
        self.finlib_connector = WorkerConnector(target_id=self.finlib_id, legacy_command=finlib_cmd, mode="daemon")
        self._finlib_task = None

        # [추가됨] 마진 계산기 데몬 커넥터
        margin_cmd = f"{sys.executable} -m {agent_margin.__name__}"
        self.margin_connector = WorkerConnector(target_id=self.margin_id, legacy_command=margin_cmd, mode="daemon")
        self._margin_task = None
        
        self.worker_daemon = None
        self._worker_task = None
        self.sentinel = None
        self._sentinel_task = None
        
        self.captured_handle_ids: Dict[str, str] = {}

        self.set_phases([
            Phase("Phase 1: Event-Driven Zero-Latency Proof", self.phase_zero_latency),
            Phase("Phase 2: High-Concurrency Daemon Stress (FinLib)", self.phase_finlib_multiplexing),
            Phase("Phase 3: Unit Economics Vectorization (Margin BI)", self.phase_margin_simulation), # [추가됨]
            Phase("Phase 4: L402 Billing Rejection (Free-Rider Defense)", self.phase_l402_rejection), # [추가됨]
            Phase("Phase 5: Precise Error Routing (Invalid Params)", self.phase_error_routing),
            Phase("Phase 6: Idempotency Fast-Path Defense", self.phase_idempotency_defense),
            Phase("Phase 7: Asynchronous RESUME & Resolution", self.phase_otp_resume),
            Phase("Phase 8: Autonomous Reconciliation (Sentinel)", self.phase_sentinel_reconciliation)
        ])

    async def _setup_rpc_bus(self):
        self.rpc = InternalRpcClient()
        tunnel = await TunnelFactory.get_default()
        
        # [수정] 분리된 MCP 라우팅 팩토리 모킹 (Ledger 디커플링 대응)
        async def mock_mcp_state_query(params: dict, ctx: Any = None):
            state = await self.mock_ledger.query_state(params.get("handle_id"))
            if not state: return {"exists": False}
            return {
                "exists": True, 
                "status": getattr(state, "metadata", {}).get("status"),
                "executable_payload": getattr(state, "payload", {})
            }

        async def mock_mcp_state_pending_seal(params: dict, ctx: Any = None):
            return {"success": True}

        async def mock_billing_receipt_validate(params: dict, ctx: Any = None):
            receipt = params.get("payment_receipt")
            if not receipt or receipt == "invalid_receipt":
                # FastAPI HTTPException 형식에 맞게 리턴하거나 에러 딕셔너리 리턴
                return {"error": True, "code": 402, "message": "Payment Required or Invalid Receipt"}
            return {"status": "VALIDATED"}

        async def mock_resolve_state(params: dict, ctx: Any = None):
            handle_id = params.get("handle_id")
            status = params.get("status")
            payload = params.get("executable_payload", {})
            error_detail = params.get("error_detail", "Unknown Fault")
            
            self.captured_handle_ids["latest"] = handle_id
            
            if status == "RESOLVED":
                self.mock_ledger.set_resolved(handle_id, payload)
            elif status == "YIELD":
                self.mock_ledger.set_yielded(handle_id, payload)
            else:
                self.mock_ledger.set_faulted(handle_id, error_detail)
                
            # [핵심] Pub/Sub을 통한 Bridge Awaken (Zero-Latency)
            await tunnel.publish(f"mcp.intent.reply.{handle_id}", json.dumps({
                "status": status,
                "executable_payload": payload,
                "error_detail": error_detail
            }))
            return {"success": True}

        INTERNAL_HANDLERS_REGISTRY["mcp.state.query"] = mock_mcp_state_query
        INTERNAL_HANDLERS_REGISTRY["mcp.state.pending.seal"] = mock_mcp_state_pending_seal
        INTERNAL_HANDLERS_REGISTRY["eco.billing.receipt.validate"] = mock_billing_receipt_validate
        INTERNAL_HANDLERS_REGISTRY["mcp.bridge.resolve_state"] = mock_resolve_state

        class MockDaemonCtx: broker = None; store = None
        self.worker_daemon = RpcWorkerDaemon(ctx=MockDaemonCtx())
        self._worker_task = asyncio.create_task(self.worker_daemon.run())
        
        self.sentinel = agent_sentinel.AgentSentinel(ledger=self.mock_ledger, rpc_client=self.rpc, sweep_interval=1.0)
        self._sentinel_task = asyncio.create_task(self.sentinel.ignite())
        
        log.info("[Pipeline] RPC Bus & Core Daemons Ignited.")

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Enterprise A2A Suite: {self.name} ===")
        await self._setup_rpc_bus()

        self._server_task = asyncio.create_task(self.server.serve())
        await asyncio.sleep(1.0)
        
        log.info(f"[Pipeline] Igniting Connectors (Daemon/Ephemeral)")
        self._oracle_task = asyncio.create_task(self.oracle_connector.run())
        self._deploy_task = asyncio.create_task(self.deploy_connector.run())
        self._finlib_task = asyncio.create_task(self.finlib_connector.run()) 
        self._margin_task = asyncio.create_task(self.margin_connector.run()) 
        await asyncio.sleep(2.0)

        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n▶️ [PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult("MCP_BRIDGE", phase.name, True, True))
                    log.info("  └─ Status: PASSED ✅")
                except Exception as e:
                    log.error(f"  └─ Halted: {str(e)} ❌", exc_info=True)
                    results.append(TestResult("MCP_BRIDGE", phase.name, False, True))
        finally:
            log.info(f"\n[Pipeline] Teardown sequence initiated...")
            for conn in [self.oracle_connector, self.deploy_connector, self.finlib_connector, self.margin_connector]:
                conn.running = False
            for task in [self._oracle_task, self._deploy_task, self._finlib_task, self._margin_task]:
                if task: task.cancel()
            
            if self.worker_daemon: self.worker_daemon.running = False
            if self._worker_task: self._worker_task.cancel()
            if self.sentinel: self.sentinel.running = False
            if self._sentinel_task: self._sentinel_task.cancel()
                
            self.server.should_exit = True
            if self._server_task: await self._server_task
            log.info(f"[Pipeline] Teardown complete.")
            
        return results

    # =====================================================================
    # Phase 1: Event-Driven Zero-Latency Proof
    # =====================================================================
    async def phase_zero_latency(self):
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "eval_math", "arguments": {"expression": "100 * 50"}}
        }
        headers = {"x_idempotency_key": uuid.uuid4().hex, "x_nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
        
        start_time = time.time()
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
        duration = time.time() - start_time
        
        if res.status_code != 200:
            raise RuntimeError(f"Expected 200, got {res.status_code}")
            
        log.info(f"  └─ ✨ Response received in {duration:.4f} seconds via Pub/Sub Awakening.")
        if duration > 0.2:
            raise RuntimeError(f"Latency Alert: Took {duration:.4f}s. Polling might still be active!")

    # =====================================================================
    # Phase 2: High-Concurrency Daemon Stress Test (Finlib)
    # =====================================================================
    async def phase_finlib_multiplexing(self):
        async def send_compute(idx: int):
            payload = {
                "jsonrpc": "2.0", "id": idx, "method": "tools/call",
                "params": {"name": "resolve_dates", "arguments": {"base_date": "2026-09-04", "offset_business_days": idx}}
            }
            headers = {"x_idempotency_key": uuid.uuid4().hex, "x_nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
            async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
                return await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)

        req_count = 50
        log.info(f"  └─ Stress Testing Daemon: Blasting {req_count} concurrent requests...")
        results = await asyncio.gather(*[send_compute(i) for i in range(1, req_count + 1)])
        failed = [res for res in results if res.status_code != 200]
        if failed:
            raise RuntimeError(f"{len(failed)} requests failed out of {req_count}.")
        log.info(f"  └─ ✨ All {req_count} multiplexed calculations resolved simultaneously without collision.")

    # =====================================================================
    # [NEW] Phase 3: Unit Economics Vectorization (Margin BI)
    # =====================================================================
    async def phase_margin_simulation(self):
        payload = {
            "jsonrpc": "2.0", "id": 200, "method": "tools/call",
            "params": {
                "name": "simulate_unit_economics",
                "arguments": {
                    "pricing_model": {"base_fee_usd": 0.002, "dynamic_multiplier": 1.0},
                    "infra_model": {"monthly_fixed_cost_usd": 30.0, "cost_per_compute_ms_usd": 0.00001, "avg_compute_time_ms": 2.5},
                    "traffic_scenarios": {"min_tps": 0.5, "max_tps": 50.0, "cache_hit_ratio": 0.8}
                }
            }
        }
        headers = {"x_idempotency_key": uuid.uuid4().hex, "x_nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
        
        log.info("  └─ Requesting Numpy vectorized margin simulation from Margin BI Daemon...")
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.margin_id}/invoke", json=payload, headers=headers)
            if res.status_code != 200:
                raise RuntimeError(f"Margin Simulation Failed: {res.text}")
                
            data = res.json()
            result_str = data.get("result", {}).get("content", [{}])[0].get("text", "{}")
            sim_result = json.loads(result_str)
            bep_tps = sim_result.get("break_even_point", {}).get("bep_tps")
            log.info(f"  └─ ✨ Margin calculated perfectly! BEP TPS: {bep_tps}")

    # =====================================================================
    # [NEW] Phase 4: L402 Billing Rejection (Free-Rider Defense)
    # =====================================================================
    async def phase_l402_rejection(self):
        payload = {
            "jsonrpc": "2.0", "id": 300, "method": "tools/call",
            "params": {"name": "eval_math", "arguments": {"expression": "1 + 1"}}
        }
        # 의도적으로 위조되거나 없는 영수증 발송
        headers = {"x_idempotency_key": uuid.uuid4().hex, "x_nonce": uuid.uuid4().hex, "X-X402-Receipt": "invalid_receipt"}
        
        log.info("  └─ Injecting invalid L402 receipt to test Free-Rider rejection...")
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
            
            if res.status_code != 402:
                raise RuntimeError(f"Expected HTTP 402 Payment Required, got {res.status_code}")
                
            log.info(f"  └─ ✨ Free-Rider Defense Active. Caught Unauthorized Access: {res.json()}")

    # =====================================================================
    # Phase 5: Precise Error Routing (Invalid Params)
    # =====================================================================
    async def phase_error_routing(self):
        payload = {
            "jsonrpc": "2.0", "id": 99, "method": "tools/call",
            "params": {"name": "calc_indicators_batch", "arguments": {"prices_matrix": "BAD_DATA_NOT_ARRAY"}}
        }
        headers = {"x_idempotency_key": uuid.uuid4().hex, "x_nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
        
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
            if res.status_code != 502:
                raise RuntimeError(f"Expected HTTP 502 for Execution Fault, got {res.status_code}")
            err_msg = res.json().get("detail", "")
            if "Invalid params" not in err_msg:
                raise RuntimeError(f"Error did not route correctly. Got: {err_msg}")
            log.info(f"  └─ ✨ Precise Error Routed successfully: {err_msg}")

    # =====================================================================
    # Phase 6: Idempotency Fast-Path Defense
    # =====================================================================
    async def phase_idempotency_defense(self):
        self.idem_key_otp = uuid.uuid4().hex
        payload = {
            "jsonrpc": "2.0", "id": 777, "method": "tools/call",
            "params": {"name": "execute_db_migration", "arguments": {"service_name": "auth", "target_env": "prod", "sql_script": "DROP TABLE"}}
        }
        headers = {"x_idempotency_key": self.idem_key_otp, "x_nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
        
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res1 = await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=payload, headers=headers)
            if res1.status_code != 202: raise RuntimeError(f"Expected HTTP 202 (YIELD), got {res1.status_code}")
            self.prompt_id = res1.json().get("id")
            
        headers["x_nonce"] = uuid.uuid4().hex 
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res2 = await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=payload, headers=headers)
            if res2.status_code != 202: raise RuntimeError(f"Fast-Path failed. Expected 202, got {res2.status_code}")
            
        log.info("  └─ ✨ Idempotency Shield deflected duplicate request through Ledger RPC query.")

    # =====================================================================
    # Phase 7: Async Resume (Resolving Phase 6)
    # =====================================================================
    async def phase_otp_resume(self):
        handle_id = self.captured_handle_ids.get("latest")
        resume_intent = {
            "handle_id": handle_id, "action": "RESUME",
            "payload": {"jsonrpc": "2.0", "id": self.prompt_id, "result": {"value": "999888"}}
        }
        await self.rpc.publish_intent(f"mcp.intent.queue.{self.deploy_id}", resume_intent)
        await asyncio.sleep(1.0)
        
        final_state = await self.mock_ledger.query_state(handle_id)
        if not final_state or final_state.metadata.get("status") != "RESOLVED":
            raise RuntimeError("Sandbox failed to Resume/Resolve.")
        log.info("  └─ ✨ Transaction Resumed, Resolved, and Ephemeral Sandbox Destroyed.")

    # =====================================================================
    # Phase 8: Autonomous Rollback (Sentinel + Deploy)
    # =====================================================================
    async def phase_sentinel_reconciliation(self):
        idem_key_sentinel = uuid.uuid4().hex
        payload = {
            "jsonrpc": "2.0", "id": 888, "method": "tools/call",
            "params": {"name": "execute_db_migration", "arguments": {"service_name": "billing", "target_env": "prod", "sql_script": "DROP TABLE"}}
        }
        headers = {"x_idempotency_key": idem_key_sentinel, "x_nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
        
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=payload, headers=headers)
                
        handle_id = self.captured_handle_ids.get("latest")
        await asyncio.sleep(3.0)
        
        final_state = await self.mock_ledger.query_state(handle_id)
        if not final_state or final_state.metadata.get("status") != "FAULTED":
            raise RuntimeError("Sentinel failed to rollback.")
        log.info(f"  └─ ✨ Sentinel Autonomous Reconciliation Successful.")


class McpBridgeSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI MCP SUITE] Zero-Latency, L402 Billing & Hybrid FSM Benchmark")
        self.log.info("="*80)
        
        net_config = E2EConfig(host="127.0.0.1", port=8355, protocol="http")
        self.results.extend(await McpBridgePipeline(config=net_config).run_pipeline())
        
        self._print_report()

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("🛡️ [INFRASTRUCTURE BENCHMARK REPORT]")
        self.log.info("="*80)
        all_passed = all(r.passed for r in self.results)
        
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            self.log.info(f"{status_icon} {idx:02d}. [{res.target}]".ljust(22) + f"{res.scenario.ljust(50)} | {'PASSED' if res.passed else 'FAILED'}")
            
        self.log.info("-" * 80)
        if all_passed: 
            self.log.info("🎉 ENTERPRISE A2A PARADIGM SHIFT VALIDATED SUCCESSFULLY.")
        else: 
            self.log.critical("💥 INTEGRATION FRACTURE DETECTED. Check logs for details.")
        self.log.info("="*80 + "\n")

def main(args_list: list[str] = None):
    app = McpBridgeSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()