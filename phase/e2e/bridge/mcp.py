# fiber.phase.e2e.bridge.mcp
import os
import sys
import time
import uuid
import json
import asyncio
import getpass
from typing import Any, List, Dict

import httpx
import uvicorn

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

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
import fiber.infra.worker.agent.validator as agent_validator  # [추가] 신규 Validator 에이전트
import fiber.infra.worker.agent.oracle as agent_oracle
import fiber.infra.worker.agent.sentinel as agent_sentinel
import fiber.infra.worker.agent.finlib as agent_finlib
import fiber.infra.worker.agent.margin as agent_margin

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from fiber.dphi.edge.payload import create_app, Config

log = get_emitter("e2e.bridge.mcp")

# =========================================================
# Mock Objects (Ledger & Stream)
# =========================================================
class MockStream:
    def __init__(self, handle_id: str, target: str, fuel: int):
        self.id = handle_id
        self.metadata = {"target": target, "fuel_locked": fuel, "risk_score": 0}

class MockLedger:
    def __init__(self):
        self.states = {}
        self.yield_timestamps = {}
        self.stale_timeout = 60.0

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
            if current_time - ts > self.stale_timeout:
                stale.append(MockStream(hid, "legacy-01", 100))
        return stale

    async def force_transition(self, handle_id: str, new_action: str, metadata: dict):
        class DummyState:
            action = new_action
            def __init__(self, md): self.metadata = md
            self.payload = {}
        self.states[handle_id] = DummyState(metadata)
        self.yield_timestamps.pop(handle_id, None)


# =========================================================
# Main Pipeline Runner
# =========================================================
class McpBridgePipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Zero-Latency, L402 Billing & Hybrid FSM Trace", scope_name="MCP_BRIDGE_SUITE")
        self.config = config

        self.oracle_id = "oracle-01"
        self.deploy_id = "legacy-01"
        self.validator_id = "agent.validator"  # [추가] RPC 라우팅을 위한 타겟 ID 일치
        self.finlib_id = "finlib-01"
        self.margin_id = "margin-01"

        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        self.mock_ledger = MockLedger()

        self.rest_app = None
        self.server = None
        self._server_task = None

        # [Connectors Setup]
        oracle_cmd = f"{sys.executable} -m {agent_oracle.__name__}"
        self.oracle_connector = WorkerConnector(target_id=self.oracle_id, legacy_command=oracle_cmd, mode="ephemeral")
        self._oracle_task = None

        deploy_cmd = f"{sys.executable} -m {agent_deploy.__name__}"
        self.deploy_connector = WorkerConnector(target_id=self.deploy_id, legacy_command=deploy_cmd, mode="ephemeral")
        self._deploy_task = None

        # [추가] Validator(Auth/Sign) 전담 에이전트 데몬
        validator_cmd = f"{sys.executable} -m {agent_validator.__name__}"
        self.validator_connector = WorkerConnector(target_id=self.validator_id, legacy_command=validator_cmd, mode="daemon")
        self._validator_task = None

        finlib_cmd = f"{sys.executable} -m {agent_finlib.__name__}"
        self.finlib_connector = WorkerConnector(target_id=self.finlib_id, legacy_command=finlib_cmd, mode="daemon")
        self._finlib_task = None

        margin_cmd = f"{sys.executable} -m {agent_margin.__name__}"
        self.margin_connector = WorkerConnector(target_id=self.margin_id, legacy_command=margin_cmd, mode="daemon")
        self._margin_task = None

        self.worker_daemon = None
        self._worker_task = None
        self.sentinel = None
        self._sentinel_task = None

        self.captured_handle_ids: Dict[str, str] = {}
        self.prompt_id = None
        self.idem_key_otp = None
        self.deploy_payload = None

        # 100% 일관성을 위한 정규화된 SPIFFE ID (cli.sign과 동일)
        self.test_short_id = "fiber"
        self.test_spiffe_id = f"spiffe://self/{self.test_short_id}"

        self.set_phases([
            Phase("Phase 1: Event-Driven Zero-Latency Proof", self.phase_zero_latency),
            Phase("Phase 2: High-Concurrency Daemon Stress (FinLib)", self.phase_finlib_multiplexing),
            Phase("Phase 3: Unit Economics Vectorization (Margin BI)", self.phase_margin_simulation),
            Phase("Phase 4: L402 Billing Rejection (Free-Rider Defense)", self.phase_l402_rejection),
            Phase("Phase 5: Precise Error Routing (Invalid Params)", self.phase_error_routing),
            Phase("Phase 6: Idempotency Fast-Path Defense (Trigger YIELD)", self.phase_idempotency_defense),
            Phase("Phase 7: MCP 2026-07-28 Stateless Re-issue & Resume", self.phase_stateless_otp_resume),
            Phase("Phase 8: Autonomous Reconciliation (Sentinel)", self.phase_sentinel_reconciliation)
        ])

    def _setup_security_context(self):
        """
        [Zero-Trust 보안 컨텍스트 주입]
        1. DB 복호화를 위한 인간의 마스터 패스프레이즈 주입 (Validator 전용)
        2. A2A 서명 및 검증을 위한 Ed25519 일회성(Ephemeral) 키 페어 주입
        """
        print("\n" + "="*80)
        print("🔐 [Security Context] Zero-Trust Auth Validator Initialization")
        pwd = getpass.getpass("👉 cli.sign 에서 설정했던 Master Passphrase를 입력하세요: ")
        os.environ["DPHI_MASTER_PASSPHRASE"] = pwd
        print("="*80 + "\n")

        # Validator와 Deployer가 서로를 신뢰하기 위한 Ed25519 키 페어 런타임 생성
        val_key = ed25519.Ed25519PrivateKey.generate()
        priv_bytes = val_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
        pub_bytes = val_key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        
        # Validator에게는 서명(Private) 권한 부여 / Deployer에게는 검증(Public) 권한만 부여
        os.environ["DPHI_VALIDATOR_PRIVATE_KEY"] = priv_bytes.hex()
        os.environ["DPHI_VALIDATOR_PUBLIC_KEY"] = pub_bytes.hex()
        
        log.info("[Pipeline] Ephemeral Cryptographic Keypair for A2A Attestation injected.")

    async def _setup_rpc_bus(self):
        self.rpc = InternalRpcClient()
        tunnel = await TunnelFactory.get_default()

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

        class MockDaemonCtx: 
            broker = None
            store = None

        self.worker_daemon = RpcWorkerDaemon(ctx=MockDaemonCtx())
        self.worker_daemon.running = True  
        self._worker_task = asyncio.create_task(self.worker_daemon.run())

        self.sentinel = agent_sentinel.AgentSentinel(ledger=self.mock_ledger, rpc_client=self.rpc, sweep_interval=1.0)
        self._sentinel_task = asyncio.create_task(self.sentinel.ignite())

        log.info("[Pipeline] RPC Bus & Core Daemons Ignited.")

    async def _setup_rest_edge(self):
        tunnel = await TunnelFactory.get_default()

        self.rest_app = create_app(
            config=Config(wasm_timeout=5.0),
            tunnel=tunnel,            
            ledger=self.mock_ledger   
        )

        u_config = uvicorn.Config(
            app=self.rest_app, 
            host="127.0.0.1", 
            port=self.config.port, 
            log_level="error", 
            access_log=False
        )
        self.server = ManagedTestServer(u_config)
        self._server_task = asyncio.create_task(self.server.serve())
        log.info("[Pipeline] REST Edge (Stateless Gateway) Bootstrapped with DI.")

    async def run_pipeline(self) -> List[TestResult]:
        # [Zero-Trust 컨텍스트 설정: 인간의 개입 및 키 분배]
        self._setup_security_context()
        
        log.info(f"\n=== Starting Enterprise A2A Suite: {self.name} ===")

        await self._setup_rpc_bus()
        await self._setup_rest_edge()
        await asyncio.sleep(1.0)

        log.info(f"[Pipeline] Igniting Connectors (Daemon/Ephemeral)")
        self._oracle_task = asyncio.create_task(self.oracle_connector.run())
        self._deploy_task = asyncio.create_task(self.deploy_connector.run())
        self._validator_task = asyncio.create_task(self.validator_connector.run())  # [추가] Validator 데몬 가동
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
            connectors = [
                self.oracle_connector, self.deploy_connector, 
                self.validator_connector, self.finlib_connector, self.margin_connector
            ]
            for conn in connectors:
                conn.running = False
                
            tasks = [
                self._oracle_task, self._deploy_task, 
                self._validator_task, self._finlib_task, self._margin_task
            ]
            for task in tasks:
                if task: task.cancel()

            if self.worker_daemon: self.worker_daemon.running = False
            if self._worker_task: self._worker_task.cancel()
            if self.sentinel: self.sentinel.running = False
            if self._sentinel_task: self._sentinel_task.cancel()

            if self.server:
                self.server.should_exit = True
            if self._server_task: 
                await self._server_task
            log.info(f"[Pipeline] Teardown complete.")

        return results

    # =====================================================================
    # Test Phases
    # =====================================================================
    async def phase_zero_latency(self):
        payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "eval_math", "arguments": {"expression": "100 * 50"}}}
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
        if res.status_code != 200: raise RuntimeError(f"Expected 200, got {res.status_code}")

    async def phase_finlib_multiplexing(self):
        async def send_compute(idx: int):
            payload = {"jsonrpc": "2.0", "id": idx, "method": "tools/call", "params": {"name": "resolve_dates", "arguments": {"base_date": "2026-09-04", "offset_business_days": idx}}}
            headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
            async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
                return await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
        req_count = 10
        results = await asyncio.gather(*[send_compute(i) for i in range(1, req_count + 1)])
        if any(res.status_code != 200 for res in results): raise RuntimeError("Multiplexing failed")

    async def phase_margin_simulation(self):
        payload = {
            "jsonrpc": "2.0", 
            "id": 200, 
            "method": "tools/call", 
            "params": {
                "name": "calculate_trajectory_margin",
                "arguments": {
                    "symbol": "BTC-USDT",
                    "observations": {
                        "arn:binance": {"rate": 0.0001, "time": int(time.time())},
                        "arn:bybit": {"rate": 0.00012, "time": int(time.time())}
                    },
                    "trade_size_usd": 10000.0,
                    "pricing": {"base_l402_fee_usd": 0.002, "profit_share_ratio": 0.05},
                    "infra": {"monthly_fixed_cost_usd": 30.0, "compute_cost_per_sec_usd": 0.00001, "avg_latency_sec": 0.05},
                    "tps_range": [1.0, 10.0, 50.0]
                }
            }
        }
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.margin_id}/invoke", json=payload, headers=headers)
            if res.status_code != 200: 
                raise RuntimeError(f"Margin Sim Failed: Expected 200, got {res.status_code} ({res.text})")

    async def phase_l402_rejection(self):
        payload = {"jsonrpc": "2.0", "id": 300, "method": "tools/call", "params": {"name": "eval_math", "arguments": {"expression": "1 + 1"}}}
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "invalid_receipt"}
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
            if res.status_code != 402: raise RuntimeError("Expected HTTP 402")

    async def phase_error_routing(self):
        payload = {"jsonrpc": "2.0", "id": 99, "method": "tools/call", "params": {"name": "calc_indicators_batch", "arguments": {"prices_matrix": "BAD_DATA"}}}
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_l402"}
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
            if res.status_code != 502: raise RuntimeError("Expected HTTP 502")

    async def phase_idempotency_defense(self):
        self.idem_key_otp = uuid.uuid4().hex
        self.deploy_payload = {
            "jsonrpc": "2.0", "id": 777, "method": "tools/call",
            "params": {"name": "execute_db_migration", "arguments": {"service_name": "auth", "target_env": "prod", "sql_script": "DROP TABLE"}}
        }
        headers = {
            "x-idempotency-key": self.idem_key_otp, 
            "x-nonce": uuid.uuid4().hex, 
            "X-X402-Receipt": "valid_l402",
            "x-spiffe-id": self.test_spiffe_id 
        }

        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res1 = await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=self.deploy_payload, headers=headers)
            if res1.status_code != 202: raise RuntimeError(f"Expected HTTP 202 (YIELD), got {res1.status_code}")

            prompt_res = res1.json()
            self.prompt_id = prompt_res.get("id")
            log.info(f"  └─ Captured Elicitation Prompt ID: {self.prompt_id}")

        # Idempotency 테스트를 위해 동일 Payload 재전송
        headers["x-nonce"] = uuid.uuid4().hex 
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res2 = await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=self.deploy_payload, headers=headers)
            if res2.status_code != 202: raise RuntimeError(f"Fast-Path failed. Expected 202, got {res2.status_code}")

        log.info("  └─ ✨ Idempotency Shield deflected duplicate request without crashing Sandbox.")

    async def phase_stateless_otp_resume(self):
        print("\n" + "="*60)
        valid_totp_code = await asyncio.to_thread(
            input, "👉 스마트폰 앱(Authenticator)에서 확인한 6자리 TOTP 코드를 입력하세요: "
        )
        valid_totp_code = valid_totp_code.strip()
        print("="*60 + "\n")

        resume_payload = dict(self.deploy_payload)
        resume_payload["params"]["_meta"] = {
            "inputResponses": {
                "jsonrpc": "2.0",
                "id": self.prompt_id,
                "result": {"value": valid_totp_code}
            }
        }

        headers = {
            "x-idempotency-key": self.idem_key_otp,
            "x-nonce": uuid.uuid4().hex, 
            "X-X402-Receipt": "valid_l402",
            "x-spiffe-id": self.test_spiffe_id 
        }

        log.info(f"  └─ Re-issuing HTTP request with TOTP code [{valid_totp_code}] to Bridge...")

        async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=resume_payload, headers=headers)

            if res.status_code != 200:
                raise RuntimeError(f"Bridge failed to Resume Sandbox. Expected 200 OK, got {res.status_code} ({res.text})")

        final_result = res.json().get("result", {}).get("content", [{}])[0].get("text", "")
        if "successfully" not in final_result:
            raise RuntimeError(f"Resume succeeded, but payload failed: {final_result}")

        log.info("  └─ ✨ Stateless Resume -> Stateful Sentinel Execution -> 200 OK Resolution Verified.")

    async def phase_sentinel_reconciliation(self):
        self.mock_ledger.stale_timeout = 1.0

        idem_key_sentinel = uuid.uuid4().hex
        payload = {
            "jsonrpc": "2.0", "id": 888, "method": "tools/call",
            "params": {"name": "execute_db_migration", "arguments": {"service_name": "billing", "target_env": "prod", "sql_script": "DROP TABLE"}}
        }
        headers = {
            "x-idempotency-key": idem_key_sentinel, 
            "x-nonce": uuid.uuid4().hex, 
            "X-X402-Receipt": "valid_l402", 
            "x-spiffe-id": self.test_spiffe_id
        }

        async with httpx.AsyncClient(base_url=self.local_url) as client:
            await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=payload, headers=headers)

        handle_id = self.captured_handle_ids.get("latest")

        log.info("  └─ Waiting for Sentinel TTL Sweep to auto-rollback zombie sandbox...")
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