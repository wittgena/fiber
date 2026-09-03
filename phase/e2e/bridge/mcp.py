# fiber.phase.e2e.bridge.mcp
import sys
import uuid
import json
import asyncio
from typing import Any, List, Dict

import httpx
import uvicorn

from fiber.dphi.rpc.client import InternalRpcClient
from fiber.infra.worker.connector import McpConnectorDaemon
from fiber.phase.e2e.edge import (
    PipelineRunner, 
    ManagedTestServer, 
    TestResult, 
    Phase, 
    E2EConfig
)

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from fiber.dphi.edge.payload import create_app, Config

log = get_emitter("e2e.bridge.mcp")

# =====================================================================
# 1. RPC & Ledger Mock Handler (코어망 합의 시뮬레이션)
# =====================================================================
class CoreLedgerMock:
    """Gateway가 폴링하는 원장의 상태를 조작하기 위한 딕셔너리 기반 Mock 원장"""
    def __init__(self):
        self.states = {}

    async def query_state(self, handle_id: str):
        # Gateway의 self.ledger.query_state()가 이 메서드를 호출한다고 가정
        return self.states.get(handle_id)

    def set_resolved(self, handle_id: str, payload: dict):
        class DummyState:
            action = "dphi.transition.resolve"
            metadata = {"status": "RESOLVED"}
            def __init__(self, p): self.payload = p
        self.states[handle_id] = DummyState(payload)

    def set_faulted(self, handle_id: str, error_detail: str):
        class DummyState:
            action = "dphi.transition.resolve"
            metadata = {"status": "FAULTED", "error_detail": error_detail}
            def __init__(self): self.payload = None
        self.states[handle_id] = DummyState()


# =====================================================================
# 2. The E2E Pipeline (Real Oracle Integration)
# =====================================================================
class McpBridgeRealOraclePipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Sync-Async Facade & REAL Oracle Connector Trace", scope_name="MCP_BRIDGE_REAL_ORACLE")
        self.config = config
        self.target_server_id = "real-oracle-01"
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        
        # 1. FastAPI 서버 및 Mock 원장 세팅
        self.mock_ledger = CoreLedgerMock()
        self.rest_app = create_app(Config(wasm_timeout=5.0))
        
        # [주의] 테스트 환경 주입: Gateway가 사용하는 Ledger를 모킹된 Ledger로 교체
        # 실전에서는 app.state.mcp_transition_adapter 에 진짜 TransitionBridge가 들어감
        if hasattr(self.rest_app.state, "mcp_transition_adapter"):
            self.rest_app.state.mcp_transition_adapter.ledger = self.mock_ledger
            
        u_config = uvicorn.Config(app=self.rest_app, host="127.0.0.1", port=self.config.port, log_level="error", access_log=False)
        self.server = ManagedTestServer(u_config)
        self._server_task = None

        import fiber.infra.worker.oracle.server as oracle_server
        oracle_cmd = f"{sys.executable} -m {server_server.__name__}"

        self.connector = McpConnectorDaemon(target_id=self.target_server_id, legacy_command=oracle_cmd)
        self._connector_task = None

        # 테스트 시나리오(Phases) 등록
        self.set_phases([
            Phase("Phase 1: Real Oracle Data Fetch (A2A Sublimation)", self.phase_real_oracle_fetch),
            Phase("Phase 2: Idempotency & Cache Hit", self.phase_idempotency_cache),
            Phase("Phase 3: Real Tool Failure (Cascading Defense)", self.phase_oracle_tool_error)
        ])

    async def _setup_rpc_bus(self):
        """커넥터가 쏘는 상태 보고(RESOLVED)를 받아서 Mock 원장을 업데이트하는 RPC 버스 리스너 구축"""
        self.rpc = InternalRpcClient()
        
        # 커넥터는 실행 완료 후 이 엔드포인트를 호출함
        async def mock_resolve_state(payload: dict):
            handle_id = payload.get("handle_id")
            status = payload.get("status")
            if status == "RESOLVED":
                self.mock_ledger.set_resolved(handle_id, payload.get("executable_payload"))
            else:
                self.mock_ledger.set_faulted(handle_id, payload.get("error_detail", "Unknown Fault"))
            return {"success": True}

        # 결제 승인 엔드포인트 무조건 통과 처리
        async def mock_intent_validate(payload: dict):
            return {"authorized": True, "budget": 1000000}

        # RPC 핸들러 강제 등록 (테스트용)
        self.rpc.register_handler("mcp.bridge.resolve_state", mock_resolve_state)
        self.rpc.register_handler("eco.compute.intent.validate", mock_intent_validate)

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting True E2E Pipeline: {self.name} ===")
        
        await self._setup_rpc_bus()

        log.info(f"[Pipeline] Booting Edge Gateway (Facade) on {self.local_url}...")
        self._server_task = asyncio.create_task(self.server.serve())
        await asyncio.sleep(2.0) # 서버 부팅 대기
        
        log.info(f"[Pipeline] Igniting REAL Connector & Subprocess Oracle Server...")
        # 커넥터 실행 루프를 백그라운드 태스크로 띄움
        self._connector_task = asyncio.create_task(self.connector.run())
        await asyncio.sleep(2.0) # 자식 프로세스 부팅 및 RPC 구독 완료 대기

        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n▶️ [PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult("MCP_BRIDGE_REAL", phase.name, True, True))
                    log.info("  └─ Status: PASSED ✅")
                except Exception as e:
                    log.error(f"  └─ Halted: {str(e)} ❌")
                    results.append(TestResult("MCP_BRIDGE_REAL", phase.name, False, True))
        finally:
            log.info(f"\n[Pipeline] Teardown sequence initiated...")
            self.connector.running = False
            if self._connector_task: 
                self._connector_task.cancel()
            self.server.should_exit = True
            if self._server_task: 
                await self._server_task
            log.info(f"[Pipeline] Teardown complete.")
            
        return results

    # ---------------------------------------------------------
    # Test Scenarios
    # ---------------------------------------------------------
    async def phase_real_oracle_fetch(self):
        """[진짜 API 통신] 바이낸스/코인베이스 데이터를 Fetch하고 암호학적 서명이 반환되는지 검증"""
        self.idem_key_golden = uuid.uuid4().hex
        
        # 진짜 MCP Server(Oracle)가 처리할 순수 페이로드
        payload = {
            "jsonrpc": "2.0", 
            "id": 99, 
            "method": "tools/call", 
            "params": {
                "name": "fetch_aggregated_kline",
                "arguments": {"symbol": "BTCUSDT", "strategy": "mean"}
            }
        }
        
        headers = {
            "x_idempotency_key": self.idem_key_golden,
            "x_nonce": uuid.uuid4().hex,
            "X-X402-Receipt": "mock_valid_l402_receipt"
        }

        log.info("  └─ Sending intent to Gateway. Waiting for real Oracle network I/O...")
        # 라이브 API를 호출하므로 timeout을 15초로 여유롭게 부여
        async with httpx.AsyncClient(base_url=self.local_url, timeout=15.0) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.target_server_id}/invoke", json=payload, headers=headers)
            
            if res.status_code != 200:
                raise RuntimeError(f"Real Oracle Fetch Failed! Expected 200, got {res.status_code}: {res.text}")
                
            data = res.json()
            # 1. MCP Result 구조 검증
            mcp_result = data.get("result", {})
            if mcp_result.get("isError"):
                raise ValueError(f"Oracle returned an error: {mcp_result}")
                
            # 2. Oracle 서명(Attestation) 데이터 파싱 및 검증
            try:
                content_list = mcp_result.get("content", [])
                oracle_output = json.loads(content_list[0]["text"])
                
                signature = oracle_output.get("attestation", {}).get("signature")
                aggregated_hash = oracle_output.get("observation", {}).get("aggregated_hash")
                
                log.info(f"  └─ ✨ Real Oracle Attestation Received!")
                log.info(f"     * Aggregated Hash: {aggregated_hash[:16]}...")
                log.info(f"     * Node Signature : {signature[:16]}...")
                
                if not signature or not aggregated_hash:
                    raise ValueError("Missing cryptographic attestation in oracle output.")
                    
            except (KeyError, IndexError, json.JSONDecodeError) as e:
                raise ValueError(f"Failed to parse Oracle's deterministic output: {e}")

    async def phase_idempotency_cache(self):
        """[멱등성 검증] 똑같은 키로 재호출 시, 외부 API 호출(딜레이) 없이 즉시 이전 데이터를 반환하는지 검증"""
        payload = {"jsonrpc": "2.0", "method": "tools/call"}
        headers = {
            "x_idempotency_key": self.idem_key_golden, # 1단계 성공 키
            "x_nonce": uuid.uuid4().hex,               # 새로운 난수
            "X-X402-Receipt": "mock_valid_l402_receipt"
        }

        log.info("  └─ Sending duplicate intent. Expecting instant cache hit...")
        # 1초 이내에 리턴되어야 함 (외부 통신 X)
        async with httpx.AsyncClient(base_url=self.local_url, timeout=1.0) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.target_server_id}/invoke", json=payload, headers=headers)
            
            if res.status_code != 200:
                raise RuntimeError("Cache Miss! Gateway did not return idempotent result.")
            log.info("  └─ ✨ Idempotency hit confirmed (No blocking I/O).")

    async def phase_oracle_tool_error(self):
        """[에러 전파 방어] 존재하지 않는 심볼이나 도구를 호출하여 레거시 서버에서 에러 발생 시, 502/에러가 우아하게 넘어오는지 검증"""
        # 고의로 이상한 인자 전달 (바이낸스에서 에러 유도 또는 서버 파싱 에러 유도)
        payload = {
            "jsonrpc": "2.0", 
            "id": 100, 
            "method": "tools/call", 
            "params": {
                "name": "fetch_aggregated_kline",
                "arguments": {"symbol": "INVALID_COIN_XYZ"}
            }
        }
        headers = {
            "x_idempotency_key": uuid.uuid4().hex,
            "x_nonce": uuid.uuid4().hex,
            "X-X402-Receipt": "mock_valid_l402_receipt"
        }

        log.info("  └─ Sending bad symbol intent to trigger legacy error...")
        async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.target_server_id}/invoke", json=payload, headers=headers)
            
            # 서버가 크래시 났다면 Gateway가 502 Bad Gateway를 던져야 하고, 
            # Oracle이 에러를 잡아 MCP 규격으로 뱉었다면 200 OK 내부에 isError=True 이거나, 502 상태여야 함.
            # 우리 코드에서 Oracle 서버는 에러를 JSON-RPC 에러 객체로 뱉도록 작성되었음.
            if res.status_code == 502:
                log.info(f"  └─ ✨ Cascading Defense Active. Caught Fault: {res.json()}")
            else:
                raise RuntimeError(f"Expected 502 Gateway Fault, got {res.status_code}: {res.text}")


# =====================================================================
# 3. Standard Entrypoint (Suite Runner)
# =====================================================================
class McpBridgeSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI MCP BRIDGE SUITE] True E2E Integration (Facade <-> Connector <-> Oracle)")
        self.log.info("="*80)
        
        net_config = E2EConfig(host="127.0.0.1", port=8355, protocol="http")
        self.results.extend(await McpBridgeRealOraclePipeline(config=net_config).run_pipeline())
        
        self._print_report()

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("🛡️ [TRUE E2E ORACLE INTEGRATION REPORT]")
        self.log.info("="*80)
        all_passed = all(r.passed for r in self.results)
        
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            self.log.info(f"{status_icon} {idx:02d}. [{res.target}]".ljust(22) + f"{res.scenario.ljust(45)} | Result: {'PASSED' if res.passed else 'FAILED'}")
            
        self.log.info("-" * 80)
        if all_passed: 
            self.log.info("🎉 REAL ORACLE NETWORK I/O & SUBLIMATION TESTS PASSED.")
        else: 
            self.log.critical("💥 INTEGRATION FRACTURE DETECTED. Check logs for details.")
        self.log.info("="*80 + "\n")


def main(args_list: list[str] = None):
    app = McpBridgeSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()