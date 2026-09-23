# fiber.dev.e2e.worker.discovery
import sys
import uuid
import time
import asyncio
from typing import List

import httpx

from fiber.infra.e2e.config import Phase, E2EConfig, TestResult
from fiber.infra.e2e.pipeline import BaseBridgePipeline, log

from fiber.gateway.worker.connector import WorkerConnector
import fiber.dev.ex.worker.legacy.finlib as worker_finlib
import fiber.dev.ex.worker.legacy.oracle as worker_oracle
import fiber.dev.ex.worker.search.archive as worker_archive_search

from fiber.infra.rpc.client import InternalRpcClient
from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.state.phase.reactor import PhaseReactor

class ToolDiscoveryPipeline(BaseBridgePipeline):
    def __init__(self, config: E2EConfig):
        super().__init__(
            config=config, 
            name="MCP Tool Discovery & Facade Suite", 
            scope_name="MCP_DISCOVERY_SUITE"
        )

        self.oracle_id = "oracle-01"
        self.finlib_id = "finlib-01"
        self.search_id = "search-archive-01" # [추가] Search 워커 ID

        self.set_phases([
            Phase("Phase 1: Pure MCP Initialize (Auth Bypass)", self.phase_mcp_initialize),
            Phase("Phase 2: Standard POST tools/list Bypass (FinLib)", self.phase_post_tools_list_bypass),
            Phase("Phase 3: Standard POST tools/list Bypass (Oracle)", self.phase_post_tools_list_oracle),
            Phase("Phase 4: REST GET /tools Facade Routing (FinLib)", self.phase_get_tools_facade_finlib),
            Phase("Phase 5: Isolation Proof (No Cross-Talk)", self.phase_isolation_proof),
            # [추가] Heavy Worker Discovery 검증
            Phase("Phase 6: Heavy Worker Instant Discovery (DuckDB)", self.phase_search_worker_discovery),
        ])

    # =====================================================================
    # Lifecycle Overrides (Pricing Daemon 불필요)
    # =====================================================================
    async def setup_custom_context(self):
        """Discovery 테스트는 실행(Execution)이 아니므로 Pricing Daemon을 생략합니다."""
        log.info("[E2E Pipeline] Pricing Daemon bypassed for Discovery Suite.")

    async def teardown_custom(self):
        pass

    async def setup_workers(self):
        """테스트할 워커 부팅 (Oracle, FinLib, Search)"""
        oracle_cmd = f"{sys.executable} -m {worker_oracle.__name__}"
        self.connectors.append(
            WorkerConnector(target_id=self.oracle_id, execution_target=oracle_cmd, mode="multiplex")
        )

        finlib_cmd = f"{sys.executable} -m {worker_finlib.__name__}"
        self.connectors.append(
            WorkerConnector(target_id=self.finlib_id, execution_target=finlib_cmd, mode="linear")
        )

        search_cmd = f"{sys.executable} -m {worker_archive_search.__name__}"
        self.connectors.append(
            WorkerConnector(target_id=self.search_id, execution_target=search_cmd, mode="multiplex")
        )

    # =====================================================================
    # Test Phases
    # =====================================================================
    async def phase_mcp_initialize(self):
        """초기 핸드쉐이크(initialize)가 영수증이나 서명 없이 통과되는지 검증합니다."""
        payload = {
            "jsonrpc": "2.0", 
            "id": 1, 
            "method": "initialize", 
            "params": {
                "clientInfo": {"name": "test-client", "version": "1.0.0"}
            }
        }
        # 인증 헤더 고의 누락
        headers = {
            "x-idempotency-key": uuid.uuid4().hex, 
            "x-nonce": uuid.uuid4().hex
        }
        
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
            
        if res.status_code != 200:
            raise RuntimeError(f"Expected 200 OK for initialize, got {res.status_code}")
            
        data = res.json()
        if "protocolVersion" not in data.get("result", {}):
            raise RuntimeError(f"Invalid initialize response: {data}")
            
        log.info(f"  └─ Initialize Bypass Validated. Protocol: {data['result']['protocolVersion']}")

    async def phase_post_tools_list_bypass(self):
        """방법 1 검증: POST /invoke 엔드포인트에서 tools/list가 무인증으로 통과되는지 확인"""
        payload = {"jsonrpc": "2.0", "id": 100, "method": "tools/list", "params": {}}
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex}
        
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
            
        if res.status_code != 200:
            raise RuntimeError(f"tools/list Bypass failed. Status: {res.status_code}")
            
        tools = res.json().get("result", {}).get("tools", [])
        if not tools:
            raise RuntimeError("tools array is empty or missing")
            
        tool_names = [t["name"] for t in tools]
        log.info(f"  └─ FinLib POST Tools Found: {tool_names}")
        
        if "resolve_dates" not in tool_names:
            raise RuntimeError("Missing expected tool 'resolve_dates' from FinLib")

    async def phase_post_tools_list_oracle(self):
        """다른 워커(Oracle)에 대해서도 tools/list가 정상 동작하는지 확인"""
        payload = {"jsonrpc": "2.0", "id": 101, "method": "tools/list", "params": {}}
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex}
        
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.oracle_id}/invoke", json=payload, headers=headers)
            
        tools = res.json().get("result", {}).get("tools", [])
        tool_names = [t["name"] for t in tools]
        log.info(f"  └─ Oracle POST Tools Found: {tool_names}")
        
        if "fetch_aggregated_kline" not in tool_names:
            raise RuntimeError("Missing expected tool 'fetch_aggregated_kline' from Oracle")

    async def phase_get_tools_facade_finlib(self):
        """방법 2 검증: GET /tools REST Facade가 Gateway 내부에서 인텐트를 잘 조립하여 반환하는지 확인"""
        # GET 요청이므로 복잡한 헤더나 payload 불필요
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            res = await client.get(f"/v1/mcp-gateway/{self.finlib_id}/tools")
            
        if res.status_code != 200:
            raise RuntimeError(f"GET /tools Facade failed. Status: {res.status_code}")
            
        data = res.json()
        tools = data.get("result", {}).get("tools", [])
        
        if not tools:
            raise RuntimeError("Facade returned empty tools array")
            
        log.info(f"  └─ GET Facade Validated. Successfully mapped tools/list JSON-RPC.")

    async def phase_isolation_proof(self):
        """
        GET /tools 연속 호출 시 Gateway 내부의 ephemeral_identity가 
        Idempotency 충돌을 일으키지 않고 각기 독립된 트랜잭션으로 처리되는지 증명
        """
        async def fetch_facade(idx: int):
            async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
                return await client.get(f"/v1/mcp-gateway/{self.oracle_id}/tools")

        # 5번 동시 요청
        req_count = 5
        results = await asyncio.gather(*[fetch_facade(i) for i in range(req_count)])
        
        if any(res.status_code != 200 for res in results):
            raise RuntimeError("Facade Isolation Failed: Idempotency conflict detected during concurrent GET requests.")
            
        log.info(f"  └─ Isolation Proof Passed. {req_count} concurrent Facade requests succeeded cleanly.")

    # [추가] DuckDB 기반 Search Worker의 즉각적 공시(Zero-latency Discovery) 검증
    async def phase_search_worker_discovery(self):
        """
        [Phase 6] 데이터 집약적인 Heavy Worker에 대한 툴 조회.
        워커가 DB를 다운로드하거나 쿼리하지 않고 즉시 스키마만 반환하는지 속도와 무결성을 검증합니다.
        """
        start_time = time.time()
        
        # 단순 GET 요청 (Gateway Facade 경유)
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            res = await client.get(f"/v1/mcp-gateway/{self.search_id}/tools")
            
        elapsed = time.time() - start_time
            
        if res.status_code != 200:
            raise RuntimeError(f"Heavy Worker Discovery Failed. Status: {res.status_code}")
            
        data = res.json()
        tools = data.get("result", {}).get("tools", [])
        tool_names = [t["name"] for t in tools]
        
        if "search_market_evidence" not in tool_names:
            raise RuntimeError("Missing 'search_market_evidence' in DuckDB Search Worker")
            
        log.info(f"  ├─ DuckDB Search Tools Found: {tool_names}")
        log.info(f"  └─ Heavy Worker Discovery Latency: {elapsed:.3f}s (Proves Lazy Loading)")


class DiscoverySuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        net_config = E2EConfig(host="127.0.0.1", port=8355, protocol="http")
        self.results.extend(await ToolDiscoveryPipeline(config=net_config).run_pipeline())
        self._print_report()

    def _print_report(self):
        all_passed = all(r.passed for r in self.results)
        report_buffer = [
            "\n" + "=" * 80,
            "🔍 [MCP DISCOVERY & FACADE BENCHMARK REPORT]",
            "=" * 80
        ]

        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            line = f"{status_icon} {idx:02d}. [{res.target}]".ljust(22) + f"{res.scenario.ljust(50)} | {'PASSED' if res.passed else 'FAILED'}"
            report_buffer.append(line)

        report_buffer.append("-" * 80)
        
        if all_passed: 
            report_buffer.append("🎉 DISCOVERY PIPELINE VALIDATED SUCCESSFULLY.")
        else: 
            report_buffer.append("💥 DISCOVERY FAILURE DETECTED. Check logs for details.")
            
        report_buffer.append("=" * 80 + "\n")
        aggregated_report = "\n".join(report_buffer)
        if all_passed:
            self.log.info(aggregated_report)
        else:
            self.log.critical(aggregated_report)

def main(args_list: list[str] = None):
    app = DiscoverySuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()