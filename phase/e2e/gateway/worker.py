# fiber.phase.e2e.gateway.worker
import sys
import time
import uuid
import asyncio
from typing import List

import httpx

from fiber.phase.e2e.infra.config import Phase, E2EConfig, TestResult
from fiber.phase.e2e.infra.bridge import BaseBridgePipeline, log
from fiber.dphi.edge.gateway.connector import WorkerConnector

import fiber.agent.worker.mcp.oracle as agent_oracle
import fiber.agent.worker.mcp.finlib as agent_finlib
import fiber.agent.worker.mcp.margin as agent_margin
from xphi.state.phase.reactor import PhaseReactor

class CoreRoutingPipeline(BaseBridgePipeline):
    def __init__(self, config: E2EConfig):
        super().__init__(
            config=config, 
            name="Core Routing & Performance Suite", 
            scope_name="MCP_CORE_SUITE"
        )

        # 타겟 워커 ID 정의
        self.oracle_id = "oracle-01"
        self.finlib_id = "finlib-01"
        self.margin_id = "margin-01"

        # 테스트 페이즈 등록 (상태 비저장 고속 병렬 처리 및 라우팅 검증)
        self.set_phases([
            Phase("Phase 1: Event-Driven Zero-Latency Proof", self.phase_zero_latency),
            Phase("Phase 2: High-Throughput Linear Queue Stress (FinLib)", self.phase_finlib_linear_queue),
            Phase("Phase 3: Native Async Multiplexing Concurrency (Oracle)", self.phase_oracle_multiplexing),
            Phase("Phase 4: Unit Economics Vectorization (Margin BI)", self.phase_margin_simulation),
            Phase("Phase 5: x402 Billing Rejection (Free-Rider Defense)", self.phase_x402_rejection),
            Phase("Phase 6: Precise Error Routing (Invalid Params)", self.phase_error_routing),
        ])

    async def setup_workers(self):
        """Phase 1-6 실행에 필요한 워커만 초기화하여 커넥터 풀에 등록합니다."""
        
        # MULTIPLEX Mode: 비동기 통신 및 락 없는 인메모리 병렬 라우팅 (Oracle)
        oracle_cmd = f"{sys.executable} -m {agent_oracle.__name__}"
        self.connectors.append(
            WorkerConnector(target_id=self.oracle_id, legacy_command=oracle_cmd, mode="multiplex")
        )

        # LINEAR Mode: 극단적 속도를 위한 콜드스타트 없는 순차적 데몬 (FinLib, Margin)
        finlib_cmd = f"{sys.executable} -m {agent_finlib.__name__}"
        self.connectors.append(
            WorkerConnector(target_id=self.finlib_id, legacy_command=finlib_cmd, mode="linear")
        )

        margin_cmd = f"{sys.executable} -m {agent_margin.__name__}"
        self.connectors.append(
            WorkerConnector(target_id=self.margin_id, legacy_command=margin_cmd, mode="linear")
        )

    # =====================================================================
    # Test Phases (1 ~ 6)
    # =====================================================================
    async def phase_zero_latency(self):
        payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "eval_math", "arguments": {"expression": "100 * 50"}}}
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_x402"}
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
        if res.status_code != 200: raise RuntimeError(f"Expected 200, got {res.status_code}")

    async def phase_finlib_linear_queue(self):
        """워커 내부는 순차 처리이나, 커넥터와 OS 단에서의 병렬 큐 밀어넣기를 테스트"""
        async def send_compute(idx: int):
            payload = {"jsonrpc": "2.0", "id": idx, "method": "tools/call", "params": {"name": "resolve_dates", "arguments": {"base_date": "2026-09-04", "offset_business_days": idx}}}
            headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_x402"}
            async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
                return await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
        
        req_count = 10
        results = await asyncio.gather(*[send_compute(i) for i in range(1, req_count + 1)])
        if any(res.status_code != 200 for res in results): raise RuntimeError("Linear Queueing failed")
        
    async def phase_oracle_multiplexing(self):
        """Oracle 워커가 단일 프로세스 내에서 다수의 거래소 API 요청을 비동기 병렬로 처리하는지 검증"""
        async def fetch_symbol(symbol: str, req_id: int):
            payload = {
                "jsonrpc": "2.0", "id": req_id, "method": "tools/call",
                "params": {"name": "fetch_aggregated_kline", "arguments": {"symbol": symbol}}
            }
            headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_x402"}
            async with httpx.AsyncClient(base_url=self.local_url, timeout=15.0) as client:
                return await client.post(f"/v1/mcp-gateway/{self.oracle_id}/invoke", json=payload, headers=headers)
                
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        start_time = time.time()
        
        results = await asyncio.gather(*[fetch_symbol(sym, 100+i) for i, sym in enumerate(symbols)])
        
        elapsed = time.time() - start_time
        log.info(f"  └─ 3 Concurrent Oracle Fetches Completed in {elapsed:.2f}s")
        
        if any(res.status_code != 200 for res in results): 
            raise RuntimeError("Oracle Multiplexing failed. One or more API calls returned error.")

    async def phase_margin_simulation(self):
        payload = {
            "jsonrpc": "2.0", "id": 200, "method": "tools/call", 
            "params": {
                "name": "calculate_trajectory_margin",
                "arguments": {
                    "symbol": "BTC-USDT",
                    "observations": {
                        "arn:binance": {"rate": 0.0001, "time": int(time.time())},
                        "arn:bybit": {"rate": 0.00012, "time": int(time.time())}
                    },
                    "trade_size_usd": 10000.0,
                    "pricing": {"base_x402_fee_usd": 0.002, "profit_share_ratio": 0.05},
                    "infra": {"monthly_fixed_cost_usd": 30.0, "compute_cost_per_sec_usd": 0.00001, "avg_latency_sec": 0.05},
                    "tps_range": [1.0, 10.0, 50.0]
                }
            }
        }
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_x402"}
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.margin_id}/invoke", json=payload, headers=headers)
            if res.status_code != 200: 
                raise RuntimeError(f"Margin Sim Failed: Expected 200, got {res.status_code} ({res.text})")

    async def phase_x402_rejection(self):
        payload = {"jsonrpc": "2.0", "id": 300, "method": "tools/call", "params": {"name": "eval_math", "arguments": {"expression": "1 + 1"}}}
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "invalid_receipt"}
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
            if res.status_code != 402: raise RuntimeError("Expected HTTP 402")

    async def phase_error_routing(self):
        payload = {"jsonrpc": "2.0", "id": 99, "method": "tools/call", "params": {"name": "calc_indicators_batch", "arguments": {"prices_matrix": "BAD_DATA"}}}
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": uuid.uuid4().hex, "X-X402-Receipt": "valid_x402"}
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.finlib_id}/invoke", json=payload, headers=headers)
            if res.status_code != 502: raise RuntimeError("Expected HTTP 502")


class CoreSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        net_config = E2EConfig(host="127.0.0.1", port=8355, protocol="http")
        self.results.extend(await CoreRoutingPipeline(config=net_config).run_pipeline())
        self._print_report()

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("⚡ [CORE ROUTING BENCHMARK REPORT]")
        self.log.info("="*80)
        all_passed = all(r.passed for r in self.results)

        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            self.log.info(f"{status_icon} {idx:02d}. [{res.target}]".ljust(22) + f"{res.scenario.ljust(50)} | {'PASSED' if res.passed else 'FAILED'}")

        self.log.info("-" * 80)
        if all_passed: 
            self.log.info("🎉 CORE ROUTING PIPELINE VALIDATED SUCCESSFULLY.")
        else: 
            self.log.critical("💥 CORE ROUTING FAILURE DETECTED. Check logs for details.")
        self.log.info("="*80 + "\n")


def main(args_list: list[str] = None):
    app = CoreSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()