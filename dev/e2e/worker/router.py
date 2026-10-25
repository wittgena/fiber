# fiber.dev.e2e.worker.router
"""
@requirements: fiber daemon 
$ XPHI_WORKERS=1 python -m xphi.node.ops.boot
"""
import sys
import time
import uuid
import json
import asyncio
from pathlib import Path
from typing import List, Dict

import httpx

from fiber.dev.base.workflow.config import Phase, E2EConfig, TestResult, get_free_port
from fiber.dev.base.workflow.pipeline import MockPipeline, log
from fiber.infra.mcp.connector import MCPServerConnector
from fiber.infra.mcp.mapper.config import McpConfigMapper
from fiber.gateway.bridge.action.connect import (
    ACTIVE_TARGETS_TUNNEL_KEY,
    ResolvedTarget,
    _specs_from_mapped_entries,
    run_connect_copy,
)
import fiber.infra.mcp.worker.price.token as worker_price_token
import fiber.infra.mcp.worker.eco.finlib as worker_finlib
import fiber.infra.mcp.worker.eco.margin as worker_margin
import fiber.infra.mcp.worker.search.archive as worker_search

from xphi.node.ops.daemon.eco.pricing import DynamicPricingDaemon
from xphi.node.space.bind.resolver import resolve_path
from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.state.phase.reactor import PhaseReactor
from xphi.arch.bound.adapter.gateway import DPoPClientGenerator

ANCHOR_ROOT = resolve_path("anchor")
ANCHOR_MCP_JSON = ANCHOR_ROOT / "mcp.json"


class MCPRouterPipeline(MockPipeline):
    def __init__(self, config: E2EConfig):
        super().__init__(
            config=config,
            name="Core Routing & Performance Suite",
            scope_name="MCP_ROUTER_SUITE",
        )
        self.oracle_id = "worker.price.token"
        self.finlib_id = "worker.eco.finlib"
        self.margin_id = "worker.eco.margin"
        self.search_id = "worker.search.archive"

        self.dpop_client = DPoPClientGenerator(key_size=2048)
        self.set_phases([
            Phase("Phase 1: Event-Driven Zero-Latency Proof", self.phase_zero_latency),
            Phase("Phase 2: High-Throughput Linear Queue Stress (FinLib)", self.phase_finlib_linear_queue),
            Phase("Phase 3: Native Async Multiplexing Concurrency (Oracle)", self.phase_oracle_multiplexing),
            Phase("Phase 4: Unit Economics Vectorization (Margin BI)", self.phase_margin_simulation),
            Phase("Phase 5: Billing Rejection (Free-Rider Defense)", self.phase_billing_rejection),
            Phase("Phase 6: Precise Error Routing (Invalid Params)", self.phase_error_routing),
            Phase("Phase 7: Data-Intensive Async Multiplexing (DuckDB Search)", self.phase_duckdb_search_worker),
            Phase("Phase 8: Autonomic Dynamic Pricing Alignment", self.phase_dynamic_pricing_alignment),
        ])

    # Lifecycle Overrides
    async def setup_custom_context(self):
        class MockPricingCtx:
            pass

        ctx = MockPricingCtx()
        ctx.tunnel = await TunnelFactory.get_default()
        ctx.rpc = InternalRpcClient()

        self.pricing_daemon = DynamicPricingDaemon(ctx=ctx)
        self.pricing_daemon.running = True
        self._pricing_task = asyncio.create_task(self.pricing_daemon.run())
        log.info("[E2E Pipeline] DynamicPricingDaemon successfully injected into test lifecycle.")

    async def teardown_custom(self):
        if hasattr(self, "pricing_daemon"):
            self.pricing_daemon.running = False
        if hasattr(self, "_pricing_task"):
            self._pricing_task.cancel()

    def _load_specs_from_anchor_root(self) -> Dict[str, ResolvedTarget]:
        """ANCHOR_ROOT/mcp.json에서 정규화된 워커 스펙을 로드 (파일이 없으면 자동 부트스트랩)"""
        if not ANCHOR_MCP_JSON.is_file():
            log.info(f"[E2E Router] {ANCHOR_MCP_JSON} not found. Bootstrapping via connect copy...")
            try:
                run_connect_copy(exec_cmd="builtin")
            except BaseException as e:
                log.warning(f"[E2E Router] Auto-bootstrap warning: {e}")

        if ANCHOR_MCP_JSON.is_file():
            entries = McpConfigMapper.parse_file(ANCHOR_MCP_JSON, agent_hint="fiber")
            specs = _specs_from_mapped_entries(entries)
            return {s.target_id: s for s in specs}
        return {}

    async def setup_workers(self):
        # 1. 외부 McpWorkerDaemon이 이미 띄워놓은 활성 타겟 목록 조회
        active_daemon_ids = set()
        try:
            tunnel = await TunnelFactory.get_default()
            raw = await tunnel.get(ACTIVE_TARGETS_TUNNEL_KEY)
            if raw:
                active_daemon_ids = set(json.loads(raw))
        except Exception:
            pass

        # 2. ANCHOR_ROOT/mcp.json 에서 스펙 로드 (폴백용 기본 스펙 준비)
        anchor_specs = self._load_specs_from_anchor_root()

        fallback_defaults = {
            self.oracle_id: (f"{sys.executable} -m {worker_price_token.__name__}", "multiplex"),
            self.finlib_id: (f"{sys.executable} -m {worker_finlib.__name__}", "linear"),
            self.margin_id: (f"{sys.executable} -m {worker_margin.__name__}", "multiplex"),
            self.search_id: (f"{sys.executable} -m {worker_search.__name__}", "multiplex"),
        }

        required_target_ids = [self.oracle_id, self.finlib_id, self.margin_id, self.search_id]

        for tid in required_target_ids:
            if tid in active_daemon_ids:
                log.info(
                    f"[E2E Router] 🔗 Attached to live McpWorkerDaemon target: [{tid}] (Zero duplicate spawn)"
                )
                continue

            # 데몬이 떠 있지 않은 경우 ANCHOR_ROOT/mcp.json 스펙(또는 폴백)으로 로컬 커넥터 기동
            if tid in anchor_specs:
                spec = anchor_specs[tid]
                log.info(
                    f"[E2E Router] Spawning connector [{tid}] from {ANCHOR_MCP_JSON} (Mode: {spec.mode.upper()})"
                )
                self.connectors.append(
                    MCPServerConnector(
                        target_id=spec.target_id,
                        execution_target=spec.exec_cmd,
                        mode=spec.mode,
                        transport_type=spec.transport,
                        preflight_pattern=spec.preflight,
                    )
                )
            else:
                cmd, mode = fallback_defaults[tid]
                log.info(f"[E2E Router] Spawning fallback connector [{tid}] (Mode: {mode.upper()})")
                self.connectors.append(
                    MCPServerConnector(target_id=tid, execution_target=cmd, mode=mode)
                )

    # Test Phases (1 ~ 8)
    async def phase_zero_latency(self):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "eval_math", "arguments": {"expression": "100 * 50"}},
        }
        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.finlib_id}/invoke"
        dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        headers = {
            "x-idempotency-key": uuid.uuid4().hex,
            "x-nonce": nonce,
            "Fuel-Receipt": "valid_receipt",
            "DPoP": dpop_proof,
        }

        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(url, json=payload, headers=headers)
        if res.status_code != 200:
            raise RuntimeError(f"Expected 200, got {res.status_code}")

    async def phase_finlib_linear_queue(self):
        async def send_compute(idx: int):
            payload = {
                "jsonrpc": "2.0",
                "id": idx,
                "method": "tools/call",
                "params": {
                    "name": "resolve_dates",
                    "arguments": {"base_date": "2026-09-04", "offset_business_days": idx},
                },
            }

            nonce = uuid.uuid4().hex
            url = f"{self.local_url}/v1/mcp-gateway/{self.finlib_id}/invoke"
            dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
            headers = {
                "x-idempotency-key": uuid.uuid4().hex,
                "x-nonce": nonce,
                "Fuel-Receipt": "valid_receipt",
                "DPoP": dpop_proof,
            }

            async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
                return await client.post(url, json=payload, headers=headers)

        req_count = 10
        results = await asyncio.gather(*[send_compute(i) for i in range(1, req_count + 1)])
        if any(res.status_code != 200 for res in results):
            raise RuntimeError("Linear Queueing failed")

    async def phase_oracle_multiplexing(self):
        async def fetch_symbol(symbol: str, req_id: int):
            payload = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {"name": "fetch_aggregated_kline", "arguments": {"symbol": symbol}},
            }

            nonce = uuid.uuid4().hex
            url = f"{self.local_url}/v1/mcp-gateway/{self.oracle_id}/invoke"
            dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
            headers = {
                "x-idempotency-key": uuid.uuid4().hex,
                "x-nonce": nonce,
                "Fuel-Receipt": "valid_receipt",
                "DPoP": dpop_proof,
            }

            async with httpx.AsyncClient(base_url=self.local_url, timeout=15.0) as client:
                return await client.post(url, json=payload, headers=headers)

        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        start_time = time.time()

        results = await asyncio.gather(*[fetch_symbol(sym, 100 + i) for i, sym in enumerate(symbols)])
        elapsed = time.time() - start_time
        log.info(f"  └─ 3 Concurrent Oracle Fetches Completed in {elapsed:.2f}s")

        if any(res.status_code != 200 for res in results):
            raise RuntimeError("Oracle Multiplexing failed. One or more API calls returned error.")

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
                        "arn:bybit": {"rate": 0.00012, "time": int(time.time())},
                    },
                    "trade_size_usd": 10000.0,
                    "pricing": {"base_fee_usd": 0.002, "profit_share_ratio": 0.05},
                    "infra": {
                        "monthly_fixed_cost_usd": 30.0,
                        "compute_cost_per_sec_usd": 0.00001,
                        "avg_latency_sec": 0.05,
                    },
                    "tps_range": [1.0, 10.0, 50.0],
                },
            },
        }

        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.margin_id}/invoke"
        dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        headers = {
            "x-idempotency-key": uuid.uuid4().hex,
            "x-nonce": nonce,
            "Fuel-Receipt": "valid_receipt",
            "DPoP": dpop_proof,
        }

        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(url, json=payload, headers=headers)
            if res.status_code != 200:
                raise RuntimeError(f"Margin Sim Failed: Expected 200, got {res.status_code} ({res.text})")

    async def phase_billing_rejection(self):
        tunnel = await TunnelFactory.get_default()
        redis_key = f"config:fuel:{self.finlib_id}"

        await tunnel.set(redis_key, "1")
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 300,
                "method": "tools/call",
                "params": {"name": "eval_math", "arguments": {"expression": "1 + 1"}},
            }
            nonce = uuid.uuid4().hex
            url = f"{self.local_url}/v1/mcp-gateway/{self.finlib_id}/invoke"
            dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
            headers = {
                "x-idempotency-key": uuid.uuid4().hex,
                "x-nonce": nonce,
                "Fuel-Receipt": "invalid_receipt",
                "DPoP": dpop_proof,
            }

            async with httpx.AsyncClient(base_url=self.local_url) as client:
                res = await client.post(url, json=payload, headers=headers)
                if res.status_code != 402:
                    raise RuntimeError(f"Expected HTTP 402, got {res.status_code}")
        finally:
            await tunnel.delete(redis_key)

    async def phase_error_routing(self):
        payload = {
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": {"name": "calc_indicators_batch", "arguments": {"prices_matrix": "BAD_DATA"}},
        }

        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.finlib_id}/invoke"
        dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        headers = {
            "x-idempotency-key": uuid.uuid4().hex,
            "x-nonce": nonce,
            "Fuel-Receipt": "valid_receipt",
            "DPoP": dpop_proof,
        }

        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(url, json=payload, headers=headers)
            if res.status_code != 502:
                raise RuntimeError("Expected HTTP 502")

    async def phase_duckdb_search_worker(self):
        req_id = 400
        payload = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "tools/call",
            "params": {
                "name": "search_market_evidence",
                "arguments": {"domain": "Domain_1_Control_Failure", "limit": 2},
            },
        }

        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.search_id}/invoke"
        dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        headers = {
            "x-idempotency-key": uuid.uuid4().hex,
            "x-nonce": nonce,
            "Fuel-Receipt": "valid_receipt",
            "DPoP": dpop_proof,
        }

        start_time = time.time()
        async with httpx.AsyncClient(base_url=self.local_url, timeout=60.0) as client:
            res = await client.post(url, json=payload, headers=headers)

        elapsed = time.time() - start_time
        if res.status_code != 200:
            raise RuntimeError(f"Search Worker Failed: Expected 200, got {res.status_code}")

        data = res.json()
        if "error" in data:
            raise RuntimeError(f"Search Worker returned JSON-RPC error: {data['error']}")

        try:
            result_content = data.get("result", {}).get("content", [])[0].get("text", "{}")
            parsed_result = json.loads(result_content)

            telemetry = parsed_result.get("telemetry", {})
            if telemetry:
                log.info(f"  └─ Total E2E Latency: {elapsed:.2f}s")
                log.info(f"  ├─ [DuckDB] Processed {telemetry.get('scanned_file_mb', 0)} MB")
                log.info(f"  ├─ [DuckDB] Extracted {telemetry.get('total_events_parsed', 0)} events")
                log.info(f"  ├─ [DuckDB] Pure SQL Time: {telemetry.get('duckdb_sql_time_sec', 0)}s")
                log.info(f"  └─ [DuckDB] Python Time: {telemetry.get('python_regex_time_sec', 0)}s")
        except Exception as e:
            raise RuntimeError(f"Search response parsing failed: {e}")

    async def phase_dynamic_pricing_alignment(self):
        tunnel = await TunnelFactory.get_default()
        redis_key = f"config:fuel:{self.search_id}"

        await tunnel.set(redis_key, "1")
        try:
            await asyncio.sleep(1.0)

            price_tag_key = f"eco:price_tag:{self.search_id}"
            dynamic_price_str = await tunnel.get(price_tag_key)

            if not dynamic_price_str:
                raise RuntimeError(f"Pricing Daemon failed to write to Redis key: {price_tag_key}")

            dynamic_price = float(dynamic_price_str)
            genesis_floor = 0.002

            log.info(f"  ├─ Genesis Floor Price: ${genesis_floor:.4f}")
            log.info(f"  ├─ New Dynamic Price  : ${dynamic_price:.4f}")

            if dynamic_price <= genesis_floor:
                raise RuntimeError(
                    f"Dynamic price (${dynamic_price}) did not increase from Genesis floor despite compute usage."
                )

            payload = {
                "jsonrpc": "2.0",
                "id": 500,
                "method": "tools/call",
                "params": {
                    "name": "search_market_evidence",
                    "arguments": {"domain": "Domain_2_Security_Audit", "limit": 1},
                },
            }

            nonce = uuid.uuid4().hex
            url = f"{self.local_url}/v1/mcp-gateway/{self.search_id}/invoke"
            dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
            headers = {
                "x-idempotency-key": uuid.uuid4().hex,
                "x-nonce": nonce,
                "Fuel-Receipt": "invalid_receipt",
                "DPoP": dpop_proof,
            }

            async with httpx.AsyncClient(base_url=self.local_url) as client:
                res = await client.post(url, json=payload, headers=headers)

            if res.status_code != 402:
                raise RuntimeError(f"Expected 402 Payment Required, got {res.status_code}")

            error_msg = res.json().get("detail", "")
            expected_fee_string = f"${dynamic_price:.4f}"

            if expected_fee_string not in error_msg:
                raise RuntimeError(f"Rejection message does not contain dynamic price! Msg: {error_msg}")

            log.info(
                f"  └─ Gateway precisely rejected underfunded request based on Dynamic Price ({expected_fee_string})."
            )
        finally:
            # 상태 복구
            await tunnel.delete(redis_key)


class CoreSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        net_config = E2EConfig(host="127.0.0.1", port=get_free_port(8355), protocol="http")
        self.results.extend(await MCPRouterPipeline(config=net_config).run_pipeline())
        self._print_report()

    def _print_report(self):
        all_passed = all(r.passed for r in self.results)
        report_buffer = [
            "\n" + "=" * 80,
            "⚡ [CORE ROUTING BENCHMARK REPORT]",
            "=" * 80,
        ]

        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            line = (
                f"{status_icon} {idx:02d}. [{res.target}]".ljust(28)
                + f"{res.scenario.ljust(50)} | {'PASSED' if res.passed else 'FAILED'}"
            )
            report_buffer.append(line)

        report_buffer.append("-" * 80)

        if all_passed:
            report_buffer.append("CORE ROUTING PIPELINE VALIDATED SUCCESSFULLY.")
        else:
            report_buffer.append("CORE ROUTING FAILURE DETECTED. Check logs for details.")

        report_buffer.append("=" * 80 + "\n")
        aggregated_report = "\n".join(report_buffer)
        if all_passed:
            self.log.info(aggregated_report)
        else:
            self.log.critical(aggregated_report)


def main(args_list: list[str] = None):
    app = CoreSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)


if __name__ == "__main__":
    main()