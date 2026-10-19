# fiber.dev.e2e.worker.network
import os
import time
import uuid
import asyncio
from typing import List, Dict, Any, Optional

import httpx

from fiber.infra.e2e.config import Phase, E2EConfig, TestResult, get_free_port
from fiber.infra.e2e.pipeline import BaseBridgePipeline, log
from fiber.infra.e2e.mcp.server import MockMcpServer
from fiber.phase.cli.connect import resolve_connector_spec
from fiber.gateway.mcp.connector import MCPServerConnector
from fiber.gateway.mcp.payload import McpPayloadBuilder

from xphi.arch.contract.config.env import DEFAULT_GATEWAY_PORT
from xphi.state.phase.reactor import PhaseReactor
from xphi.arch.bound.adapter.gateway import DPoPClientGenerator

class MCPNetworkPipeline(BaseBridgePipeline):
    def __init__(self, config: E2EConfig):
        super().__init__(
            config=config,
            name="Network Transport (Local HTTP/SSE & Remote Context7 MCP) Suite",
            scope_name="MCP_NETWORK_SUITE",
        )
        self.mcp = McpPayloadBuilder()
        self.local_http_id = "net-sqlite-fs-01"
        self.local_sse_id = "net-sse-stream-01"
        self.remote_context7_id = "net-context7-remote-01"

        self.env_key_name = "CONTEXT7_API_KEY"
        user_key = os.environ.get(self.env_key_name, "").strip()
        self._injected_temp_env = not bool(user_key)
        self.active_api_key = user_key or f"ctx7_live_e2e_{uuid.uuid4().hex[:16]}"
        os.environ[self.env_key_name] = self.active_api_key

        self.mock_mcp = MockMcpServer(host="127.0.0.1", port=8359, expected_api_key=self.active_api_key)
        self.active_context7_url = os.environ.get("CONTEXT7_MCP_URL", "").strip() or self.mock_mcp.context7_url

        self.dpop_client = DPoPClientGenerator(key_size=2048)
        self.http_client: Optional[httpx.AsyncClient] = None

        self.set_phases([
            Phase("Phase 1: Connect Resolver & Mcp-Session-Id Header Persistence", self.phase_session_persistence),
            Phase("Phase 2: Local URL Real SQLite3 Execution & Standard Telemetry", self.phase_local_url_sqlite),
            Phase("Phase 3: High-Concurrency HTTP Multiplexing (Filesystem SHA-256)", self.phase_http_multiplex_concurrency),
            Phase("Phase 4: Streamable HTTP (text/event-stream SSE) Parsing", self.phase_sse_stream_transport),
            Phase("Phase 5: Remote Context7 MCP Docs Search with Env CONTEXT7_API_KEY", self.phase_remote_context7_docs),
            Phase("Phase 6: Remote API_KEY Tamper Defense (HTTP 401 -> Gateway 502)", self.phase_remote_auth_rejection),
            Phase("Phase 7: Network Error Routing (502 Bad Gateway on SQL Reject)", self.phase_network_error_routing),
        ])

    async def setup_custom_context(self) -> None:
        os.environ.setdefault("DPHI_MASTER_PASSPHRASE", "e2e-test-passphrase")
        self.http_client = httpx.AsyncClient(base_url=self.local_url, timeout=20.0)
        await self.mock_mcp.start()
        log.info(f"[E2E Network] Context7 Target: {self.active_context7_url}")

    async def teardown_custom(self) -> None:
        if self._injected_temp_env:
            os.environ.pop(self.env_key_name, None)
        if self.http_client:
            await self.http_client.aclose()
        await self.mock_mcp.stop()

    async def setup_workers(self) -> None:
        targets = [
            (self.local_http_id, self.mock_mcp.mcp_url),
            (self.local_sse_id, self.mock_mcp.sse_url),
            (self.remote_context7_id, self.active_context7_url),
        ]
        for tid, url in targets:
            spec = resolve_connector_spec(target=tid, exec_cmd=url)
            self.connectors.append(MCPServerConnector(spec.target_id, spec.exec_cmd, spec.mode, spec.transport))

    async def _post_gateway(self, target_id: str, payload: Dict[str, Any], timeout: float = 15.0) -> httpx.Response:
        nonce, url = uuid.uuid4().hex, f"{self.local_url}/v1/mcp-gateway/{target_id}/invoke"
        headers = {
            "x-idempotency-key": uuid.uuid4().hex,
            "x-nonce": nonce,
            "Fuel-Receipt": "valid_receipt",
            "DPoP": self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce),
        }
        return await self.http_client.post(url, json=payload, headers=headers, timeout=timeout)

    async def _call_tool(self, target_id: str, tool_name: str, timeout: float = 15.0, **arguments: Any) -> httpx.Response:
        return await self._post_gateway(target_id, self.mcp.next_tool_call(tool_name, **arguments), timeout=timeout)

    @staticmethod
    def _extract_data(res: httpx.Response, parse_json: bool = True) -> Any:
        if res.status_code != 200:
            raise RuntimeError(f"Gateway HTTP {res.status_code}: {res.text}")
        return McpPayloadBuilder.extract_data(res.content, parse_json=parse_json)

    async def phase_session_persistence(self) -> None:
        res = await self._post_gateway(self.local_http_id, self.mcp.next_initialize())
        conn = next(c for c in self.connectors if c.target_id == self.local_http_id)
        session_id = getattr(conn.shared_transport, "_mcp_session_id", None)
        if res.status_code != 200 or conn.transport_type != "network" or conn.mode != "multiplex" or not session_id:
            raise RuntimeError("Failed to resolve network/multiplex or capture Mcp-Session-Id.")
        log.info(f"  └─ Captured Mcp-Session-Id: {session_id} (Resolver Mode: {conn.mode.upper()})")

    async def phase_local_url_sqlite(self) -> None:
        res = await self._call_tool(
            self.local_http_id,
            "sqlite_query",
            query="SELECT tool_name, latency_ms FROM mcp_audit_logs WHERE status = 'OK' ORDER BY latency_ms ASC",
        )
        data = self._extract_data(res)
        if data.get("row_count") != 4:
            raise RuntimeError(f"Expected 4 rows, got {data.get('row_count')}")
        log.info(f"  └─ SQLite3 Rows Verified: {data['row_count']} rows")

    async def phase_http_multiplex_concurrency(self) -> None:
        t0 = time.perf_counter()
        results = await asyncio.gather(*[self._call_tool(self.local_http_id, "filesystem_inspect", path=__file__) for _ in range(8)])
        if any(r.status_code != 200 for r in results):
            raise RuntimeError("Concurrent HTTP multiplexed file inspection failed.")
        sample = self._extract_data(results[0])
        log.info(f"  └─ 8 Concurrent HTTP File SHA-256 Checks in {time.perf_counter() - t0:.2f}s (Hash: {sample['sha256'][:12]}...)")

    async def phase_sse_stream_transport(self) -> None:
        res = await self._call_tool(self.local_sse_id, "sqlite_query", query="SELECT COUNT(*) AS total FROM mcp_audit_logs")
        total = self._extract_data(res)["rows"][0]["total"]
        if total != 4:
            raise RuntimeError(f"Expected count 4 over SSE stream, got {total}")
        log.info(f"  └─ SSE (text/event-stream) Frame Parsed & Resolved Successfully (total={total})")

    async def phase_remote_context7_docs(self) -> None:
        list_res = await self._post_gateway(self.remote_context7_id, self.mcp.next_request("tools/list"))
        tools = [t.get("name") for t in (McpPayloadBuilder.parse_response(list_res.content).result or {}).get("tools", [])] if list_res.status_code == 200 else []
        tool_name = "query-docs" if "query-docs" in tools else ("get-library-docs" if "get-library-docs" in tools else "resolve-library-id")

        res = await self._call_tool(
            self.remote_context7_id,
            tool_name,
            timeout=20.0,
            context7CompatibleLibraryID="/duckdb/duckdb",
            libraryId="/duckdb/duckdb",
            libraryName="duckdb",
            topic="read_json_auto",
            query="read_json_auto",
        )
        content_text = self._extract_data(res, parse_json=False)
        log.info(f"  └─ Context7 Docs Search ({tool_name}) Authorized & Executed ({len(content_text)} bytes)")

    async def phase_remote_auth_rejection(self) -> None:
        original_key = os.environ.get(self.env_key_name, "")
        os.environ[self.env_key_name] = "ctx7_invalid_compromised_token_000"
        try:
            res = await self._call_tool(self.remote_context7_id, "get-library-docs", context7CompatibleLibraryID="/vercel/next.js")
            if res.status_code != 502:
                raise RuntimeError(f"Expected HTTP 502 on 401 Unauthorized, got {res.status_code}")
            log.info("  └─ Invalid CONTEXT7_API_KEY (HTTP 401) accurately translated to Gateway HTTP 502.")
        finally:
            os.environ[self.env_key_name] = original_key

    async def phase_network_error_routing(self) -> None:
        res = await self._call_tool(self.local_http_id, "sqlite_query", query="DROP TABLE mcp_audit_logs")
        if res.status_code != 502:
            raise RuntimeError(f"Expected HTTP 502 on invalid SQL mutation, got {res.status_code}")
        log.info("  └─ NetworkTransport JSON-RPC Error accurately mapped to Gateway HTTP 502.")


async def run_suite() -> None:
    pipeline = MCPNetworkPipeline(E2EConfig(host="127.0.0.1", port=get_free_port(DEFAULT_GATEWAY_PORT), protocol="http"))
    results: List[TestResult] = await pipeline.run_pipeline()
    all_passed = all(r.passed for r in results)

    lines = ["\n" + "=" * 80, "🌐 [NETWORK TRANSPORT & REMOTE CONTEXT7 MCP E2E REPORT]", "=" * 80]
    for idx, r in enumerate(results, 1):
        lines.append(f"{'✅' if r.passed else '❌'} {idx:02d}. [{r.target}]".ljust(22) + f"{r.scenario.ljust(50)} | {'PASSED' if r.passed else 'FAILED'}")
    lines.extend(["-" * 80, "NETWORK PIPELINE VALIDATED SUCCESSFULLY." if all_passed else "NETWORK PIPELINE FAILURE DETECTED.", "=" * 80 + "\n"])
    print("\n".join(lines), flush=True)


def main(args_list: Optional[List[str]] = None) -> None:
    PhaseReactor.ignite(main_coro_func=run_suite)


if __name__ == "__main__":
    main()