# fiber.dev.base.mcp.server
## @lineage: fiber.dev.e2e.base.mcp.server
## @lineage: fiber.infra.bound.e2e.mcp.server
## @lineage: fiber.infra.e2e.mcp.server
import time
import uuid
import sqlite3
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional

import orjson
from aiohttp import web

from fiber.infra.mcp.payload import McpRequest, McpPayloadBuilder
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.mcp.server")

_AUDIT_SEED = [
    ("filesystem.read", 1.25, "OK"),
    ("sqlite.query", 3.40, "OK"),
    ("repomix.pack", 11.20, "OK"),
    ("context7.get_docs", 42.50, "OK"),
]

_CONTEXT7_SEED = [
    ("/duckdb/duckdb", "duckdb", "v1.2.0", "read_json_auto", "### DuckDB JSON\n`SELECT * FROM read_json_auto(...)`", 4800),
    ("/vercel/next.js", "nextjs", "v15.1.0", "app-router-cache", "### Next.js 15 Cache\nUse `use cache` directive.", 6200),
]

_SUPPORTED_TOOLS = [
    {"name": "sqlite_query"},
    {"name": "filesystem_inspect"},
    {"name": "get-library-docs"},
    {"name": "query-docs"},
]


class MockMcpServer:
    """로컬 HTTP(/mcp), SSE(/sse-mcp), Context7 인증 Fallback(/context7-mcp) 통합 검증 서버"""

    def __init__(self, host: str = "127.0.0.1", port: int = 8359, expected_api_key: str = ""):
        self.host, self.port, self.expected_api_key = host, port, expected_api_key
        self.runner: Optional[web.AppRunner] = None
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init_schema()

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def mcp_url(self) -> str:
        return f"{self.base_url}/mcp"

    @property
    def sse_url(self) -> str:
        return f"{self.base_url}/sse-mcp"

    @property
    def context7_url(self) -> str:
        return f"{self.base_url}/context7-mcp"

    def _init_schema(self) -> None:
        cur = self.db.cursor()
        cur.execute("CREATE TABLE mcp_audit_logs (id INTEGER PRIMARY KEY, tool_name TEXT, latency_ms REAL, status TEXT)")
        cur.executemany("INSERT INTO mcp_audit_logs (tool_name, latency_ms, status) VALUES (?, ?, ?)", _AUDIT_SEED)
        cur.execute(
            "CREATE TABLE context7_docs_index ("
            "library_id TEXT PRIMARY KEY, library_name TEXT, version TEXT, topic TEXT, content_md TEXT, tokens_saved INTEGER)"
        )
        cur.executemany("INSERT INTO context7_docs_index VALUES (?, ?, ?, ?, ?, ?)", _CONTEXT7_SEED)
        self.db.commit()

    async def start(self) -> None:
        app = web.Application()
        app.router.add_post("/mcp", self.handle_mcp_http)
        app.router.add_post("/sse-mcp", self.handle_mcp_sse)
        app.router.add_post("/context7-mcp", self.handle_remote_context7_mcp)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        await web.TCPSite(self.runner, self.host, self.port).start()
        log.info(f"[MockMcpServer] Listening on {self.base_url} (/mcp, /sse-mcp, /context7-mcp)")

    async def stop(self) -> None:
        if self.db:
            self.db.close()
        if self.runner:
            await self.runner.cleanup()

    async def _execute_tool(self, req_id: Any, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        t0 = time.perf_counter()

        if tool_name == "sqlite_query":
            sql = args.get("query", "")
            if not sql.strip().upper().startswith("SELECT"):
                return McpPayloadBuilder.error(req_id, -32602, "Only SELECT queries are permitted in read-only mode.")
            try:
                rows = [dict(r) for r in self.db.execute(sql).fetchall()]
            except sqlite3.Error as e:
                return McpPayloadBuilder.error(req_id, -32603, f"SQLite execution error: {e}")
            return McpPayloadBuilder.tool_ok(
                req_id,
                {"rows": rows, "row_count": len(rows), "telemetry": {"compute_time_sec": round(time.perf_counter() - t0, 4)}},
            )

        if tool_name == "filesystem_inspect":
            target = Path(args.get("path", __file__)).resolve()
            if not target.exists():
                return McpPayloadBuilder.error(req_id, -32602, f"Path not found: {target}")
            raw = target.read_bytes() if target.is_file() else b""
            return McpPayloadBuilder.tool_ok(
                req_id,
                {"name": target.name, "size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()},
            )

        if tool_name in ("resolve-library-id", "get-library-docs", "query-docs", "context7_search"):
            q = (
                args.get("libraryName")
                or args.get("context7CompatibleLibraryID")
                or args.get("libraryId")
                or args.get("topic")
                or args.get("query")
                or ""
            )
            rows = [
                dict(r)
                for r in self.db.execute(
                    "SELECT * FROM context7_docs_index WHERE library_name LIKE ? OR library_id LIKE ? OR topic LIKE ?",
                    (f"%{q}%", f"%{q}%", f"%{q}%"),
                ).fetchall()
            ]
            return McpPayloadBuilder.tool_ok(
                req_id,
                {"object": "context7_documentation", "matched_count": len(rows), "results": rows},
            )

        return McpPayloadBuilder.error(req_id, -32601, f"Method/Tool not found: {tool_name}")

    async def _dispatch_rpc(self, req: McpRequest) -> Dict[str, Any]:
        """HTTP 및 SSE 엔드포인트 공통 JSON-RPC 메서드 라우터"""
        if req.method == "initialize":
            return McpPayloadBuilder.result(req.id, {"protocolVersion": "2026-07-28", "capabilities": {"tools": {}}})
        if req.method == "tools/list":
            return McpPayloadBuilder.result(req.id, {"tools": _SUPPORTED_TOOLS})
        if req.method == "tools/call":
            return await self._execute_tool(req.id, req.params.get("name", ""), req.params.get("arguments", {}))
        return McpPayloadBuilder.error(req.id, -32601, f"Unknown method {req.method}")

    async def handle_mcp_http(self, request: web.Request) -> web.Response:
        req = McpPayloadBuilder.parse_request(await request.read())
        body = await self._dispatch_rpc(req)
        headers = {"Mcp-Session-Id": request.headers.get("Mcp-Session-Id", f"mcp-sess-{uuid.uuid4().hex[:8]}")}
        return web.json_response(body, headers=headers)

    async def handle_remote_context7_mcp(self, request: web.Request) -> web.Response:
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or auth.split(" ", 1)[1] != self.expected_api_key:
            return web.json_response({"error": "Invalid Context7 API key"}, status=401)

        raw_bytes = await request.read()
        req = McpPayloadBuilder.parse_request(raw_bytes)
        if "_http_headers" in req:
            return web.json_response(
                McpPayloadBuilder.error(req.id, -32600, "'_http_headers' leaked into JSON-RPC body"),
                status=400,
            )

        body = await self._dispatch_rpc(req)
        headers = {"Mcp-Session-Id": request.headers.get("Mcp-Session-Id", f"mcp-sess-{uuid.uuid4().hex[:8]}")}
        return web.json_response(body, headers=headers)

    async def handle_mcp_sse(self, request: web.Request) -> web.StreamResponse:
        req = McpPayloadBuilder.parse_request(await request.read())
        resp = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Mcp-Session-Id": request.headers.get("Mcp-Session-Id", f"sse-sess-{uuid.uuid4().hex[:8]}"),
            },
        )
        await resp.prepare(request)
        await resp.write(b": stream-established\n\n")
        result = await self._dispatch_rpc(req)
        sse_frame = f"event: message\ndata: {orjson.dumps(result).decode('utf-8')}\n\n"
        await resp.write(sse_frame.encode("utf-8"))
        await resp.write_eof()
        return resp