# dphi.gov.server.pypi
## @lineage: arch.gov.server.pypi
import asyncio
import datetime
import hashlib
import json
import re
import sys
from typing import Any, Literal, TypedDict, Tuple

from aiohttp import web, ClientSession
from mcp.server.streamable_http import EventStore
from pydantic_settings import BaseSettings, SettingsConfigDict

from dphi.gov.ingress.sentinel import get_projector, SecurityContext, MetaRuleDef
from dphi.gov.server.mcp import SecureMCPServer
from dphi.gov.audit.warden import AuditWarden
from watcher.plane.emitter import get_emitter

class ServerRunConfig(TypedDict, total=False):
    host: str
    transport: Literal["stdio", "sse", "streamable-http"]
    port: int
    event_store: EventStore | None
    retry_interval: int
    uvicorn_kwargs: dict[str, Any]

class PyPIProxySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PYPI_")
    host: str = "127.0.0.1"
    proxy_port: int = 8083  # Data Plane (PyPI 트래픽)
    mcp_port: int = 8084    # Control Plane (보안 룰 제어)
    upstream_url: str = "https://pypi.org"
    transport_mode: Literal["stdio", "sse"] = "sse"

# ---------------------------------------------------------
# Core Server Class (Stateful Membrane)
# ---------------------------------------------------------
class PyPIMembraneServer:
    def __init__(self, settings: PyPIProxySettings):
        """서버 초기화 및 의존성, 상태(State) 할당"""
        self.settings = settings
        self.log = get_emitter("pypi.server", phase="INGRESS")
        self.mcp = SecureMCPServer(name="mcp-brane-membrane", version="1.5")
        self.projector = get_projector()
        
        self.client_session: ClientSession | None = None
        
        # [제어/데이터 평면이 공유하는 휘발성 상태]
        self.artifact_cache: dict[str, Any] = {}
        self.quarantine_db: dict[str, Any] = {}
        
        self._register_mcp_tools()

    def _register_mcp_tools(self):
        """인스턴스 메모리를 직접 조작하는 MCP 제어 도구 등록"""
        self.mcp.tool()(self.tool_get_time)
        self.mcp.tool()(self.inject_mock_vulnerability)
        self.mcp.tool()(self.inject_meta_rule)
        self.mcp.tool()(self.clear_cache)

    # -----------------------------------------------------
    # MCP Tools (Control Plane)
    # -----------------------------------------------------
    async def tool_get_time(self) -> dict[str, str | float]:
        now = datetime.datetime.now()
        return {
            "current_time": now.isoformat(),
            "timezone": "KST",
            "timestamp": now.timestamp(),
        }

    async def inject_mock_vulnerability(self, package_name: str, action: str, cve_id: str) -> str:
        """Kernel 인가 후 런타임 메모리(quarantine_db)를 즉시 변형하여 프록시 트래픽을 차단"""
        
        # [FIX] 입력값 무결성(Sanitization) 검증 추가: Command Injection 및 Path Traversal 원천 차단
        if not re.match(r"^[a-zA-Z0-9_\-\.]+$", package_name):
            self.log.warning(f"Injection attempt detected in package_name: {package_name}")
            return "[ERROR] Invalid package name format. Alphanumeric, dash, underscore, dot only."
            
        if not re.match(r"^CVE-\d{4}-\d+$", cve_id) and cve_id != "CUSTOM":
            return "[ERROR] Invalid CVE ID format. Must match 'CVE-YYYY-NNNN'."

        valid_actions = ["block", "tamper_hash"]
        if action not in valid_actions:
            return f"[ERROR] Invalid action. Must be one of {valid_actions}."
        
        is_authorized = await self.projector.gateway.authorize(
            action_id=f"quarantine_{package_name}",
            action="INJECT_QUARANTINE_RULE",
            payload={"target": package_name, "action": action, "cve": cve_id}
        )
        
        if not is_authorized:
            AuditWarden.record_anomaly("mcp.quarantine_rejected", f"Kernel rejected {package_name}")
            return f"[ERROR] Quarantine rule for {package_name} rejected by Kernel."
        
        self.quarantine_db[package_name.lower()] = {"action": action, "cve": cve_id}
        self.log.info(f"[Membrane] Rule applied for {package_name} ({cve_id}) - Zero latency block active.")
        return f"[SUCCESS] Rule applied and sealed for {package_name}."

    async def inject_meta_rule(self, rule_id: str, rule_json: str) -> str:
        # [FIX] Rule ID 형식을 엄격히 검증하여 논리적 오염 방지
        if not re.match(r"^[a-zA-Z0-9_\-]+$", rule_id):
            return "[ERROR] Invalid rule_id format."
            
        try:
            rule_def = MetaRuleDef.model_validate_json(rule_json)
            
            # [FIX] 주입되는 Action이 시스템 권한을 상승시키는지 검사 (최소 권한 원칙)
            if rule_def.action not in ["block", "ledger_tension"]:
                return "[ERROR] Meta-rules can only enforce blocking actions."

            is_authorized = await self.projector.gateway.authorize(
                action_id=f"metarule_{rule_id}",
                action="INJECT_META_RULE",
                payload={"rule_id": rule_id, "definition": rule_json}
            )
            if not is_authorized:
                return f"[ERROR] Meta-rule {rule_id} rejected."

            self.projector.load_rule(rule_id, rule_def)
            self.log.info(f"[Membrane] Meta-rule {rule_id} loaded.")
            return f"[SUCCESS] Projector Meta-rule injected."
        except Exception as e:
            return f"[ERROR] Validation failed: {str(e)}"

    async def clear_cache(self) -> str:
        self.artifact_cache.clear()
        return "[SUCCESS] Internal PyPI cache cleared."

    # -----------------------------------------------------
    # HTTP Handlers (Data Plane Pipeline)
    # -----------------------------------------------------
    async def membrane_handler(self, request: web.Request) -> web.Response:
        """
        @desc: Core HTTP relay pipeline
        @flow: Boundary Check -> Local Quarantine -> Pre-fetch -> Upstream Fetch -> Post-fetch -> Cache
        """
        path = request.path

        try:
            # [FIX] 최전방 Boundary Check (Path Traversal 방어)
            if ".." in path or "%" in path:
                AuditWarden.record_anomaly("proxy.path_traversal", f"Traversal attempt blocked: {path}")
                raise web.HTTPForbidden(reason="Boundary breach: Path traversal attempt blocked.")

            # [FIX] 정규식을 통한 엄격한 패키지명 추출 (정상 PyPI 인덱스 규격만 허용)
            match = re.match(r"^/(simple|packages)/([a-zA-Z0-9_\-\.]+)/?.*?$", path)
            if not match:
                raise web.HTTPForbidden(reason="Boundary breach: Malformed PyPI URI.")
            
            package_name = match.group(2).lower()

            # 1. Zero-latency Local Quarantine Check
            self._enforce_local_quarantine(package_name)

            # 2. Pre-Fetch Security Evaluation (안전하게 정제된 컨텍스트 주입)
            ctx = SecurityContext(
                origin_ip=request.remote, 
                auth_header=request.headers.get('Authorization'),
                envelope_path=path, 
                envelope_method=request.method,
                nominal_name=package_name, 
                topology_version=None, 
                substance_hash=None
            )
            await self.projector.evaluate_pre_fetch(ctx)

            # 3. Cache Check
            if cached := self.artifact_cache.get(path):
                return web.Response(body=cached['body'], content_type=cached['content_type'])

            # 4. Fetch from Upstream
            content, content_type = await self._fetch_upstream(path)

            # 5. Post-Fetch Security Evaluation (Hash Check)
            ctx.substance_hash = hashlib.sha256(content).hexdigest()
            try:
                await self.projector.evaluate_post_fetch(ctx)
            except Exception as e:
                # [FIX] 캐시 오염 방어: 검증에 실패한 악성 페이로드는 메모리에서 즉시 파기(Zeroing)
                del content 
                raise e

            # 6. Cache and Return
            self.artifact_cache[path] = {"content_type": content_type, "body": content}
            return web.Response(body=content, content_type=content_type)

        except web.HTTPForbidden as hf:
            return web.Response(status=403, text=str(hf.reason))
        except Exception as e:
            self.log.error(f"Internal Proxy Error: {str(e)}")
            return web.Response(status=500, text="Internal Server Error: Secure Membrane Engaged")

    # --- Internal Pipeline Steps ---
    def _enforce_local_quarantine(self, package_name: str):
        if package_name and package_name in self.quarantine_db:
            policy = self.quarantine_db[package_name]
            if policy.get("action") == "block":
                cve = policy.get('cve')
                AuditWarden.record_anomaly("proxy.block.cve", f"Blocked {package_name} due to {cve}")
                raise web.HTTPForbidden(reason=f"Brane Security Membrane: Blocked ({cve})")

    async def _fetch_upstream(self, path: str) -> Tuple[bytes, str]:
        if not self.client_session:
            raise RuntimeError("ClientSession not initialized")
        
        target_url = f"{self.settings.upstream_url}{path}"
        async with self.client_session.get(target_url, headers={'User-Agent': 'pip/24.0 (Brane Proxy)'}) as resp:
            if resp.status != 200:
                raise web.HTTPForbidden(reason=f"Upstream returned {resp.status}")
            
            # [FIX] Volumetric 제한: Upstream 응답이 지나치게 클 경우(예: 50MB 이상) OOM 방지
            content_length = int(resp.headers.get('Content-Length', 0))
            if content_length > 52428800:  # 50MB
                raise web.HTTPForbidden(reason="Upstream response exceeds volumetric limits.")

            content = await resp.read()
            content_type = resp.headers.get('Content-Type', 'application/octet-stream')
            return content, content_type

    # -----------------------------------------------------
    # MCP Protocol Handlers
    # -----------------------------------------------------
    async def mcp_sse_handler(self, request: web.Request) -> web.Response:
        return await self.mcp.handle_sse_connection(request)

    async def mcp_message_handler(self, request: web.Request) -> web.Response:
        return await self.mcp.handle_post_message(request)

    # -----------------------------------------------------
    # Server Lifecycle (Dual App Setup)
    # -----------------------------------------------------
    async def startup_context(self, app: web.Application):
        if not self.client_session:
            self.client_session = ClientSession()
            self.log.info("Initialized global ClientSession.")

    async def cleanup_context(self, app: web.Application):
        if self.client_session and not self.client_session.closed:
            await self.client_session.close()
            self.log.info("Closed global ClientSession.")

    async def start_dual_servers(self):
        """데이터 평면(Proxy)과 제어 평면(MCP)을 서로 다른 포트/앱으로 격리하여 구동"""
        
        proxy_app = web.Application()
        proxy_app.on_startup.append(self.startup_context)
        proxy_app.on_cleanup.append(self.cleanup_context)
        proxy_app.router.add_route('*', '/{tail:.*}', self.membrane_handler)
        
        mcp_app = web.Application()
        mcp_app.router.add_get('/mcp/sse', self.mcp_sse_handler)
        mcp_app.router.add_post('/mcp/messages', self.mcp_message_handler)
        
        proxy_runner = web.AppRunner(proxy_app)
        mcp_runner = web.AppRunner(mcp_app)
        await proxy_runner.setup()
        await mcp_runner.setup()
        
        proxy_site = web.TCPSite(proxy_runner, self.settings.host, self.settings.proxy_port)
        mcp_site = web.TCPSite(mcp_runner, self.settings.host, self.settings.mcp_port)
        
        await asyncio.gather(proxy_site.start(), mcp_site.start())
        
        self.log.info(json.dumps({
            "msg": "🚀 Async Membrane Activated",
            "proxy_port": self.settings.proxy_port,
            "mcp_port": self.settings.mcp_port
        }), file=sys.stderr)

    def run(self):
        """서버 구동 진입점"""
        if self.settings.transport_mode == "sse":
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.start_dual_servers())
                loop.run_forever()
            except KeyboardInterrupt:
                self.log.info("Server shutting down gracefully...")
            finally:
                loop.close()
        else:
            self.log.info(json.dumps({"msg": "Running in STDIO mode (PyPI HTTP relay disabled)"}))
            self.mcp.run()

if __name__ == "__main__":
    server = PyPIMembraneServer(PyPIProxySettings())
    server.run()