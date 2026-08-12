# receptor.ingress.server.gateway
import asyncio
import json
import re
import sys
import datetime
from typing import Literal

from aiohttp import web, ClientSession
from pydantic_settings import BaseSettings, SettingsConfigDict
from watcher.plane.emitter import get_emitter
from receptor.ingress.server.mcp import SecureMCPServer

class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GATEWAY_")
    host: str = "0.0.0.0"
    proxy_port: int = 443         # Data Plane: 외부 클라이언트가 접근하는 Public Port
    mcp_port: int = 8084          # Control Plane: 보안 정책을 주입하는 MCP Port
    upstream_url: str = "http://127.0.0.1:8000"  # 숨겨진 내부 REST API (FastAPI) 주소
    transport_mode: Literal["stdio", "sse"] = "sse"


class DphiGatewayServer:
    def __init__(self, settings: GatewaySettings):
        self.settings = settings
        self.log = get_emitter("ingress.gateway", phase="GATEWAY")
        self.mcp = SecureMCPServer(name="mcp-gateway-control", version="1.0")
        
        self.client_session: ClientSession | None = None
        
        # L7 WAF (웹 방화벽) 메모리 데이터베이스
        self.firewall_rules = {
            "blocked_ips": set(),
            "quarantine_paths": set()
        }
        
        self._register_mcp_tools()

    def _register_mcp_tools(self):
        """MCP를 통한 동적 방화벽 제어 도구 (Zero-latency 룰 주입)"""
        
        @self.mcp.tool()
        async def block_ip(ip_address: str, reason: str = "Malicious activity") -> str:
            """특정 IP의 접근을 즉각 차단합니다."""
            self.firewall_rules["blocked_ips"].add(ip_address)
            self.log.warning(f"[Firewall] IP {ip_address} blocked. Reason: {reason}")
            return f"[SUCCESS] IP {ip_address} is now blocked at the Edge."

        @self.mcp.tool()
        async def quarantine_path(path_pattern: str) -> str:
            """특정 URI 경로에 대한 접근을 전면 차단합니다."""
            self.firewall_rules["quarantine_paths"].add(path_pattern)
            self.log.warning(f"[Firewall] Path {path_pattern} quarantined.")
            return f"[SUCCESS] Path {path_pattern} is quarantined."

        @self.mcp.tool()
        async def get_gateway_status() -> dict:
            """게이트웨이의 현재 상태와 룰 셋을 반환합니다."""
            return {
                "status": "OPERATIONAL",
                "upstream": self.settings.upstream_url,
                "blocked_ips_count": len(self.firewall_rules["blocked_ips"]),
                "quarantine_paths_count": len(self.firewall_rules["quarantine_paths"]),
                "timestamp": datetime.datetime.now().isoformat()
            }

    async def gateway_handler(self, request: web.Request) -> web.Response:
        """Data Plane: 외부 트래픽 수신 -> L7 보안 검사 -> 백엔드 릴레이"""
        client_ip = request.remote
        path = request.path
        
        # 1. 동적 방화벽 검사 (L7 WAF)
        if client_ip in self.firewall_rules["blocked_ips"]:
            self.log.warning(f"Blocked traffic from quarantined IP: {client_ip}")
            raise web.HTTPForbidden(reason="Brane Security: IP Quarantined.")
            
        if any(re.search(qp, path) for qp in self.firewall_rules["quarantine_paths"]):
            self.log.warning(f"Blocked traffic to quarantined path: {path}")
            raise web.HTTPForbidden(reason="Brane Security: Path Quarantined.")

        # 2. 헤더 조작 및 X-Forwarded-For 주입
        headers = dict(request.headers)
        # aiohttp가 자체적으로 Host를 세팅하도록 기존 Host 제거
        headers.pop("Host", None) 
        headers['X-Forwarded-For'] = client_ip
        headers['X-Gateway-Passed'] = "true"

        # 3. 백엔드(FastAPI)로 트래픽 릴레이 (Upstream Forwarding)
        target_url = f"{self.settings.upstream_url}{path}"
        data = await request.read()
        
        try:
            async with self.client_session.request(
                method=request.method,
                url=target_url,
                headers=headers,
                data=data,
                params=request.query
            ) as resp:
                response_body = await resp.read()
                
                # Upstream의 응답 헤더 필터링 (Hop-by-hop 헤더 제거)
                hop_by_hop = {'connection', 'keep-alive', 'proxy-authenticate', 
                              'proxy-authorization', 'te', 'trailers', 'transfer-encoding', 'upgrade'}
                clean_headers = {k: v for k, v in resp.headers.items() if k.lower() not in hop_by_hop}
                
                return web.Response(
                    body=response_body, 
                    status=resp.status, 
                    headers=clean_headers
                )
        except Exception as e:
            self.log.error(f"Upstream Relay Error: {e}")
            return web.Response(status=502, text="Bad Gateway: Internal REST Edge is unreachable.")

    # --- MCP Control Plane Handlers ---
    async def mcp_sse_handler(self, request: web.Request) -> web.Response:
        return await self.mcp.handle_sse_connection(request)

    async def mcp_message_handler(self, request: web.Request) -> web.Response:
        return await self.mcp.handle_post_message(request)

    # --- Lifecycle Hooks ---
    async def startup_context(self, app: web.Application):
        if not self.client_session:
            # 커넥션 풀 최적화를 위해 전역 Session 사용
            self.client_session = ClientSession()
            self.log.info("Initialized global ClientSession for Upstream Relay.")

    async def cleanup_context(self, app: web.Application):
        if self.client_session and not self.client_session.closed:
            await self.client_session.close()
            self.log.info("Closed global ClientSession.")

    async def start_dual_servers(self):
        """데이터 평면(Proxy)과 제어 평면(MCP)을 서로 다른 포트로 격리하여 구동"""
        # 1. Proxy Application (Data Plane)
        proxy_app = web.Application()
        proxy_app.on_startup.append(self.startup_context)
        proxy_app.on_cleanup.append(self.cleanup_context)
        proxy_app.router.add_route('*', '/{tail:.*}', self.gateway_handler)
        
        # 2. MCP Application (Control Plane)
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
            "msg": "🚀 Gateway Membrane Activated",
            "public_proxy_port": self.settings.proxy_port,
            "control_mcp_port": self.settings.mcp_port,
            "shielding_upstream": self.settings.upstream_url
        }), file=sys.stderr)

    def run(self):
        """단독 스크립트로 실행될 때 사용하는 진입점"""
        if self.settings.transport_mode == "sse":
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.start_dual_servers())
                loop.run_forever()
            except KeyboardInterrupt:
                self.log.info("Gateway shutting down gracefully...")
            finally:
                loop.close()
        else:
            self.log.info(json.dumps({"msg": "Running in STDIO mode (HTTP relay disabled)"}))
            self.mcp.run()

if __name__ == "__main__":
    server = DphiGatewayServer(GatewaySettings())
    server.run()