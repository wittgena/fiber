# fiber.phase.kernel.daemon.rest
import os
import asyncio
import json
import sys
import datetime
from typing import Optional, List, Literal
import uvicorn
from contextlib import suppress
from aiohttp import web, ClientSession
from pydantic_settings import BaseSettings, SettingsConfigDict

from fiber.dphi.edge.payload import create_app, Config
from xphi.arch.contract.registry.unified import contract
from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.kernel.ops.reaper import SystemOps
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.server.mcp import SecureMCPServer
from xphi.watcher.receptor.policy.router import RoutingPolicyEngine, ClusterStateMesh
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.dphi.ledger.consensus import KernelLedger

log = get_emitter("daemon.edge")

# =========================================================================
# Shared Utility: Port Reaper (권한 및 시스템 포트 충돌 방어 처리)
# =========================================================================
async def clear_zombie_ports(ports: List[int], tag: str):
    """지정된 포트들을 점유하고 있는 좀비 프로세스를 정리하는 공통 유틸리티"""
    reaper = SystemOps(redis_conn=None, tag=tag)
    my_pid = str(os.getpid())
    is_root = os.geteuid() == 0 if hasattr(os, 'geteuid') else False
    
    for port in set(ports):
        # 특권 포트(443 등)에 대한 무의미한 OS 시스템 프로세스 킬링 방지
        if port < 1024 and not is_root:
            log.warning(f"[{tag}] Port {port} is a privileged port. Reaper bypassed due to non-root execution.")
            continue

        try:
            pids = await reaper.get_pids_from_port(port)
            for pid in pids:
                if pid == my_pid:
                    continue
                log.warning(f"[{tag}] Port {port} is occupied by PID {pid}. Attempting to reap...")
                try:
                    await reaper._execute_kill(pid, force=True)
                except Exception as kill_err:
                    log.error(f"[{tag}] Failed to kill PID {pid} (Possible OS/Permission restriction): {kill_err}")
            
            if pids:
                await asyncio.sleep(0.5)
        except Exception as e:
            log.warning(f"[{tag}] Error scanning port {port}: {e}")


# =========================================================================
# Internal Component: Gateway Core Server (aiohttp)
# =========================================================================
class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GATEWAY_")
    host: str = "0.0.0.0"
    # 시스템 환경변수가 없으면 권한 제약이 없는 8443을 기본값으로 사용하여 충돌 차단
    proxy_port: int = int(os.getenv("GATEWAY_PROXY_PORT", 8443)) 
    mcp_port: int = int(os.getenv("GATEWAY_MCP_PORT", 8084))
    upstream_url: str = "http://127.0.0.1:8000"
    transport_mode: Literal["stdio", "sse"] = "sse"

class DphiGatewayServer:
    def __init__(self, settings: GatewaySettings):
        self.settings = settings
        self.log = get_emitter("ingress.gateway", phase="GATEWAY")
        self.mcp = SecureMCPServer(name="mcp-gateway-control", version="1.0")
        self.client_session: ClientSession | None = None
        self.firewall_rules = {"blocked_ips": set(), "quarantine_paths": set()}
        self._register_mcp_tools()

    def _register_mcp_tools(self):
        @self.mcp.tool()
        async def block_ip(ip_address: str, reason: str = "Malicious activity") -> str:
            self.firewall_rules["blocked_ips"].add(ip_address)
            return f"[SUCCESS] IP {ip_address} is now blocked."

        @self.mcp.tool()
        async def get_gateway_status() -> dict:
            return {
                "status": "OPERATIONAL",
                "upstream": self.settings.upstream_url,
                "blocked_ips_count": len(self.firewall_rules["blocked_ips"]),
                "timestamp": datetime.datetime.now().isoformat()
            }

    async def gateway_handler(self, request: web.Request) -> web.Response:
        client_ip = request.remote
        path = request.path
        
        if not path.startswith("/v1/public"):
            raise web.HTTPForbidden(reason="Brane Security: Access Denied.")
        if client_ip in self.firewall_rules["blocked_ips"]:
            raise web.HTTPForbidden(reason="Brane Security: IP Quarantined.")

        headers = dict(request.headers)
        headers.pop("Host", None) 
        headers['X-Forwarded-For'] = client_ip
        headers['X-Gateway-Passed'] = "true"

        target_url = f"{self.settings.upstream_url}{path}"
        data = await request.read()
        
        try:
            async with self.client_session.request(
                method=request.method, url=target_url, headers=headers, data=data, params=request.query
            ) as resp:
                response_body = await resp.read()
                clean_headers = {k: v for k, v in resp.headers.items() if k.lower() not in {'connection', 'keep-alive', 'upgrade'}}
                return web.Response(body=response_body, status=resp.status, headers=clean_headers)
        except Exception as e:
            self.log.error(f"Upstream Relay Error: {e}")
            return web.Response(status=502, text="Bad Gateway.")

    async def mcp_sse_handler(self, request: web.Request) -> web.Response:
        return await self.mcp.handle_sse_connection(request)

    async def mcp_message_handler(self, request: web.Request) -> web.Response:
        return await self.mcp.handle_post_message(request)

    async def startup_context(self, app: web.Application):
        if not self.client_session:
            self.client_session = ClientSession()

    async def cleanup_context(self, app: web.Application):
        if self.client_session and not self.client_session.closed:
            await self.client_session.close()

    async def start_dual_servers(self):
        proxy_app = web.Application()
        proxy_app.on_startup.append(self.startup_context)
        proxy_app.on_cleanup.append(self.cleanup_context)
        proxy_app.router.add_route('*', '/{tail:.*}', self.gateway_handler)
        
        mcp_app = web.Application()
        mcp_app.router.add_get('/mcp/sse', self.mcp_sse_handler)
        mcp_app.router.add_post('/mcp/messages', self.mcp_message_handler)
        
        proxy_runner = web.AppRunner(proxy_app)
        mcp_runner = web.AppRunner(mcp_app)
        await proxy_runner.setup()
        await mcp_runner.setup()
        
        proxy_site = web.TCPSite(proxy_runner, self.settings.host, self.settings.proxy_port)
        mcp_site = web.TCPSite(mcp_runner, self.settings.host, self.settings.mcp_port)
        
        try:
            await asyncio.gather(proxy_site.start(), mcp_site.start())
        except OSError as e:
            self.log.error(f"Failed to bind ports ({self.settings.proxy_port}, {self.settings.mcp_port}). Permission denied or port in use: {e}")
            raise e
        
        self.log.info(json.dumps({
            "msg": "🚀 Gateway Membrane Activated",
            "public_proxy_port": self.settings.proxy_port,
            "control_mcp_port": self.settings.mcp_port,
            "shielding_upstream": self.settings.upstream_url
        }), file=sys.stderr)


# =========================================================================
# 1. Gateway Edge Daemon (Public / External Traffic)
# =========================================================================
@contract.daemon("gateway_edge")
class GatewayEdgeDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("GatewayEdgeDaemon")
        self.ctx = ctx
        self.settings = GatewaySettings()
        self.gateway_server: Optional[DphiGatewayServer] = None
        self._tasks = set()
        
    async def _setup_routing_mesh(self):
        topology = os.getenv("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        log.info(f"[{self.name}] Assembling Unified Membrane in {topology} mode.")

        broker_facade = getattr(self.ctx, 'tunnel', None)
        if not broker_facade:
            raise RuntimeError("Tunnel dependencies not injected in RuntimeContext.")

        policy_engine = RoutingPolicyEngine(broker_facade)
        state_mesh = ClusterStateMesh(broker_facade)
        
        await policy_engine.synchronize_initial_state()
        self._tasks.add(asyncio.create_task(policy_engine.watch_policy_updates()))
        self._tasks.add(asyncio.create_task(state_mesh.start_mesh_sync()))
        
        if topology == "EXT_PROC":
            from xphi.watcher.receptor.policy.router import ExtProcStreamHandler
            stream_handler = ExtProcStreamHandler(policy_engine, state_mesh)
            self._tasks.add(asyncio.create_task(stream_handler.serve()))

    async def run(self):
        log.info(f"[{self.name}] Initiating Autonomous Gateway Daemon...")
        try:
            await clear_zombie_ports([self.settings.proxy_port, self.settings.mcp_port], tag=self.name)
            await self._setup_routing_mesh()
            
            log.info(f"[{self.name}] Igniting Public Gateway & MCP Control Plane...")
            self.gateway_server = DphiGatewayServer(self.settings)
            gw_task = asyncio.create_task(self.gateway_server.start_dual_servers())
            self._tasks.add(gw_task)
            
            while self.running:
                if gw_task.done():
                    exc = gw_task.exception()
                    if exc:
                        log.error(f"[{self.name}] Gateway dual servers crashed explicitly: {exc}", exc_info=exc)
                    else:
                        log.error(f"[{self.name}] Gateway dual servers exited unexpectedly.")
                    break
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            log.info(f"[{self.name}] Cancel signal received.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal execution error. Evaporating daemon: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def _teardown(self):
        log.info(f"[{self.name}] Releasing Gateway Edge resources...")
        if self.gateway_server and getattr(self.gateway_server, "client_session", None):
            with suppress(Exception):
                await self.gateway_server.client_session.close()
        
        for task in self._tasks:
            if not task.done():
                task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info(f"[{self.name}] Resource cleanup complete.")


# =========================================================================
# 2. REST Edge Daemon (Internal / Microservice Traffic)
# =========================================================================
@contract.daemon("rest_edge")
class RestEdgeDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("RestEdgeDaemon")
        self.ctx = ctx
        self.target_port = int(os.getenv("REST_PORT", 8000))
        self.server: Optional[uvicorn.Server] = None
        self._server_task: Optional[asyncio.Task] = None
        
        # 인프라 자원 상태 변수
        self._tunnel = None

    async def run(self):
        log.info(f"[{self.name}] Initiating Autonomous REST Edge Daemon...")
        try:
            await clear_zombie_ports([self.target_port], tag=self.name)
            
            # ---------------------------------------------------------
            # 1. 리소스 선점 (데몬 주도 인프라 초기화)
            # ---------------------------------------------------------
            # TunnelFactory: 노드 전체 터널 객체를 데몬이 확보
            self._tunnel = await TunnelFactory.get_default()
            
            # Ledger: 커널에서 주입받거나, 없을 경우 로컬 인스턴스로 자동 초기화 (Auto-Role)
            ledger = getattr(self.ctx, "ledger", None)
            if ledger is None:
                log.info(f"[{self.name}] Ledger not found in context. Bootstrapping local KernelLedger (Auto-Role).")
                ledger = KernelLedger()

            # ---------------------------------------------------------
            # 2. 환경변수 캡슐화 및 하향식 Config 구성
            # ---------------------------------------------------------
            resolved_internal_url = os.getenv("INTERNAL_EDGE_URL", f"http://127.0.0.1:{self.target_port}")
            runtime_config = Config(
                internal_edge_url=resolved_internal_url,
                redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"), # Config 내 설정값으로만 전달
                max_payload_size=int(os.getenv("MAX_PAYLOAD_SIZE", 1024 * 1024 * 10)),
                wasm_timeout=float(os.getenv("WASM_TIMEOUT", 10.0)),
                pubsub_channel=os.getenv("PUBSUB_CHANNEL", "audit_channel")
            )

            # ---------------------------------------------------------
            # 3. 의존성 주입 (DI) 기반 API 애플리케이션 생성
            # ---------------------------------------------------------
            # rest.api의 create_app은 주입된 객체만 사용하여 상태 비저장 형태로 기동됨
            injected_app = create_app(
                config=runtime_config,
                tunnel=self._tunnel,
                ledger=ledger
            )

            config = uvicorn.Config(
                app=injected_app,
                host="127.0.0.1",
                port=self.target_port,
                loop="none",
                log_level="warning",
                access_log=False
            )
            self.server = uvicorn.Server(config)
            
            self._server_task = asyncio.create_task(self.server.serve())
            log.info(f"[{self.name}] REST Edge safely listening on http://127.0.0.1:{self.target_port}")
            log.info(f"[{self.name}] Routing internal traffic to: {resolved_internal_url}")
            
            while self.running:
                if self._server_task.done():
                    exc = self._server_task.exception()
                    if exc:
                        log.error(f"[{self.name}] Uvicorn server crashed: {exc}", exc_info=exc)
                    else:
                        log.error(f"[{self.name}] Uvicorn server exited unexpectedly.")
                    break
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            log.info(f"[{self.name}] Cancel signal received.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal error. Evaporating daemon: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def _teardown(self):
        log.info(f"[{self.name}] Releasing REST Edge resources...")
        
        # 1. API 어플리케이션(Uvicorn)에 안전 종료 시그널 전달
        if self.server:
            self.server.should_exit = True
            
        # 2. Race Condition 방지: API의 Graceful Shutdown을 위한 충분한 타임아웃 보장
        shutdown_timeout = float(os.getenv("SHUTDOWN_TIMEOUT", 15.0))
        
        if self._server_task and not self._server_task.done():
            try:
                log.info(f"[{self.name}] Waiting up to {shutdown_timeout}s for API graceful shutdown...")
                await asyncio.wait_for(self._server_task, timeout=shutdown_timeout)
            except asyncio.TimeoutError:
                log.warning(f"[{self.name}] Shutdown timeout exceeded. Forcing task cancellation.")
                self._server_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._server_task

        # 3. 글로벌 자원 명시적 회수 (API의 월권 행위를 데몬이 정상 회수 처리)
        log.info(f"[{self.name}] Reaping injected global resources...")

        try:
            await TunnelFactory.close_all()
            log.info(f"[{self.name}] TunnelFactory closed securely at daemon level.")
        except Exception as e:
            log.error(f"[{self.name}] Error closing TunnelFactory: {e}")

        log.info(f"[{self.name}] Resource cleanup complete.")