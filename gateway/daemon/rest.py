# fiber.gateway.daemon.rest
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

from fiber.gateway.rest.payload import create_app, Config
from xphi.arch.contract.registry.unified import contract
from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.kernel.ops.reaper import SystemOps
from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.state.anchor.consensus import PhaseStore
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("daemon.rest")


async def clear_zombie_ports(ports: List[int], tag: str):
    """지정된 포트들을 점유하고 있는 기존 프로세스를 정리하는 공통 유틸리티"""
    reaper = SystemOps(redis_conn=None, tag=tag)
    my_pid = str(os.getpid())
    is_root = os.geteuid() == 0 if hasattr(os, 'geteuid') else False
    
    for port in set(ports):
        if port < 1024 and not is_root:
            log.warning(f"[{tag}] Port {port} requires elevated privileges. Cleanup bypassed.")
            continue

        try:
            pids = await reaper.get_pids_from_port(port)
            for pid in pids:
                if pid == my_pid:
                    continue
                log.warning(f"[{tag}] Port {port} is occupied by PID {pid}. Attempting to terminate...")
                try:
                    await reaper._execute_kill(pid, force=True)
                except Exception as kill_err:
                    log.error(f"[{tag}] Failed to terminate PID {pid} (Permission/OS restriction): {kill_err}")
            
            if pids:
                await asyncio.sleep(1.5)
        except Exception as e:
            log.warning(f"[{tag}] Error scanning port {port}: {e}")

@contract.daemon("rest_edge")
class RestEdgeDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("RestEdgeDaemon")
        self.ctx = ctx
        self.target_port = int(os.getenv("REST_PORT", 8000))
        self.server: Optional[uvicorn.Server] = None
        self._server_task: Optional[asyncio.Task] = None
        
        self._tunnel = None

    async def run(self):
        log.info(f"[{self.name}] Starting REST Edge Daemon...")
        try:
            await clear_zombie_ports([self.target_port], tag=self.name)
            self._tunnel = await TunnelFactory.get_default()

            phase_store = getattr(self.ctx, "phase_store", None)
            if phase_store is None:
                log.info(f"[{self.name}] PhaseStore not found in context. Bootstrapping local PhaseStore.")
                phase_store = PhaseStore()

            resolved_internal_url = os.getenv("INTERNAL_EDGE_URL", f"http://127.0.0.1:{self.target_port}")
            runtime_config = Config(
                internal_edge_url=resolved_internal_url,
                redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
                max_payload_size=int(os.getenv("MAX_PAYLOAD_SIZE", 1024 * 1024 * 10)),
                wasm_timeout=float(os.getenv("WASM_TIMEOUT", 10.0)),
                pubsub_channel=os.getenv("PUBSUB_CHANNEL", "audit_channel")
            )

            injected_app = create_app(
                config=runtime_config,
                tunnel=self._tunnel,
                phase_store=phase_store
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
            log.info(f"[{self.name}] REST Edge listening on http://127.0.0.1:{self.target_port}")
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
            log.info(f"[{self.name}] Shutdown signal received.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal error. Terminating daemon: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def _teardown(self):
        log.info(f"[{self.name}] Releasing REST Edge resources...")
        
        if self.server:
            self.server.should_exit = True
            
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

        log.info(f"[{self.name}] Releasing injected global resources...")
        try:
            await TunnelFactory.close_all()
            log.info(f"[{self.name}] TunnelFactory closed successfully.")
        except Exception as e:
            log.error(f"[{self.name}] Error closing TunnelFactory: {e}")

        log.info(f"[{self.name}] REST Edge resource cleanup complete.")