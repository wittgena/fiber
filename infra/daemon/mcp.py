# fiber.infra.daemon.mcp
## @lineage: fiber.gateway.daemon.mcp
import os
import asyncio
from contextlib import suppress
from typing import Dict, List

import orjson

from fiber.gateway.mcp.connector import MCPServerConnector
from fiber.gateway.mcp.mapper.config import McpConfigMapper
from fiber.phase.cli.connect import (
    ResolvedTarget,
    discover_builtin_workers,
    resolve_connector_spec,
)
from xphi.arch.contract.registry.unified import contract
from xphi.node.ops.daemon.base import AbstractDaemon
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("daemon.mcp")

ACTIVE_TARGETS_TUNNEL_KEY = "mcp:registry:active_targets"


@contract.daemon("mcp_worker")
class McpWorkerDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("McpWorkerDaemon")
        self.ctx = ctx
        self.base_package = os.getenv("MCP_WORKER_PACKAGE", "fiber.dev.worker")
        self.connectors: Dict[str, MCPServerConnector] = {}
        self._connector_tasks: Dict[str, asyncio.Task] = {}
        self._target_specs: Dict[str, ResolvedTarget] = {}

    def _collect_target_specs(self) -> List[ResolvedTarget]:
        self._target_specs.clear()
        builtin_map = discover_builtin_workers(self.base_package)

        # 1. 내장 워커 수집: 중복 실행 방지를 위해 대표 별칭(Canonical ID) 기준 1차 추출
        seen_cmds = set()
        for key, val in builtin_map.items():
            # 3-튜플(canonical_id, cmd, default_mode) 및 레거시 2-튜플(cmd, default_mode) 모두 호환
            if len(val) == 3:
                tid, cmd, default_mode = val
            else:
                tid = key
                cmd, default_mode = val[0], val[1]

            if cmd not in seen_cmds:
                seen_cmds.add(cmd)
                self._target_specs[tid] = ResolvedTarget(
                    target_id=tid,
                    exec_cmd=cmd,
                    mode=default_mode,
                    transport="stdio",
                )

        # 2. 외부 에이전트 MCP 설정 자동 스캔 및 통합
        for entry in McpConfigMapper.scan_and_parse():
            if entry.target_id in self._target_specs:
                continue
            if entry.env:
                os.environ.update(entry.env)
            try:
                spec = resolve_connector_spec(
                    target=entry.target_id,
                    exec_cmd=entry.exec_cmd,
                )
                self._target_specs[spec.target_id] = spec
            except BaseException as e:
                log.error(f"[{self.name}] Failed to resolve mapped target '{entry.target_id}': {e}")

        return list(self._target_specs.values())

    async def _sync_active_targets(self, active_ids: List[str]):
        # 초기/폴백 참조를 위한 컨텍스트 상태 바인딩
        if self.ctx is not None:
            with suppress(Exception):
                if isinstance(self.ctx, dict):
                    self.ctx["mcp_active_targets"] = active_ids
                else:
                    setattr(self.ctx, "mcp_active_targets", active_ids)

        # 게이트웨이 간접 조회를 위한 Tunnel 레지스트리 동기화
        try:
            tunnel = await TunnelFactory.get_default()
            await tunnel.set(ACTIVE_TARGETS_TUNNEL_KEY, orjson.dumps(active_ids).decode("utf-8"))
        except Exception as e:
            log.warning(f"[{self.name}] Failed to sync active targets to tunnel: {e}")

    def _spawn_connector_task(self, spec: ResolvedTarget):
        connector = MCPServerConnector(
            target_id=spec.target_id,
            execution_target=spec.exec_cmd,
            mode=spec.mode,
            transport_type=spec.transport,
        )
        self.connectors[spec.target_id] = connector
        task = asyncio.create_task(
            connector.run(),
            name=f"mcp-connector-{spec.target_id}",
        )
        self._connector_tasks[spec.target_id] = task
        log.info(
            f"[{self.name}] 🔌 Mounted [{spec.target_id}] -> {spec.exec_cmd} "
            f"(Mode: {spec.mode.upper()}, Transport: {spec.transport.upper()})"
        )

    async def run(self):
        log.info(f"[{self.name}] Starting Autonomous MCP Worker Supervisor Daemon...")
        try:
            specs = self._collect_target_specs()
            if not specs:
                log.warning(f"[{self.name}] No MCP workers discovered or configured.")
                await self._sync_active_targets([])
            else:
                log.info(f"[{self.name}] Igniting {len(specs)} resolved MCP worker connector(s)...")
                for spec in specs:
                    self._spawn_connector_task(spec)
                await self._sync_active_targets(list(self._target_specs.keys()))

            # 데몬 메인 감시 루프: 비정상 종료된 커넥터 감지 및 자동 재기동(Self-Healing)
            while self.running:
                for target_id, task in list(self._connector_tasks.items()):
                    if task.done():
                        with suppress(asyncio.CancelledError):
                            exc = task.exception()
                            if exc:
                                log.error(
                                    f"[{self.name}] Connector [{target_id}] fractured with error: {exc}. "
                                    f"Respawning in 2s...",
                                    exc_info=exc,
                                )
                            else:
                                log.warning(f"[{self.name}] Connector [{target_id}] exited unexpectedly. Respawning...")

                        if self.running:
                            await asyncio.sleep(2.0)
                            spec = self._target_specs.get(target_id)
                            if spec:
                                self._spawn_connector_task(spec)

                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            log.info(f"[{self.name}] Shutdown signal received.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal error in McpWorkerDaemon: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def _teardown(self):
        log.info(f"[{self.name}] Releasing {len(self._connector_tasks)} MCP Worker Connector(s)...")
        shutdown_timeout = float(os.getenv("SHUTDOWN_TIMEOUT", 10.0))

        await self._sync_active_targets([])

        for connector in self.connectors.values():
            connector.running = False

        pending_tasks = [t for t in self._connector_tasks.values() if not t.done()]
        for task in pending_tasks:
            task.cancel()

        if pending_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending_tasks, return_exceptions=True),
                    timeout=shutdown_timeout,
                )
            except asyncio.TimeoutError:
                log.warning(f"[{self.name}] Timed out waiting for connectors to evaporate.")

        self._connector_tasks.clear()
        self.connectors.clear()
        self._target_specs.clear()
        log.info(f"[{self.name}] MCP Worker Daemon resource cleanup complete.")