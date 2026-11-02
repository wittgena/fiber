# fiber.infra.daemon.mcp
import os
import asyncio
from contextlib import suppress
from typing import Dict, List, Optional

import orjson

from fiber.infra.mcp.connector import MCPServerConnector
from fiber.infra.mcp.mapper.config import McpConfigMapper
from fiber.gateway.bridge.action.connect import (
    DEFAULT_WORKER_PACKAGE,
    ResolvedTarget,
    discover_builtin_workers,
    resolve_connector_spec,
    _specs_from_mapped_entries,
    _sync_to_anchor_config,
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
        self.base_package = os.getenv("MCP_WORKER_PACKAGE", DEFAULT_WORKER_PACKAGE)
        self.connectors: Dict[str, MCPServerConnector] = {}
        self._connector_tasks: Dict[str, asyncio.Task] = {}
        self._target_specs: Dict[str, ResolvedTarget] = {}
        self._last_anchor_mtime: float = 0.0

    def _get_anchor_mtime(self) -> float:
        anchor_path = McpConfigMapper.get_anchor_config_path()
        if anchor_path.is_file():
            with suppress(OSError):
                return anchor_path.stat().st_mtime
        return 0.0

    def _build_desired_specs(self) -> Dict[str, ResolvedTarget]:
        desired: Dict[str, ResolvedTarget] = {}
        anchor_path = McpConfigMapper.get_anchor_config_path()

        if not anchor_path.is_file():
            log.info(f"[{self.name}] Anchor config not found at {anchor_path}. No workers will be started.")
            self._last_anchor_mtime = 0.0
            return desired

        log.info(f"[{self.name}] Loading canonical MCP config from: {anchor_path}")
        anchor_entries = McpConfigMapper.scan_and_parse(agent="fiber")

        try:
            for spec in _specs_from_mapped_entries(anchor_entries):
                desired[spec.target_id] = spec
            log.info(f"[{self.name}] 🔗 Loaded {len(desired)} target(s) from mcp.json: {list(desired.keys())}")
        except BaseException as e:
            log.error(f"[{self.name}] Failed to resolve anchor config entries: {e}")

        self._last_anchor_mtime = self._get_anchor_mtime()
        return desired

    def _collect_target_specs(self) -> List[ResolvedTarget]:
        self._target_specs = self._build_desired_specs()
        return list(self._target_specs.values())

    def _log_boot_roster(self, specs: List[ResolvedTarget]):
        lines = [
            f"\n{'=' * 95}",
            f"[{self.name}] MCP WORKER BOOT ROSTER (Total: {len(specs)} Target(s))",
            f"{'-' * 95}",
            f"{'No.':<4} | {'Target ID':<24} | {'Mode':<10} | {'Transport':<9} | {'Pre-Flight Guard':<20} | {'Command / URL'}",
            f"{'-' * 95}",
        ]
        for idx, s in enumerate(specs, 1):
            guard_str = f"/{s.preflight}/i" if s.preflight else "None"
            lines.append(
                f"{idx:02d}.  | {s.target_id:<24} | {s.mode.upper():<10} | "
                f"{s.transport.upper():<9} | {guard_str:<20} | {s.exec_cmd}"
            )
        lines.append(f"{'=' * 95}")
        log.info("\n".join(lines))

    @staticmethod
    def _extract_connector_pid(connector: MCPServerConnector) -> Optional[int]:
        for attr in ("shared_transport", "transport", "_transport"):
            tr = getattr(connector, attr, None)
            if tr is not None:
                proc = getattr(tr, "process", None) or getattr(tr, "_process", None)
                if proc is not None and getattr(proc, "pid", None):
                    return proc.pid
                if getattr(tr, "pid", None):
                    return tr.pid
        return None

    async def _verify_booted_processes(self):
        await asyncio.sleep(0.8)
        status_lines = []
        for tid, conn in self.connectors.items():
            task = self._connector_tasks.get(tid)
            is_alive = task is not None and not task.done()
            pid = self._extract_connector_pid(conn)

            if pid:
                status_lines.append(f"{tid} (PID: {pid}, Status: RUNNING)")
            elif conn.mode == "ephemeral":
                status_lines.append(f"{tid} (Lazy/Zero-Spawn Standby, Status: READY)")
            elif conn.transport_type == "network":
                status_lines.append(f"{tid} (Network Multiplex, Status: {'READY' if is_alive else 'DEAD'})")
            else:
                status_lines.append(f"{tid} (Task Alive: {is_alive})")

        log.info(
            f"[{self.name}] ✅ Worker Process Verification ({len(status_lines)} active): "
            + " | ".join(status_lines)
        )

    async def _sync_active_targets(self, active_ids: List[str]):
        if self.ctx is not None:
            with suppress(Exception):
                if isinstance(self.ctx, dict):
                    self.ctx["mcp_active_targets"] = active_ids
                else:
                    setattr(self.ctx, "mcp_active_targets", active_ids)

        try:
            tunnel = await TunnelFactory.get_default()
            payload = orjson.dumps(active_ids).decode("utf-8")
            await tunnel.set(ACTIVE_TARGETS_TUNNEL_KEY, payload)
            log.info(
                f"[{self.name}] 📡 Synced {len(active_ids)} active target(s) to Tunnel "
                f"[{ACTIVE_TARGETS_TUNNEL_KEY}]: {active_ids}"
            )
        except Exception as e:
            log.warning(f"[{self.name}] Failed to sync active targets to tunnel: {e}")

    def _spawn_connector_task(self, spec: ResolvedTarget):
        guard_badge = f", Pre-Flight: /{spec.preflight}/i" if spec.preflight else ""
        connector = MCPServerConnector(
            target_id=spec.target_id,
            execution_target=spec.exec_cmd,
            mode=spec.mode,
            transport_type=spec.transport,
            preflight_pattern=spec.preflight,
        )
        self.connectors[spec.target_id] = connector
        task = asyncio.create_task(
            connector.run(),
            name=f"mcp-connector-{spec.target_id}",
        )
        self._connector_tasks[spec.target_id] = task
        log.debug(
            f"[{self.name}] Mounted [{spec.target_id}] -> {spec.exec_cmd} "
            f"(Mode: {spec.mode.upper()}, Transport: {spec.transport.upper()}{guard_badge})"
        )

    async def _stop_single_connector(self, target_id: str):
        connector = self.connectors.pop(target_id, None)
        if connector:
            connector.running = False

        task = self._connector_tasks.pop(target_id, None)
        if task and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(task, timeout=3.0)

    async def _reconcile_anchor_config(self):
        current_mtime = self._get_anchor_mtime()
        if current_mtime <= self._last_anchor_mtime:
            return

        log.info(f"[{self.name}] Detected update in anchor mcp.json. Reconciling MCP workers...")
        desired_specs = self._build_desired_specs()
        changed = False

        for tid in list(self._target_specs.keys()):
            if tid not in desired_specs:
                log.info(f"[{self.name}] Unmounting removed target [{tid}]...")
                await self._stop_single_connector(tid)
                self._target_specs.pop(tid, None)
                changed = True

        for tid, spec in desired_specs.items():
            existing = self._target_specs.get(tid)
            if existing != spec:
                if existing is not None:
                    log.info(f"[{self.name}] ♻️ Hot-reloading updated target [{tid}]...")
                    await self._stop_single_connector(tid)
                self._target_specs[tid] = spec
                self._spawn_connector_task(spec)
                changed = True

        if changed:
            await self._sync_active_targets(list(self._target_specs.keys()))
            await self._verify_booted_processes()

    async def run(self):
        log.info(f"[{self.name}] Starting Autonomous MCP Worker Supervisor Daemon...")
        try:
            specs = self._collect_target_specs()
            if not specs:
                log.warning(f"[{self.name}] ⚠️ No MCP workers discovered or configured.")
                await self._sync_active_targets([])
            else:
                self._log_boot_roster(specs)
                log.info(f"[{self.name}] Igniting {len(specs)} resolved MCP worker connector(s)...")
                for spec in specs:
                    self._spawn_connector_task(spec)
                await self._sync_active_targets(list(self._target_specs.keys()))
                await self._verify_booted_processes()

            while self.running:
                await self._reconcile_anchor_config()

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
                                await self._verify_booted_processes()

                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            log.info(f"[{self.name}] Shutdown signal received.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal error in McpWorkerDaemon: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def _teardown(self):
        log.info(
            f"[{self.name}] Releasing {len(self._connector_tasks)} MCP Worker Connector(s): "
            f"{list(self._connector_tasks.keys())}"
        )
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