# fiber.dev.console.mcp
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import time
from typing import Any
from nicegui import ui
import orjson

from fiber.gateway.bridge.schema import (
    McpConfigCandidate,
    McpTopologyScanResult,
    McpWorkerInfo,
)
from fiber.infra.ops.account.state import state
import fiber.dev.console.ui.diag as diag

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_last_config_stat_sig: tuple[tuple[str, int, int], ...] = ()
_cached_mapped_entries: list[Any] = []


def _inspect_and_sanitize_config_candidates(
    candidate_map: dict[str, list[Path]],
) -> tuple[list[McpConfigCandidate], tuple[tuple[str, int, int], ...]]:
    config_list: list[McpConfigCandidate] = []
    stat_sig: list[tuple[str, int, int]] = []

    for agent_name, paths in candidate_map.items():
        existing = next((p for p in paths if p.is_file()), None)
        primary_path = existing or (paths[0] if paths else None)
        if primary_path is None:
            continue

        has_valid_content = False
        mtime_ns = 0
        size = 0

        if existing is not None:
            try:
                raw_text = existing.read_text(encoding="utf-8", errors="replace").strip()
                if not raw_text:
                    existing.write_text("{}\n", encoding="utf-8")
                    raw_text = "{}"
                st = existing.stat()
                mtime_ns = st.st_mtime_ns
                size = st.st_size
                has_valid_content = raw_text not in ("", "{}")
            except OSError:
                pass
        elif agent_name != "fiber" and not primary_path.parent.exists():
            continue

        stat_sig.append((str(primary_path), mtime_ns, size))
        config_list.append(
            McpConfigCandidate(
                agent=agent_name,
                path=str(primary_path),
                name=primary_path.name,
                exists=existing is not None and has_valid_content,
                file_present=existing is not None,
            )
        )

    return config_list, tuple(stat_sig)


async def scan_mcp_topology_and_health(gateway_url: str, *, force_parse: bool = False) -> McpTopologyScanResult:
    global _last_config_stat_sig, _cached_mapped_entries
    import httpx
    from fiber.infra.mcp.mapper.config import McpConfigMapper
    from fiber.gateway.bridge.action.connect import discover_builtin_workers, resolve_connector_spec
    from xphi.node.space.tunnel.factory import TunnelFactory

    candidate_map = McpConfigMapper._resolve_candidate_paths()
    config_list, current_stat_sig = await asyncio.to_thread(
        _inspect_and_sanitize_config_candidates, candidate_map
    )

    live_targets: set[str] = set()
    try:
        tunnel = await TunnelFactory.get_default()
        if tunnel:
            raw = await tunnel.get("mcp:registry:active_targets")
            if raw:
                parsed = orjson.loads(raw)
                if isinstance(parsed, list):
                    live_targets = {str(x) for x in parsed}
    except Exception:
        pass

    if force_parse or current_stat_sig != _last_config_stat_sig:
        _cached_mapped_entries = await asyncio.to_thread(McpConfigMapper.scan_and_parse)
        _last_config_stat_sig = current_stat_sig

    builtins = await asyncio.to_thread(discover_builtin_workers)
    builtin_default_modes: dict[str, tuple[str, str]] = {}
    for _, (canon_id, cmd, def_mode) in builtins.items():
        if canon_id not in builtin_default_modes:
            builtin_default_modes[canon_id] = (cmd, def_mode)

    workers_by_id: dict[str, McpWorkerInfo] = {}
    for entry in _cached_mapped_entries:
        env_mode = entry.env.get("FIBER_MODE") if entry.env else None
        fallback_mode = builtin_default_modes.get(entry.target_id, ("", None))[1]
        effective_mode = env_mode or fallback_mode

        env_preflight = entry.env.get("FIBER_PREFLIGHT_PATTERN") if entry.env else None
        spec = resolve_connector_spec(
            target=entry.target_id,
            exec_cmd=entry.exec_cmd,
            mode=effective_mode,
            preflight=env_preflight,
        )
        workers_by_id[spec.target_id] = McpWorkerInfo(
            target_id=spec.target_id,
            source=entry.source_agent,
            transport=spec.transport,
            mode=spec.mode,
            preflight=env_preflight or spec.preflight or "",
            is_live=spec.target_id in live_targets,
            exec_cmd=spec.exec_cmd,
        )

    # 3. 아직 설정 파일에 없더라도 내장 워커(builtin)가 누락되지 않도록 병합
    for canon_id, (cmd, def_mode) in builtin_default_modes.items():
        if canon_id not in workers_by_id:
            workers_by_id[canon_id] = McpWorkerInfo(
                target_id=canon_id,
                source="builtin",
                transport="stdio",
                mode=def_mode,
                preflight="",
                is_live=canon_id in live_targets,
                exec_cmd=cmd,
            )

    workers = list(workers_by_id.values())

    t0 = time.perf_counter()
    gw_online = False
    gw_lat = 0.0
    try:
        async with httpx.AsyncClient(timeout=0.45) as client:
            resp = await client.get(f"{gateway_url}/_health")
            if resp.status_code == 200:
                gw_online = True
                gw_lat = round((time.perf_counter() - t0) * 1000, 1)
    except Exception:
        pass

    return McpTopologyScanResult(
        configs=config_list,
        workers=workers,
        active_target_ids=sorted(live_targets),
        gateway_online=gw_online,
        gateway_latency_ms=gw_lat,
        checked_at=time.strftime("%H:%M:%S"),
    )


class McpActionBridge:
    async def scan_mcp_topology_and_health(self, silent: bool = True) -> None:
        s = state.security
        try:
            snap = await scan_mcp_topology_and_health(s.gateway_url, force_parse=not silent)
            prev_fp = json.dumps(
                [s.discovered_configs, s.discovered_workers, s.gateway_online, s.active_target_ids], sort_keys=True
            )

            s.discovered_configs = [c.model_dump() for c in snap.configs]
            s.discovered_workers = [w.model_dump() for w in snap.workers]
            s.active_target_ids = snap.active_target_ids
            s.gateway_online = snap.gateway_online or bool(snap.active_target_ids)
            s.gateway_latency_ms = snap.gateway_latency_ms
            s.gateway_last_check = snap.checked_at

            new_fp = json.dumps(
                [s.discovered_configs, s.discovered_workers, s.gateway_online, s.active_target_ids], sort_keys=True
            )
            if prev_fp != new_fp:
                s.mcp_topology_dirty = True

            if not silent:
                ui.notify(
                    f"🔍 Active in Daemon: {len(s.active_target_ids)} / Total Discovered: {len(s.discovered_workers)}",
                    type="info",
                )
        except Exception as exc:
            diag.exception("Failed to scan MCP topology and gateway health", exc=exc)

    async def mcp_gateway_health_worker(self) -> None:
        while True:
            await self.scan_mcp_topology_and_health(silent=True)
            await asyncio.sleep(3.0)


mcp = McpActionBridge()