# fiber.phase.action.connect
import os
import sys
import ast
import shlex
import asyncio
import importlib.util
from pathlib import Path
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, List

import typer
from fiber.infra.mcp.connector import MCPServerConnector
from fiber.infra.mcp.preflight import PreflightGuard
from fiber.infra.mcp.mapper.config import McpConfigMapper, MappedMcpEntry
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fiber.cli.connect")

@dataclass(frozen=True)
class ResolvedTarget:
    target_id: str
    exec_cmd: str
    mode: str
    transport: str
    preflight: Optional[str] = None


BUILTIN_ALIASES: Dict[str, str] = {
    "deployer": "worker.deployer",
    "oracle": "worker.eco.oracle",
    "finlib": "worker.eco.finlib",
    "margin": "worker.eco.margin",
    "archive": "worker.eco.archive",
}


def discover_builtin_workers(base_package: str = "fiber.dev.worker") -> Dict[str, Tuple[str, str, str]]:
    """내장 워커 AST 스캔 -> {매칭키: (canonical_id, cmd, default_mode)} 반환"""
    discovered: Dict[str, Tuple[str, str, str]] = {}
    try:
        spec = importlib.util.find_spec(base_package)
        if not spec or not spec.submodule_search_locations:
            return discovered
        root_dir = Path(list(spec.submodule_search_locations)[0]).resolve()
    except Exception:
        return discovered

    for py_file in sorted(root_dir.rglob("*.py")):
        if py_file.name.startswith("_"):
            continue
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        rel_parts = py_file.relative_to(root_dir).with_suffix("").parts
        module_path = f"{base_package}.{'.'.join(rel_parts)}"

        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            bases = {
                b.id if isinstance(b, ast.Name) else getattr(b, "attr", "")
                for b in node.bases
            }
            matched = bases & {"WorkerProtocol", "AsyncWorkerProtocol"}
            if not matched:
                continue

            default_mode = "multiplex" if "AsyncWorkerProtocol" in matched else "linear"
            leaf = rel_parts[-1]
            canonical_id = BUILTIN_ALIASES.get(leaf, f"worker.{leaf}")
            cmd = f"{sys.executable} -m {module_path}"

            entry = (canonical_id, cmd, default_mode)
            discovered[leaf] = entry
            discovered[canonical_id] = entry
            discovered[f"worker.{'.'.join(rel_parts)}"] = entry
            break

    return discovered


def _infer_fallback_target_id(raw_cmd: str, is_http_url: bool) -> str:
    if is_http_url:
        parsed = urlparse(raw_cmd)
        return parsed.hostname or "remote-mcp"
    try:
        parts = shlex.split(raw_cmd)
    except ValueError:
        parts = raw_cmd.split()
    return Path(parts[0]).stem if parts else "local-mcp"


def resolve_connector_spec(
    target: Optional[str] = None,
    exec_cmd: str = "",
    mode: Optional[str] = None,
    transport: Optional[str] = None,
    preflight: Optional[str] = None,
) -> ResolvedTarget:
    raw_cmd = exec_cmd.strip()
    if not raw_cmd:
        log.error("[Fiber] Empty execution command provided.")
        raise typer.Exit(1)

    inferred_target: Optional[str] = None
    inferred_default_mode: Optional[str] = None

    # 내장 워커(fiber.dev.worker.*) 동적 탐색 매칭
    builtin_map = discover_builtin_workers()
    if raw_cmd in builtin_map:
        inferred_target, resolved_cmd, inferred_default_mode = builtin_map[raw_cmd]
    else:
        resolved_cmd = raw_cmd

    # 실행 타겟 유형 판별
    is_http_url = resolved_cmd.startswith(("http://", "https://"))
    is_managed_bin_dir = not is_http_url and Path(resolved_cmd).is_dir()

    # Transport 결정 (명시적 인자 우선 -> URL/디렉토리 기반 자동 추론 -> 'stdio')
    if transport is not None:
        resolved_transport = transport.lower().strip()
    else:
        resolved_transport = "network" if (is_http_url or is_managed_bin_dir) else "stdio"

    if resolved_transport not in ("stdio", "network"):
        log.error(f"[Fiber] Invalid transport '{resolved_transport}'. Allowed values: 'stdio', 'network'.")
        raise typer.Exit(1)

    # Target ID 결정 (명시적 인자 -> 내장 워커 정규화 ID -> 명령어/URL 기반 추론)
    resolved_target = (
        target
        or inferred_target
        or _infer_fallback_target_id(resolved_cmd, is_http_url)
    ).strip()

    # Mode 결정 (명시적 인자 -> 내장 워커 프로토콜 추론 -> Transport별 기본값)
    if mode is not None:
        resolved_mode = mode.lower().strip()
    elif inferred_default_mode is not None:
        resolved_mode = inferred_default_mode
    else:
        resolved_mode = "multiplex" if resolved_transport == "network" else "ephemeral"

    if resolved_mode not in ("ephemeral", "linear", "multiplex"):
        log.error(f"[Fiber] Invalid mode '{resolved_mode}'. Allowed values: 'ephemeral', 'linear', 'multiplex'.")
        raise typer.Exit(1)

    # PreflightGuard의 단일 해석 메서드로 위임
    resolved_preflight = PreflightGuard.resolve_pattern(resolved_target, preflight)

    return ResolvedTarget(
        target_id=resolved_target,
        exec_cmd=resolved_cmd,
        mode=resolved_mode,
        transport=resolved_transport,
        preflight=resolved_preflight,
    )

def _specs_from_mapped_entries(
    entries: List[MappedMcpEntry],
    target_filter: Optional[str] = None,
    mode_override: Optional[str] = None,
    preflight_override: Optional[str] = None,
) -> List[ResolvedTarget]:
    specs: List[ResolvedTarget] = []
    for entry in entries:
        if target_filter and entry.target_id != target_filter:
            continue

        entry_preflight, clean_env = PreflightGuard.extract_from_env_dict(entry.env, preflight_override)
        if clean_env:
            os.environ.update(clean_env)

        specs.append(
            resolve_connector_spec(
                target=entry.target_id,
                exec_cmd=entry.exec_cmd,
                mode=mode_override,
                preflight=entry_preflight,
            )
        )
    return specs


async def launch_connector_async(spec: ResolvedTarget):
    guard_badge = f", Pre-Flight: /{spec.preflight}/i" if spec.preflight else ""
    log.info(
        f"[Fiber] Sublimating server [{spec.target_id}] into the MCP Server Connector "
        f"(Mode: {spec.mode.upper()}, Transport: {spec.transport.upper()}{guard_badge})..."
    )
    daemon = MCPServerConnector(
        target_id=spec.target_id,
        execution_target=spec.exec_cmd,
        mode=spec.mode,
        transport_type=spec.transport,
        preflight_pattern=spec.preflight,
    )
    try:
        await daemon.run()
    finally:
        log.info(f"[Fiber] [{spec.target_id}] Disconnected from A2A Network.")


async def _launch_all_async(specs: List[ResolvedTarget]):
    if len(specs) == 1:
        await launch_connector_async(specs[0])
    else:
        await asyncio.gather(*(launch_connector_async(spec) for spec in specs))


def run_connect(
    exec_cmd: Optional[str] = None,
    config: Optional[Path] = None,
    target: Optional[str] = None,
    mode: Optional[str] = None,
    preflight: Optional[str] = None,
):
    specs: List[ResolvedTarget] = []

    if config is not None:
        mapped_entries = McpConfigMapper.parse_file(config)
        specs.extend(
            _specs_from_mapped_entries(
                mapped_entries, target_filter=target, mode_override=mode, preflight_override=preflight
            )
        )

    if exec_cmd is not None:
        specs.append(resolve_connector_spec(target=target, exec_cmd=exec_cmd, mode=mode, preflight=preflight))

    if config is None and exec_cmd is None:
        mapped_entries = McpConfigMapper.scan_and_parse()
        specs.extend(
            _specs_from_mapped_entries(
                mapped_entries, target_filter=target, mode_override=mode, preflight_override=preflight
            )
        )

    if not specs:
        log.error("[Fiber] No valid MCP target resolved. Provide --exec (-e), --config (-c), or ensure agent config exists.")
        raise typer.Exit(1)

    try:
        asyncio.run(_launch_all_async(specs))
    except KeyboardInterrupt:
        log.info("\n[Fiber] Connector shutting down...")