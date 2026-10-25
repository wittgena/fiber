# fiber.phase.bridge.action.connect
import os
import sys
import ast
import shlex
import asyncio
import importlib.util
from pathlib import Path
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, List, Any

import orjson
import typer
from fiber.infra.mcp.connector import MCPServerConnector
from fiber.infra.mcp.preflight import PreflightGuard
from fiber.infra.mcp.mapper.config import McpConfigMapper, MappedMcpEntry

from xphi.node.space.bind.resolver import resolve_path
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("action.connect")

ACTIVE_TARGETS_TUNNEL_KEY = "mcp:registry:active_targets"

ANCHOR_ROOT = resolve_path("anchor")
ANCHOR_MCP_PATH = ANCHOR_ROOT / "mcp.json"
DEFAULT_WORKER_PACKAGE = "fiber.infra.mcp.worker"

@dataclass(frozen=True)
class ResolvedTarget:
    target_id: str
    exec_cmd: str
    mode: str
    transport: str
    preflight: Optional[str] = None

BUILTIN_ALIASES: Dict[str, str] = {
    "deployer": "worker.deployer",
    "finops": "worker.finops",
    "reaper": "worker.reaper",
    "eco.margin": "worker.eco.margin",
    "eco.finlib": "worker.eco.finlib",
    "price.token": "worker.price.token",
    "price.model": "worker.price.model",
    "search.archive": "worker.search.archive",
}

def _extract_base_name(b: ast.expr) -> str:
    """AST 부모 클래스 노드에서 이름 추출 (제네릭 Subscript 및 Attribute 모두 대응)"""
    if isinstance(b, ast.Name):
        return b.id
    if isinstance(b, ast.Attribute):
        return b.attr
    if isinstance(b, ast.Subscript):
        return _extract_base_name(b.value)
    return ""

def discover_builtin_workers(
    base_package: str = DEFAULT_WORKER_PACKAGE,
) -> Dict[str, Tuple[str, str, str]]:
    discovered: Dict[str, Tuple[str, str, str]] = {}
    try:
        spec = importlib.util.find_spec(base_package)
        if not spec or not spec.submodule_search_locations:
            return discovered
        root_dir = Path(list(spec.submodule_search_locations)[0]).resolve()
    except Exception:
        return discovered

    target_bases = {"WorkerProtocol", "AsyncWorkerProtocol", "BaseWorker", "McpWorker"}

    for py_file in sorted(root_dir.rglob("*.py")):
        if py_file.name.startswith("_") or "e2e" in py_file.parts or "tests" in py_file.parts:
            continue
        try:
            source_text = py_file.read_text(encoding="utf-8")
            tree = ast.parse(source_text)
        except Exception:
            continue

        rel_parts = py_file.relative_to(root_dir).with_suffix("").parts
        rel_key = ".".join(rel_parts)  # 예: 'deployer', 'eco.oracle', 'model.oracle', 'search.archive'
        module_path = f"{base_package}.{rel_key}"
        leaf = rel_parts[-1]

        matched_mode: Optional[str] = None

        # 1차: AST 클래스 상속 검사
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            bases = {_extract_base_name(b) for b in node.bases}
            matched = bases & target_bases
            if matched or any("Worker" in b for b in bases):
                matched_mode = "multiplex" if "AsyncWorkerProtocol" in matched else "linear"
                break

        # 2차 폴백: BUILTIN_ALIASES에 등록된 경로이거나 실행 엔트리가 존재하는 워커 파일
        if matched_mode is None and (rel_key in BUILTIN_ALIASES or leaf in BUILTIN_ALIASES or "__main__" in source_text):
            matched_mode = "multiplex" if "Async" in source_text else "linear"

        if matched_mode is not None:
            # rel_key('eco.oracle' vs 'model.oracle')를 우선 조회하여 동명 파일 충돌 방지
            canonical_id = BUILTIN_ALIASES.get(rel_key, f"worker.{rel_key}")
            cmd = f"{sys.executable} -m {module_path}"
            entry = (canonical_id, cmd, matched_mode)

            # 정규화 ID 및 상대 경로 키 등록
            discovered[canonical_id] = entry
            discovered[rel_key] = entry
            discovered[f"worker.{rel_key}"] = entry
            if leaf not in discovered or BUILTIN_ALIASES.get(leaf) == canonical_id:
                discovered[leaf] = entry

            for alias_key, target_cid in BUILTIN_ALIASES.items():
                if target_cid == canonical_id:
                    discovered[alias_key] = entry

    return discovered


def _resolve_all_builtin_specs(
    target_filter: Optional[str] = None,
    mode_override: Optional[str] = None,
    preflight_override: Optional[str] = None,
    base_package: str = DEFAULT_WORKER_PACKAGE,
) -> List[ResolvedTarget]:
    builtin_map = discover_builtin_workers(base_package)
    seen_ids: set[str] = set()
    specs: List[ResolvedTarget] = []
    normalized_filter = BUILTIN_ALIASES.get(target_filter, target_filter) if target_filter else None

    for tid, cmd, default_mode in builtin_map.values():
        if tid in seen_ids:
            continue
        if normalized_filter and tid != normalized_filter and target_filter != tid.split(".")[-1]:
            continue
        seen_ids.add(tid)
        specs.append(
            resolve_connector_spec(
                target=tid,
                exec_cmd=cmd,
                mode=mode_override or default_mode,
                transport="stdio",
                preflight=preflight_override,
            )
        )
    return specs


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

    builtin_map = discover_builtin_workers()
    if raw_cmd in builtin_map:
        inferred_target, resolved_cmd, inferred_default_mode = builtin_map[raw_cmd]
    else:
        resolved_cmd = raw_cmd

    is_http_url = resolved_cmd.startswith(("http://", "https://"))
    is_managed_bin_dir = not is_http_url and Path(resolved_cmd).is_dir()

    if transport is not None:
        resolved_transport = transport.lower().strip()
    else:
        resolved_transport = "network" if (is_http_url or is_managed_bin_dir) else "stdio"

    if resolved_transport not in ("stdio", "network"):
        log.error(f"[Fiber] Invalid transport '{resolved_transport}'. Allowed values: 'stdio', 'network'.")
        raise typer.Exit(1)

    resolved_target = (
        target
        or inferred_target
        or _infer_fallback_target_id(resolved_cmd, is_http_url)
    ).strip()

    if mode is not None:
        resolved_mode = mode.lower().strip()
    elif inferred_default_mode is not None:
        resolved_mode = inferred_default_mode
    else:
        resolved_mode = "multiplex" if resolved_transport == "network" else "ephemeral"

    if resolved_mode not in ("ephemeral", "linear", "multiplex"):
        log.error(f"[Fiber] Invalid mode '{resolved_mode}'. Allowed values: 'ephemeral', 'linear', 'multiplex'.")
        raise typer.Exit(1)

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
        env_mode = clean_env.pop("FIBER_MODE", None) if clean_env else None
        if clean_env:
            os.environ.update(clean_env)

        effective_mode = mode_override or env_mode

        specs.append(
            resolve_connector_spec(
                target=entry.target_id,
                exec_cmd=entry.exec_cmd,
                mode=effective_mode,
                preflight=entry_preflight,
            )
        )
    return specs


def _sync_to_anchor_config(
    specs: List[ResolvedTarget],
    env_lookup: Optional[Dict[str, Dict[str, str]]] = None,
) -> Optional[Path]:
    """
    해석된 모든 ResolvedTarget(내장 워커 + 외부 워커)을
    ~/.anchor/mcp.json의 표준 mcpServers 포맷으로 병합(Upsert)합니다.
    """
    if not specs:
        return None

    ANCHOR_MCP_PATH.parent.mkdir(parents=True, exist_ok=True)
    data: Dict[str, Any] = {"mcpServers": {}}

    if ANCHOR_MCP_PATH.is_file():
        try:
            loaded = orjson.loads(ANCHOR_MCP_PATH.read_bytes())
            if isinstance(loaded, dict):
                data = loaded
                if not isinstance(data.get("mcpServers"), dict):
                    data["mcpServers"] = {}
        except Exception as e:
            log.warning(f"[Fiber] Failed to read existing {ANCHOR_MCP_PATH}, resetting: {e}")

    servers = data["mcpServers"]
    env_lookup = env_lookup or {}

    for spec in specs:
        existing_entry = servers.get(spec.target_id, {})
        merged_env: Dict[str, str] = {}
        if isinstance(existing_entry, dict) and isinstance(existing_entry.get("env"), dict):
            merged_env.update({str(k): str(v) for k, v in existing_entry["env"].items()})
        if spec.target_id in env_lookup:
            merged_env.update(env_lookup[spec.target_id])

        merged_env["FIBER_MODE"] = spec.mode
        if spec.preflight:
            merged_env["FIBER_PREFLIGHT_PATTERN"] = spec.preflight

        if spec.exec_cmd.startswith(("http://", "https://")):
            entry_dict: Dict[str, Any] = {"url": spec.exec_cmd}
        else:
            try:
                parts = shlex.split(spec.exec_cmd)
            except ValueError:
                parts = spec.exec_cmd.split()
            entry_dict = {
                "command": parts[0] if parts else spec.exec_cmd,
                "args": parts[1:] if len(parts) > 1 else [],
            }

        if merged_env:
            entry_dict["env"] = merged_env

        servers[spec.target_id] = entry_dict

    ANCHOR_MCP_PATH.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2))
    log.info(
        f"[Fiber] Copied & locked {len(specs)} target(s) into canonical config: {ANCHOR_MCP_PATH}"
    )
    return ANCHOR_MCP_PATH

def _collect_specs_and_envs(
    exec_cmd: Optional[str] = None,
    config: Optional[Path] = None,
    target: Optional[str] = None,
    mode: Optional[str] = None,
    preflight: Optional[str] = None,
    include_builtins_on_empty: bool = True,
) -> Tuple[List[ResolvedTarget], Dict[str, Dict[str, str]]]:
    """CLI 입력 인자로부터 ResolvedTarget 리스트와 원본 환경변수 맵을 추출하는 공통 헬퍼"""
    specs: List[ResolvedTarget] = []
    env_lookup: Dict[str, Dict[str, str]] = {}

    if config is not None:
        mapped_entries = McpConfigMapper.parse_file(config)
        for e in mapped_entries:
            env_lookup[e.target_id] = dict(e.env)
        specs.extend(
            _specs_from_mapped_entries(
                mapped_entries, target_filter=target, mode_override=mode, preflight_override=preflight
            )
        )

    if exec_cmd is not None:
        if exec_cmd.strip().lower() in ("builtin", "builtins", "all"):
            specs.extend(_resolve_all_builtin_specs(target, mode, preflight))
        else:
            specs.append(resolve_connector_spec(target=target, exec_cmd=exec_cmd, mode=mode, preflight=preflight))

    if config is None and exec_cmd is None:
        seen_ids: set[str] = set()
        if include_builtins_on_empty:
            builtin_specs = _resolve_all_builtin_specs(target, mode, preflight)
            seen_ids.update(s.target_id for s in builtin_specs)
            specs.extend(builtin_specs)

        mapped_entries = McpConfigMapper.scan_and_parse()
        for e in mapped_entries:
            env_lookup[e.target_id] = dict(e.env)
        external_specs = _specs_from_mapped_entries(
            mapped_entries, target_filter=target, mode_override=mode, preflight_override=preflight
        )
        for ext_spec in external_specs:
            if ext_spec.target_id in seen_ids:
                specs = [ext_spec if s.target_id == ext_spec.target_id else s for s in specs]
            else:
                specs.append(ext_spec)
                seen_ids.add(ext_spec.target_id)

    return specs, env_lookup

async def _filter_unmounted_specs(specs: List[ResolvedTarget]) -> List[ResolvedTarget]:
    """데몬에 이미 떠 있는 타겟을 제외하여 중복 실행 방지"""
    try:
        tunnel = await TunnelFactory.get_default()
        raw = await tunnel.get(ACTIVE_TARGETS_TUNNEL_KEY)
        if raw:
            active_ids = set(orjson.loads(raw))
            unmounted = [s for s in specs if s.target_id not in active_ids]
            skipped = [s.target_id for s in specs if s.target_id in active_ids]
            if skipped:
                log.info(
                    f"[Fiber] Skipping {len(skipped)} target(s) already active in McpWorkerDaemon: {skipped}"
                )
            return unmounted
    except Exception:
        pass
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

# 1. 역할 A: `fiber connect copy` (설정 복제 및 ~/.anchor/mcp.json 생성/갱신 전용)
def run_connect_copy(
    exec_cmd: Optional[str] = None,
    config: Optional[Path] = None,
    target: Optional[str] = None,
    mode: Optional[str] = None,
    preflight: Optional[str] = None,
) -> Path:
    specs, env_lookup = _collect_specs_and_envs(
        exec_cmd=exec_cmd,
        config=config,
        target=target,
        mode=mode,
        preflight=preflight,
        include_builtins_on_empty=True,
    )
    if not specs:
        log.error("[Fiber] No MCP targets found to copy.")
        raise typer.Exit(1)

    out_path = _sync_to_anchor_config(specs, env_lookup=env_lookup)
    for s in specs:
        guard_info = f" (Pre-Flight: {s.preflight})" if s.preflight else ""
        log.info(f"  ├─ [{s.target_id}] -> {s.exec_cmd} [{s.mode.upper()}]{guard_info}")
    return out_path or ANCHOR_MCP_PATH

# `fiber connect` (워커 프로세스 런타임 실행 전용)
def run_connect(
    exec_cmd: Optional[str] = None,
    config: Optional[Path] = None,
    target: Optional[str] = None,
    mode: Optional[str] = None,
    preflight: Optional[str] = None,
):
    specs, _ = _collect_specs_and_envs(
        exec_cmd=exec_cmd,
        config=config,
        target=target,
        mode=mode,
        preflight=preflight,
        include_builtins_on_empty=True,
    )
    if not specs:
        log.error("[Fiber] No valid MCP target resolved. Provide --exec (-e), --config (-c), or run 'fiber connect copy'.")
        raise typer.Exit(1)

    async def _orchestrate_launch():
        runnable_specs = await _filter_unmounted_specs(specs)
        if not runnable_specs:
            log.info("[Fiber] All resolved targets are already running under McpWorkerDaemon.")
            return
        await _launch_all_async(runnable_specs)

    try:
        asyncio.run(_orchestrate_launch())
    except KeyboardInterrupt:
        log.info("\n[Fiber] Connector shutting down...")