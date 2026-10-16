# fiber.phase.cli.connect
import sys
import ast
import shutil
import asyncio
import subprocess
import importlib.util
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

import typer
from fiber.gateway.mcp.connector import MCPServerConnector
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fiber.cli.connect")


@dataclass(frozen=True)
class ResolvedTarget:
    target_id: str
    exec_cmd: str
    mode: str
    transport: str


# 1. 내장 워커 표준 별칭 (기존 하위호환성 유지)
BUILTIN_ALIASES: Dict[str, str] = {
    "deployer": "worker.deploy",
    "oracle": "worker.oracle",
    "finlib": "worker.finlib",
    "margin": "worker.margin",
    "archive": "worker.data",
    "context7": "worker.docs",
    "security": "worker.sec",
}

# 2. 외부 오픈소스 MCP 스캔 및 프로비저닝 대행 카탈로그
# (내장 워커로 직접 구현되어 있지 않을 때, 로컬 설치 스캔 후 npx/uvx/pip로 실행 대행)
EXTERNAL_MCP_CATALOG: Dict[str, Dict[str, str]] = {
    "worker.repomix": {
        "bin": "repomix",
        "args": "--mcp",
        "ecosystem": "node",
        "pkg": "repomix",
        "default_mode": "multiplex",
    },
    "worker.office": {
        "bin": "markitdown-mcp",
        "args": "",
        "ecosystem": "python",
        "pkg": "markitdown-mcp",
        "default_mode": "multiplex",
    },
    "worker.playwright": {
        "bin": "mcp-server-playwright",
        "args": "",
        "ecosystem": "node",
        "pkg": "@playwright/mcp@latest",
        "default_mode": "linear",
    },
}


def discover_builtin_workers(base_package: str = "fiber.dev.worker") -> Dict[str, Tuple[str, str]]:
    """
    fiber.dev.worker 하위를 AST로 스캔하여 {target_id: (exec_cmd, default_mode)} 매핑을 반환합니다.
    """
    discovered: Dict[str, Tuple[str, str]] = {}
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

            # 기본값을 기존 stdio 동작과 맞추거나 프로토콜 특성에 따라 설정
            default_mode = "multiplex" if "AsyncWorkerProtocol" in matched else "linear"
            leaf = rel_parts[-1]
            canonical_id = BUILTIN_ALIASES.get(leaf, f"worker.{leaf}")
            cmd = f"{sys.executable} -m {module_path}"

            discovered[canonical_id] = (cmd, default_mode)
            discovered[f"worker.{'.'.join(rel_parts)}"] = (cmd, default_mode)
            break

    return discovered


def _provision_and_resolve_external(preset_key: str, spec: Dict[str, str]) -> str:
    """
    외부 MCP 서버의 로컬 설치 여부를 스캔하고, 미설치 시 npx / uvx / pip 대행 명령어를 구성합니다.
    """
    bin_name = spec["bin"]
    args = spec.get("args", "").strip()
    ecosystem = spec.get("ecosystem", "node")
    pkg = spec["pkg"]

    # Step 1: 로컬에 이미 바이너리가 설치되어 있는지 스캔 (가장 빠른 경로)
    local_bin = shutil.which(bin_name)
    if local_bin:
        log.info(f"[Fiber:Connect] ✅ Found local binary for '{preset_key}': {local_bin}")
        return f"{local_bin} {args}".strip()

    # Step 2: 미설치 시 생태계별 런타임 대행(Delegation)
    if ecosystem == "node":
        npx_bin = shutil.which("npx")
        if not npx_bin:
            log.error(f"[Fiber:Connect] '{preset_key}' requires Node.js ('npx'), which was not found in PATH.")
            raise typer.Exit(1)
        log.info(f"[Fiber:Connect] 📦 Delegating '{preset_key}' execution to npx ({pkg})...")
        return f"{npx_bin} -y {pkg} {args}".strip()

    elif ecosystem == "python":
        uvx_bin = shutil.which("uvx")
        if uvx_bin:
            log.info(f"[Fiber:Connect] 📦 Delegating '{preset_key}' execution to uvx ({pkg})...")
            return f"{uvx_bin} {pkg} {args}".strip()

        # uvx가 없을 경우 현재 Python 가상환경에 자동 pip 설치 대행 후 바이너리 연결
        log.info(f"[Fiber:Connect] 📦 '{bin_name}' not found. Auto-installing '{pkg}' via pip...")
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "--quiet", pkg],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            log.error(f"[Fiber:Connect] Failed to auto-install '{pkg}': {e}")
            raise typer.Exit(1)

        installed_bin = shutil.which(bin_name) or str(Path(sys.executable).parent / bin_name)
        return f"{installed_bin} {args}".strip()

    return f"{bin_name} {args}".strip()


def resolve_connector_spec(
    target: str,
    exec_cmd: str,
    mode: Optional[str] = None,
    transport: Optional[str] = None,
) -> ResolvedTarget:
    """
    내장 워커 -> 외부 대행 카탈로그 -> URL/디렉토리/일반 명령어 순으로 실행 스펙을 확정합니다.
    """
    raw_cmd = exec_cmd.strip()
    inferred_default_mode: Optional[str] = None

    # 1. 내장 워커(fiber.dev.worker.*) 동적 탐색 매칭
    builtin_map = discover_builtin_workers()
    if raw_cmd in builtin_map:
        resolved_cmd, inferred_default_mode = builtin_map[raw_cmd]
    # 2. 외부 오픈소스 MCP 카탈로그 스캔 및 대행 매칭
    elif raw_cmd in EXTERNAL_MCP_CATALOG:
        ext_spec = EXTERNAL_MCP_CATALOG[raw_cmd]
        resolved_cmd = _provision_and_resolve_external(raw_cmd, ext_spec)
        inferred_default_mode = ext_spec.get("default_mode")
    else:
        resolved_cmd = raw_cmd

    # 3. 실행 타겟 유형 정밀 판별 (기존 로직 100% 보존)
    is_http_url = resolved_cmd.startswith(("http://", "https://"))
    is_managed_bin_dir = not is_http_url and Path(resolved_cmd).is_dir()

    # 4. Transport 결정 (명시적 인자 우선 -> 자동 추론 -> 기본값 'stdio')
    if transport is not None:
        resolved_transport = transport.lower().strip()
    else:
        resolved_transport = "network" if (is_http_url or is_managed_bin_dir) else "stdio"

    if resolved_transport not in ("stdio", "network"):
        log.error(f"[Fiber] Invalid transport '{resolved_transport}'. Allowed values: 'stdio', 'network'.")
        raise typer.Exit(1)

    # 5. Mode 결정 (명시적 인자 우선 -> Transport별 기본값 보존: stdio='ephemeral', network='multiplex')
    if mode is not None:
        resolved_mode = mode.lower().strip()
    else:
        resolved_mode = "multiplex" if resolved_transport == "network" else "ephemeral"

    if resolved_mode not in ("ephemeral", "linear", "multiplex"):
        log.error(f"[Fiber] Invalid mode '{resolved_mode}'. Allowed values: 'ephemeral', 'linear', 'multiplex'.")
        raise typer.Exit(1)

    return ResolvedTarget(
        target_id=target,
        exec_cmd=resolved_cmd,
        mode=resolved_mode,
        transport=resolved_transport,
    )


async def launch_connector_async(spec: ResolvedTarget):
    log.info(
        f"[Fiber] 🔌 Sublimating server [{spec.target_id}] into the MCP Server Connector "
        f"(Mode: {spec.mode.upper()}, Transport: {spec.transport.upper()})..."
    )
    daemon = MCPServerConnector(
        target_id=spec.target_id,
        execution_target=spec.exec_cmd,
        mode=spec.mode,
        transport_type=spec.transport,
    )
    try:
        await daemon.run()
    finally:
        log.info("[Fiber] Disconnected from A2A Network.")


def run_connect(
    target: str,
    exec_cmd: str,
    mode: Optional[str] = None,
    transport: Optional[str] = None,
):
    """cli.main에서 직접 호출하는 동기 진입점"""
    spec = resolve_connector_spec(target=target, exec_cmd=exec_cmd, mode=mode, transport=transport)
    try:
        asyncio.run(launch_connector_async(spec))
    except KeyboardInterrupt:
        log.info("\n[Fiber] Connector shutting down...")