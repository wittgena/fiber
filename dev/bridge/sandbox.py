# fiber.dev.bridge.sandbox
"""
WARNING: Local Dev Sandbox Only. Do not use in production.
"""
import sys
import os
import threading
import tomllib
import importlib.metadata
import re
from pathlib import Path

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bridge.sandbox")

"""Environment Fencing (Local Dev Only)"""
class EnvironmentViolationError(Exception):
    pass

class SecurityViolationError(Exception):
    pass

def verify_local_dev_environment(allow_ci: bool = False):
    """Blocks execution in server/production environments."""
    if not sys.stdout.isatty() and not allow_ci:
        raise EnvironmentViolationError("Sandbox cannot run in a background/detached process.")

    if hasattr(os, "geteuid") and os.geteuid() == 0:
        raise EnvironmentViolationError("Sandbox cannot be executed with root/sudo privileges.")

    server_envs = {"KUBERNETES_SERVICE_HOST", "AWS_EXECUTION_ENV", "K_SERVICE", "FLY_APP_NAME"}
    if any(env in os.environ for env in server_envs):
        raise EnvironmentViolationError("Cloud environment detected. Sandbox is for local dev only.")

    node_profile = os.environ.get("NODE_PROFILE", "UNKNOWN")
    if node_profile in ("EDGE", "COMPUTE", "CONTROL"):
        raise EnvironmentViolationError(f"Cannot run Sandbox on production node profile ({node_profile}).")


"""Dynamic Dependency Resolution (TOML Based)"""
def _resolve_transitive_dependencies() -> list[str]:
    current_dir = Path.cwd()
    toml_path = None
    for _ in range(3):
        if (current_dir / "pyproject.toml").exists():
            toml_path = current_dir / "pyproject.toml"
            break
        current_dir = current_dir.parent

    if not toml_path:
        log.warning("[Fiber Sandbox] ⚠️ pyproject.toml not found. Strict sandbox might block valid modules.")
        return []

    # 2. TOML에서 1차 의존성(Base Dependencies) 추출
    base_deps = []
    try:
        with open(toml_path, "rb") as f:
            data = tomllib.load(f)
            
        # project.dependencies 및 tool.hatch...base_dependencies 모두 수용
        base_deps.extend(data.get("project", {}).get("dependencies", []))
        base_deps.extend(
            data.get("tool", {}).get("hatch", {}).get("metadata", {})
            .get("hooks", {}).get("custom", {}).get("base_dependencies", [])
        )
    except Exception as e:
        log.error(f"[Fiber Sandbox] ⚠️ Failed to parse pyproject.toml: {e}")
        return []

    # 3. 하위 의존성 재귀적 추적 (BFS)
    resolved_pkgs = set()
    queue = list(base_deps)

    while queue:
        raw_pkg = queue.pop(0)
        # PEP 508 문자열 정리: 'mcp==2.0.0a3; python_version < "3.12"' -> 'mcp'
        clean_pkg = re.split(r'[<>=!~;\s]', raw_pkg)[0].strip().lower()
        
        if not clean_pkg or clean_pkg in resolved_pkgs:
            continue
            
        resolved_pkgs.add(clean_pkg)

        try:
            reqs = importlib.metadata.requires(clean_pkg) or []
            for req in reqs:
                queue.append(re.split(r'[<>=!~;\s]', req)[0].strip())
        except importlib.metadata.PackageNotFoundError:
            pass

    # 4. 샌드박스 경로 포맷으로 변환
    allowed_paths = set()
    for pkg in resolved_pkgs:
        pkg_dash = pkg.replace('_', '-')
        pkg_under = pkg.replace('-', '_')
        allowed_paths.add(f"site-packages/{pkg_dash}")
        allowed_paths.add(f"site-packages/{pkg_under}")
        allowed_paths.add(f"dist-packages/{pkg_under}")

    return list(allowed_paths)


"""Enhanced Security Sandbox"""
AUTHORIZED_FIBER_PATHS = [
    "fiber/dev", "fiber/gateway", "fiber/infra", "fiber/llm", "fiber/phase", "xphi/node", "lib/python"
] + _resolve_transitive_dependencies()

_audit_hook_local = threading.local()

def get_caller_origin(limit: int = 15) -> str:
    """Lightweight stack inspection using sys._getframe to avoid I/O triggers."""
    try:
        frame = sys._getframe(2)
        count = 0
        while frame and count < limit:
            path = frame.f_code.co_filename.replace('\\', '/')
            if "bridge/sandbox.py" not in path and "importlib" not in path:
                return path
            frame = frame.f_back
            count += 1
    except ValueError:
        pass
    return "Unknown"

def is_authorized_caller(caller_path: str) -> bool:
    return any(auth_path in caller_path for auth_path in AUTHORIZED_FIBER_PATHS)

def create_security_sandbox(vcr_mode: str = "live"):
    """
    Injects PEP-578 audit hooks with recursion guards.
    Now acts purely as a security enforcer, decoupled from test execution.
    """
    def audit_hook(event, args):
        if getattr(_audit_hook_local, 'in_hook', False):
            return
            
        _audit_hook_local.in_hook = True
        try:
            caller_path = get_caller_origin()
            
            # 1. Block C-Level FFI to prevent sandbox escape
            if event in ("ctypes.dlopen", "ctypes.dlsym", "ctypes.c_void_p"):
                if not is_authorized_caller(caller_path):
                    raise SecurityViolationError(f"C-Level FFI access blocked from {caller_path}")

            # 2. Block unauthorized OS command execution
            if event in ("os.system", "subprocess.Popen", "os.posix_spawn", "os.execv"):
                if not is_authorized_caller(caller_path):
                    raise SecurityViolationError(f"OS command execution blocked from {caller_path}")
            
            # 3. Block sensitive file access
            if event == "open" and isinstance(args[0], str):
                try:
                    real_path = os.path.realpath(args[0])
                    if real_path.startswith(("/etc/", "/root/", "/.ssh/")):
                        raise SecurityViolationError(f"Sensitive file access blocked: {real_path}")
                except Exception:
                    pass

            # 4. Enforce strict network air-gap in REPLAY mode
            if vcr_mode == "replay" and event == "socket.connect":
                 if len(args) > 0 and isinstance(args[0], tuple):
                     host = args[0][0]
                     if host not in ("127.0.0.1", "localhost", "::1"):
                         raise SecurityViolationError(f"External network connection blocked in REPLAY mode: {host}")
        finally:
            _audit_hook_local.in_hook = False

    try:
        sys.addaudithook(audit_hook)
        log.info(f"[Fiber Sandbox] 🛡️ PEP-578 Audit Hooks Injected (Mode: {vcr_mode.upper()}).")
    except Exception as e:
        log.error(f"[Fiber Sandbox] 💥 Failed to inject audit hook: {e}")
        raise