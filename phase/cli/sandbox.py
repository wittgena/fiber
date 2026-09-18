# fiber.phase.cli.sandbox
"""
WARNING: Local Dev Sandbox Only. Do not use in production.
"""
import sys
import os
import runpy
import typer
import threading
from typing import Annotated, Optional

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fiber.cli.sandbox")

"""Environment Fencing (Local Dev Only)"""
class EnvironmentViolationError(Exception):
    pass

class SecurityViolationError(Exception):
    pass

def verify_local_dev_environment(allow_ci: bool = False):
    """Blocks execution in server/production environments."""
    if not sys.stdout.isatty() and not allow_ci:
        raise EnvironmentViolationError("VCR cannot run in a background/detached process.")

    if hasattr(os, "geteuid") and os.geteuid() == 0:
        raise EnvironmentViolationError("VCR cannot be executed with root/sudo privileges.")

    server_envs = {"KUBERNETES_SERVICE_HOST", "AWS_EXECUTION_ENV", "K_SERVICE", "FLY_APP_NAME"}
    if any(env in os.environ for env in server_envs):
        raise EnvironmentViolationError("Cloud environment detected. VCR is for local dev only.")

    node_profile = os.environ.get("NODE_PROFILE", "UNKNOWN")
    if node_profile in ("EDGE", "COMPUTE", "CONTROL"):
        raise EnvironmentViolationError(f"Cannot run VCR on production node profile ({node_profile}).")


"""Enhanced Security Sandbox"""
AUTHORIZED_FIBER_PATHS = (
    "fiber/dev/trace", "xphi/kernel", "fiber/llm/entry", "fiber/phase/cli",
    "site-packages/opentelemetry", "site-packages/redis", "lib/python"
)
_audit_hook_local = threading.local()

def get_caller_origin(limit: int = 15) -> str:
    """Lightweight stack inspection using sys._getframe to avoid I/O triggers."""
    try:
        frame = sys._getframe(2) # 0=get_caller, 1=audit_hook, 2=actual caller
        count = 0
        while frame and count < limit:
            path = frame.f_code.co_filename.replace('\\', '/')
            if "cli/sandbox.py" not in path and "importlib" not in path:
                return path
            frame = frame.f_back
            count += 1
    except ValueError:
        pass
    return "Unknown"

def is_authorized_caller(caller_path: str) -> bool:
    return any(auth_path in caller_path for auth_path in AUTHORIZED_FIBER_PATHS)

def create_security_sandbox(vcr_mode: str):
    """Injects PEP-578 audit hooks with recursion guards."""
    
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

    sys.addaudithook(audit_hook)
    log.info("[Fiber VCR] 🛡️ PEP-578 Audit Hooks Injected (Local Sandbox Active).")


"""VCR Execution Core Logic"""
def execute_vcr_logic(
    ctx: typer.Context,
    target_script: str,
    mode: str,
    speed: str,
    chaos: float,
):
    is_ci = os.environ.get("CI") in ("true", "1")
    try:
        verify_local_dev_environment(allow_ci=is_ci)
    except EnvironmentViolationError as env_err:
        log.error(f"\n[Fiber VCR] 🚫 EXECUTION DENIED: {env_err}")
        sys.exit(1)

    log.info(f"[Fiber VCR] 🌀 Activating Phase Airlock ({mode.upper()} mode)...")
    
    from xphi.kernel.space.bind.redirector import PhaseAirlock
    PhaseAirlock.establish(
        legacy_path="litellm", 
        canonical_path="fiber.llm.entry",
        submodules=["utils", "types", "exceptions"] 
    )
    
    if mode in ("record", "replay"):
        from fiber.dev.trace.llm.vcr import VCRInjector, VCRPlaybackConfig
        from xphi.kernel.space.bind.resolver import resolve_path
        
        config = VCRPlaybackConfig(mode=mode, speed=speed, chaos_latency_ms=chaos)
        VCRInjector.apply(config=config, fixture_dir=resolve_path("fixture"))
        log.info(f"[Fiber VCR] 📼 VCR Engine applied (Speed: {speed}, Chaos: {chaos}ms)")
    else:
        log.info("[Fiber VCR] 🟢 LIVE mode. Passing through to live network without recording.")

    log.info(f"[Fiber VCR] 🚀 Launching Target Script: {target_script}")
    log.info("=" * 60)
    
    sys.argv = [target_script] + ctx.args
    
    try:
        create_security_sandbox(vcr_mode=mode)
        runpy.run_path(target_script, run_name="__main__")
        
    except SecurityViolationError as se:
        log.error(f"\n[Fiber VCR] 🚨 THREAT DETECTED: {se}")
        log.error("[Fiber VCR] Execution forcefully terminated by Fiber Kernel.")
        sys.exit(1)
    except KeyboardInterrupt:
        log.info("\n[Fiber VCR] 🛑 Interrupted by user.")
    except Exception as e:
        log.error(f"\n[Fiber VCR] 💥 Target script execution failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        log.info("\n" + "=" * 60)
        log.info(f"[Fiber VCR] Execution of {target_script} Finished.")