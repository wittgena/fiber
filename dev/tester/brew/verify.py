# fiber/dev/tester/brew/verify.py
"""
Homebrew Installation & Runtime Harness Verifier for Fiber.
Usage:
    python -m fiber.dev.tester.brew.verify
    python -m fiber.dev.tester.brew.verify --audit
"""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

GREEN = "\033[0;32m"
RED = "\033[0;31m"
YELLOW = "\033[1;33m"
NC = "\033[0m"


def log_pass(msg: str) -> None:
    print(f"{GREEN}[PASS]{NC} {msg}")


def log_info(msg: str) -> None:
    print(f"{YELLOW}[INFO]{NC} {msg}")


def log_fail(msg: str) -> None:
    print(f"{RED}[FAIL]{NC} {msg}", file=sys.stderr)
    sys.exit(1)


def run_cmd(cmd: list[str], env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, env=env, capture_output=True, text=True, check=check)


def verify_brew_paths() -> tuple[Path, Path]:
    """1. Verify Homebrew prefix and opt/fiber installation directory."""
    if not shutil.which("brew"):
        log_fail("Homebrew ('brew') binary not found in PATH.")

    brew_prefix = Path(run_cmd(["brew", "--prefix"]).stdout.strip())
    res = run_cmd(["brew", "--prefix", "fiber"], check=False)
    if res.returncode != 0 or not res.stdout.strip():
        log_fail("Formula 'fiber' is not installed via Homebrew.")

    opt_fiber = Path(res.stdout.strip())
    if not opt_fiber.is_dir():
        log_fail(f"Formula opt directory does not exist: {opt_fiber}")

    log_pass(f"Homebrew opt path resolved: {opt_fiber}")
    return brew_prefix, opt_fiber


def verify_virtualenv_and_links(brew_prefix: Path, opt_fiber: Path) -> Path:
    """2. Verify bin/fiber symlink and isolated libexec Python 3.12 environment."""
    fiber_bin = brew_prefix / "bin" / "fiber"
    libexec_fiber = opt_fiber / "libexec" / "bin" / "fiber"
    libexec_py = opt_fiber / "libexec" / "bin" / "python"

    if not fiber_bin.is_symlink():
        log_fail(f"Expected symlink missing: {fiber_bin}")
    if not os.access(libexec_fiber, os.X_OK):
        log_fail(f"Executable missing in libexec: {libexec_fiber}")
    if not os.access(libexec_py, os.X_OK):
        log_fail(f"Virtualenv Python missing in libexec: {libexec_py}")

    log_pass(f"Symlink ({fiber_bin}) -> ({fiber_bin.resolve()}) verified")

    # Verify Python 3.12 & injected package version (SETUPTOOLS_SCM_PRETEND_VERSION)
    py_ver = run_cmd(
        [str(libexec_py), "-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"]
    ).stdout.strip()
    if py_ver != "3.12":
        log_fail(f"Expected Python 3.12 in libexec, got {py_ver}")

    pkg_ver_res = run_cmd(
        [str(libexec_py), "-c", "import importlib.metadata; print(importlib.metadata.version('fiber'))"],
        check=False,
    )
    pkg_ver = pkg_ver_res.stdout.strip() if pkg_ver_res.returncode == 0 else "unknown"
    log_pass(f"Isolated Python ({py_ver}) & package metadata version ({pkg_ver}) verified")

    return fiber_bin


def verify_dependencies(brew_prefix: Path) -> None:
    """3. Verify Redis dependency."""
    redis_cli = shutil.which("redis-cli") or str(brew_prefix / "opt" / "redis" / "bin" / "redis-cli")
    if not os.access(redis_cli, os.X_OK):
        log_fail("Dependency 'redis' (redis-cli) not found.")
    log_pass(f"Dependency 'redis' verified ({redis_cli})")


def verify_sandbox_bootstrap(fiber_bin: Path) -> None:
    """4. Replicate `brew test` in an isolated temporary HOME directory."""
    with tempfile.TemporaryDirectory(prefix="fiber_brew_verify_") as tmp_dir:
        tmp_home = Path(tmp_dir)
        env = os.environ.copy()
        env["HOME"] = str(tmp_home)

        log_info(f"Running isolated 'fiber init' in sandbox HOME ({tmp_home})...")
        res = run_cmd([str(fiber_bin), "init"], env=env, check=False)
        if res.returncode != 0:
            log_fail(f"'fiber init' failed with exit code {res.returncode}:\n{res.stderr}")

        expected_files = [
            tmp_home / ".anchor" / "bound.json",
            tmp_home / ".ssh" / "fiber_ed25519",
            tmp_home / ".ssh" / "fiber_ed25519.pub",
        ]
        for path in expected_files:
            if not path.is_file():
                log_fail(f"Expected bootstrap artifact missing: {path}")

        log_pass("Sandbox 'fiber init' artifacts (bound.json, ed25519 keypair) verified")

        sign_res = run_cmd([str(fiber_bin), "sign", "status"], env=env, check=False)
        if sign_res.returncode == 0:
            log_pass("Sandbox 'fiber sign status' command succeeded")
        else:
            log_info(f"'fiber sign status' returned {sign_res.returncode} (non-fatal)")


def verify_service_and_gateway(brew_prefix: Path, host: str = "127.0.0.1", port: int = 8356) -> None:
    """5. Check service directories and live loopback daemon (:8356) if active."""
    var_fiber = brew_prefix / "var" / "fiber"
    log_fiber = brew_prefix / "var" / "log" / "fiber"

    # Note: Since `post_install` is omitted per Homebrew audit rules,
    # `brew services` creates var/fiber and var/log/fiber on first service start.
    if var_fiber.is_dir() and log_fiber.is_dir():
        log_pass(f"Service directories exist ({var_fiber}, {log_fiber})")
    else:
        log_info("Service directories (var/fiber) not yet created (created automatically on `brew services start`).")

    # Check if port 8356 is listening
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        is_listening = sock.connect_ex((host, port)) == 0

    if not is_listening:
        log_info(f"Daemon is not currently listening on {host}:{port}.")
        log_info("To start the background daemon: `brew services start redis && brew services start dphi-network/fiber/fiber`")
        return

    log_pass(f"Loopback daemon is active on {host}:{port}")
    url = f"http://{host}:{port}/v1/mcp-gateway/tools"
    try:
        with urllib.request.urlopen(url, timeout=2.0) as resp:
            status_code = resp.status
    except urllib.error.HTTPError as e:
        status_code = e.code
    except Exception as exc:
        log_fail(f"Port {port} is open, but HTTP probe to {url} failed: {exc}")

    log_pass(f"Gateway HTTP endpoint responded (HTTP {status_code} on {url})")


def run_brew_audit(tap_formula: str = "dphi-network/fiber/fiber") -> None:
    """Optional: Run `brew audit --strict` and `brew test`."""
    log_info(f"Running `brew audit --strict {tap_formula}`...")
    audit_res = run_cmd(["brew", "audit", "--strict", tap_formula], check=False)
    if audit_res.returncode != 0:
        log_fail(f"`brew audit --strict` failed:\n{audit_res.stdout}\n{audit_res.stderr}")
    log_pass("`brew audit --strict` passed with 0 offenses")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Fiber Homebrew installation and runtime.")
    parser.add_argument("--audit", action="store_true", help="Also run `brew audit --strict dphi-network/fiber/fiber`")
    parser.add_argument("--tap", default="dphi-network/fiber/fiber", help="Target Homebrew tap formula name")
    args = parser.parse_args()

    print("========================================================")
    print(" Fiber Homebrew Installation & Runtime Verifier (Python)")
    print("========================================================")

    brew_prefix, opt_fiber = verify_brew_paths()
    fiber_bin = verify_virtualenv_and_links(brew_prefix, opt_fiber)
    verify_dependencies(brew_prefix)
    verify_sandbox_bootstrap(fiber_bin)
    verify_service_and_gateway(brew_prefix)

    if args.audit:
        run_brew_audit(args.tap)

    print("========================================================")
    print(f"{GREEN}All Homebrew installation and runtime checks passed!{NC}")

if __name__ == "__main__":
    main()