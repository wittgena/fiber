# fiber.phase.cli
import os
import sys
import json
import socket
import asyncio
import inspect
import importlib
import subprocess
from pathlib import Path
from typing import Annotated, Optional, Callable, Any
import typer

try:
    import dotenv
except ImportError:
    dotenv = None

from xphi.node.space.bind.around import determine_execution_mode, project_user_mode
if determine_execution_mode() == "USER" and not (Path.home() / ".anchor" / "bound.json").exists():
    project_user_mode(Path.home() / ".anchor")

from xphi.state.phase.reactor import PhaseReactor
from xphi.node.ops.boot import main_async, teardown
from xphi.watcher.plane.emitter import get_emitter
from xphi.node.space.bind.resolver import resolve_path
from fiber.gateway.bridge.action.sign import app as sign_app

log = get_emitter("phase.cli")

DAEMON_PORT = 8356
CONSOLE_PORT = 8357

EnvFileOpt = Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True, help="Path to .env file")]

app = typer.Typer(
    help="Fiber: The Universal Integration Boundary & OS Kernel",
    no_args_is_help=True,
    add_completion=False,
)
compat_app = typer.Typer(
    help="Manage LLM compatibility rules and test fixtures (fiber-compats)",
    no_args_is_help=True,
)
app.add_typer(compat_app, name="compat")
app.add_typer(sign_app, name="sign")


def _is_port_active(port: int = DAEMON_PORT, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.2)
        return s.connect_ex((host, port)) == 0


def _load_env(env_file: Optional[str]):
    if not env_file:
        return
    if not dotenv:
        log.warning("[Fiber] python-dotenv is not installed. Ignoring --env-file option.")
        return
    try:
        dotenv.load_dotenv(env_file)
        log.info(f"[Fiber] Loaded environment from {env_file}")
    except Exception as e:
        log.error(f"[Fiber] Failed to load .env file: {e}")
        raise typer.Exit(1)


def _run_guarded(label: str, fn: Callable[[], Any]):
    try:
        res = fn()
        if inspect.iscoroutine(res):
            asyncio.run(res)
    except KeyboardInterrupt:
        log.info(f"\n[Fiber] {label} manually terminated by user.")
    except typer.Exit:
        raise
    except Exception as e:
        log.error(f"[Fiber] {label} failed: {e}", exc_info=True)
        sys.exit(1)


@app.command("init")
def run_init(
    rotate_cipher: Annotated[bool, typer.Option("--rotate-cipher", help="Regenerate local_cipher.key (invalidates existing TOTP DB)")] = False,
    env_file: EnvFileOpt = None,
):
    """Initializes workspace boundary (~/.anchor/bound.json) and local node cryptographic keys."""
    _load_env(env_file)
    log.info("[Fiber] Bootstrapping workspace and node identity...")

    if determine_execution_mode() == "USER":
        project_user_mode(Path.home() / ".anchor")

    from fiber.gateway.bridge.action.sign import ensure_node_keys
    info = ensure_node_keys(force_regenerate_cipher=rotate_cipher)

    ssh_msg = "newly created" if info["ssh_key_status"] == "CREATED" else "already exists (skipped)"
    cip_msg = "newly created" if info["local_cipher_status"] == "CREATED" else "already exists (skipped)"
    log.info(f"Node Cryptographic Identity Initialized (SSH: {ssh_msg}, Cipher: {cip_msg}):")
    print(json.dumps(info, indent=2, ensure_ascii=False))


@app.command("daemon")
def run_daemon(
    start: Annotated[str, typer.Option("--start", "-s", help="Daemons to start or topology preset (e.g., edge, all)")] = "edge",
    detach: Annotated[bool, typer.Option("--detach", "-d", help="Run daemon in the background and log to <log>/daemon.log")] = False,
    env_file: EnvFileOpt = None,
):
    """Boots Fiber node daemons (:8356). Defaults to 'edge' (REST Edge + RPC + MCP Worker)."""
    if _is_port_active(DAEMON_PORT):
        log.info(f"🟢 [Fiber] Daemon is already running on http://127.0.0.1:{DAEMON_PORT}.")
        return

    if detach:
        log_dir = Path(str(resolve_path("log")))
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "daemon.log"
        cmd = [sys.executable, "-m", "fiber.phase.cli", "daemon", "-s", start] + (["-f", env_file] if env_file else [])
        with open(log_file, "a", encoding="utf-8") as out:
            proc = subprocess.Popen(cmd, stdout=out, stderr=out, start_new_session=True)
        log.info(f"🚀 [Fiber] Background daemon started (PID: {proc.pid}) -> Log: {log_file}")
        return

    _load_env(env_file)
    presets = {
        "edge": "rest_edge,rpc_worker,mcp_worker",
        "all": "rest_edge,rpc_worker,mcp_worker,dynamic_pricing,risk_vault",
    }
    resolved = presets.get(start.lower(), start)
    os.environ["NODE_DAEMONS"] = resolved
    os.environ["GATEWAY_TOPOLOGY"] = "EMBEDDED_BYPASS"
    os.environ["NODE_PROFILE"] = "EDGE" if all(d.strip() == "rest_edge" for d in resolved.split(",")) else "ALL"

    log.info("[Fiber] Igniting Kernel in GATEWAY DAEMON mode...")
    _run_guarded("Gateway Daemon", lambda: PhaseReactor.ignite(main_coro_func=main_async, teardown_hook=teardown))


@app.command("console")
def run_console(
    port: Annotated[int, typer.Option("--port", "-p", help="Port for Control Studio UI")] = CONSOLE_PORT,
    auto_daemon: Annotated[bool, typer.Option("--auto-daemon/--no-auto-daemon", help="Auto-spawn local daemon (:8356) if offline")] = True,
    env_file: EnvFileOpt = None,
):
    """Launches Fiber Control Studio (:8357) and ensures the local gateway daemon (:8356) is active."""
    _load_env(env_file)
    from fiber.dev.console.app import launch_console
    launch_console(port=port, auto_daemon=auto_daemon, env_file=env_file)


@app.command("connect")
def run_connect_cmd(
    agent: Annotated[
        Optional[str],
        typer.Argument(help="IDE agent to scan & sync ('claude', 'cursor', 'vscode', 'all') or 'standalone' to run in foreground"),
    ] = None,
    exec_cmd: Annotated[
        Optional[str],
        typer.Option("--exec", "-e", help="Shell command, bin_root dir, or HTTP(S) URL"),
    ] = None,
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", exists=True, help="External mcp.json file to sync or mount"),
    ] = None,
    target: Annotated[
        Optional[str],
        typer.Option("--target", "-t", help="Optional Target ID override (auto-inferred if omitted)"),
    ] = None,
    mode: Annotated[
        Optional[str],
        typer.Option("--mode", "-m", help="Optional mode override: ephemeral, linear, or multiplex"),
    ] = None,
    preflight: Annotated[
        Optional[str],
        typer.Option("--preflight", "-p", help="Regex pattern to lock as FIBER_PREFLIGHT_PATTERN (HITL YIELD)"),
    ] = None,
    standalone: Annotated[
        bool,
        typer.Option("--standalone", "-s", help="Run MCP server(s) directly in the foreground without writing to ~/.anchor/mcp.json"),
    ] = False,
    env_file: EnvFileOpt = None,
):
    """Syncs IDE/external MCP configs into ~/.anchor/mcp.json (or runs in foreground via 'standalone' / '-s')."""
    _load_env(env_file)
    from fiber.gateway.bridge.action.connect import run_connect_standalone, run_connect_copy

    norm_agent = agent.strip().lower() if agent else None
    if standalone or norm_agent == "standalone":
        run_connect_standalone(exec_cmd=exec_cmd, config=config, target=target, mode=mode, preflight=preflight)
        return

    effective_agent = "all" if norm_agent == "copy" and not (exec_cmd or config) else (None if norm_agent == "copy" else norm_agent)
    run_connect_copy(
        agent=effective_agent,
        exec_cmd=exec_cmd,
        config=config,
        target=target,
        mode=mode,
        preflight=preflight,
    )


@app.command("account", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run_account(ctx: typer.Context, env_file: EnvFileOpt = None):
    """Manages local/tenant FinOps accounts, EdDSA JWT quotas, fuel allocate, and live Kill-Switches."""
    _load_env(env_file)
    from fiber.gateway.bridge.action.account import main as account_main
    _run_guarded("Account operation", lambda: account_main(ctx.args))


@app.command("e2e", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run_e2e(
    ctx: typer.Context,
    target: Annotated[str, typer.Argument(help="Target test suite")],
    env_file: EnvFileOpt = None,
):
    _load_env(env_file)
    known = ["fsm.tester", "gateway.llm.guardrail", "gateway.compliance", "gateway.inter.finops", "infra.wasm.phase", "infra.plane.flare"]
    targets = known if target == "all" else [target]

    log.info(f"[Fiber] Igniting E2E Test Suite(s): {', '.join(targets)}")
    for t in targets:
        module_path = f"fiber.dev.e2e.{t}"
        try:
            mod = importlib.import_module(module_path)
            if not hasattr(mod, "main"):
                log.error(f"[Fiber] Module {module_path} lacks 'main'. Skipping.")
                continue
            log.info(f"\n{'='*60}\nLaunching Suite: {module_path}\n{'='*60}")
            mod.main(ctx.args) if inspect.signature(mod.main).parameters else mod.main()
        except ImportError as e:
            log.error(f"[Fiber] Test module not found: {module_path} (Reason: {e})")
            if target != "all":
                sys.exit(1)
        except Exception as e:
            log.error(f"[Fiber] E2E Test {module_path} failed: {e}", exc_info=True)
            sys.exit(1)


@compat_app.command("fixture")
def manage_compat_fixtures(
    model: Annotated[str, typer.Option("--model", "-m", help="Model name (e.g., ollama/gemma:2b)")],
    vcr: Annotated[str, typer.Option("--vcr", help="VCR mode: 'record' (gen) or 'replay' (test)")] = "record",
    vcr_speed: Annotated[str, typer.Option("--vcr-speed", help="Replay speed control: 'max' or 'real'")] = "max",
    vcr_chaos: Annotated[float, typer.Option("--vcr-chaos", help="Latency jitter (ms) for real-world simulation")] = 0.0,
    vcr_tick: Annotated[float, typer.Option("--vcr-tick", help="Time-window (ms) for coalescing chunks during record")] = 100.0,
    vcr_targets: Annotated[str, typer.Option("--vcr-targets", help="Specific trace IDs to target (e.g., 'scenario:trace_id')")] = "",
    api_key: Annotated[Optional[str], typer.Option("--api-key", "-k", envvar="COMPAT_API_KEY", help="API Key (Optional if set in Env)")] = None,
    target_dir: Annotated[Optional[str], typer.Option("--target-dir", "-o", help="Custom target directory for fixtures")] = None,
    env_file: EnvFileOpt = None,
):
    """Manages VCR fixtures for compatibility testing ('--vcr record' or '--vcr replay')."""
    _load_env(env_file)
    mode, speed = vcr.lower().strip(), vcr_speed.lower().strip()
    if speed not in ("max", "real"):
        log.error(f"[Fiber] Invalid --vcr-speed: '{vcr_speed}'. Supported values are 'max' or 'real'.")
        raise typer.Exit(1)

    from fiber.gateway.bridge.action import compat
    if mode in ("record", "gen", "generate"):
        _run_guarded("Compat fixture generation", lambda: compat.run_gen_fixture(model=model, api_key=api_key, target_dir=target_dir, vcr_tick=vcr_tick))
    elif mode in ("replay", "test", "verify"):
        _run_guarded("Compat fixture testing", lambda: compat.run_test_fixture(model=model, target_dir=target_dir, vcr_speed=speed, vcr_chaos=vcr_chaos, vcr_targets=vcr_targets))
    else:
        log.error(f"[Fiber] Invalid --vcr mode: '{vcr}'. Supported modes are 'record' (gen) or 'replay' (test).")
        raise typer.Exit(1)

def main():
    app()

if __name__ == "__main__":
    main()