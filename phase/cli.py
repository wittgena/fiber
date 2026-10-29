# fiber.phase.cli
import os
import sys
import socket
import asyncio
import inspect
import importlib
import subprocess
from pathlib import Path
from typing import Annotated, Optional
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
from xphi.node.space.tunnel.factory import TunnelFactory
from fiber.gateway.bridge.action.sign import app as sign_app

log = get_emitter("phase.cli")

DAEMON_PORT = 8356
CONSOLE_PORT = 8357

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

connect_app = typer.Typer(
    help="Sublimate MCP servers or copy/sync configurations into ~/.anchor/mcp.json",
    invoke_without_command=True,
    no_args_is_help=False,
)
app.add_typer(connect_app, name="connect")


def _is_port_active(port: int = DAEMON_PORT, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.2)
        return s.connect_ex((host, port)) == 0


def _load_env(env_file: Optional[str]):
    if env_file:
        if dotenv:
            try:
                dotenv.load_dotenv(env_file)
                log.info(f"[Fiber] Loaded environment from {env_file}")
            except Exception as e:
                log.error(f"[Fiber] Failed to load .env file: {e}")
                raise typer.Exit(1)
        else:
            log.warning("[Fiber] python-dotenv is not installed. Ignoring --env-file option.")


def boot_kernel(mode_name: str):
    log.info(f"[Fiber] Igniting Kernel in {mode_name.upper()} mode...")
    try:
        PhaseReactor.ignite(main_coro_func=main_async, teardown_hook=teardown)
    except KeyboardInterrupt:
        log.info("\n[Fiber] Manual interrupt (SIGINT) received. Safely collapsing kernel...")
    except Exception as e:
        log.error(f"[Fiber] FATAL Kernel panic: {e}", exc_info=True)
        sys.exit(1)


@app.command("init")
def run_init(
    rotate_cipher: Annotated[bool, typer.Option("--rotate-cipher", help="Regenerate local_cipher.key (invalidates existing TOTP DB)")] = False,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Initializes workspace boundary (~/.anchor/bound.json) and local node cryptographic keys."""
    _load_env(env_file)
    log.info("[Fiber] Bootstrapping workspace and node identity...")

    if determine_execution_mode() == "USER":
        project_user_mode(Path.home() / ".anchor")

    from fiber.gateway.bridge.action.sign import ensure_node_keys
    import json
    info = ensure_node_keys(force_regenerate_cipher=rotate_cipher)

    ssh_msg = "newly created" if info["ssh_key_status"] == "CREATED" else "already exists (skipped)"
    cip_msg = "newly created" if info["local_cipher_status"] == "CREATED" else "already exists (skipped)"
    log.info(f"Node Cryptographic Identity Initialized (SSH: {ssh_msg}, Cipher: {cip_msg}):")
    print(json.dumps(info, indent=2, ensure_ascii=False))


@app.command("daemon")
def run_daemon(
    start: Annotated[str, typer.Option("--start", "-s", help="Daemons to start or topology preset (e.g., edge, all)")] = "edge",
    detach: Annotated[bool, typer.Option("--detach", "-d", help="Run daemon in the background and log to <log>/daemon.log")] = False,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Boots Fiber node daemons (:8356). Defaults to 'edge' (REST Edge + RPC + MCP Worker)."""
    if _is_port_active(DAEMON_PORT):
        log.info(f"🟢 [Fiber] Daemon is already running on http://127.0.0.1:{DAEMON_PORT}.")
        return

    if detach:
        log_dir = Path(str(resolve_path("log")))
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "daemon.log"
        cmd = [sys.executable, "-m", "fiber.phase.cli", "daemon", "-s", start]
        if env_file:
            cmd.extend(["-f", env_file])
        with open(log_file, "a", encoding="utf-8") as out:
            proc = subprocess.Popen(cmd, stdout=out, stderr=out, start_new_session=True)
        log.info(f"🚀 [Fiber] Background daemon started (PID: {proc.pid}) -> Log: {log_file}")
        return

    _load_env(env_file)
    PRESETS = {
        "edge": "rest_edge,rpc_worker,mcp_worker",
        "all": "rest_edge,rpc_worker,mcp_worker,dynamic_pricing,risk_vault",
    }

    resolved_daemons = PRESETS.get(start.lower(), start)
    daemons_list = [d.strip() for d in resolved_daemons.split(",")]

    os.environ["NODE_DAEMONS"] = resolved_daemons
    os.environ["GATEWAY_TOPOLOGY"] = "EMBEDDED_BYPASS"

    is_edge_only = all(d in ["rest_edge"] for d in daemons_list)
    if is_edge_only:
        os.environ["NODE_PROFILE"] = "EDGE"
    else:
        os.environ["NODE_PROFILE"] = "ALL"

    boot_kernel("Gateway Daemon")


@app.command("console")
def run_console(
    port: Annotated[int, typer.Option("--port", "-p", help="Port for Control Studio UI")] = CONSOLE_PORT,
    auto_daemon: Annotated[bool, typer.Option("--auto-daemon/--no-auto-daemon", help="Auto-spawn local daemon (:8356) if offline")] = True,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Launches Fiber Control Studio (:8357) and ensures the local gateway daemon (:8356) is active."""
    _load_env(env_file)
    from fiber.dev.console.app import launch_console
    launch_console(port=port, auto_daemon=auto_daemon, env_file=env_file)


@app.command("account", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run_account(
    ctx: typer.Context,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Manages local/tenant FinOps accounts, EdDSA JWT quotas, fuel allocate, and live Kill-Switches."""
    _load_env(env_file)
    from fiber.gateway.bridge.action.account import main as account_main
    try:
        account_main(ctx.args)
    except KeyboardInterrupt:
        log.info("\n[Fiber] Account operation manually terminated by user.")
    except Exception as e:
        log.error(f"[Fiber] Account command failed: {e}", exc_info=True)
        sys.exit(1)


@app.command("e2e", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run_e2e(
    ctx: typer.Context,
    target: Annotated[str, typer.Argument(help="Target test suite")],
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    _load_env(env_file)
    extra_args = ctx.args
    KNOWN_SUITES = [
        "llm.trace",
        "llm.guardrail",
        "gateway.compliance",
        "inter.wasm.phase",
        "inter.plane.flare",
        "inter.finops",
    ]
    targets = KNOWN_SUITES if target == "all" else [target]

    log.info(f"[Fiber] Igniting E2E Test Suite(s): {', '.join(targets)}")
    for t in targets:
        module_path = f"fiber.dev.e2e.{t}"
        try:
            test_module = importlib.import_module(module_path)
            if hasattr(test_module, "main"):
                log.info(f"\n{'='*60}\nLaunching Suite: {module_path}\n{'='*60}")
                sig = inspect.signature(test_module.main)
                if len(sig.parameters) > 0:
                    test_module.main(extra_args)
                else:
                    test_module.main()
            else:
                log.error(f"[Fiber] Module {module_path} lacks 'main'. Skipping.")
                continue
        except ImportError as e:
            log.error(f"[Fiber] Test module not found: {module_path} (Reason: {e})")
            if target != "all":
                sys.exit(1)
        except Exception as e:
            log.error(f"[Fiber] E2E Test {module_path} failed: {e}", exc_info=True)
            sys.exit(1)


@connect_app.callback()
def run_connector(
    ctx: typer.Context,
    exec_cmd: Annotated[
        Optional[str],
        typer.Option("--exec", "-e", help="Worker preset ('builtin' or alias), shell cmd, bin_root dir, or HTTP(S) URL"),
    ] = None,
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", exists=True, help="Path to standard mcp.json file to mount"),
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
        typer.Option("--preflight", "-p", help="Regex pattern to trigger Zero-Code HITL YIELD before worker execution"),
    ] = None,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """[Runtime Launcher] Sublimates local stdio commands, builtin workers, or remote HTTP(S) MCP servers."""
    if ctx.invoked_subcommand is not None:
        return
    _load_env(env_file)
    from fiber.gateway.bridge.action.connect import run_connect
    run_connect(
        exec_cmd=exec_cmd,
        config=config,
        target=target,
        mode=mode,
        preflight=preflight,
    )


@connect_app.command("copy")
def copy_connector_config(
    exec_cmd: Annotated[
        Optional[str],
        typer.Option("--exec", "-e", help="Worker preset ('builtin' or alias), shell cmd, or HTTP(S) URL to lock into ~/.anchor/mcp.json"),
    ] = None,
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", exists=True, help="External mcp.json file to copy/merge into ~/.anchor/mcp.json"),
    ] = None,
    target: Annotated[
        Optional[str],
        typer.Option("--target", "-t", help="Optional Target ID override"),
    ] = None,
    mode: Annotated[
        Optional[str],
        typer.Option("--mode", "-m", help="Optional mode override: ephemeral, linear, or multiplex"),
    ] = None,
    preflight: Annotated[
        Optional[str],
        typer.Option("--preflight", "-p", help="Regex pattern to lock as FIBER_PREFLIGHT_PATTERN in ~/.anchor/mcp.json"),
    ] = None,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """[Config Provisioner] Copies & locks builtin workers and external MCP configs into ~/.anchor/mcp.json without spawning processes."""
    _load_env(env_file)
    from fiber.gateway.bridge.action.connect import run_connect_copy
    run_connect_copy(
        exec_cmd=exec_cmd,
        config=config,
        target=target,
        mode=mode,
        preflight=preflight,
    )


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
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """
    @desc: Manages VCR fixtures for compatibility testing
    - Use '--vcr record' to generate raw payloads, or '--vcr replay' (with '--vcr-speed' / '--vcr-chaos') to verify extraction rules offline.
    """
    _load_env(env_file)
    mode = vcr.lower().strip()
    speed = vcr_speed.lower().strip()
    if speed not in ("max", "real"):
        log.error(f"[Fiber] Invalid --vcr-speed: '{vcr_speed}'. Supported values are 'max' or 'real'.")
        raise typer.Exit(1)

    if mode in ("record", "gen", "generate"):
        from fiber.gateway.bridge.action.compat import run_gen_fixture
        try:
            asyncio.run(
                run_gen_fixture(
                    model=model,
                    api_key=api_key,
                    target_dir=target_dir,
                    vcr_tick=vcr_tick,
                )
            )
        except KeyboardInterrupt:
            log.info("\n[Fiber] Compat fixture generation manually terminated by user.")
        except Exception as e:
            log.error(f"[Fiber] Fixture generation failed: {e}", exc_info=True)
            sys.exit(1)
    elif mode in ("replay", "test", "verify"):
        from fiber.gateway.bridge.action.compat import run_test_fixture
        try:
            asyncio.run(
                run_test_fixture(
                    model=model,
                    target_dir=target_dir,
                    vcr_speed=speed,
                    vcr_chaos=vcr_chaos,
                    vcr_targets=vcr_targets,
                )
            )
        except KeyboardInterrupt:
            log.info("\n[Fiber] Compat fixture testing manually terminated by user.")
        except Exception as e:
            log.error(f"[Fiber] Fixture testing failed: {e}", exc_info=True)
            sys.exit(1)
    else:
        log.error(f"[Fiber] Invalid --vcr mode: '{vcr}'. Supported modes are 'record' (gen) or 'replay' (test).")
        raise typer.Exit(1)


def main():
    app()


if __name__ == "__main__":
    main()