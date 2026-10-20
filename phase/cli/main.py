# fiber.phase.cli.main
import os
import sys
import asyncio
import inspect
import importlib
from pathlib import Path
from typing import Annotated, Optional
import typer

try:
    import dotenv
except ImportError:
    dotenv = None

from xphi.state.phase.reactor import PhaseReactor
from xphi.node.ops.boot import main_async, teardown
from xphi.watcher.plane.emitter import get_emitter
from xphi.node.space.tunnel.factory import TunnelFactory
from fiber.phase.cli.sign import app as sign_app

log = get_emitter("phase.cli.main")

app = typer.Typer(
    help="Fiber: The Universal Integration Boundary & OS Kernel",
    no_args_is_help=True,
    add_completion=False
)

compat_app = typer.Typer(
    help="Manage LLM compatibility rules and test fixtures (fiber-compats)",
    no_args_is_help=True
)
app.add_typer(compat_app, name="compat")
app.add_typer(sign_app, name="sign")


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


@app.command("daemon")
def run_daemon(
    start: Annotated[str, typer.Option("--start", "-s", help="Daemons to start or topology preset (e.g., core, eco)")] = "core",
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Boots Fiber node daemons. Defaults to 'core' (Edge + RPC) for standalone operation."""
    _load_env(env_file)
    PRESETS = {
        "edge": "rest_edge,rpc_worker,mcp_worker",
        "ALL": "rest_edge,rpc_worker,mcp_worker,dynamic_pricing,risk_vault",
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


@app.command("account", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run_account(
    ctx: typer.Context,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Manages local/tenant FinOps accounts, EdDSA JWT quotas, fuel allocate, and live Kill-Switches."""
    _load_env(env_file)
    from fiber.phase.cli.account import main as account_main
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
    KNOWN_SUITES = ["llm.trace", "gateway.compliance", "inter.wasm.phase", "inter.plane.flare", "inter.finops"]
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


@app.command("connect")
def run_connector(
    exec_cmd: Annotated[Optional[str], typer.Option("--exec", "-e", help="Worker preset, shell cmd, bin_root dir, or HTTP(S) URL")] = None,
    config: Annotated[Optional[Path], typer.Option("--config", "-c", exists=True, help="Path to standard mcp.json file")] = None,
    target: Annotated[Optional[str], typer.Option("--target", "-t", help="Optional Target ID override (auto-inferred if omitted)")] = None,
    mode: Annotated[Optional[str], typer.Option("--mode", "-m", help="Optional mode override: ephemeral, linear, or multiplex")] = None,
    preflight: Annotated[Optional[str], typer.Option("--preflight", "-p", help="Regex pattern to trigger Zero-Code HITL YIELD before worker execution")] = None,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Sublimates local stdio commands, managed JVM binaries, or local/remote HTTP(S) MCP servers"""
    _load_env(env_file)
    from fiber.phase.cli.connect import run_connect
    run_connect(exec_cmd=exec_cmd, config=config, target=target, mode=mode, preflight=preflight)


@compat_app.command("fixture")
def manage_compat_fixtures(
    model: Annotated[str, typer.Option("--model", "-m", help="Model name (e.g., ollama/gemma:2b)")],
    vcr: Annotated[str, typer.Option("--vcr", "-v", help="Mode: 'record' (gen) or 'replay' (test)")] = "record",
    api_key: Annotated[Optional[str], typer.Option("--api-key", "-k", envvar="COMPAT_API_KEY", help="API Key (Optional if set in Env)")] = None,
    target_dir: Annotated[Optional[str], typer.Option("--target-dir", "-o", help="Custom target directory for fixtures")] = None,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """
    @desc: Manages VCR fixtures for compatibility testing
    - Use '--vcr record' to generate raw payloads, or '--vcr replay' to verify extraction rules offline.
    """
    _load_env(env_file)
    mode = vcr.lower()
    if mode in ("record", "gen", "generate"):
        from fiber.phase.cli.compat import run_gen_fixture
        try:
            asyncio.run(run_gen_fixture(model=model, api_key=api_key, target_dir=target_dir))
        except KeyboardInterrupt:
            log.info("\n[Fiber] Compat fixture generation manually terminated by user.")
        except Exception as e:
            log.error(f"[Fiber] Fixture generation failed: {e}", exc_info=True)
            sys.exit(1)
    elif mode in ("replay", "test", "verify"):
        from fiber.phase.cli.compat import run_test_fixture
        try:
            asyncio.run(run_test_fixture(model=model, target_dir=target_dir))
        except KeyboardInterrupt:
            log.info("\n[Fiber] Compat fixture testing manually terminated by user.")
        except Exception as e:
            log.error(f"[Fiber] Fixture testing failed: {e}", exc_info=True)
            sys.exit(1)
    else:
        log.error(f"[Fiber] Invalid VCR mode: '{vcr}'. Supported modes are 'record' (gen) or 'replay' (test).")
        raise typer.Exit(1)


def main():
    app()


if __name__ == "__main__":
    main()