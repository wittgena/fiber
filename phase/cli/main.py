# fiber.phase.cli.main
import os
import sys
import asyncio
import inspect
import importlib
from typing import Annotated, Optional
import typer

try:
    import dotenv
except ImportError:
    dotenv = None

from fiber.phase.cli.sandbox import execute_vcr_logic

from xphi.kernel.ops.shell.entry import EcosystemShell
from xphi.state.phase.reactor import PhaseReactor
from xphi.kernel.ops.boot import main_async, teardown
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.tunnel.factory import TunnelFactory

log = get_emitter("fiber.cli")

app = typer.Typer(
    help="Fiber: The Universal Integration Boundary & OS Kernel",
    no_args_is_help=True,
    add_completion=False
)

# =========================================================================
# 코어 유틸리티
# =========================================================================
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

# =========================================================================
# 명령어 정의
# =========================================================================
@app.command("vcr", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run_vcr(
    ctx: typer.Context,
    target_script: Annotated[str, typer.Argument(help="Target legacy python script (e.g., target.py)")],
    mode: Annotated[str, typer.Option("--mode", "-m", help="VCR mode: live, record, or replay")] = "record",
    speed: Annotated[str, typer.Option("--speed", "-s", help="Replay speed: max or real")] = "real",
    chaos: Annotated[float, typer.Option("--chaos", "-c", help="Inject artificial latency jitter (ms)")] = 0.0,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Execute a legacy LiteLLM script safely inside the Fiber VCR Reality Distortion Field."""
    _load_env(env_file)
    execute_vcr_logic(ctx, target_script, mode, speed, chaos)

@app.command("daemon")
def run_daemon(
    start: Annotated[str, typer.Option("--start", "-s", help="Comma separated daemons to start")],
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    _load_env(env_file)
    os.environ["KERNEL_DAEMONS"] = start
    os.environ["GATEWAY_TOPOLOGY"] = "EMBEDDED_BYPASS"
    daemons = [d.strip() for d in start.split(",")]
    if all(d in ["gateway_edge", "rest_edge"] for d in daemons):
        os.environ["NODE_PROFILE"] = "EDGE"
    elif "risk_vault" in daemons or "rpc_worker" in daemons:
        os.environ["NODE_PROFILE"] = "COMPUTE"
    else:
        os.environ["NODE_PROFILE"] = "ALL"
    boot_kernel("Subordinate Daemon")

@app.command("trace")
def run_trace(
    target: Annotated[str, typer.Option("--target", "-t")],
    config: Annotated[Optional[str], typer.Option("--config", "-c")] = None,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    _load_env(env_file)
    os.environ["KERNEL_DAEMONS"] = "tracer_controller"
    os.environ["TRACE_TARGET"] = target
    os.environ["NODE_PROFILE"] = "CONTROL"
    if config:
        os.environ["TRACE_CONFIG_PATH"] = config
    boot_kernel("Master Hypervisor")

@app.command("deploy")
def run_deploy(
    topology: Annotated[str, typer.Option("--topology", "-t")] = "master",
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    _load_env(env_file)
    os.environ["KERNEL_DAEMONS"] = "topology_manager"
    os.environ["DEPLOY_TOPOLOGY"] = topology
    os.environ["NODE_PROFILE"] = "CONTROL"
    boot_kernel("Deployment Manager")

@app.command("shell")
def run_shell(
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    _load_env(env_file)
    async def _launch_console():
        tunnel = await TunnelFactory.get_default()
        shell = EcosystemShell(tunnel)
        try:
            await shell.run()
        finally:
            await shell.manifold.close()
            await tunnel.close()
            log.info("[Fiber] System resources released.")
    try:
        asyncio.run(_launch_console())
    except KeyboardInterrupt:
        log.info("\n[Fiber] 👋 Exiting Console...")

@app.command("e2e", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run_e2e(
    ctx: typer.Context,
    target: Annotated[str, typer.Argument(help="Target test suite")],
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    _load_env(env_file)
    extra_args = ctx.args 
    KNOWN_SUITES = ["llm.trace", "edge.compliance", "dphi.clearing", "dphi.wasm.entry", "plane.flare"]
    targets = KNOWN_SUITES if target == "all" else [target]
    
    log.info(f"[Fiber] 🧪 Igniting E2E Test Suite(s): {', '.join(targets)}")
    for t in targets:
        module_path = f"fiber.dev.e2e.{t}"
        try:
            test_module = importlib.import_module(module_path)
            if hasattr(test_module, "main"):
                log.info(f"\n{'='*60}\n▶️ Launching Suite: {module_path}\n{'='*60}")
                sig = inspect.signature(test_module.main)
                if len(sig.parameters) > 0:
                    test_module.main(extra_args)
                else:
                    test_module.main()
            else:
                log.error(f"[Fiber] ❌ Module {module_path} lacks 'main'. Skipping.")
                continue
        except ImportError as e:
            log.error(f"[Fiber] ❌ Test module not found: {module_path} (Reason: {e})")
            if target != "all": sys.exit(1)
        except Exception as e:
            log.error(f"[Fiber] 💥 E2E Test {module_path} failed: {e}", exc_info=True)
            sys.exit(1)

@app.command("connect")
def run_connector(
    target: Annotated[str, typer.Option("--target", "-t")],
    exec_cmd: Annotated[str, typer.Option("--exec", "-e")],
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    _load_env(env_file)
    import fiber.dphi.worker.legacy.sandbox as agent_deploy
    import fiber.dphi.worker.mcp.oracle as agent_oracle

    KNOWN_AGENTS = {
        "agent.deploy": f"{sys.executable} -m {agent_deploy.__name__}",
        "agent.oracle": f"{sys.executable} -m {agent_oracle.__name__}"
    }
    resolved_cmd = KNOWN_AGENTS.get(exec_cmd, exec_cmd)

    async def _launch_connector():
        from fiber.dphi.worker.connector import WorkerConnector
        log.info(f"[Fiber] 🔌 Sublimating legacy server [{target}] into the A2A network...")
        daemon = WorkerConnector(target_id=target, execution_target=resolved_cmd)
        try:
            await daemon.run()
        finally:
            log.info("[Fiber] Disconnected from A2A Network.")
    try:
        asyncio.run(_launch_connector())
    except KeyboardInterrupt:
        log.info("\n[Fiber] 👋 Connector shutting down...")

def main():
    app()

if __name__ == "__main__":
    main()