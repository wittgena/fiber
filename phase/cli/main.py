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

from fiber.phase.cli.observer import run_observer

from xphi.kernel.ops.shell import ShellEntry
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
    
    # 1. Expand topology presets into explicit daemon lists
    PRESETS = {
        "core": "rest_edge,rpc_worker",                           # Base MCP Bridge
        "eco": "rest_edge,rpc_worker,dynamic_pricing",            # Bridge + Pricing
        "full": "rest_edge,rpc_worker,dynamic_pricing,risk_vault",# Entire Eco-system
        "edge_only": "rest_edge",                                 # API Ingress only
        "compute_only": "rpc_worker"                              # Headless RPC only
    }
    
    resolved_daemons = PRESETS.get(start.lower(), start)
    daemons_list = [d.strip() for d in resolved_daemons.split(",")]
    
    os.environ["KERNEL_DAEMONS"] = resolved_daemons
    os.environ["GATEWAY_TOPOLOGY"] = "EMBEDDED_BYPASS"
    
    # 2. Simplified NODE_PROFILE routing: EDGE (lightweight) vs ALL (spawns workers)
    is_edge_only = all(d in ["gateway_edge", "rest_edge"] for d in daemons_list)
    
    if is_edge_only:
        os.environ["NODE_PROFILE"] = "EDGE"
    else:
        os.environ["NODE_PROFILE"] = "ALL"
        
    boot_kernel("Gateway Daemon")

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
        shell = ShellEntry(tunnel)
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
    import fiber.gateway.node.worker.legacy.sandbox as agent_deploy
    import fiber.gateway.node.worker.mcp.oracle as agent_oracle

    KNOWN_AGENTS = {
        "agent.deploy": f"{sys.executable} -m {agent_deploy.__name__}",
        "agent.oracle": f"{sys.executable} -m {agent_oracle.__name__}"
    }
    resolved_cmd = KNOWN_AGENTS.get(exec_cmd, exec_cmd)

    async def _launch_connector():
        from fiber.gateway.node.worker.connector import WorkerConnector
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

@app.command("observe")
def start_observer(
    target: Annotated[str, typer.Option("--target", "-t", help="Observation target or scenario name (e.g., kube, kube_oom)")] = "kube",
    namespace: Annotated[str, typer.Option("--namespace", "-n", help="Namespace to observe")] = "fiber-topos",
    delay: Annotated[int, typer.Option("--delay", "-d", help="Polling interval in seconds")] = 5,
    chaos: Annotated[bool, typer.Option("--chaos", "-c", help="Enable Active Chaos Injection")] = False,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Starts a real-time observer daemon to stream infrastructure status, optionally injecting chaos."""
    _load_env(env_file)
    try:
        asyncio.run(run_observer(target=target, namespace=namespace, delay=delay, chaos=chaos))
    except KeyboardInterrupt:
        log.info("\n[Fiber] 👋 Observer manually terminated by user.")

def main():
    app()

if __name__ == "__main__":
    main()