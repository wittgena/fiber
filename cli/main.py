# fiber.cli.main
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

from fiber.phase.kernel.shell.entry import EcosystemShell

from xphi.kernel.phase.reactor import PhaseReactor
from xphi.kernel.ops.boot import main_async, teardown
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory

log = get_emitter("fiber.cli")

# --- Typer App Setup ---
app = typer.Typer(
    help="Fiber: The Universal Integration Boundary & OS Kernel",
    no_args_is_help=True,
    add_completion=False
)

def _load_env(env_file: Optional[str]):
    """환경변수 파일(.env)을 안전하게 시스템에 주입하여 런타임 Context 구성"""
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
    """지정된 환경변수를 쥐고 커널을 붕괴(Collapse)시켜 PhaseReactor를 점화 (Server/Host 역할)"""
    log.info(f"[Fiber] Igniting Kernel in {mode_name.upper()} mode...")
    try:
        PhaseReactor.ignite(main_coro_func=main_async, teardown_hook=teardown)
    except KeyboardInterrupt:
        log.info("\n[Fiber] Manual interrupt (SIGINT) received. Safely collapsing kernel...")
    except Exception as e:
        log.error(f"[Fiber] FATAL Kernel panic: {e}", exc_info=True)
        sys.exit(1)


# =========================================================
# 1. The Subordinate (Kube/Edge 종속 데몬 모드)
# =========================================================
@app.command("daemon")
def run_daemon(
    start: Annotated[str, typer.Option("--start", "-s", help="Comma separated daemons to start (e.g., rest_edge,gateway_edge)")],
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True, help="Path to .env file")] = None,
):
    """Run as a subordinate edge node (K8s/Docker container)."""
    _load_env(env_file)
    os.environ["KERNEL_DAEMONS"] = start
    os.environ["GATEWAY_TOPOLOGY"] = "EMBEDDED_BYPASS"
    
    # [정밀 정렬] 쉼표로 분리하여 정확한 데몬 매칭 수행
    daemons = [d.strip() for d in start.split(",")]
    
    if all(d in ["gateway_edge", "rest_edge"] for d in daemons):
        # 순수 네트워크/프록시 엣지인 경우에만 무거운 WASM 워커를 띄우지 않는 EDGE 프로파일 적용
        os.environ["NODE_PROFILE"] = "EDGE"
    elif "risk_vault" in daemons or "rpc_worker" in daemons:
        # 연산 및 자율 에이전트가 포함된 경우 COMPUTE 프로파일 강제 (네트워크 데몬 무시)
        os.environ["NODE_PROFILE"] = "COMPUTE"
    else:
        # 그 외의 복합 구성일 경우 기본 풀노드(ALL)로 동작
        os.environ["NODE_PROFILE"] = "ALL"

    boot_kernel("Subordinate Daemon")


# =========================================================
# 2. The Master Hypervisor (통제 및 관측 샌드박스)
# =========================================================
@app.command("trace")
def run_trace(
    target: Annotated[str, typer.Option("--target", "-t", help="Target tracer to execute (e.g., repro_worker, oom_tracer)")],
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Optional path to custom trace manifest")] = None,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Run the Flare Controller & Sandboxed infrastructure testing."""
    _load_env(env_file)
    
    # [정밀 정렬] verse 분리 및 순수 트레이서 컨트롤러만 격리 구동
    os.environ["KERNEL_DAEMONS"] = "tracer_controller"
    os.environ["TRACE_TARGET"] = target
    os.environ["NODE_PROFILE"] = "CONTROL" # 제어 및 관측 전용 경량 프로파일 적용
    
    if config:
        os.environ["TRACE_CONFIG_PATH"] = config

    boot_kernel("Master Hypervisor")


# =========================================================
# 3. The Deployment Manager (인프라 제어/오케스트레이션)
# =========================================================
@app.command("deploy")
def run_deploy(
    topology: Annotated[str, typer.Option("--topology", "-t", help="Target cluster topology")] = "master",
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Manage multi-node orchestration and Kube/Edge scaling."""
    _load_env(env_file)
    os.environ["KERNEL_DAEMONS"] = "topology_manager"
    os.environ["DEPLOY_TOPOLOGY"] = topology
    os.environ["NODE_PROFILE"] = "CONTROL"
    boot_kernel("Deployment Manager")


# =========================================================
# 4. The Client Console (God-Mode 접속용 쉘)
# =========================================================
@app.command("shell")
def run_shell(
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Launch the Interactive Ecosystem Observatory (Connect to existing Kernel)."""
    _load_env(env_file)
    
    async def _launch_console():
        # PhaseReactor 부팅 없이, 순수하게 Tunnel(분산망)에만 클라이언트로 접속
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


# =========================================================
# 5. The Integration Tester (E2E Test Runner)
# =========================================================
@app.command("e2e", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run_e2e(
    ctx: typer.Context,  # Typer 컨텍스트 (수집된 extra_args 포함)
    target: Annotated[str, typer.Argument(help="Target test suite (e.g., defin, eco, edge, flare, llm.compat, all)")],
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True)] = None,
):
    """Run End-to-End integration tests for specific domains."""
    _load_env(env_file)
    extra_args = ctx.args 
    KNOWN_SUITES = ["defin", "eco", "edge", "flare", "wasm.entry", "llm.compat"]
    targets = KNOWN_SUITES if target == "all" else [target]
    
    log.info(f"[Fiber] 🧪 Igniting E2E Test Suite(s): {', '.join(targets)}")
    if extra_args:
        log.info(f"[Fiber] ↪ Forwarding extra arguments to suite: {' '.join(extra_args)}")
    
    for t in targets:
        module_path = f"fiber.phase.e2e.{t}"
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
                log.error(f"[Fiber] ❌ Module {module_path} lacks a standard 'main' entrypoint. Skipping.")
                continue
        except ImportError:
            log.error(f"[Fiber] ❌ Test module not found: {module_path}")
            if target != "all":
                sys.exit(1)
        except Exception as e:
            log.error(f"[Fiber] 💥 E2E Test {module_path} failed: {e}", exc_info=True)
            sys.exit(1)

def main():
    app()

if __name__ == "__main__":
    main()