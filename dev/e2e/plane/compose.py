# fiber.dev.e2e.plane.compose
import os
import sys
import argparse
import logging
import asyncio
from pathlib import Path
from typing import Any, List, Dict

from fiber.infra.plane.compose import ComposeOrchestrator, ComposeContext

from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

FIBER_ROOT = resolve_path("fiber")
log = get_emitter("e2e.plane.compose")

class ComposeWorkflowScene:
    """
    Executes a pure Docker Compose runtime to cross-validate System E2E tests,
    ensuring topology binding, infrastructure (Redis), and execution logic are intact.
    """
    def __init__(self, broker: Any = None, context: ComposeContext = None):
        self.broker = broker
        self.context = context
        
        self.adapter = context.adapter if context else None
        self.auditors = context.auditors if context else {}
        
        self.fail_count = 0
        self.failed_cases: List[Dict[str, str]] = []
        self.log = get_emitter("scene.compose")

    async def phase_system_e2e_test(self):
        self.log.info("  ▶️ [TEST] System E2E Job (Infrastructure & Intent Validation)")
        e2e_bash_script = """
set -e
trap 'echo "[CI-SYNC] 🧹 Executing Kernel Reaper (Teardown)..."; python -m xphi.kernel.ops.reaper' EXIT

echo "[CI-SYNC] 1. Bootstrap Topology Boundary..."
python -m xphi.kernel.space.bind.around

echo "[CI-SYNC] 2. Booting Kernel (Background)..."
nohup python -u -m xphi.kernel.ops.boot > kernel_boot.log 2>&1 &

echo "[CI-SYNC] 3. Waiting for Kernel Healthcheck..."
timeout 30 bash -c 'while ! curl -s http://127.0.0.1:8000/v1/public/keys > /dev/null; do sleep 1; done' || { echo -e "\n🔥 KERNEL BOOT FAILED! DUMPING LOG: 🔥\n"; cat kernel_boot.log; exit 1; }
echo "✅ Kernel is fully up and running!"

echo "[CI-SYNC] 4. Executing Core E2E Client..."
python -m dev.e2e.edge.client

echo "[CI-SYNC] 5. Executing WASM & Flare E2E Suites..."
fiber e2e dphi.wasm.phase
VCR_MODE=replay python -m fiber.dev.ex.switch
fiber e2e plane.flare --mode dev
"""
        
        try:
            success = await self.adapter.apply_job(
                job_name="system-e2e-test", 
                env={
                    "XPHI_ENV": "ci",
                    "REDIS_URL": "redis://redis:6379/0",
                    "FIBER_E2E_STEPS": e2e_bash_script.strip()
                }
            )
            
            if not success:
                raise RuntimeError("System E2E job fractured. Check the streamed Docker Compose logs above for specific traceback.")
                
            self.log.info("  └─ System E2E Test Passed ✅ (Services & Binding OK)")
        except Exception as e:
            self.fail_count += 1
            self.failed_cases.append({"title": "System E2E Test Job", "error": str(e)})
            self.log.error(f"  [SCENARIO HALTED] System E2E Job: {e}")

    async def run_all(self):
        self.log.info("\n=== [START] Executing COMPOSE CI/CD Workflow Scenes ===")
        
        if not self.adapter:
            self.log.error("  └─ Compose Runtime Availability: Failed 🔴")
            self.fail_count += 1
            return

        self.log.info("  └─ Compose Runtime Availability: Confirmed 🟢")

        # 단일 E2E 테스트만 실행되도록 릴리즈 빌드 오딧 제거 완료
        await self.phase_system_e2e_test()
        
        if self.fail_count == 0:
            self.log.info("=== [DONE] All Workflow Scenes Passed Successfully ===")
        else:
            self.log.warning(f"=== [DONE] Workflow Scenes Completed with {self.fail_count} Failures ===")


class ComposeFlow:
    """CLI Control Plane for orchestrating Compose-based CI pipeline validations."""
    def __init__(self, mode: str = "dev", keep_workspace: bool = False, rebuild: bool = False):
        self.mode = mode
        self.keep_workspace = keep_workspace
        self.rebuild = rebuild

    async def test(self):
        self.log = log
        self.log.info(f"\n[PHASE 1] Initializing COMPOSE Orchestrator in [{self.mode.upper()}] mode")
        
        original_cwd = Path.cwd()
        if FIBER_ROOT:
            os.chdir(FIBER_ROOT)
            self.log.info(f"  └─ Workspace Context Switched to FIBER_ROOT: {FIBER_ROOT}")

        try:
            controller = ComposeOrchestrator(
                mode=self.mode,
                suites={"workflow_validation": ComposeWorkflowScene},
                rebuild=self.rebuild
            )
            controller.keep_workspace = self.keep_workspace
            
            success, err_msg = await controller.execute(broker=None)
            
            self.log.info("\n" + "="*75)
            self.log.info(f"🚀 COMPOSE CI/CD PIPELINE EXECUTION REPORT 🚀".center(75))
            self.log.info("="*75)
            
            if success:
                self.log.info(f"🟢 [SUCCESS] All Workflow Declarative Tests PASSED.")
                self.log.info("="*75 + "\n")
            else:
                self.log.critical(f"🔴 [FAILED] COMPOSE CI Test execution terminated with errors.")
                if err_msg:
                     self.log.error(f"Reason: {err_msg}")
                sys.exit(1)
                
        finally:
            os.chdir(original_cwd)

    async def run(self):
        await self.test()


def main(args_list: list[str] = None):
    parser = argparse.ArgumentParser(description="Fiber CI/CD E2E Orchestrator via DOCKER COMPOSE")
    parser.add_argument("--mode", choices=["dev", "deploy"], default="dev")
    parser.add_argument("--keep-workspace", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    
    args, _ = parser.parse_known_args(args_list)

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        log.info("🐛 [DEBUG MODE] Internal execution logging is ENABLED.")

    app = ComposeFlow(mode=args.mode, keep_workspace=args.keep_workspace, rebuild=args.rebuild)
    PhaseReactor.ignite(main_coro_func=app.run)

if __name__ == "__main__":
    main()