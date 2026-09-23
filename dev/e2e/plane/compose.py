# fiber.dev.e2e.plane.compose
import os
import sys
import argparse
import logging
import asyncio
from pathlib import Path
from typing import Any, List, Dict

from fiber.dev.infra.plane.compose import ComposeOrchestrator, ComposeContext

from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

FIBER_ROOT = resolve_path("fiber")
log = get_emitter("e2e.plane.compose")

DEFAULT_E2E_SUITE = [
    "VCR_MODE=replay python -m fiber.dev.ex.switch",
    "fiber e2e dphi.wasm.phase"
]

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
        
        try:
            success = await self.adapter.apply_job(
                job_name="system-e2e-test", 
                env={
                    "XPHI_ENV": "ci",
                    "REDIS_URL": "redis://redis:6379/0",
                    "FIBER_E2E_STEPS": " && ".join(DEFAULT_E2E_SUITE) 
                }
            )
            
            if not success:
                # [개선] 실패 시 어디를 봐야 할지 명확히 안내
                raise RuntimeError("System E2E job fractured. Check the streamed Docker Compose logs above for specific traceback.")
                
            self.log.info("  └─ System E2E Test Passed ✅ (Services & Binding OK)")
                
        except Exception as e:
            self.fail_count += 1
            self.failed_cases.append({"title": "System E2E Test Job", "error": str(e)})
            self.log.error(f"  [SCENARIO HALTED] System E2E Job: {e}")

    async def phase_build_release_audit(self):
        self.log.info("  ▶️ [TEST] Release Build Job (Deterministic Remote Enforcement)")
        try:
            success = await self.adapter.apply_job(
                job_name="build-release", 
                env={"FIBER_BUILD_DIST": "1"}
            )
            
            if not success:
                raise RuntimeError("Release Build job fractured. Check the build logs.")
            
            if "determinism" in self.auditors:
                auditor = self.auditors["determinism"]
                is_clean = await auditor.verify()
                
                if not is_clean:
                    msg = "FATAL: Artifact Determinism check failed! Local paths detected."
                    self.log.error(f"  [FATAL_RUPTURE] {msg}")
                    raise RuntimeError(msg)
                else:
                    self.log.info("  └─ Artifact Boundary Intact: Wheel determinism verified ✅")
                    
        except Exception as e:
            self.fail_count += 1
            self.failed_cases.append({"title": "Release Build & Audit Job", "error": str(e)})
            self.log.error(f"  [SCENARIO HALTED] Release Build Job: {e}")

    async def run_all(self):
        self.log.info("\n=== [START] Executing COMPOSE CI/CD Workflow Scenes ===")
        
        if not self.adapter:
            self.log.error("  └─ Compose Runtime Availability: Failed 🔴")
            self.fail_count += 1
            return

        self.log.info("  └─ Compose Runtime Availability: Confirmed 🟢")

        await self.phase_system_e2e_test()
        await self.phase_build_release_audit()
        
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
        log.info(f"\n[PHASE 1] Initializing COMPOSE Orchestrator in [{self.mode.upper()}] mode")
        
        original_cwd = Path.cwd()
        if FIBER_ROOT:
            os.chdir(FIBER_ROOT)
            log.info(f"  └─ Workspace Context Switched to FIBER_ROOT: {FIBER_ROOT}")

        try:
            controller = ComposeOrchestrator(
                mode=self.mode,
                suites={"workflow_validation": ComposeWorkflowScene},
                rebuild=self.rebuild
            )
            controller.keep_workspace = self.keep_workspace
            
            success, err_msg = await controller.execute(broker=None)
            
            log.info("\n" + "="*75)
            log.info(f"🚀 COMPOSE CI/CD PIPELINE EXECUTION REPORT 🚀".center(75))
            log.info("="*75)
            
            if success:
                log.info(f"🟢 [SUCCESS] All Workflow Declarative Tests PASSED.")
                log.info("="*75 + "\n")
            else:
                log.critical(f"🔴 [FAILED] COMPOSE CI Test execution terminated with errors.")
                # [개선] 실패 원인 요약을 출력
                if err_msg:
                     log.error(f"Reason: {err_msg}")
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