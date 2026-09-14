# fiber.dev.e2e.plane.act
import os
import sys
import argparse
import logging
import asyncio
from pathlib import Path
from typing import Any, List, Dict

from xphi.watcher.plane.phase.act import ActOrchestrator, ActContext
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

FIBER_ROOT = resolve_path("fiber")
log = get_emitter("e2e.plane.act")

# =====================================================================
# 1. CI/CD Scene (System E2E & Release Auditor)
# =====================================================================

class ActWorkflowScene:
    """
    @desc: 실제 GitHub Actions 환경과 동일하게 nektos/act 런타임을 구동하여,
    빌드, 의존성 바인딩(XPHI), 그리고 백그라운드 인프라(Redis 등)가 연동된 
    System E2E 테스트가 성공적으로 완수되는지 횡단 검증하는 Scene.
    """
    def __init__(self, broker: Any = None, context: ActContext = None):
        self.broker = broker
        self.context = context
        
        self.adapter = context.adapter if context else None
        self.auditors = context.auditors if context else {}
        
        self.fail_count = 0
        self.failed_cases: List[Dict[str, str]] = []
        self.log = get_emitter("scene.act")

    async def phase_system_e2e_test(self):
        """
        [PHASE 1: System E2E & Remote Binding] 
        단순 패키지 설치를 넘어, 실제 Redis 사이드카 컨테이너가 뜬 상태에서
        Fiber <-> XPHI 간의 상태 전이 및 인텐트 제어가 정상 작동하는지 통합 검증합니다.
        """
        self.log.info("  ▶️ [TEST] System E2E Job (Infrastructure & Intent Validation)")
        
        try:
            # 새로 설계된 build.yml의 'system-e2e-test' Job을 타겟팅
            success = await self.adapter.apply_job(
                job_name="system-e2e-test", 
                env={}
            )
            
            if not success:
                raise RuntimeError("System E2E job fractured. Check act logs for background service or test failures.")
                
            self.log.info("  └─ System E2E Test Passed ✅ (Services & Binding OK)")
                
        except Exception as e:
            self.fail_count += 1
            self.failed_cases.append({"title": "System E2E Test Job", "error": str(e)})
            self.log.error(f"  [SCENARIO HALTED] System E2E Job: {e}")

    async def phase_build_release_audit(self):
        """
        [PHASE 2: Deterministic Release Build]
        E2E 테스트를 통과한 코드에 대해 배포용 빌드(Release Build)를 실행하고, 
        산출물(.whl) 내부에 로컬 경로가 오염되지 않았는지 사후 감사 수행.
        """
        self.log.info("  ▶️ [TEST] Release Build Job (Deterministic Remote Enforcement)")
        
        try:
            # FIBER_BUILD_DIST=1 을 주입하여 강제로 원격 Git URL 바인딩 유도
            success = await self.adapter.apply_job(
                job_name="build-release", 
                env={"FIBER_BUILD_DIST": "1"}
            )
            
            if not success:
                raise RuntimeError("Release Build job fractured.")
            
            # [Cross-Validation] Auditor를 이용한 배포 아티팩트 물리적 무결성 단언
            if "determinism" in self.auditors:
                auditor = self.auditors["determinism"]
                is_clean = await auditor.verify()
                
                if not is_clean:
                    msg = "FATAL: Artifact Determinism check failed! Local paths detected."
                    self.log.error(f"  [FATAL_RUPTURE] {msg}")
                    raise RuntimeError(msg)
                else:
                    self.log.info("  └─ Artifact Boundary Intact: Wheel determinism mathematically verified ✅")
                    
        except Exception as e:
            self.fail_count += 1
            self.failed_cases.append({"title": "Release Build & Audit Job", "error": str(e)})
            self.log.error(f"  [SCENARIO HALTED] Release Build Job: {e}")

    async def run_all(self):
        self.log.info("\n=== [START] Executing ACT CI/CD Workflow Scenes ===")
        
        if not self.adapter:
            self.log.error("  └─ ACT Runtime Availability: Failed 🔴 (Adapter missing)")
            self.fail_count += 1
            self.failed_cases.append({"title": "ACT Runtime Check", "error": "NektosActAdapter is not initialized."})
            return

        self.log.info("  └─ ACT Runtime Availability: Confirmed 🟢")

        # 2. 독립된 CI/CD 시나리오 순차 실행
        await self.phase_system_e2e_test()
        await self.phase_build_release_audit()
        
        # 3. 결과 정리
        if self.fail_count == 0:
            self.log.info("=== [DONE] All Workflow Scenes Passed Successfully ===")
        else:
            self.log.warning(f"=== [DONE] Workflow Scenes Completed with {self.fail_count} Failures ===")


# =====================================================================
# 2. Act Orchestrator Flow (E2E Runner)
# =====================================================================

class ActFlow:
    """
    @desc: CLI 기반으로 CI 파이프라인 E2E 검증기를 구동하고 레포팅하는 컨트롤 플레인.
    """
    def __init__(self, mode: str = "dev", keep_workspace: bool = False, rebuild: bool = False):
        self.mode = mode
        self.keep_workspace = keep_workspace
        self.rebuild = rebuild

    async def test(self):
        log.info(f"\n[PHASE 1] Initializing ACT Orchestrator in [{self.mode.upper()}] mode")
        
        original_cwd = Path.cwd()
        if FIBER_ROOT:
            os.chdir(FIBER_ROOT)
            log.info(f"  └─ Workspace Context Switched to FIBER_ROOT: {FIBER_ROOT}")
        else:
            log.warning("  └─ FIBER_ROOT not resolved. Falling back to current directory.")

        try:
            broker = None 

            controller = ActOrchestrator(
                mode=self.mode,
                suites={"workflow_validation": ActWorkflowScene},
                rebuild=self.rebuild
            )
            controller.keep_workspace = self.keep_workspace
            
            success, err_msg = await controller.execute(broker=broker)
            
            log.info("\n" + "="*75)
            log.info(f"🚀 ACT CI/CD PIPELINE EXECUTION REPORT 🚀".center(75))
            log.info("="*75)
            
            if success:
                log.info(f"🟢 [SUCCESS] All Workflow Declarative Tests PASSED.")
                log.info("="*75 + "\n")
            else:
                log.critical(f"🔴 [FAILED] ACT CI Test execution terminated with errors.")
                for line in err_msg.split('\n'):
                    if line.strip():
                        log.critical(f"📝 {line}")
                    
                if hasattr(controller, 'suite_runners') and controller.suite_runners:
                    log.info("\n" + "🔥"*37)
                    log.info("🚨 DETAILED CI WORKFLOW FAILURE TRACES 🚨".center(75))
                    log.info("🔥"*37)
                    
                    for suite_name, runner in controller.suite_runners.items():
                        fail_cnt = getattr(runner, 'fail_count', 0)
                        failed_cases = getattr(runner, 'failed_cases', [])
                        
                        if fail_cnt > 0 or failed_cases:
                            log.info(f"\n❌ [SUITE: {suite_name.upper()}] ➔ {fail_cnt} Test(s) Failed")
                            for idx, fc in enumerate(failed_cases, 1):
                                title = fc.get('title', 'Unknown Test Case')
                                err = fc.get('error', 'No error details provided')
                                log.info(f"  └─ {idx}. {title}")
                                log.info(f"     [Reason] {err}\n")
                    log.info("="*75 + "\n")
                sys.exit(1)
                
        finally:
            os.chdir(original_cwd)

    async def run(self):
        await self.test()


# =====================================================================
# 3. Standard Entrypoint
# =====================================================================

def main(args_list: list[str] = None):
    parser = argparse.ArgumentParser(description="Fiber CI/CD (GitHub Actions) E2E Orchestrator via ACT")
    parser.add_argument("--mode", choices=["dev", "deploy"], default="dev", help="Execution mode")
    parser.add_argument("--keep-workspace", action="store_true", help="Preserve artifact temp directory after test")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    parser.add_argument("--rebuild", action="store_true", help="Force act to rebuild container and clear cache")
    
    args, _ = parser.parse_known_args(args_list)

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        log.info("🐛 [DEBUG MODE] Internal execution logging is ENABLED.")

    app = ActFlow(
        mode=args.mode,
        keep_workspace=args.keep_workspace,
        rebuild=args.rebuild
    )
    PhaseReactor.ignite(main_coro_func=app.run)

if __name__ == "__main__":
    main()