# fiber.dev.e2e.dphi.wasm.entry
import sys
import argparse
import importlib
from dataclasses import dataclass, field
from typing import List, Dict

import fiber.dev.e2e.dphi.scene as scene_module

from xphi.arch.dev.wasm.builder import WasmBuilder
from xphi.arch.dev.wasm.tester import WasmTester
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("wasm.entry")

MODULE_PATH = scene_module.__name__

@dataclass
class PipelineConfig:
    suites_registry: Dict[str, str] = field(default_factory=lambda: {
        "sandbox": f"{MODULE_PATH}.sandbox:SandboxScene",
        "anchor": f"{MODULE_PATH}.eco:AnchorScene",
        "eco": f"{MODULE_PATH}.eco:EcoScene",
        "cert": f"{MODULE_PATH}.sandbox:CertProofScene",
        "dynamics": f"{MODULE_PATH}.sandbox:DynamicsScene", 
    })
    
    default_suites: List[str] = field(default_factory=lambda: [
        "sandbox",     # 1. 런타임 보안 및 단일 샌드박스 격리 검증 (L1)
        "anchor",      # 2. 영지식 증명, 다중 서명, 탈중앙 합의 로직 검증 (L3)
        "cert",        # 3. 극한 환경 엣지 케이스 방어 및 무결성 최종 인증 (L4)
        "dynamics",    # 4. [NEW] O(N^2) 수학 연산 무결성 및 성능, 위상 변이 검증 (L2)
    ])
    wasm_filename: str = "dphi.wasm"


class DphiFlow:
    """
    @role: WASM Pipeline Orchestrator (Build & Test)
    @desc: WASM 빌드를 수행하고, WasmTester를 통해 In-process(로컬 격리) 환경에서 
           명확하고 추적 가능한 엔드투엔드 통합 테스트를 수행합니다.
    """
    def __init__(self, should_build: bool = False, suites: List[str] = None, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.should_build = should_build
        
        if not suites or suites == ["all"]:
            self.suites = self.config.default_suites
        else:
            self.suites = suites
        
        self.log = get_emitter("wasm.entry")
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / self.config.wasm_filename

    def _resolve_suite_class(self, suite_name_or_path: str):
        module_path_str = self.config.suites_registry.get(suite_name_or_path, suite_name_or_path)
        try:
            if ":" not in module_path_str:
                raise ValueError(f"Invalid suite path format '{module_path_str}'. Expected 'module.path:ClassName'")

            mod_name, cls_name = module_path_str.split(":")
            module = importlib.import_module(mod_name)
            return getattr(module, cls_name)
        except Exception as e:
            self.log.error(f"[CLI] Failed to dynamically load suite '{suite_name_or_path}': {e}")
            return None # 예외 발생 시 None을 반환하여 상위에서 처리하도록 위임

    async def build(self) -> bool:
        """WASM 바이너리를 빌드합니다. 성공 시 True, 실패 시 False 반환."""
        self.log.info("\n[CLI] Starting standalone WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        
        if builder.rupture_confirmed:
            self.log.error("❌ [CLI] Builder encountered a fatal rupture.")
            return False
            
        self.log.info("✅ [CLI] Builder completed successfully.")
        return True

    async def test(self) -> bool:
        """로컬 격리 테스트를 수행합니다. 성공 시 True, 실패 시 False 반환."""
        self.log.info(f"\n[CLI] Starting Isolated WasmTester for suites: {self.suites}...")
        
        if not self.dest_wasm_file.exists():
            self.log.error(f"❌ [CLI] Missing WASM binary at {self.dest_wasm_file}. Please run with '--build' option or pre-compile it.")
            return False
            
        suite_map = {}
        for suite_name in self.suites:
            if suite_name not in self.config.suites_registry:
                self.log.warning(f"[CLI] Unknown suite '{suite_name}', skipping...")
                continue
                
            resolved_class = self._resolve_suite_class(suite_name)
            if not resolved_class:
                return False
            suite_map[suite_name] = resolved_class
            
        if not suite_map:
            self.log.error("❌ [CLI] No valid test suites found to execute.")
            return False

        tester = WasmTester(
            wasm_module_path=str(self.dest_wasm_file),
            sandbox_root=str(self.time_root),
            suites=suite_map
        )
        
        success, err_msg = await tester.execute()
        
        log.info("\n" + "="*75)
        log.info("🚀 PIPELINE EXECUTION REPORT".center(75))
        log.info("="*75)
        
        if success:
            self.log.info(f"🟢 [SUCCESS] All Test Suites ({', '.join(suite_map.keys())}) PASSED.")
            if getattr(tester, 'test_execution_hash', None):
                self.log.info(f"🔗 Execution Canonical Hash: {tester.test_execution_hash}")
            log.info("="*75 + "\n")
            return True
        else:
            self.log.critical(f"🔴 [FAILED] Test execution terminated with errors.")
            self.log.critical(f"📝 [SUMMARY] {err_msg}")
            
            if hasattr(tester, 'suite_runners') and tester.suite_runners:
                log.info("\n" + "🔥"*37)
                log.info("🚨 DETAILED FAILURE TRACES 🚨".center(75))
                log.info("🔥"*37)
                
                for suite_name, runner in tester.suite_runners.items():
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
            return False

    async def run(self):
        """최상위 실행 파이프라인. 논리적 흐름을 제어하고 실패 시 시스템 종료 코드를 반환합니다."""
        try:
            # 1. 빌드 옵션이 활성화된 경우에만 빌드 수행
            if self.should_build:
                if not await self.build():
                    sys.exit(1)
                    
            # 2. 테스트 수행
            if not await self.test():
                sys.exit(1)
                
        except SystemExit:
            raise  # 정상적인 파이프라인 실패로 인한 종료는 통과시킴
        except Exception as e:
            self.log.error(f"💥 [FATAL] Unhandled exception in E2E runner: {e}", exc_info=True)
            sys.exit(1)


def main(args_list: list[str] = None):
    parser = argparse.ArgumentParser(description="WASM Distributed Sandbox & Autonomous Agent CLI (Isolated CI)")
    parser.add_argument("--suites", nargs="+", default=["all"], help="List of suites to run (e.g. sandbox, anchor, cert, dynamics)")
    parser.add_argument("--build", action="store_true", help="Force compile the Rust WASM artifact before testing.")

    args, _ = parser.parse_known_args(args_list)
    suites = getattr(args, "suites", ["all"])
    should_build = getattr(args, "build", False)
    
    config = PipelineConfig()
    app = DphiFlow(should_build=should_build, suites=suites, config=config)
    
    try:
        PhaseReactor.ignite(app.run)
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        log.error(f"💥 [FATAL] PhaseReactor crashed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()