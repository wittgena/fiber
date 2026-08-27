# workflow.wasm.dphi
import sys
import argparse
import importlib
from dataclasses import dataclass, field
from typing import List, Dict

from xphi.watcher.wasm.builder import WasmBuilder
import fiber.workflow.scene as scene_module
from xphi.watcher.wasm.tester import WasmTester

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("workflow.wasm.dphi")

MODULE_PATH = scene_module.__name__

@dataclass
class PipelineConfig:
    suites_registry: Dict[str, str] = field(default_factory=lambda: {
        "sandbox": f"{MODULE_PATH}.sandbox:SandboxScene",
        "eco": f"{MODULE_PATH}.anchor:EcoScene",
        "anchor": f"{MODULE_PATH}.anchor:AnchorScene",
        "cert": f"{MODULE_PATH}.cert:CertProofScene",
    })
    
    default_suites: List[str] = field(default_factory=lambda: [
        "sandbox",     # 1. 런타임 보안 및 단일 샌드박스 격리 검증 (L1)
        "anchor",      # 2. 영지식 증명, 다중 서명, 탈중앙 합의 로직 검증 (L3)
        "cert",        # 3. 극한 환경 엣지 케이스 방어 및 무결성 최종 인증 (L4)
    ])
    wasm_filename: str = "dphi.wasm"


class DphiFlow:
    """
    @role: WASM Pipeline Orchestrator (Build & Test)
    @desc: WASM 빌드를 수행하고, WasmTester를 통해 In-process(로컬 격리) 환경에서 
           명확하고 추적 가능한 엔드투엔드 통합 테스트를 수행합니다.
    """
    def __init__(self, command: str = "all", suites: List[str] = None, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.command = command
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
            sys.exit(1)

    async def build(self):
        self.log.info("\n[CLI] Starting standalone WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        
        if builder.rupture_confirmed:
            self.log.error("❌ [CLI] Builder encountered a fatal rupture.")
            sys.exit(1)
        self.log.info("✅ [CLI] Builder completed successfully.")

    async def test(self):
        self.log.info(f"\n[CLI] Starting Isolated WasmTester for suites: {self.suites}...")
        
        if not self.dest_wasm_file.exists():
            self.log.error(f"❌ [CLI] Missing WASM binary at {self.dest_wasm_file}. Run 'build' first.")
            sys.exit(1)
            
        suite_map = {}
        for suite_name in self.suites:
            if suite_name not in self.config.suites_registry:
                self.log.warning(f"[CLI] Unknown suite '{suite_name}', skipping...")
                continue
            suite_map[suite_name] = self._resolve_suite_class(suite_name)
            
        if not suite_map:
            self.log.error("❌ [CLI] No valid test suites found to execute.")
            sys.exit(1)

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
            else:
                self.log.error("Detailed failure traces could not be extracted (no runners found).")
                log.info("="*75 + "\n")
            
            sys.exit(1)

    async def pipeline(self):
        self.log.info(f"\n[CLI] Starting Full Pipeline (Build ➔ Test {self.suites})...")
        await self.build()
        await self.test()
        self.log.info("🚀 [CLI] Full pipeline executed and validated successfully.")

    async def run(self):
        command_map = {
            "build": self.build,
            "test": self.test,
            "all": self.pipeline
        }
        target_action = command_map.get(self.command, self.pipeline)
        await target_action()


def main():
    parser = argparse.ArgumentParser(description="WASM Distributed Sandbox & Autonomous Agent CLI (Isolated CI)")
    parser.add_argument("--suites", nargs="+", default=["all"], help="List of suites to run (e.g. sandbox, anchor, cert)")
    subparsers = parser.add_subparsers(dest="command", help="Execution modes")
    subparsers.add_parser("build", help="Compile the Rust WASM artifact only.")
    subparsers.add_parser("test", help="Run the Isolated Test scenarios only.")
    subparsers.add_parser("all", help="Run the full pipeline (Build -> Isolated Test).")

    args = parser.parse_args()
    command = args.command or "all"
    config = PipelineConfig()
    app = DphiFlow(command=command, suites=args.suites, config=config)
    
    # 커스텀 리액터를 통한 실행
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()