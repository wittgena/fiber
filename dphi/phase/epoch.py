# dphi.phase.epoch
import sys
import argparse
import importlib
from dataclasses import dataclass, field
from typing import List, Dict

from watcher.wasm.builder import WasmBuilder
import dphi.phase.scene as scene_module
from watcher.wasm.tester import WasmTester

from arch.topos.tunnel.factory import TunnelFactory
from kernel.bind.resolver import resolve_path
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter

MODULE_PATH = scene_module.__name__

@dataclass
class PipelineConfig:
    suites_registry: Dict[str, str] = field(default_factory=lambda: {
        "sandbox": f"{MODULE_PATH}.sandbox:SandboxScene",
        "eco": f"{MODULE_PATH}.anchor:EcoScene",
        "anchor": f"{MODULE_PATH}.anchor:AnchorScene",
        "cert": f"{MODULE_PATH}.cert:CertProofScene"
    })
    
    default_suites: List[str] = field(default_factory=lambda: [
        "sandbox",     # 1. 런타임 보안 및 단일 샌드박스 격리 검증 (L1)
        "anchor",      # 2. 영지식 증명, 다중 서명, 탈중앙 합의 로직 검증 (L3)
        "cert"         # 3. 극한 환경 엣지 케이스 방어 및 무결성 최종 인증 (L4)
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
        # 'all' 키워드가 명시적으로 들어왔을 때만 default_suites 매핑
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
        """
        [개편 핵심] 라이브 브로커 직접 호출 대신, WasmTester를 이용해 로컬에서 통합 실행합니다.
        이렇게 하면 워커 데몬의 로그와 테스트 로그가 동일한 콘솔에서 정렬되어 출력됩니다.
        """
        self.log.info(f"\n[CLI] Starting Isolated WasmTester for suites: {self.suites}...")
        
        if not self.dest_wasm_file.exists():
            self.log.error(f"❌ [CLI] Missing WASM binary at {self.dest_wasm_file}. Run 'build' first.")
            sys.exit(1)
            
        # 1. 실행할 Suite 클래스 객체들을 딕셔너리로 준비
        suite_map = {}
        for suite_name in self.suites:
            if suite_name not in self.config.suites_registry:
                self.log.warning(f"[CLI] Unknown suite '{suite_name}', skipping...")
                continue
            suite_map[suite_name] = self._resolve_suite_class(suite_name)
            
        if not suite_map:
            self.log.error("❌ [CLI] No valid test suites found to execute.")
            sys.exit(1)

        # 2. WasmTester 인스턴스화 (데몬, 브로커, 스키마 오디터 내부 통합 관리)
        tester = WasmTester(
            wasm_module_path=str(self.dest_wasm_file),
            sandbox_root=str(self.time_root),
            suites=suite_map
        )
        
        # 3. 테스트 실행 및 결과 명시적 반환
        success, err_msg = await tester.execute()
        
        # 4. 성공/실패 여부를 콘솔에 명확하게 강제 출력
        print("\n" + "="*60)
        if success:
            self.log.info(f"🟢 [SUCCESS] All Test Suites ({', '.join(suite_map.keys())}) PASSED.")
            self.log.info(f"🔗 Execution Canonical Hash: {tester.test_execution_hash}")
        else:
            self.log.critical(f"🔴 [FAILED] Test execution terminated with errors.")
            self.log.critical(f"Details: {err_msg}")
            print("="*60 + "\n")
            sys.exit(1)
        print("="*60 + "\n")

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
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()