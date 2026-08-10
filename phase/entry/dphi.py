# phase.entry.dphi
import sys
import argparse
import importlib
from dataclasses import dataclass, field
from typing import List, Dict

from dphi.wasm.builder import WasmBuilder
import phase.epoch.scene as scene_module

from dphi.tracer.tester.wasm import WasmTester
from dphi.tracer.dphi import DphiTracer

from kernel.bind.resolver import resolve_path
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter

MODULE_PATH = scene_module.__name__

@dataclass
class PipelineConfig:
    suites_registry: Dict[str, str] = field(default_factory=lambda: {
        "sandbox": f"{MODULE_PATH}.sandbox:SandboxScene",
        "eco": f"{MODULE_PATH}.eco:EcoScene",
        "anchor": f"{MODULE_PATH}.anchor:AnchorScene"
    })
    default_suites: List[str] = field(default_factory=lambda: ["all"])
    wasm_filename: str = "dphi.wasm"

class DphiFlow:
    """
    무거운 물리 환경(Membrane) 셋업 없이,
    순수하게 WASM 모듈을 빌드하고 경량 씬(sandbox 등)을 테스트하는 파이프라인.
    """
    def __init__(self, command: str = "all", suites: List[str] = None, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.command = command
        self.suites = suites or self.config.default_suites
        
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

    def _create_tester(self) -> WasmTester:
        target_names = list(self.config.suites_registry.keys()) if "all" in self.suites else self.suites
        resolved_suites = {name: self._resolve_suite_class(name) for name in target_names}
        
        return WasmTester(
            wasm_module_path=str(self.dest_wasm_file),
            sandbox_root=str(self.time_root),
            suites=resolved_suites
        )

    # -------------------------------------------------------------------------
    # 기존 _setup_physical_membrane, _teardown_physical_membrane 및
    # 데몬 워커 프로세스 스폰 로직은 모두 entry.concurrency로 이관(삭제)되었습니다.
    # -------------------------------------------------------------------------

    async def build(self):
        self.log.info("[CLI] Starting standalone WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        
        if builder.rupture_confirmed:
            self.log.error("[CLI] Builder encountered a fatal rupture.")
            sys.exit(1)
        self.log.info("[CLI] Builder completed successfully.")

    async def test(self):
        self.log.info(f"[CLI] Starting standalone WasmTester for suites: {self.suites}...")
        if not self.dest_wasm_file.exists():
            self.log.error(f"[CLI] Missing WASM binary at {self.dest_wasm_file}. Run 'build' first.")
            sys.exit(1)
            
        tester = self._create_tester()
        success, err_msg = await tester.execute()
        
        if not success:
            self.log.error(f"[CLI] Tester failed: {err_msg}")
            sys.exit(1)
        self.log.info("[CLI] Tester completed successfully.")

    async def pipeline(self):
        self.log.info(f"[CLI] Starting Full Pipeline (Build ➔ Test {self.suites} ➔ Tracer Loop)...")
        
        tester = self._create_tester()
        tracer = DphiTracer(tester=tester)
        await tracer.trace()
        
        if getattr(tracer, 'rupture_confirmed', False):
            self.log.warning("[CLI] Pipeline ended in a Rupture/Collapse state.")
            sys.exit(1)
        self.log.info("[CLI] Full pipeline executed successfully.")

    async def run(self):
        command_map = {
            "build": self.build,
            "test": self.test,
            "all": self.pipeline
        }
        target_action = command_map.get(self.command, self.pipeline)
        await target_action()

def main():
    parser = argparse.ArgumentParser(description="WASM Distributed Sandbox & Autonomous Agent CLI (Lightweight)")
    parser.add_argument("--suites", nargs="+", default=["all"], help="List of suites to run (e.g. sandbox, anchor)")
    subparsers = parser.add_subparsers(dest="command", help="Execution modes")
    subparsers.add_parser("build", help="Compile the Rust WASM artifact only.")
    subparsers.add_parser("test", help="Run the WasmTester scenarios only.")
    subparsers.add_parser("all", help="Run the full pipeline (Build -> Test -> Trace loop).")

    args = parser.parse_args()
    command = args.command or "all"
    config = PipelineConfig()
    app = DphiFlow(command=command, suites=args.suites, config=config)
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()