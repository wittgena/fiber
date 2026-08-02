# dphi.entry
## @lineage: agent.dphi.entry
import sys
import argparse
import asyncio
import importlib

from dphi.wasm.tester.dphi import WasmTester
from dphi.wasm.tracer import WasmTracer

from kernel.phase.reactor import KernelReactor
from phase.wasm.builder import WasmBuilder
from phase.bind.resolver import resolve_path

import watcher.dphi.scheme.scenario as scene_module
from watcher.plane.emitter import get_emitter

MODULE_PATH = scene_module.__name__
DEFAULT_SUITES = {
    "sandbox": f"{MODULE_PATH}.sandbox:SandboxScenarios",
    "ledger": f"{MODULE_PATH}.ledger:LedgerScenarios",
    "a2a": f"{MODULE_PATH}.a2a:A2AScenarios",
    "ecosystem": f"{MODULE_PATH}.ecosystem:EcosystemScenarios",
    "anchor": f"{MODULE_PATH}.anchor:AnchorScenarios"
}

class WasmPipelineCLI:
    def __init__(self, suites: list[str] = None):
        self.log = get_emitter("wasm.entry")
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / "dphi.wasm"
        self.suites = suites or ["all"]

    def _resolve_suite_class(self, suite_name_or_path: str):
        module_path_str = DEFAULT_SUITES.get(suite_name_or_path, suite_name_or_path)
        try:
            if ":" not in module_path_str:
                raise ValueError(f"Invalid suite path format '{module_path_str}'. Expected 'module.path:ClassName'")

            mod_name, cls_name = module_path_str.split(":")
            module = importlib.import_module(mod_name)
            suite_cls = getattr(module, cls_name)
            return suite_cls
        except Exception as e:
            self.log.error(f"[CLI] Failed to dynamically load suite '{suite_name_or_path}': {e}")
            sys.exit(1)

    def _create_tester(self) -> WasmTester:
        target_suite_names = list(DEFAULT_SUITES.keys()) if "all" in self.suites else self.suites
        resolved_suites = {}
        for name in target_suite_names:
            resolved_suites[name] = self._resolve_suite_class(name)
            
        return WasmTester(
            wasm_module_path=str(self.dest_wasm_file),
            sandbox_root=str(self.time_root),
            suites=resolved_suites
        )

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
            self.log.error(f"[CLI] Missing WASM binary at {self.dest_wasm_file}. Please run 'build' command first.")
            sys.exit(1)
            
        tester = self._create_tester()
        success, err_msg = await tester.execute()
        
        if not success:
            self.log.error(f"[CLI] Tester failed: {err_msg}")
            sys.exit(1)
            
        self.log.info("[CLI] Tester completed successfully. All selected scenarios passed.")

    async def pipeline(self):
        self.log.info(f"[CLI] Starting Full Pipeline (Build ➔ Test {self.suites} ➔ Tracer Autonomous Loop)...")
        
        tester = self._create_tester()
        tracer = WasmTracer(tester=tester) # 여기서 WasmTracer의 import 경로(dphi.wasm.tracer) 확인 필요
        
        await tracer.trace()
        if getattr(tracer, 'rupture_confirmed', False):
            self.log.warning("[CLI] Pipeline ended in a Rupture/Collapse state (Intended for fatal tests).")
            sys.exit(1)
            
        self.log.info("[CLI] Full pipeline executed successfully.")

    async def execute(self, command: str):
        command_map = {
            "build": self.build,
            "test": self.test,
            "all": self.pipeline
        }
        target_action = command_map.get(command, self.pipeline)
        await target_action()

    @classmethod
    def run_cli(cls):
        parser = argparse.ArgumentParser(description="WASM Distributed Sandbox & Autonomous Agent CLI")
        subparsers = parser.add_subparsers(dest="command", help="Execution modes")
        
        subparsers.add_parser("build", help="Compile the Rust WASM artifact only.")
        test_parser = subparsers.add_parser("test", help="Run the WasmTester scenarios only.")
        test_parser.add_argument("--suites", nargs="+", default=["all"],
                                 help="List of suites to run (e.g. sandbox, anchor, or custom.module:MyClass)")
        
        all_parser = subparsers.add_parser("all", help="Run the full pipeline (Build -> Test -> Trace loop).")
        all_parser.add_argument("--suites", nargs="+", default=["all"],
                                help="List of suites to run (e.g. sandbox, anchor, or custom.module:MyClass)")
        
        args = parser.parse_args()
        command = args.command or "all"
        suites = getattr(args, "suites", ["all"])
        
        app = cls(suites=suites)
        KernelReactor.ignite(lambda: app.execute(command))

if __name__ == "__main__":
    WasmPipelineCLI.run_cli()