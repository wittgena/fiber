# ops.dphi.entry
import sys
import argparse
import asyncio

from phase.bind.resolver import resolve_path
from phase.wasm.tester.dphi import WasmTester

from phase.wasm.builder import WasmBuilder
from phase.wasm.tracer import WasmTracer
from watcher.plane.emitter import get_emitter

class WasmPipelineCLI:
    def __init__(self, suites: list[str] = None):
        self.log = get_emitter("wasm.entry")
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / "dphi.wasm"
        self.suites = suites or ["all"]

    def _create_tester(self) -> WasmTester:
        """Tester 객체 생성을 위한 헬퍼 메서드"""
        return WasmTester(
            wasm_module_path=str(self.dest_wasm_file),
            sandbox_root=str(self.time_root),
            suites=self.suites
        )

    async def build(self):
        """WASM 컴파일 및 레지스트리 생성만 단독으로 수행합니다."""
        self.log.info("[CLI] Starting standalone WasmBuilder...")
        builder = WasmBuilder()
        
        await builder.trace()
        if builder.rupture_confirmed:
            self.log.error("[CLI] Builder encountered a fatal rupture.")
            sys.exit(1)
            
        self.log.info("[CLI] Builder completed successfully.")

    async def test(self):
        """빌드된 WASM 바이너리를 기반으로 시나리오 테스트만 단독으로 수행합니다."""
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
        """Build -> Test -> Tracer 자율 루프 파이프라인 전체를 수행합니다."""
        self.log.info(f"[CLI] Starting Full Pipeline (Build ➔ Test {self.suites} ➔ Tracer Autonomous Loop)...")
        
        tester = self._create_tester()
        tracer = WasmTracer(tester=tester)
        
        await tracer.trace()
        if tracer.rupture_confirmed:
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
        test_parser.add_argument("--suites", nargs="+", choices=["sandbox", "ledger", "a2a", "ecosystem", "all"], default=["all"], help="Choose specific suites to run.")
        all_parser = subparsers.add_parser("all", help="Run the full pipeline (Build -> Test -> Trace loop).")
        all_parser.add_argument("--suites", nargs="+", choices=["sandbox", "ledger", "a2a", "ecosystem", "all"], default=["all"], help="Choose specific suites to run.")
        
        args = parser.parse_args()
        command = args.command or "all"
        suites = getattr(args, "suites", ["all"])
        
        app = cls(suites=suites)
        try:
            asyncio.run(app.execute(command))
        except KeyboardInterrupt:
            app.log.warning("\n[CLI] Process interrupted by user. Shutting down gracefully...")
            sys.exit(0)


if __name__ == "__main__":
    WasmPipelineCLI.run_cli()