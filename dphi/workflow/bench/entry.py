# dphi.workflow.bench.entry
## @lineage: dphi.bound.bench.entry
## @lineage: phase.dphi.bench.entry
import sys
import argparse
from pathlib import Path

from fiber.dphi.workflow.bench.profile import ProfileBenchmarker, BENCH_TARGETS

from xphi.kernel.bind.resolver import resolve_path
from xphi.kernel.phase.reactor import PhaseReactor

from xphi.watcher.wasm.tester import WasmTester
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bench.entry")

class DphiBenchFlow:
    def __init__(self, targets: list, scale: float, verbose: bool):
        self.targets = targets
        self.scale = scale
        self.verbose = verbose
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / "dphi.wasm"

    async def run_benchmark(self):
        if not self.dest_wasm_file.exists():
            log.error(f"❌ [Bench] Missing WASM binary at {self.dest_wasm_file}. Run build first.")
            sys.exit(1)

        ProfileBenchmarker.bench_config = {
            "targets": self.targets,
            "scale": self.scale,
            "verbose": self.verbose
        }

        suite_map = {
            "profile": ProfileBenchmarker
        }

        log.info("\n[Bench] Initializing WasmTester Environment for Profiling...")
        tester = WasmTester(
            wasm_module_path=str(self.dest_wasm_file),
            sandbox_root=str(self.time_root),
            suites=suite_map
        )
        
        success, err_msg = await tester.execute()
        if not success:
            log.warning("[Bench] Profiling completed, but some targets missed the performance thresholds.")
        else:
            log.info("🟢 [Bench] Profiling completed successfully. All targets met.")

def main():
    parser = argparse.ArgumentParser(description="DPHI Core Micro-Benchmark & Profiling Tool")
    available_targets = list(BENCH_TARGETS.keys()) + ["all"]
    parser.add_argument("--targets", nargs="+", default=["all"], choices=available_targets, help="Specific benchmark targets to run (e.g., cold_boot, core_throughput)")
    parser.add_argument("--scale", type=float, default=1.0, help="Multiplier for the number of iterations (default: 1.0)")
    parser.add_argument("--verbose", action="store_true", help="Enable intermediate logs (disables silent mode during profiling)")

    args = parser.parse_args()
    app = DphiBenchFlow(targets=args.targets, scale=args.scale, verbose=args.verbose)
    PhaseReactor.ignite(app.run_benchmark)

if __name__ == "__main__":
    main()