# phase.entry.concurrency
import sys
import argparse
import asyncio
import subprocess
import os

from dphi.wasm.builder import WasmBuilder
from phase.epoch.scene.concurrency import ConcurrencyScene
from dphi.tracer.tester.wasm import WasmTester
from dphi.tracer.dphi import DphiTracer

from arch.topos.tunnel.factory import TunnelFactory
from kernel.bind.resolver import resolve_path
from kernel.phase.runtime.gateway import RuntimeGateway
from kernel.phase.reactor import PhaseReactor
from watcher.receptor.bootstrap import receptor_bootstrap
from watcher.plane.emitter import get_emitter

class ConcurrencyFlow:
    def __init__(self, fixed_workers: int = 4):
        self.log = get_emitter("wasm.concurrency")
        self.fixed_workers = fixed_workers
        
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / "dphi.wasm"
        self.suites = {"concurrency": ConcurrencyScene}
        
        self.master_process: subprocess.Popen = None
        self._watcher_task: asyncio.Task = None
        
        # Master Node가 이 환경 변수를 읽고 내부 Worker 프로세스를 스폰합니다.
        os.environ["DPHI_FIXED_WORKERS"] = str(self.fixed_workers)

    def _create_tester(self) -> WasmTester:
        return WasmTester(
            wasm_module_path=str(self.dest_wasm_file),
            sandbox_root=str(self.time_root),
            suites=self.suites
        )

    async def _setup_physical_membrane(self):
        """단일 Master Node를 Control Plane으로 프로비저닝합니다."""
        self.log.info(f"[Concurrency] Provisioning Master-Worker Membrane (Workers: {self.fixed_workers})...")
        
        await RuntimeGateway.assemble(None)
        tunnel = await TunnelFactory.get_default()
        
        # 1. Receptor(Watcher) 부팅
        self._watcher_task = asyncio.create_task(receptor_bootstrap(tunnel))

        # 2. 단 1개의 Master Node Process 스폰
        env = os.environ.copy()
        env["DPHI_WASM_STREAM_SUFFIX"] = "tester_isolated"
        env["DPHI_PHASE_ENV"] = "TEST"
        
        self.log.warning("[Membrane] 🟢 Spawning Master Runtime Node...")
        self.master_process = subprocess.Popen([sys.executable, "-m", "kernel.phase.runtime.node"], env=env)
        
        warmup_time = max(5.0, self.fixed_workers * 1.0)
        self.log.info(f"[Concurrency] Waiting {warmup_time:.1f}s for Master and {self.fixed_workers} Workers to fully boot...")
        await asyncio.sleep(warmup_time)

    def _teardown_physical_membrane(self):
        self.log.info("[Concurrency] Tearing down Master Node and internal workers...")
        if self.master_process:
            self.master_process.terminate()
            try:
                self.master_process.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                self.master_process.kill()
        
        if self._watcher_task: 
            self._watcher_task.cancel()

    async def run(self):
        self.log.info("[Concurrency] Starting Dedicated Pipeline (Build ➔ Membrane ➔ Tracer Loop)...")
        self.log.info("[Concurrency] 1. Starting WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        
        if builder.rupture_confirmed:
            self.log.error("[Concurrency] Builder encountered a fatal rupture.")
            sys.exit(1)
            
        await self._setup_physical_membrane()
        try:
            self.log.info("[Concurrency] 2. Starting WasmTester for Concurrency...")
            tester = self._create_tester()
            tracer = DphiTracer(tester=tester)
            await tracer.trace()
            
            if getattr(tracer, 'rupture_confirmed', False):
                self.log.warning("[Concurrency] Pipeline ended in a Rupture/Collapse state.")
                sys.exit(1)
                
            self.log.info("[Concurrency] Pipeline executed successfully.")
            
        finally:
            self._teardown_physical_membrane()

def main():
    parser = argparse.ArgumentParser(description="DPHI Concurrency Pipeline")
    parser.add_argument(
        "--workers", 
        type=int, 
        default=4, # 기본 4코어 할당 테스트
        help="Number of internal worker processes to spawn within the Master node."
    )
    args = parser.parse_args()

    app = ConcurrencyFlow(fixed_workers=args.workers)
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()