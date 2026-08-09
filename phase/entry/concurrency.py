# phase.entry.concurrency
import sys
import asyncio
import subprocess
import os
from typing import List

from phase.epoch.config.builder.wasm import WasmBuilder
from dphi.tracer.tester.wasm import WasmTester
from dphi.tracer.dphi import DphiTracer

from kernel.bind.resolver import resolve_path
from kernel.phase.runtime.gateway import RuntimeGateway
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter

from phase.chain.loop.autoscaler import PhaseAutoScaler
from arch.topos.tunnel.factory import TunnelFactory
from watcher.receptor.bootstrap import receptor_bootstrap
from phase.epoch.scene.concurrency import ConcurrencyScene

_worker_spawn_count = 0

class ConcurrencyFlow:
    def __init__(self):
        self.log = get_emitter("wasm.concurrency")
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / "dphi.wasm"
        self.suites = {"concurrency": ConcurrencyScene}
        
        self.worker_processes: List[subprocess.Popen] = []
        self._autoscaler_task: asyncio.Task = None
        self._watcher_task: asyncio.Task = None

    def _create_tester(self) -> WasmTester:
        return WasmTester(
            wasm_module_path=str(self.dest_wasm_file),
            sandbox_root=str(self.time_root),
            suites=self.suites
        )

    async def _setup_physical_membrane(self):
        """테스트 시작 전 Concurrency 동작을 위한 Control Plane과 Data Plane을 구성합니다."""
        self.log.info("[Concurrency] Provisioning Physical Membrane for N-Core Scale-out...")
        
        await RuntimeGateway.assemble(None)
        tunnel = await TunnelFactory.get_default()
        self._watcher_task = asyncio.create_task(receptor_bootstrap(tunnel))
        autoscaler = PhaseAutoScaler(
            tunnel=tunnel,
            spawn_hook=self._spawn_worker,
            despawn_hook=lambda: self.worker_processes.pop() if self.worker_processes else None,
            get_worker_count=lambda: len(self.worker_processes),
            max_workers=16
        )
        self._autoscaler_task = asyncio.create_task(autoscaler.run())
        self._spawn_worker()
        await asyncio.sleep(3.0)

    def _spawn_worker(self):
        """독립된 서브프로세스로 워커 노드를 OS 레벨에 스폰합니다."""
        global _worker_spawn_count
        env = os.environ.copy()
        env["DPHI_WORKER_IDX"] = str(_worker_spawn_count)
        
        self.log.warning(f"[AutoScaler] 🟢 Spawning Physical Phase Runtime (Worker Node: Idx {_worker_spawn_count})...")
        proc = subprocess.Popen([sys.executable, "-m", "kernel.phase.runtime.node"], env=env)
        self.worker_processes.append(proc)
        
        _worker_spawn_count += 1

    def _teardown_physical_membrane(self):
        """테스트 종료 후 스폰된 모든 워커 프로세스 및 백그라운드 태스크를 정리합니다."""
        self.log.info(f"[Concurrency] Tearing down {len(self.worker_processes)} physical worker nodes...")
        for proc in self.worker_processes:
            proc.terminate()
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.kill()
        
        if self._autoscaler_task: 
            self._autoscaler_task.cancel()
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
    app = ConcurrencyFlow()
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()