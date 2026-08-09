# phase.chain.debug
import os
import sys
import asyncio
import subprocess
import json
from typing import List

from phase.chain.loop.autoscaler import PhaseAutoScaler

from arch.topos.tunnel.factory import TunnelFactory
from kernel.phase.reactor import PhaseReactor
from kernel.phase.runtime.gateway import RuntimeGateway
from watcher.receptor.bootstrap import receptor_bootstrap
from watcher.receptor.kernel import CHANNEL_SIGNAL_MUTATION
from watcher.plane.emitter import get_emitter

log = get_emitter("chain.debug")

class ChainDiagnosticFlow:
    def __init__(self):
        self.worker_processes: List[subprocess.Popen] = []
        self._autoscaler_task: asyncio.Task = None
        self._watcher_task: asyncio.Task = None
        self._injector_task: asyncio.Task = None
        self.scale_out_success = asyncio.Event()

    async def _setup_physical_membrane(self, tunnel):
        log.info("\n" + "="*55)
        log.info(" [Phase 1] Provisioning Initial Membrane (Node Idx 0)")
        log.info("="*55)
        
        await RuntimeGateway.assemble(None)
        self._watcher_task = asyncio.create_task(receptor_bootstrap(tunnel))
        
        autoscaler = PhaseAutoScaler(
            tunnel=tunnel,
            spawn_hook=self._spawn_worker,
            despawn_hook=lambda: self.worker_processes.pop() if self.worker_processes else None,
            get_worker_count=lambda: len(self.worker_processes),
            max_workers=16,
            debug_event=self.scale_out_success
        )
        self._autoscaler_task = asyncio.create_task(autoscaler.run())
        
        self._spawn_worker()

    def _spawn_worker(self):
        worker_idx = len(self.worker_processes)
        env = os.environ.copy()
        env["DPHI_WORKER_IDX"] = str(worker_idx)
        log.warning(f"[AutoScaler] 🟢 Spawning Physical Phase Runtime (Worker Node: Idx {worker_idx})...")
        proc = subprocess.Popen([sys.executable, "-m", "kernel.phase.runtime.node"], env=env)
        self.worker_processes.append(proc)

    async def _inject_synthetic_load(self, tunnel):
        """
        성공 시그널(scale_out_success)이 올 때까지 0.1초 간격으로 
        지수적으로(Exponentially) 폭발하는 부하를 쏴서 HIGH_TENSION(Scale-Out)을 유도합니다.
        """
        log.info("\n" + "="*55)
        log.info(" [Phase 2] Injecting Synthetic High Load Trajectory")
        log.info("="*55)
        
        counter = 0
        try:
            while not self.scale_out_success.is_set():
                load_value = float((counter ** 2) * 10) 
                
                payload = {
                    "signal_id": "node_load_synthetic_99",
                    "value": load_value
                }
                
                await tunnel.publish(CHANNEL_SIGNAL_MUTATION, json.dumps(payload))
                
                # 부하가 증가하는 것을 시각적으로 확인하기 위해 0.5초마다 로그 출력
                if counter % 5 == 0:
                    log.info(f" 📈 Injecting tension pulse: {load_value} ...")
                    
                counter += 1
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass

    def _teardown_physical_membrane(self):
        log.info(f"\n[Diag] Tearing down {len(self.worker_processes)} physical worker nodes...")
        for proc in self.worker_processes:
            proc.terminate()
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.kill()
        
        if self._autoscaler_task: self._autoscaler_task.cancel()
        if self._watcher_task: self._watcher_task.cancel()
        if self._injector_task: self._injector_task.cancel()

    async def run(self):
        log.info("=== [START] Diagnostic: N-Core Scale-Out Event Chain ===\n")
        tunnel = await TunnelFactory.get_default()
        
        try:
            await self._setup_physical_membrane(tunnel)
            
            # 워커와 데몬이 켜질 물리적 시간을 4초 부여 (Pre-flight 대기 포함)
            log.info("[Diag] Waiting 4 seconds for all daemons and listeners to fully boot...")
            await asyncio.sleep(4.0)
            
            self._injector_task = asyncio.create_task(self._inject_synthetic_load(tunnel))
            
            try:
                # 스케일 아웃 이벤트 감지 대기
                await asyncio.wait_for(self.scale_out_success.wait(), timeout=15.0)
                
                log.warning("\n" + "="*55)
                log.warning(" [Phase 3] 🚀 Scale-Out Triggered! Verifying Node 1...")
                log.warning("="*55)
                log.info("[Diag] Waiting 4 seconds to observe Node 1 ignition logs...")
                await asyncio.sleep(4.0)
                
                log.info("\n🎉 [SUCCESS] PERFECT! The Real Physical Pub/Sub chain is connected.")
                log.info(f"🎉 Total running worker nodes verified in OS: {len(self.worker_processes)}")
            except asyncio.TimeoutError:
                log.error("\n💀 [FAIL] The chain is broken. Receptor did not emit rupture OR AutoScaler missed it.")
                
        finally:
            self._teardown_physical_membrane()

def main():
    app = ChainDiagnosticFlow()
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()