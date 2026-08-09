# phase.entry.dphi
import sys
import argparse
import asyncio
import importlib
import subprocess
import os
from dataclasses import dataclass, field
from typing import List, Dict

from phase.epoch.config.builder.wasm import WasmBuilder
from phase.loop.autoscaler import PhaseAutoScaler
import phase.epoch.scene as scene_module

from dphi.tracer.tester.wasm import WasmTester
from dphi.tracer.dphi import DphiTracer

from arch.topos.tunnel.factory import TunnelFactory
from kernel.bind.resolver import resolve_path
from kernel.phase.boot import KernelGateway
from kernel.phase.reactor import PhaseReactor

from watcher.receptor.bootstrap import receptor_bootstrap
from watcher.plane.emitter import get_emitter

MODULE_PATH = scene_module.__name__

_worker_spawn_count = 0

@dataclass
class PipelineConfig:
    suites_registry: Dict[str, str] = field(default_factory=lambda: {
        "sandbox": f"{MODULE_PATH}.sandbox:SandboxScene",
        "concurrency": f"{MODULE_PATH}.concurrency:ConcurrencyScene",
        "eco": f"{MODULE_PATH}.eco:EcoScene",
        "anchor": f"{MODULE_PATH}.anchor:AnchorScene"
    })
    default_suites: List[str] = field(default_factory=lambda: ["all"])
    wasm_filename: str = "dphi.wasm"

class DphiFlow:
    def __init__(self, command: str = "all", suites: List[str] = None, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.command = command
        self.suites = suites or self.config.default_suites
        
        self.log = get_emitter("wasm.entry")
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / self.config.wasm_filename
        
        self.worker_processes: List[subprocess.Popen] = []
        self._autoscaler_task: asyncio.Task = None
        self._watcher_task: asyncio.Task = None

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

    # =========================================================================
    # [핵심] N-Core Scale-out 물리 환경 모방 (Auto-Scaler & Watcher)
    # =========================================================================
    async def _setup_physical_membrane(self):
        """테스트 시작 전 실제 Boot와 동일한 Control Plane과 Data Plane을 구성합니다."""
        self.log.info("[CLI] Provisioning Physical Membrane (Control Plane & Worker)...")
        
        # 1. Gateway(라우팅 정책망) 구성
        await KernelGateway.assemble(None)
        
        # 2. Watcher(Receptor) 가동
        tunnel = await TunnelFactory.get_default()
        self._watcher_task = asyncio.create_task(receptor_bootstrap(tunnel))
        
        # 3. [핵심 개선] 모듈화된 AutoScaler 의존성 주입 및 가동
        autoscaler = PhaseAutoScaler(
            tunnel=tunnel,
            spawn_hook=self._spawn_worker,
            despawn_hook=lambda: self.worker_processes.pop() if self.worker_processes else None,
            get_worker_count=lambda: len(self.worker_processes),
            max_workers=16
        )
        self._autoscaler_task = asyncio.create_task(autoscaler.run())
        
        # 4. 최초 워커 노드 1기 스폰
        self._spawn_worker()
        
        # 데몬들이 완전히 켜지고 구독을 준비할 수 있도록 여유 부여
        await asyncio.sleep(3.0)

    def _spawn_worker(self):
        """독립된 서브프로세스로 워커 노드를 OS 레벨에 스폰합니다."""
        global _worker_spawn_count
        env = os.environ.copy()
        
        # 코어 라운드로빈 바인딩을 위해 순차적인 인덱스를 주입
        env["DPHI_WORKER_IDX"] = str(_worker_spawn_count)
        
        self.log.warning(f"[AutoScaler] 🟢 Spawning Physical Phase Runtime (Worker Node: Idx {_worker_spawn_count})...")
        proc = subprocess.Popen([sys.executable, "-m", "kernel.phase.runtime.node"], env=env)
        self.worker_processes.append(proc)
        
        _worker_spawn_count += 1

    def _teardown_physical_membrane(self):
        """테스트 종료 후 스폰된 모든 워커 프로세스 및 백그라운드 태스크를 정리합니다."""
        self.log.info(f"[CLI] Tearing down {len(self.worker_processes)} physical worker nodes...")
        for proc in self.worker_processes:
            proc.terminate()
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.kill()
        
        if self._autoscaler_task: self._autoscaler_task.cancel()
        if self._watcher_task: self._watcher_task.cancel()

    # =========================================================================

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
            
        await self._setup_physical_membrane()
        
        try:
            tester = self._create_tester()
            success, err_msg = await tester.execute()
            
            if not success:
                self.log.error(f"[CLI] Tester failed: {err_msg}")
                sys.exit(1)
            self.log.info("[CLI] Tester completed successfully.")
        finally:
            self._teardown_physical_membrane()

    async def pipeline(self):
        self.log.info(f"[CLI] Starting Full Pipeline (Build ➔ Test {self.suites} ➔ Tracer Loop)...")
        
        await self._setup_physical_membrane()
        
        try:
            tester = self._create_tester()
            tracer = DphiTracer(tester=tester)
            await tracer.trace()
            
            if getattr(tracer, 'rupture_confirmed', False):
                self.log.warning("[CLI] Pipeline ended in a Rupture/Collapse state.")
                sys.exit(1)
            self.log.info("[CLI] Full pipeline executed successfully.")
        finally:
            self._teardown_physical_membrane()

    async def run(self):
        command_map = {
            "build": self.build,
            "test": self.test,
            "all": self.pipeline
        }
        target_action = command_map.get(self.command, self.pipeline)
        await target_action()

def main():
    parser = argparse.ArgumentParser(description="WASM Distributed Sandbox & Autonomous Agent CLI")
    parser.add_argument("--suites", nargs="+", default=["all"], help="List of suites to run (e.g. sandbox, anchor, or custom.module:MyClass)")
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