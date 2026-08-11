# phase.entry.dphi
import sys
import argparse
import importlib
from dataclasses import dataclass, field
from typing import List, Dict

from dphi.wasm.builder import WasmBuilder
import phase.epoch.scene as scene_module

from arch.topos.tunnel.factory import TunnelFactory
from kernel.bind.resolver import resolve_path
from kernel.dphi.broker import DphiBroker
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter
from kernel.phase.daemon.bootstrap import KEY_HEARTBEAT_PATTERN

MODULE_PATH = scene_module.__name__

@dataclass
class PipelineConfig:
    suites_registry: Dict[str, str] = field(default_factory=lambda: {
        "sandbox": f"{MODULE_PATH}.sandbox:SandboxScene",
        "eco": f"{MODULE_PATH}.anchor:EcoScene",
        "anchor": f"{MODULE_PATH}.anchor:AnchorScene"
    })
    default_suites: List[str] = field(default_factory=lambda: ["sandbox", "anchor"])
    wasm_filename: str = "dphi.wasm"

class DphiFlow:
    """
    @role: Live Attach Integration Pipeline
    @desc: 물리 환경(Membrane)을 직접 띄우지 않고, 기동 중인 클러스터에 접속(Attach)하여
           WASM 모듈 빌드 및 통합 테스트(Sandbox, Anchor 등)를 고속으로 수행합니다.
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
        self.log.info("[CLI] Starting standalone WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        
        if builder.rupture_confirmed:
            self.log.error("[CLI] Builder encountered a fatal rupture.")
            sys.exit(1)
        self.log.info("[CLI] Builder completed successfully.")

    async def _execute_live_suites(self):
        """라이브 클러스터에 접속하여 지정된 테스트 씬들을 DphiBroker를 통해 분산 실행합니다."""
        tunnel = await TunnelFactory.get_default()
        
        try:
            active_nodes = await tunnel.keys(KEY_HEARTBEAT_PATTERN)
            if not active_nodes:
                self.log.error("❌ [CLI] No active nodes detected! Please run `python -m phase.node.boot` first.")
                sys.exit(1)
                
            self.log.info(f"[CLI] Connected to live system. Detected {len(active_nodes)} active nodes.")
            
            # 라이브 워커들과 통신할 통합 브로커 생성
            broker = DphiBroker()
            
            for suite_name in self.suites:
                if suite_name not in self.config.suites_registry:
                    self.log.warning(f"[CLI] Unknown suite '{suite_name}', skipping...")
                    continue
                
                suite_class = self._resolve_suite_class(suite_name)
                self.log.info(f"\n>>> [PHASE] Starting Test Suite: {suite_name.upper()} <<<")
                
                # 씬 인스턴스화 (Broker 주입)
                scene_instance = suite_class(broker=broker)
                
                # 테스트 실행
                await scene_instance.run_all()
                
        finally:
            # DeprecationWarning 방지를 위한 안전한 Tunnel 종료
            if hasattr(tunnel, 'aclose'):
                await tunnel.aclose()
            else:
                await tunnel.close()

    async def test(self):
        self.log.info(f"[CLI] Starting Live Attach Tester for suites: {self.suites}...")
        if not self.dest_wasm_file.exists():
            self.log.error(f"[CLI] Missing WASM binary at {self.dest_wasm_file}. Run 'build' first.")
            sys.exit(1)
            
        await self._execute_live_suites()
        self.log.info("[CLI] Tester completed successfully.")

    async def pipeline(self):
        self.log.info(f"[CLI] Starting Full Pipeline (Build ➔ Test {self.suites})...")
        await self.build()
        await self.test()
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
    parser = argparse.ArgumentParser(description="WASM Distributed Sandbox & Autonomous Agent CLI (True Attach Mode)")
    parser.add_argument("--suites", nargs="+", default=["all"], help="List of suites to run (e.g. sandbox, anchor)")
    subparsers = parser.add_subparsers(dest="command", help="Execution modes")
    subparsers.add_parser("build", help="Compile the Rust WASM artifact only.")
    subparsers.add_parser("test", help="Run the Live Test scenarios only.")
    subparsers.add_parser("all", help="Run the full pipeline (Build -> Live Test).")

    args = parser.parse_args()
    command = args.command or "all"
    config = PipelineConfig()
    app = DphiFlow(command=command, suites=args.suites, config=config)
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()