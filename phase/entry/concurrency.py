# phase.entry.concurrency
import sys
import argparse
import asyncio
import os

from dphi.wasm.builder import WasmBuilder
from phase.epoch.scene.concurrency import ConcurrencyScene
from kernel.dphi.broker import DphiBroker

from arch.topos.tunnel.factory import TunnelFactory
from kernel.bind.resolver import resolve_path
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter

class ConcurrencyFlow:
    def __init__(self):
        self.log = get_emitter("wasm.concurrency")
        
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / "dphi.wasm"

    async def run(self):
        self.log.info("[Concurrency] Starting Dedicated Pipeline (True Attach Mode)...")
        
        # 1. WASM 바이너리 빌드 (워커들이 최신 로직을 쓸 수 있도록)
        self.log.info("[Concurrency] 1. Starting WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        
        if builder.rupture_confirmed:
            self.log.error("[Concurrency] Builder encountered a fatal rupture.")
            sys.exit(1)
            
        tunnel = await TunnelFactory.get_default()
        
        # 물리적 노드가 켜져 있는지 확인 (Heartbeat 기반)
        from kernel.phase.daemon.bootstrap import KEY_HEARTBEAT_PATTERN
        active_nodes = await tunnel.keys(KEY_HEARTBEAT_PATTERN)
        if not active_nodes:
            self.log.error("❌ [Concurrency] No active nodes detected! Please run `python -m phase.node.boot` first.")
            await tunnel.close()
            sys.exit(1)
            
        self.log.info(f"[Concurrency] Connected to live system. Detected {len(active_nodes)} active nodes (Master/Workers).")

        try:
            self.log.info("[Concurrency] 2. Injecting Concurrency Scene into Live Manifold...")
            
            broker = DphiBroker()
            scene = ConcurrencyScene(broker=broker, suites={})
            await scene.run_all()
            self.log.info("[Concurrency] Pipeline executed successfully.")
            
        finally:
            if hasattr(tunnel, 'aclose'):
                await tunnel.aclose()
            else:
                await tunnel.close()

def main():
    parser = argparse.ArgumentParser(description="DPHI Concurrency Pipeline (True Attach Mode)")
    args = parser.parse_args()

    app = ConcurrencyFlow()
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()