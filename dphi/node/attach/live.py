# dphi.node.attach.live
## @lineage: dphi.phase.attach.live
## @lineage: phase.attach.live
import sys
import argparse
import asyncio
import importlib
from typing import List

from watcher.wasm.builder import WasmBuilder
from arch.topos.tunnel.factory import TunnelFactory
from kernel.bind.resolver import resolve_path
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter

import dphi.node.attach.scene as scene_module
from dphi.node.attach.manifold import LiveManifold

MODULE_PATH = scene_module.__name__

class LiveAttachFlow:
    def __init__(self, suites: List[str]):
        self.log = get_emitter("attach.entry")
        self.suites = suites if suites else ["concurrency"]
        self.registry = {
            "concurrency": f"{MODULE_PATH}.concurrency:ConcurrencyScene",
            "anchor": f"{MODULE_PATH}.anchor:LiveAnchorScene",
            "cert": f"{MODULE_PATH}.cert:LiveCertScene"
        }

    def _resolve_suite(self, name: str):
        path = self.registry.get(name)
        if not path:
            self.log.error(f"[Attach] Unknown live suite: {name}")
            sys.exit(1)
        mod_name, cls_name = path.split(":")
        return getattr(importlib.import_module(mod_name), cls_name)

    async def run(self):
        self.log.info(f"[Attach] Starting Dedicated Pipeline for {self.suites}...")
        builder = WasmBuilder()
        await builder.trace()
        if builder.rupture_confirmed:
            sys.exit(1)
            
        tunnel = await TunnelFactory.get_default()
        
        # 인프라스트럭처 제어를 LiveManifold 객체로 위임
        manifold = LiveManifold(tunnel)
        
        try:
            if not await manifold.boot():
                sys.exit(1)
                
            self.log.info(f"[Attach] 🤝 Handshake: {manifold.discovery.worker_count} Workers, {manifold.discovery.total_capacity} Slots.")
            self.log.info(f"[Attach] 2. Injecting Live Scenes {self.suites} into Live Manifold...")
            
            for suite_name in self.suites:
                scene_class = self._resolve_suite(suite_name)
                # 씬(Scene)에는 broker 단일체가 아닌 통합 인프라 객체(manifold)를 주입
                scene = scene_class(manifold=manifold, suites=["all"]) if suite_name == "concurrency" else scene_class(manifold=manifold)
                    
                await scene.run_all()
                if getattr(scene, 'fail_count', 0) > 0:
                    self.log.error(f"❌ [Attach] Suite '{suite_name}' failed.")
                    break
                    
            self.log.info("🟢 [Attach] Live cluster pipeline executed successfully.")
        finally:
            await manifold.close()

def main():
    parser = argparse.ArgumentParser(description="DPHI Live Cluster Tester (True Attach Mode)")
    parser.add_argument("--suites", nargs="+", default=["concurrency"])
    args = parser.parse_args()

    app = LiveAttachFlow(suites=args.suites)
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()