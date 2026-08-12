# dphi.node.attach.live
import sys
import argparse
import asyncio
import importlib
import json
from typing import List

from watcher.wasm.builder import WasmBuilder
from arch.topos.tunnel.factory import TunnelFactory
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter
from kernel.dphi.broker import DphiBroker
from kernel.daemon.bootstrap import KEY_HEARTBEAT_PATTERN

import dphi.node.attach.scene as scene_module

MODULE_PATH = scene_module.__name__
log = get_emitter("attach.live")

class LiveManifold:
    """Unified Manifold for Cluster Discovery, Control, and Execution"""
    def __init__(self, tunnel):
        self.tunnel = tunnel
        self.broker = DphiBroker()
        self.worker_count = 0
        self.total_capacity = 0

    async def boot(self) -> bool:
        if not await self.perform_handshake():
            log.error("❌ No execution capacity found during handshake.")
            return False
        if not await self.verify_data_plane():
            log.error("❌ Data plane ping failed.")
            return False
        return True

    async def perform_handshake(self) -> bool:
        """[Handshake Control] 노드 탐색 및 상태/가용량 확인"""
        active_keys = await self.tunnel.keys(KEY_HEARTBEAT_PATTERN)
        if not active_keys:
            return False
            
        self.worker_count = 0
        self.total_capacity = 0
        
        for key in active_keys:
            raw_meta = await self.tunnel.get(key)
            if not raw_meta: 
                continue
            try:
                meta_str = raw_meta.decode('utf-8') if isinstance(raw_meta, bytes) else raw_meta
                meta = json.loads(meta_str)
                if meta.get("role") == "worker":
                    self.worker_count += 1
                    self.total_capacity += int(meta.get("capacity", 0))
            except Exception as e:
                log.debug(f"Handshake parse error: {e}")
                
        return self.total_capacity > 0

    async def oob_health_check(self) -> int:
        """DDoS 상황 등 Data Plane 마비 시 별도 채널을 통한 생존 노드 수 반환"""
        keys = await self.tunnel.keys(KEY_HEARTBEAT_PATTERN)
        return len(keys)

    async def verify_data_plane(self) -> bool:
        """[Cluster Control] Data Plane 상태 제어 및 확인"""
        # [교정] Boot 시점의 Ping이 무한 대기(Deadlock)에 빠지지 않도록 타임아웃 5초 강제
        res = await self.broker.execute(code="print('Pong')", timeout=5.0)
        return getattr(res, 'success', False)

    async def close(self):
        """[Cluster Control] 자원 정리 및 Detach 수행"""
        await self.broker.close()
        log.info("[Manifold] Detaching from Live Cluster gracefully. Daemons will self-heal.")
        if self.tunnel:
            if hasattr(self.tunnel, 'aclose'):
                await self.tunnel.aclose()
            else:
                await self.tunnel.close()


class LiveAttachFlow:
    def __init__(self, suites: List[str]):
        self.log = log
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
        manifold = LiveManifold(tunnel)
        
        try:
            if not await manifold.boot():
                sys.exit(1)
                
            self.log.info(f"[Attach] 🤝 Handshake: {manifold.worker_count} Workers, {manifold.total_capacity} Slots.")
            self.log.info(f"[Attach] 2. Injecting Live Scenes {self.suites} into Live Manifold...")
            
            for suite_name in self.suites:
                scene_class = self._resolve_suite(suite_name)
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