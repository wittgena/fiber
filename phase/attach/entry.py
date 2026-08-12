# phase.attach.entry
import sys
import argparse
import asyncio
import importlib
import json
from typing import List
from contextlib import suppress

from dphi.wasm.builder import WasmBuilder
from kernel.dphi.broker import DphiBroker

from arch.topos.tunnel.factory import TunnelFactory
from kernel.bind.resolver import resolve_path
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter
import phase.attach.scene as scene_module

MODULE_PATH = scene_module.__name__

class LiveAttachFlow:
    def __init__(self, suites: List[str]):
        self.log = get_emitter("attach.entry")
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / "dphi.wasm"
        self.suites = suites if suites else ["concurrency"]
        self.registry = {
            "concurrency": f"{MODULE_PATH}.concurrency:ConcurrencyScene",
            "anchor": f"{MODULE_PATH}.anchor:LiveAnchorScene",
            "cert": f"{MODULE_PATH}.cert:LiveCertScene"
        }
        self.tunnel = None

    def _resolve_suite(self, name: str):
        path = self.registry.get(name)
        if not path:
            self.log.error(f"[Attach] Unknown live suite: {name}")
            sys.exit(1)
        mod_name, cls_name = path.split(":")
        return getattr(importlib.import_module(mod_name), cls_name)

    async def run(self):
        self.log.info(f"[Attach] Starting Dedicated Pipeline for {self.suites} (True Attach Mode)...")
        self.log.info("[Attach] 1. Starting WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        if builder.rupture_confirmed:
            self.log.error("❌ [Attach] Builder encountered a fatal rupture.")
            sys.exit(1)
            
        self.tunnel = await TunnelFactory.get_default()
        
        # -----------------------------------------------------------------
        # [Handshake & Discovery] 물리적 라이브 노드 확인 및 진정한 Capacity 도출
        # -----------------------------------------------------------------
        from kernel.phase.daemon.bootstrap import KEY_HEARTBEAT_PATTERN
        
        # 1. Heartbeat 패턴으로 모든 활성 노드의 Key 수집
        active_keys = await self.tunnel.keys(KEY_HEARTBEAT_PATTERN)
        
        if not active_keys:
            self.log.error("❌ [Attach] No active nodes detected! Please run `python -m phase.node.boot` first.")
            await self.tunnel.close()
            sys.exit(1)
            
        worker_count = 0
        cluster_capacity = 0
        
        # 2. 각 노드가 쏜 Heartbeat 메타데이터(JSON)를 분석하여 실제 Worker Capacity만 합산
        for key in active_keys:
            raw_meta = await self.tunnel.get(key)
            if not raw_meta:
                continue
                
            try:
                # bytes 타입일 경우 디코딩
                meta_str = raw_meta.decode('utf-8') if isinstance(raw_meta, bytes) else raw_meta
                meta = json.loads(meta_str)
                
                # Master/Receptor 등 연산 능력이 없는 노드는 제외
                if meta.get("role") == "worker":
                    worker_count += 1
                    cluster_capacity += int(meta.get("capacity", 0))
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                self.log.debug(f"[Attach] Failed to parse heartbeat metadata from {key}: {e}")
                
        if cluster_capacity == 0:
            self.log.error("❌ [Attach] Active nodes found, but none of them have Worker execution capacity. Aborting.")
            await self.tunnel.close()
            sys.exit(1)

        self.log.info(f"[Attach] 🤝 Handshake Complete: {worker_count} Execution Workers Active -> Total Cluster Capacity: {cluster_capacity} slots.")

        try:
            broker = DphiBroker()
            
            # [Pre-flight] 본격적인 스트레스 테스트 전 시스템 응답성 확인
            self.log.info("[Attach] Pre-flight Ping Check...")
            ping_res = await broker.execute(code="print('Pong')")
            if not getattr(ping_res, 'success', False):
                self.log.error("❌ [Attach] Live cluster is not responding to basic execution requests. Aborting.")
                sys.exit(1)
                
            self.log.info(f"[Attach] 2. Injecting Live Scenes {self.suites} into Live Manifold...")
            
            for suite_name in self.suites:
                suite_class = self._resolve_suite(suite_name)
                
                # [Adaptive Alignment] 계산된 Capacity를 씬에 주입하여 테스트 강도를 자율 조절하게 함
                if suite_name == "concurrency":
                    scene = suite_class(broker=broker, cluster_capacity=cluster_capacity, suites=["all"])
                else:
                    scene = suite_class(broker=broker, cluster_capacity=cluster_capacity)
                    
                await scene.run_all()
                
                if getattr(scene, 'fail_count', 0) > 0:
                    self.log.error(f"❌ [Attach] Suite '{suite_name}' failed in live environment.")
                    break
                    
            self.log.info("🟢 [Attach] Live cluster pipeline executed successfully.")
            
        finally:
            # [Cleanup] 이중 채널화 및 데몬의 자율 치유(Early Load Shedding) 로직이 완성되었으므로,
            # NOGROUP 크래시를 유발했던 파괴적인 Stream 삭제(Flush) 로직을 제거하고 우아하게 퇴장합니다.
            self.log.info("[Attach] Detaching from Live Manifold. Daemons will shed remaining zombie queues autonomously.")

            if self.tunnel:
                if hasattr(self.tunnel, 'aclose'):
                    await self.tunnel.aclose()
                else:
                    await self.tunnel.close()

def main():
    parser = argparse.ArgumentParser(description="DPHI Live Cluster Tester (True Attach Mode)")
    parser.add_argument("--suites", nargs="+", default=["concurrency"], help="Suites to run (e.g. concurrency, anchor, cert)")
    args = parser.parse_args()

    app = LiveAttachFlow(suites=args.suites)
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()