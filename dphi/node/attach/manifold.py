# dphi.node.attach.manifold
## @lineage: dphi.phase.attach.manifold
## @lineage: phase.attach.manifold
import json
import asyncio
from watcher.plane.emitter import get_emitter
from kernel.dphi.broker import DphiBroker
from kernel.phase.daemon.bootstrap import KEY_HEARTBEAT_PATTERN

log = get_emitter("attach.manifold")

class ClusterDiscovery:
    """[Handshake Control] Control Plane: 노드 탐색 및 상태/가용량 확인 전담"""
    def __init__(self, tunnel):
        self.tunnel = tunnel
        self.worker_count = 0
        self.total_capacity = 0

    async def perform_handshake(self) -> bool:
        active_keys = await self.tunnel.keys(KEY_HEARTBEAT_PATTERN)
        if not active_keys:
            return False
            
        self.worker_count = 0
        self.total_capacity = 0
        
        for key in active_keys:
            raw_meta = await self.tunnel.get(key)
            if not raw_meta: continue
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


class ClusterController:
    """[Cluster Control] Data/Control Plane 상태 제어 및 정리 전담"""
    def __init__(self, tunnel, broker: DphiBroker):
        self.tunnel = tunnel
        self.broker = broker

    async def verify_data_plane(self) -> bool:
        res = await self.broker.execute(code="print('Pong')")
        return getattr(res, 'success', False)

    async def detach_gracefully(self):
        log.info("[Manifold] Detaching from Live Cluster gracefully. Daemons will self-heal.")
        if self.tunnel:
            if hasattr(self.tunnel, 'aclose'):
                await self.tunnel.aclose()
            else:
                await self.tunnel.close()


class LiveManifold:
    """
    [Facade] Attach 파이프라인 전체에서 공유되는 통합 인프라 객체.
    Scene들은 더 이상 터널을 직접 만지지 않고 이 객체만 참조합니다.
    """
    def __init__(self, tunnel):
        self.tunnel = tunnel
        self.broker = DphiBroker()
        self.discovery = ClusterDiscovery(tunnel)
        self.controller = ClusterController(tunnel, self.broker)

    async def boot(self) -> bool:
        if not await self.discovery.perform_handshake():
            log.error("❌ No execution capacity found during handshake.")
            return False
        if not await self.controller.verify_data_plane():
            log.error("❌ Data plane ping failed.")
            return False
        return True
        
    async def close(self):
        await self.broker.close()
        await self.controller.detach_gracefully()