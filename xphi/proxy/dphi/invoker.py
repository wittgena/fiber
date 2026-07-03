# xphi.proxy.dphi.invoker
## @lineage: xphi.reflect.dphi.invoker
"""
@phase:
Ψ (event ingress via Redis)
 → EventBus (queue)
 → Φ (dphi projection via StreamClient)
 → ∂Φ (boundary execution & Redis result listen)
 → Ψ′ (re-entry or state mutation)
"""
import asyncio
import os
import sys
import json
import threading
import urllib.parse
from typing import Callable, List, Dict, Any
import redis.asyncio as redis_async

from arch.contract.interface import IEventBus, IPhaseField, IPhaseAtor
from arch.proto.event.psi import PsiEvent

from phase.bind.resolver import find_current_self, resolve_path
from phase.bind.client.stream import StreamClient
from phase.bind.client.surface import RedisClient, SurfaceClient

from watcher.plane.emitter import get_logger
from xphi.proxy.dphi.runtime import DPhiRuntime

log = get_logger("dphi.invoker")

try:
    SELF_ROOT = find_current_self()
    LIB_ROOT = resolve_path("lib")
except Exception as e:
    log.error(f"[Φ₀] anchor resolve fail: {e}")
    sys.exit(1)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DPHI_API_BASE = os.getenv("DPHI_API_BASE", "http://localhost:8080/judgment")

class QueueEventBus(IEventBus):
    """@role: ψ-router (queue 기반 adapter)"""

    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.subscribers: List[tuple] = []

    async def publish(self, event: PsiEvent) -> None:
        await self.queue.put(event)

    def subscribe(self, ator, predicate: Callable) -> None:
        self.subscribers.append((ator, predicate))

class EventReceptor(IPhaseAtor):
    """@role: ψ ingress (external Redis → EventBus)"""

    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.redis = redis_async.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}",
            decode_responses=True
        )

    async def start(self):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("psi:judgment:exec")  # 토픽명은 환경에 맞게 조정

        log.info("[Ψ] listening for events on 'psi:judgment:exec'")

        async for msg in pubsub.listen():
            if msg["type"] == "message":
                event = PsiEvent(payload=msg["data"])
                await self.queue.put(event)


class PhiConnector(SurfaceClient, IPhaseAtor):
    """
    @role: Φ(t) Projector -> phi 연동 액터
    """
    def __init__(self, ator_id="phi.projector"):
        super().__init__(
            stream_client=StreamClient(),
            bootstrap_runtime=DPhiRuntime(LIB_ROOT),
            redis_surface=RedisClient(REDIS_HOST, REDIS_PORT),
            source_name="loop.judgment",
            fallback_url=DPHI_API_BASE,
            path_prefix=""
        )
        self._id = ator_id
        self._state = {"last_job": None}

    @property
    def ator_id(self) -> str:
        return self._id

    async def react(self, event: PsiEvent, bus: IEventBus):
        """이벤트 발생 시 비동기 루프를 블로킹하지 않도록 스레드로 오프로드"""
        await asyncio.to_thread(self._invoke_phi, event)

    def _invoke_phi(self, event: PsiEvent):
        # 1. 위상 해석 (Carrier/Payload 파싱)
        payload = event.payload
        try:
            data = json.loads(payload) if isinstance(payload, str) else payload
            # 예시: data 내부에서 액션과 타겟 경로를 추출
            action = data.get("action", "process") 
            target_path = data.get("path", "/")
        except Exception:
            log.warning(f"[Φ] Invalid payload format, treating as raw path: {payload}")
            action = "process"
            target_path = str(payload)

        query = f"/{action}?" + urllib.parse.urlencode({"path": target_path})
        log.info(f"[Φ(t) Modulation] Routing event to Xphi: {query}")

        try:
            # 2. Xphi 런타임으로 요청 전송 (overlay.xor 방식)
            for msg in self.request(query_path=query, method="POST", is_json=False):
                if msg.startswith("jobId:"):
                    job_id = msg.split("jobId:")[1].strip()
                    log.info(f"[job] Assigned Job ID: {job_id}")
                    self._state["last_job"] = job_id
                    
                    # 3. 경계 실행 (∂Φ) - 결과 수신을 위한 백그라운드 리스너 기동
                    threading.Thread(target=self._listen_job_result, args=(job_id,), daemon=True).start()
                else:
                    log.info(f"[Xphi REST] {msg}")
        except Exception as e:
            log.error(f"[Actuation Error] Xphi projection failed: {e}")

    def _listen_job_result(self, job_id: str):
        """
        @role: ∂Φ (boundary execution handling)
        """
        channel = f"judgment:result:{job_id}"
        log.info(f"[∂Φ] Boundary listening on {channel}")
        
        for data in self.surface.listen_job(channel):
            blocks = len(data.get("blocks", []))
            log.info(f"[Redis Result] Job {job_id} finished. blocks={blocks}")
            # 필요하다면 여기서 Ψ′ (재진입) 이벤트를 EventBus로 다시 쏠 수 있습니다.


class Loop:
    """@role: phase loop (implicit EventBus + runtime)"""
    def __init__(self):
        self.queue = asyncio.Queue()
        self.bus = QueueEventBus(self.queue)
        
        # 위상 액터 초기화
        self.listener = EventReceptor(self.queue)
        self.projector = PhiConnector()

    async def bootstrap(self):
        # 초기화 이벤트 주입
        await self.bus.publish(PsiEvent(payload=json.dumps({"action": "ping", "path": "init"})))

    async def run(self):
        # 1. 외부 이벤트 리스너(Redis PubSub) 가동
        asyncio.create_task(self.listener.start())
        await self.bootstrap()

        # 2. 메인 이벤트 루프
        while True:
            event: PsiEvent = await self.queue.get()
            log.info(f"[Ψ Event] Received: {event.payload}")
            
            # 3. Projector(Φ)에게 이벤트 위임
            await self.projector.react(event, self.bus)

async def main():
    loop = Loop()
    await loop.run()

if __name__ == "__main__":
    asyncio.run(main())