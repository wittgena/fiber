# anchor.phase.reflect.proxy.dphi.invoker
## @lineage: bound.reflect.proxy.dphi.invoker
## @lineage: xphi.reflect.proxy.dphi.invoker
## @lineage: xphi.proxy.dphi.invoker
"""
@desc: Phase Invoker - Action & Perception Orchestrator
@flow:
-> sense: Ψ (Event ingress via Tunnel) ↦ EventBus
-> eval: EventBus ↦ Φ (Connector/Projector)
-> action: Φ ↦ SurfaceClient.stream_job (HTTP Dispatch)
-> listen: SurfaceClient.stream_job ↦ MQ Result (without threads)
-> reentry: Ψ′ ↦ State mutation or Re-entry
"""
import asyncio
import os
import sys
import json
import time
import subprocess
import urllib.parse
import atexit
from typing import Callable, List, Dict, Any
from pathlib import Path

from arch.topos.bound.sandbox.adapter.config import resolve_default_config
from arch.topos.bound.sandbox.tunnel import TunnelFactory
from arch.contract.interface import IEventBus, IPhaseAtor, IPhaseField 
from arch.contract.event.psi import PsiEvent, PsiCarrier
from arch.topos.bound.sandbox.surface import SurfaceMQ, SurfaceClient
from phase.bind.resolver import find_current_self, resolve_path
from phase.bind.client.stream import StreamClient
from watcher.plane.emitter import get_logger

log = get_logger("dphi.invoker")

try:
    SELF_ROOT = find_current_self()
    LIB_ROOT = resolve_path("lib")
except Exception as e:
    log.error(f"[Φ₀] anchor resolve fail: {e}")
    sys.exit(1)

_mq_config = resolve_default_config()
MQ_HOST = _mq_config.host
MQ_PORT = _mq_config.port
DPHI_API_BASE = os.getenv("DPHI_API_BASE", "http://localhost:8080/judgment")

class DPhiRuntime:
    """activate resolver when boundary has no handler"""
    def __init__(self, jar_root: Path = LIB_ROOT):
        self.jar_root = jar_root
        self.process_name = "dphi-node" 
        self.proc: subprocess.Popen = None

    def ensure(self):
        """@flow: 런타임 강제 기동 및 레지스트리(State Store) 등록"""
        jars = sorted(self.jar_root.glob("dphi-*.jar"))
        if not jars:
            raise RuntimeError("dphi jar not found")

        jar = jars[-1]
        log.info(f"[bootstrap] start dphi: {jar}")

        cmd = [
            "bash", "-c",
            f"exec -a {self.process_name} java -Dreaper.tag={self.process_name} -jar {jar}"
        ]

        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        # [FIX] 프로세스 수확기 등록: 부모(Python)가 죽을 때 자식(Java)도 함께 종료시킵니다.
        atexit.register(lambda: self.proc.terminate() if self.proc else None)

        pid = self.proc.pid
        try:
            state_store = TunnelFactory.get_isolated_sync()
            state_store.sadd("system:xphi:pids", str(pid))
            if hasattr(state_store, "close"):
                state_store.close()
                
            log.info(f"[bootstrap] Registered PID {pid} to State Store (system:xphi:pids)")
        except Exception as e:
            log.warning(f"[bootstrap] Failed to register PID to State Store (Continuing anyway): {e}")

        log.info("[bootstrap] Waiting for resonance (3s)...")
        time.sleep(3)

class QueueEventBus(IEventBus):
    """@role: ψ-router (Queue-based asynchronous bus)"""
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.subscribers: List[tuple] = []

    async def publish(self, event: PsiEvent) -> None:
        await self.queue.put(event)

    def subscribe(self, ator: IPhaseAtor, predicate: Callable) -> None:
        self.subscribers.append((ator, predicate))


class EventReceptor:
    """@role: Ψ ingress (External Matrix ↦ EventBus)"""
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.mq_client = None

    async def start(self):
        """@flow: Background listening loop using TunnelFactory"""
        self.mq_client = await TunnelFactory.get_default()
        pubsub = self.mq_client.pubsub()
        await pubsub.subscribe("psi:judgment:exec") 

        log.info("[Ψ] Listening for events on 'psi:judgment:exec'")

        try:
            async for msg in pubsub.listen():
                if msg["type"] not in ("message", "pmessage"):
                    continue
                
                raw_data = msg["data"]
                try:
                    event = PsiEvent.from_json(raw_data)
                except Exception:
                    event = PsiEvent(
                        event_id="ingress-legacy",
                        parent_id=None,
                        source_id="receptor",
                        scope="GLOBAL",
                        tick=0,
                        carrier=PsiCarrier(kind="EXEC", tag="LEGACY", payload=raw_data)
                    )
                await self.queue.put(event)
        finally:
            await pubsub.close()


# ==========================================
# 3. 위상 액터 (PhiConnector & Field)
# ==========================================
class PhiConnector(IPhaseAtor):
    """
    @role: Φ(t) Projector & Actuator
    @desc: 뇌(판단)와 팔(실행)의 분리. SurfaceClient를 상속받지 않고 조립(Composition)하여 사용.
    """
    def __init__(self, ator_id="phi.connector"):
        self._id = ator_id
        self._state: Dict[str, Any] = {"last_job": None, "status": "idle"}
        
        self.surface = SurfaceClient(
            stream_client=StreamClient(),
            bootstrap_runtime=DPhiRuntime(LIB_ROOT),
            mq_surface=SurfaceMQ(),
            source_name="loop.judgment",
            fallback_url=DPHI_API_BASE,
            path_prefix=""
        )

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> Dict[str, Any]:
        return self._state

    def set_state(self, new_state: str) -> None:
        self._state["status"] = new_state

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus) -> None:
        """@flow: Offload blocking unified pipeline to background thread pool"""
        self.set_state("projecting")
        await asyncio.to_thread(self._invoke_phi, event, bus)
        self.set_state("idle")

    def _invoke_phi(self, event: PsiEvent, bus: IEventBus):
        """@flow: Execution without explicit threading"""
        payload = event.payload
        try:
            data = json.loads(payload) if isinstance(payload, str) else payload
            action = data.get("action", "process") 
            target_path = data.get("path", "/")
        except Exception:
            log.warning(f"[Φ] Invalid payload format, treating as raw path: {payload}")
            action = "process"
            target_path = str(payload)

        query = f"/{action}?" + urllib.parse.urlencode({"path": target_path})
        log.info(f"[Φ(t) Modulation] Routing event to Xphi: {query}")

        try:
            pipeline = self.surface.stream_job(query, channel_prefix="judgment:result:", method="POST", is_json=False)
            
            for source, data in pipeline:
                if source == "http":
                    if isinstance(data, str) and data.startswith("jobId:"):
                        job_id = data.split("jobId:")[1].strip()
                        log.info(f"[job] Assigned Job ID: {job_id}")
                        self._state["last_job"] = job_id
                    else:
                        log.info(f"[Xphi REST] {data}")
                        
                elif source == "mq":
                    blocks = len(data.get("blocks", []))
                    log.info(f"[MQ Result] Job finished. blocks={blocks}")
                    
        except Exception as e:
            log.error(f"[Actuation Error] Xphi projection failed: {e}")

class DummyPhaseField(IPhaseField):
    """
    @role: 구조적 완결성을 위한 임시 Field 구현체.
    현재 루프 구조에서 실제 Field 로직이 붙기 전까지 IPhaseAtor.react의 인자를 채워줍니다.
    """
    def get_state(self) -> Dict[str, float]: return {}
    def evolve(self, dt: float) -> None: pass
    def compute_gradient(self) -> Dict[str, float]: return {}


# ==========================================
# 4. 오케스트레이션 루프 (Loop)
# ==========================================
class Loop:
    """@role: Phase loop (Implicit EventBus + Runtime)"""
    def __init__(self):
        self.queue = asyncio.Queue()
        self.bus = QueueEventBus(self.queue)
        self.field = DummyPhaseField()
        
        self.listener = EventReceptor(self.queue)
        self.projector = PhiConnector()

    async def bootstrap(self):
        log.info("[Loop] Bootstrapping DPhi Runtime in background...")
        await asyncio.to_thread(self.projector.surface.bootstrap_runtime.ensure)
        
        dummy_event = PsiEvent(
            event_id="boot", parent_id=None, source_id="system", scope="LOCAL", tick=0,
            carrier=PsiCarrier(kind="PING", tag="INIT", payload=json.dumps({"action": "ping", "path": "init"}))
        )
        await self.bus.publish(dummy_event)

    async def run(self):
        listener_task = asyncio.create_task(self.listener.start())
        await self.bootstrap()

        try:
            while True:
                event: PsiEvent = await self.queue.get()
                log.info(f"[Ψ Event] Received: {event.payload}")
                
                await self.projector.react(event, self.field, self.bus)
        except asyncio.CancelledError:
            log.info("[Loop] Phase loop collapsing. Shutting down...")
            listener_task.cancel()


# ==========================================
# 5. [Test & Simulation Matrix] 
# ==========================================
async def mock_stimulus_injector():
    """
    @test: 런타임이 안정화된 후 가상의 판단 이벤트를 주기적으로 주입하여 전체 파이프라인 동시 테스트
    """
    await asyncio.sleep(8.0)
    log.info("\n🧪 [Test Matrix] Injecting mock stimuli into 'psi:judgment:exec'...\n")
    
    mq = await TunnelFactory.get_default()
    
    test_payloads = [
        {"action": "analyze", "path": "/test/manifold_alpha"},
        {"action": "validate", "path": "/test/manifold_beta"}
    ]
    
    for payload in test_payloads:
        await mq.publish("psi:judgment:exec", json.dumps(payload))
        log.info(f"🧪 [Test Matrix] Injected: {payload}")
        await asyncio.sleep(3.0)


async def main():
    invoker_loop = Loop()
    log.info("Starting Invoker Matrix with Concurrent Test...")
    tasks = [
        asyncio.create_task(invoker_loop.run()),
        asyncio.create_task(mock_stimulus_injector())
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n## System gracefully terminated.")