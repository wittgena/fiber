# agent.nexus.topos.bound.genesis
## @lineage: nexus.agent.topos.bound.genesis
## @lineage: meta.agent.topos.bound.genesis
## @lineage: topos.bound.genesis
## @lineage: kernel.phase.runtime.flow.gen
import asyncio
import uuid
import time
import json
import random
from typing import Dict, Any, Optional, Set

from arch.topos.tunnel.factory import TunnelFactory, UniversalFacade, from_url
from arch.contract.event.psi import PsiEvent, PsiCarrier
from arch.contract.event.next import next_id

from agent.nexus.topos.bound.bridge.rhythm import RhythmBridge
from watcher.plane.emitter import get_emitter

log = get_emitter("bound.genesis")

class ToposManifold(type):
    """Metaclass for Topos Particles"""
    MAX_LIMIT = 50
    _semaphore: Optional[asyncio.Semaphore] = None
    _instances: list = [] 
    _active_tasks: set[asyncio.Task] = set()

    void_gap: Optional[asyncio.Queue] = None      
    projection_flow: Optional[asyncio.Queue] = None  
    collapse_field: Optional[asyncio.Queue] = None
    psi_queue: Optional[asyncio.Queue] = None

    global_tick: int = 0
    _tick_lock: Optional[asyncio.Lock] = None

    @classmethod
    def ignite_manifold(mcs):
        """이벤트 루프가 시작된 직후(main 함수 내부) 최초 1회 호출"""
        if mcs.void_gap is None:
            mcs.void_gap = asyncio.Queue()
            mcs.projection_flow = asyncio.Queue()
            mcs.collapse_field = asyncio.Queue()
            mcs.psi_queue = asyncio.Queue()
            mcs._semaphore = asyncio.Semaphore(mcs.MAX_LIMIT)
            mcs._tick_lock = asyncio.Lock()
            get_emitter("manifold", phase="TOPOS").info("ToposManifold Ignited: Quantums initialized.")

    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        ToposManifold._instances.append(instance)
        
        if cls.void_gap is None:
            raise RuntimeError("ToposManifold.ignite_manifold() must be called inside the event loop before creating particles.")

        async def managed_exist():
            async with cls._semaphore:
                try:
                    await instance.exist()
                except asyncio.CancelledError:
                    instance.log.info("[Phase] Particle existence cancelled gracefully.")
                except Exception as e:
                    instance.log.error(f"[Phase] Particle collapse due to anomaly: {e}")
                finally:
                    if instance in ToposManifold._instances:
                        ToposManifold._instances.remove(instance)

        # Task 강한 참조 유지 및 완료 시 제거 콜백 등록
        task = asyncio.create_task(managed_exist())
        ToposManifold._active_tasks.add(task)
        task.add_done_callback(ToposManifold._active_tasks.discard)
        return instance


class Particle(metaclass=ToposManifold):
    """@scale: cellular-unit (위상 잔여물 및 기본 노드)"""
    def __init__(self, phase_name="TOPOS", bridge=None):
        self.trace_id = f"{self.__class__.__name__}.{uuid.uuid4().hex[:4]}"
        self.bridge = bridge
        self.log = get_emitter(self.trace_id, phase=phase_name)
        self.potential = 0.0

    async def shock_reset(self):
        """@phase: phase reset (Defibrillation)"""
        self.potential = 0.0
        self.log.warn(f"[⚡] Shock applied: Phase reset for {self.trace_id}")

    async def emit_external(self, kind: str, tag: str, payload: dict = None, parent_id: str = None) -> PsiEvent:
        """@phase: transduction (Internal -> External)"""
        async with ToposManifold._tick_lock:
            ToposManifold.global_tick += 1
            current_tick = ToposManifold.global_tick

        carrier = PsiCarrier(kind=kind, tag=tag, payload=payload or {"timestamp": time.time()})
        event = PsiEvent(
            event_id=next_id(),
            parent_id=parent_id,
            source_id=self.trace_id,
            scope="GLOBAL",
            tick=current_tick,
            carrier=carrier,
            context={"domain": "toposbeat", "phase": "rhythm"}
        )
        if self.bridge:
            await self.bridge.emit(event)
        return event

    async def exist(self):
        """Subclasses must implement this topological heartbeat."""
        pass


class TensionAccumulator(Particle):
    """@phase: Tension Accumulation (결핍 축적 및 자율 파열)"""
    def __init__(self, bridge=None, threshold=1.0, leakage=0.15, **kwargs):
        super().__init__(phase_name="TENSION_NODE", bridge=bridge, **kwargs)
        self.threshold = threshold
        self.leakage = leakage

    async def exist(self):
        while True:
            self.potential += self.leakage + random.uniform(-0.01, 0.01)
            if self.potential >= self.threshold:
                self.potential = 0.0
                pulse_id = f"beat.{uuid.uuid4().hex[:4]}"
                
                event = await self.emit_external(kind="PULSE", tag="TOPOS:TENSION_NODE", payload={"pulse_id": pulse_id})
                try:
                    ToposManifold.void_gap.put_nowait({"id": pulse_id, "parent_id": event.event_id})
                    log.info(time.time(), "TENSION_NODE", f"[♥] Pulse Fired: {pulse_id}", "SYS")
                except asyncio.QueueFull:
                    self.log.debug(f"Pulse {pulse_id} dropped (Refractory Period)")

            await asyncio.sleep(0.1)


class PhaseProjector(Particle):
    def __init__(self, bridge=None, **kwargs):
        super().__init__(phase_name="PROJECTOR", bridge=bridge, **kwargs)

    async def exist(self):
        while True:
            data = await ToposManifold.void_gap.get()
            await asyncio.sleep(0.2) 
            
            vector_id = f"vector({data['id']})"
            event = await self.emit_external(
                kind="PROJECTION", tag="TOPOS:PROJECTOR", 
                payload={"vector_id": vector_id}, parent_id=data["parent_id"]
            )

            try:
                ToposManifold.projection_flow.put_nowait({"id": vector_id, "parent_id": event.event_id})
            except asyncio.QueueFull:
                pass


class ToposCollapse(Particle):
    """@phase: Collapse (위상 붕괴 및 결론 도출)"""
    def __init__(self, bridge=None, **kwargs):
        super().__init__(phase_name="COLLAPSE", bridge=bridge, **kwargs)

    async def exist(self):
        while True:
            data = await ToposManifold.projection_flow.get()
            await asyncio.sleep(0.4) 
            
            phi_id = f"Phi({uuid.uuid4().hex[:3]})"
            event = await self.emit_external(
                kind="COLLAPSE", tag="TOPOS:COLLAPSE", 
                payload={"phi_id": phi_id}, parent_id=data["parent_id"]
            )
            
            try:
                ToposManifold.collapse_field.put_nowait({"id": phi_id, "parent_id": event.event_id})
            except asyncio.QueueFull:
                pass
            log.info(time.time(), "COLLAPSE", f"[♥] Contraction: {phi_id}", "INFO")


class ReentryInversion(Particle):
    """@phase: Inversion (여백 확보 및 재진입 준비)"""
    def __init__(self, bridge=None, **kwargs):
        phase_name = kwargs.pop("phase_name", "INVERSION") 
        super().__init__(phase_name=phase_name, bridge=bridge, **kwargs)
        self.reentry_multiplier = 1.0

    async def update_multiplier(self, new_multiplier: float):
        """@phase: 외부 자극에 의한 위상 반전 계수 재설정"""
        self.reentry_multiplier = new_multiplier
        self.log.info(f"[△] Re-entry multiplier updated to {self.reentry_multiplier}")

    async def exist(self):
        while True:
            data = await ToposManifold.collapse_field.get()
            await asyncio.sleep(0.3)
            current_tension = ToposManifold.void_gap.qsize()
            base_reflow = 5
            actual_reflow = int(base_reflow * self.reentry_multiplier)

            for _ in range(actual_reflow):
                reflow_id = f"reflow.{uuid.uuid4().hex[:4]}"
                try:
                    ToposManifold.void_gap.put_nowait({"id": reflow_id, "parent_id": data["parent_id"]})
                except asyncio.QueueFull:
                    break 

            log.info(
                time.time(), 
                "INVERSION", 
                f"[△] Inversion complete. Reflowed: {actual_reflow} (Tension: {current_tension})", 
                "SYS"
            )


class ToposField:
    def __init__(self, connection_url: Optional[str] = None):
        self.connection_url = connection_url
        self.bridge: Optional[RhythmBridge] = None
        self.dynamics_task: Optional[asyncio.Task] = None
        self.listener_task: Optional[asyncio.Task] = None
        self.tunnel: Optional[UniversalFacade] = None

    async def _init_tunnel(self):
        """@point: Tunnel 인스턴스 지연 초기화 (Lazy Initialization)"""
        if not self.tunnel:
            if self.connection_url:
                self.tunnel = await from_url(self.connection_url)
            else:
                self.tunnel = await TunnelFactory.get_default()

    async def _flow_dynamics(self):
        log.info("## Topos Autonomous Dynamics Online")
        await self._init_tunnel()
        self.bridge = RhythmBridge(self.tunnel, "rhythm.topos")
        
        # 메타클래스 상태 선초기화
        ToposManifold.ignite_manifold()
        
        # Particle 서브클래스 생성
        TensionAccumulator(bridge=self.bridge)
        PhaseProjector(bridge=self.bridge)
        ToposCollapse(bridge=self.bridge)
        ReentryInversion(bridge=self.bridge)

        while True:
            await asyncio.sleep(1)
            log.info(f"[Monitor] Field active. Nodes: {len(ToposManifold._instances)} | Tick: {ToposManifold.global_tick}")

    async def _listen_signals(self):
        ## @phase: Signal ingress boundary (External -> Internal) via Universal Tunnel
        await self._init_tunnel()
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe("runtime:signal")
        
        log.info("[System] Dynamics Signal Listener Online via Universal Tunnel. Waiting for triggers...")
        async for msg in pubsub.listen():
            if msg["type"] != "message": 
                continue
            try:
                data = json.loads(msg["data"])
                sig_type = data.get("type")
                await self._handle_signal(sig_type, data)
            except Exception as e:
                log.info(f"[Signal Error] Failed to process perturbation: {e}")

    async def _handle_signal(self, sig_type: str, data: dict):
        """@regime.change: Contextual orbital intervention based on signal topology"""
        if sig_type == "topos:perturb":
            log.info(time.time(), "PERTURB", "[⚡] ENTROPY FLUSH: Global Phase Reset", "CRIT")
            # phase_reset -> shock_reset 으로 메서드 호출명 수정
            tasks = [inst.shock_reset() for inst in ToposManifold._instances if hasattr(inst, 'shock_reset')]
            if tasks: await asyncio.gather(*tasks)
            for q in (ToposManifold.void_gap, ToposManifold.projection_flow, ToposManifold.collapse_field):
                while not q.empty(): 
                    q.get_nowait()
        
        elif sig_type == "topos:inject":
            log.info(time.time(), "INJECT", "[External] Demand Tension Injected", "WARN")
            await ToposManifold.void_gap.put({"id": f"rupture.inject.{uuid.uuid4().hex[:4]}", "parent_id": "ext-inject-event"})
        
        elif sig_type == "origin:run":
            if not ToposManifold._instances and self.dynamics_task is None:
                self.dynamics_task = asyncio.create_task(self._flow_dynamics())
            else:
                log.info("[System] Dynamics field is already oscillating.")
        
        elif sig_type == "topos:tune_reentry":
            new_factor = float(data.get("factor", 1.0))
            log.info(time.time(), "TUNE", f"[External] Tuning Re-entry Multiplier to {new_factor}", "WARN")
            tasks = [inst.update_multiplier(new_factor) for inst in ToposManifold._instances if isinstance(inst, ReentryInversion)]
            if tasks: await asyncio.gather(*tasks)

    async def start(self, auto_run: bool = False):
        """@point: Physical execution root"""
        self.listener_task = asyncio.create_task(self._listen_signals())
        if auto_run:
            self.dynamics_task = asyncio.create_task(self._flow_dynamics())
            await asyncio.gather(self.listener_task, self.dynamics_task)
        else:
            await self.listener_task