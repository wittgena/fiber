# phase.epoch.loop.autoscaler
import asyncio
import json
from typing import Callable, Optional

from arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType
from arch.contract.event.next import next_id
from watcher.plane.emitter import get_emitter
from watcher.receptor.kernel import CHANNEL_AUTOSCALER

log = get_emitter("loop.autoscaler")

class PhaseAutoScaler:
    def __init__(
        self,
        tunnel,
        spawn_hook: Callable[[], None],
        despawn_hook: Callable[[], None],
        get_worker_count: Callable[[], int],
        max_workers: int = 16,
        debug_event: Optional[asyncio.Event] = None
    ):
        self.tunnel = tunnel
        self.spawn_hook = spawn_hook
        self.despawn_hook = despawn_hook
        self.get_worker_count = get_worker_count
        self.max_workers = max_workers
        self.debug_event = debug_event

    async def run(self):
        """Watcher가 방출하는 궤적 파열(Rupture) 시그널을 듣고 스케일 인/아웃을 수행합니다."""
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(CHANNEL_AUTOSCALER)
        log.info(f"[AutoScaler] Actuator Listening for Tension Ruptures on '{CHANNEL_AUTOSCALER}'...")
        
        try:
            async for msg in pubsub.listen():
                if msg.get('type') == 'message':
                    try:
                        raw_data = msg["data"]
                        if isinstance(raw_data, bytes):
                            raw_data = raw_data.decode('utf-8')
                            
                        data = json.loads(raw_data)
                        if data.get("event") == "xphi_structure_event":
                            rupture_type = data.get("rupture_type")
                            if rupture_type in ("KINEMATIC_TENSION_HIGH", "KINEMATIC_VOLATILITY"):
                                log.info(f"🔥 [AutoScaler] {rupture_type} detected on '{data.get('signal')}'! Triggering Scale-Out.")
                                
                                if self.get_worker_count() < self.max_workers:
                                    self.spawn_hook()
                                    if self.debug_event:
                                        self.debug_event.set()
                            elif rupture_type == "KINEMATIC_FLATLINE":
                                if self.get_worker_count() > 1:
                                    log.warning("[AutoScaler] 🛑 Flatline detected. Emitting Evaporation signal...")
                                    evap_event = PsiEvent(
                                        event_id=next_id(), parent_id=None, source_id="autoscaler", scope="GLOBAL", tick=0, phase_id=0, context={},
                                        carrier=PsiCarrier(kind="system:shutdown", tag="evaporate", payload={}, carrier_type=CarrierType.FIXED)
                                    )
                                    await self.tunnel.state_store.xadd("runtime:bus:stream", {"data": json.dumps(evap_event.__dict__)})
                                    self.despawn_hook()
                    except Exception as e:
                        log.error(f"[AutoScaler] Error processing signal: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(CHANNEL_AUTOSCALER)