# phase.kernel.signal
## @lineage: nexus.phase.kernel.signal
## @lineage: meta.phase.kernel.signal
## @lineage: kernel.phase.signal
## @lineage: watcher.kernel.phase.signal
## @lineage: logos.kernel.phase.signal
import asyncio
from typing import Optional

from arch.contract.event.next import LogEvent
from arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType 
from arch.contract.event.bus import AsyncEventBus

from watcher.plane.observer.event import EventObserver
from watcher.plane.emitter import get_emitter

class PhaseSignal(EventObserver):
    def __init__(self, event_bus: AsyncEventBus):
        self.bus = event_bus
        self.log = get_emitter("anchor.bridge", phase="BRANE")

    def update(self, event: LogEvent) -> None:
        if event.level != "SIGNAL":
            return
            
        event_message = str(event.message).strip()
        payload = event.context.copy() if event.context else {}
        payload.update({
            "bridged_from": "Surface.LogEvent",
            "original_source": getattr(event, 'source_id', 'unknown'),
            "boundary": "brane_to_kernel"
        })

        carrier = PsiCarrier(
            kind="SIGNAL",          
            tag=event_message,      
            payload=payload,
            carrier_type=CarrierType.FIXED
        )

        psi_event = PsiEvent(
            event_id=event.event_id,
            parent_id=getattr(event, 'parent_id', None),
            source_id="anchor.phase.bridge", 
            scope=getattr(event, 'scope', 'GLOBAL'),
            tick=getattr(event, 'tick', 0) or 0,
            phase_id=getattr(event, 'phase_id', 0), # 상위 계층의 phase_id가 있다면 승계
            carrier=carrier,
            context={"domain": "boundary.crossing"}
        )

        try:
            loop = asyncio.get_running_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
            loop.create_task(self._async_dispatch(psi_event, event_message))
        except RuntimeError as e:
            self.log.error(f"[PhaseBridge] Cannot dispatch SIGNAL. Event loop unavailable: {e}")

    async def _async_dispatch(self, psi_event: PsiEvent, original_msg: str):
        try:
            symbol = getattr(psi_event, 'symbol', original_msg)
            self.log.trace(f"[PhaseBridge] Elevating SIGNAL -> Psi: {symbol}")
            await self.bus.publish(psi_event)
        except Exception as e:
            self.log.error(f"[PhaseBridge] Dispatch failed for {original_msg}: {e}", exc_info=True)