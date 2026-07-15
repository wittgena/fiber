# bound.bridge.phase.signal
import asyncio
from typing import Optional

from arch.contract.event.next import LogEvent
from arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType 
from arch.contract.event.bus import AsyncEventBus

from watcher.plane.observer.surface import EventObserver
from watcher.plane.emitter import get_emitter

class PhaseSignal(EventObserver):
    """
    @desc: 관제망(Surface)과 제어망(Bus)의 위상 브릿지 (Brane 계층)
    @action: SIGNAL 레벨의 로그를 시스템의 상태 전이 벡터(PsiEvent)로 승격(Elevate)시켜
             하위 커널(Dynamics)에 섭동(Perturbation)으로 주입합니다.
    """
    def __init__(self, event_bus: AsyncEventBus):
        self.bus = event_bus
        # @point 1: 계층 상승에 맞추어 로거 식별자와 phase를 갱신 (KERNEL -> BRANE/GATEWAY)
        self.log = get_emitter("anchor.bridge", phase="BRANE")

    def update(self, event: LogEvent) -> None:
        if event.level != "SIGNAL":
            return
            
        event_message = str(event.message).strip()
        payload = event.context.copy() if event.context else {}
        
        # @point 2: 차원 경계를 넘었다는 것을 명확히 마킹 (추적 용이성 확보)
        payload.update({
            "bridged_from": "Surface.LogEvent",
            "original_source": getattr(event, 'source_id', 'unknown'),
            "boundary": "brane_to_kernel"
        })

        # 1. PsiCarrier 조립 (에너지의 형태 정의)
        carrier = PsiCarrier(
            kind="SIGNAL",          
            tag=event_message,      
            payload=payload,
            carrier_type=CarrierType.FIXED
        )

        # 2. PsiEvent 조립 (시공간 좌표 부여)
        psi_event = PsiEvent(
            event_id=event.event_id,
            parent_id=getattr(event, 'parent_id', None),
            # @point 3: 출처(source_id)를 브릿지의 현재 위치에 맞게 수정
            source_id="anchor.phase.bridge", 
            scope=getattr(event, 'scope', 'GLOBAL'),
            tick=getattr(event, 'tick', 0) or 0,
            phase_id=getattr(event, 'phase_id', 0), # 상위 계층의 phase_id가 있다면 승계
            carrier=carrier,
            context={"domain": "boundary.crossing"}
        )

        # 3. 비동기 버스 발행 (스레드/루프 안전성 보강)
        try:
            loop = asyncio.get_running_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
            loop.create_task(self._async_dispatch(psi_event, event_message))
        except RuntimeError as e:
            self.log.error(f"[PhaseBridge] Cannot dispatch SIGNAL. Event loop unavailable: {e}")

    async def _async_dispatch(self, psi_event: PsiEvent, original_msg: str):
        try:
            # @point 4: psi_event에 symbol 속성이 보장되지 않을 수 있으므로 안전한 로깅 적용
            symbol = getattr(psi_event, 'symbol', original_msg)
            self.log.trace(f"[PhaseBridge] Elevating SIGNAL -> Psi: {symbol}")
            await self.bus.publish(psi_event)
        except Exception as e:
            self.log.error(f"[PhaseBridge] Dispatch failed for {original_msg}: {e}", exc_info=True)