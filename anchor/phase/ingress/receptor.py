# anchor.phase.ingress.receptor
## @lineage: bound.ingress.receptor
"""
@desc: Polymorphic Ingress Boundary (Data Plane Receptor).
       Binds external proxy traffic into Brane's internal routing mechanisms.
"""
import asyncio
import os
from typing import Optional, Dict, Any

from arch.proto.event.bus import AsyncEventBus
from arch.proto.event.psi import PsiEvent, PsiCarrier
from phase.runtime.node import NodeRuntime
from watcher.plane.emitter import get_emitter

log = get_emitter("ingress.receptor", phase="anchor")

class PolymorphicReceptor:
    """
    @role: Context-aware traffic receiver. 
    @action: Routes traffic directly via memory bridge (if local) or via EventBus (if distributed).
    """
    def __init__(self, node: NodeRuntime, bridge: Optional[Any] = None):
        self.node = node
        self.bus: AsyncEventBus = node.bus
        self.bridge = bridge  # Injected by Orchestrator
        self.mode = os.environ.get("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        
    async def ingest_traffic(self, raw_payload: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Normalizes external payload and delegates to the appropriate routing plane."""
        intent = raw_payload.get("intent", f"traffic.{source}")

        # 1. Local/Embedded Mode: Zero-latency memory bridge
        if self.bridge:
            log.debug(f"[Receptor] Bypassing bus. Delegating {intent} to memory bridge.")
            decision = await self.bridge.dispatch(intent=intent, payload=raw_payload)
            # AcpMemoryBridge와 DirectMemoryBridge의 리턴 타입이 다를 수 있으므로 범용 처리
            return {"status": "processed_locally", "result": decision}

        # 2. Distributed Mode: Fire and Forget into the AsyncEventBus
        carrier = PsiCarrier(symbol=intent, kind="ingress.request", payload=raw_payload)
        event = PsiEvent(carrier=carrier)
        
        await self.bus.publish(event)
        log.debug(f"[Receptor] Event {event.symbol} published to EventBus.")
        return {"status": "event_published", "psi_symbol": event.symbol}

    async def listen(self) -> None:
        """Activates the receptor based on the sniffed topology."""
        if self.mode == "KUBE_GRPC":
            log.info("[Receptor] Kube Environment detected. Starting gRPC ext_proc listener...")
            await asyncio.sleep(36000)
        elif self.mode == "LOCAL_DAEMON":
            log.info("[Receptor] Local Daemon port detected. Starting IPC Socket...")
            await asyncio.sleep(36000)
        else:
            log.info("[Receptor] Embedded Bypass mode. Listening directly via method calls. (Holding loop)")
            await asyncio.Event().wait()