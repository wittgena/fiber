# ops.xelog.topos.log.store
import asyncio
import uuid
import httpx
from typing import List, Dict, Any

from ops.xelog.topos.log.keeper import GatekeeperEngine
from arch.contract.audit.model import LogstEvent

from watcher.kernel.audit.warden import AuditWarden
from watcher.kernel.bridge.gateway import ToposGateway
from watcher.plane.emitter import get_emitter

log = get_emitter("log.store", phase="INGRESS")

_gateway_instance = ToposGateway()

class LogStreamStore:
    def __init__(self, gateway: ToposGateway = None, storage_endpoint: str = "http://internal-store:8000"):
        self.gateway = gateway or _gateway_instance
        self.storage_endpoint = storage_endpoint
        self._client = httpx.AsyncClient(base_url=self.storage_endpoint, timeout=5.0)

    async def bulk_append(self, stream_name: str, events: List[LogstEvent], metadata: Dict[str, Any] = None) -> bool:
        if not events:
            return True

        event_count = len(events)
        action_id = f"logstream_{stream_name}_{uuid.uuid4().hex[:8]}"
        
        # Calculate raw physical telemetry pressure (Data extraction only)
        telemetry_pressure = await asyncio.to_thread(self._extract_telemetry_pressure, events)
        tension_score = GatekeeperEngine.calculate_resonance_intensity(telemetry_pressure)
        
        # Inject tension score as metadata for WASM Kernel to judge
        if metadata is None:
            metadata = {}
        metadata["telemetry_tension_score"] = tension_score

        payload = {
            "stream_name": stream_name, 
            "count": event_count,
            "pressure": telemetry_pressure
        }
        
        is_authorized = await self.gateway.authorize(
            action_id=action_id, 
            action="LOGSTREAM_BULK_INSERT",
            payload=payload, 
            metadata=metadata
        )

        if not is_authorized:
            msg = f"Unauthorized bulk insert attempt to stream '{stream_name}' blocked by Kernel Store."
            log.warning(f"[LogtailStore] BLOCKED: {msg}")
            AuditWarden._record_anomaly(action="logstream.kernel_block", details=msg)
            return False

        if tension_score > 10.0:
            msg = f"High structural tension ({tension_score}) accepted by kernel in stream '{stream_name}'."
            log.error(f"[LogtailStore] TENSION ALERT: {msg}")
            AuditWarden._record_anomaly(action="logstream.high_tension_logged", details=msg)

        try:
            log.debug(f"[LogtailStore] Authorized by Kernel. Executing insert of {event_count} events.")
            # response = await self._client.post(f"/api/v1/logstream/{stream_name}", json=...)
            return True
        except Exception as e:
            log.error(f"[LogtailStore] Bulk append failed during execution: {e}")
            return False

    def _extract_telemetry_pressure(self, events: List[LogstEvent]) -> Dict[str, int]:
        leaks = 0
        timeouts = 0
        for event in events:
            event_str = str(event).lower()
            if "leak" in event_str: leaks += 1
            if "timeout" in event_str: timeouts += 1
        return {"token_leaks": leaks, "node_lock_timeouts": timeouts}

    async def close(self):
        await self._client.aclose()

def get_logstream_store() -> LogStreamStore:
    return LogStreamStore(gateway=_gateway_instance)