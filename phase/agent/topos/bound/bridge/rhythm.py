# phase.agent.topos.bound.bridge.rhythm
## @lineage: agent.nexus.topos.bound.bridge.rhythm
## @lineage: nexus.agent.topos.bound.bridge.rhythm
## @lineage: meta.agent.topos.bound.bridge.rhythm
## @lineage: topos.bound.bridge.rhythm
## @lineage: kernel.bind.rhythm.bridge
import json
from typing import Dict, Any, Optional
from xphi.arch.contract.event.next import next_id, parse_id
from xphi.arch.topos.tunnel.factory import UniversalFacade

class RhythmBridge:
    def __init__(self, tunnel: UniversalFacade, channel: str):
        self.tunnel = tunnel
        self.channel = channel

    async def emit(self, psi: Any):
        if not getattr(psi, 'event_id', None):
            psi.event_id = next_id()
        
        payload = {
            "event_id": psi.event_id,
            "phase_id": getattr(psi, 'phase_id', 0),
            "kind": getattr(psi, 'kind', 'unknown'),
            "tag": getattr(psi, 'tag', ''),
            "tick": getattr(psi, 'tick', 0),
            "timestamp": parse_id(psi.event_id)['timestamp_ms']
        }
        await self.tunnel.publish(self.channel, json.dumps(payload))