# phase.kernel.attach.inject
## @lineage: dphi.node.attach.inject
## @lineage: phase.node.attach.inject
import json
import random
from dataclasses import asdict
from typing import Any

from xphi.arch.contract.event.psi import PsiEvent, PsiCarrier
from xphi.arch.contract.event.next import next_id
from xphi.kernel.daemon.bootstrap import TOPIC_BUS_STREAM
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("attach.inject")

class PhysicsInjector:
    def __init__(self, tunnel: Any):
        self.tunnel = tunnel

    async def inject_kinetic_pressure(self, size: int):
        log.info(f"☄️ [Physics] Injecting kinetic payload (Mass: {size}) into Phase Field...")
        massive_payload = [{"mass": i, "impact": random.random()} for i in range(size)]
        trigger_event = PsiEvent(
            event_id=next_id(), source_id="debug.shell", scope="NETWORK", parent_id=None, tick=1, phase_id=0,
            carrier=PsiCarrier(kind="COMMAND", tag="flow.dynamics", payload={"_context": {"command": "flow.absorb"}}),
            context={"payload": massive_payload}
        )
        
        event_dict = asdict(trigger_event) if hasattr(trigger_event, '__dataclass_fields__') else trigger_event.__dict__
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(event_dict)})
        log.info(" └─ 🌊 Kinetic wave unleashed. Observe Pressure/Topology metrics in boot logs.")

    async def inject_ator_mutation(self, node_id: str, new_state: str):
        """특정 ToposAtor를 조작하여 진동 공간에 특이점(Attractor/Reflector) 생성"""
        valid_states = ["ATTRACTOR", "REFLECTOR", "NORMAL"]
        if new_state not in valid_states:
            log.info(f"⚠️ State must be one of: {valid_states}")
            return
            
        log.info(f"🌀 [Topology] Mutating Ator '{node_id}' -> {new_state}...")
        trigger_event = PsiEvent(
            event_id=next_id(), source_id="debug.shell", scope="SYSTEMIC", parent_id=None, tick=1, phase_id=0,
            carrier=PsiCarrier(kind="MUTATE", tag="ATOR_STATE", payload={"ator_id": node_id, "state": new_state}),
            context={}
        )
        
        event_dict = asdict(trigger_event) if hasattr(trigger_event, '__dataclass_fields__') else trigger_event.__dict__
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(event_dict)})
        log.info(" └─ 🌌 Spatial distortion applied. Local phase convergence expected.")

    async def inject_forced_rupture(self):
        """BoundObserver의 한계치를 돌파하는 텐션을 쏴서 강제로 우주를 찢음 (Epoch.flip)"""
        log.info("💥 [Rupture] Forcing systemic tension overload (Tension = 1.5)...")
        
        trigger_event = PsiEvent(
            event_id=next_id(), source_id="debug.shell", scope="SYSTEMIC", parent_id=None, tick=1, phase_id=0,
            carrier=PsiCarrier(kind="INJECT", tag="TENSION_SPIKE", payload={"target_node": "0", "tension": 1.5}),
            context={}
        )
        
        event_dict = asdict(trigger_event) if hasattr(trigger_event, '__dataclass_fields__') else trigger_event.__dict__
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(event_dict)})
        log.info(" └─ ⚡ Overload injected. Waiting for BoundObserver -> Epoch.flip (Rupture) log.")