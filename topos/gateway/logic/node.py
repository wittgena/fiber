# topos.gateway.logic.node
## @lineage: void.topos.gateway.logic.node
## @lineage: topos.edge.gateway.logic.node
## @lineage: edge.gateway.logic.node
## @lineage: fiber.gateway.logic.node
## @lineage: meta.logic.gate.node
## @lineage: logict.flow.gate.node
import math
import random
from typing import Dict, Any
from arch.contract.registry.unified import contract
from arch.contract.interface import IPhaseAtor, IPhaseField
from arch.contract.event.psi import PsiEvent, PsiCarrier

@contract.ator("gate.node")
class GateNode(IPhaseAtor):
    """
    @role: Kernel(AtorSensor)의 물리적 압력과 외부의 의미적 이벤트(Psi) 사이의 통역자
    """
    def __init__(self, **kwargs):
        self._id = kwargs.get("node_id", "unknown")
        self.initial_state = kwargs.get("initial_state", "NORMAL")
        self.tolerance_threshold = kwargs.get("tolerance_threshold", 8.0) # 인지 부조화 한계점

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> Dict[str, Any]:
        return {"status": self.initial_state}

    def set_state(self, new_state: str) -> None:
        pass

    async def react(self, event: PsiEvent, field: IPhaseField, bus: Any) -> None:
        # 1. Field(공유 공간)에서 나와 타인들의 현재 물리 상태를 가져옴
        states = field.get_state()
        my_data = states.get(self.ator_id)
        if not my_data: return

        # AtorSensor(Kernel)가 계산한 나의 인지 부조화(Tension)가 한계를 넘었을 때
        if my_data["tension"] >= self.tolerance_threshold:
            if my_data["state"] == "NORMAL":
                # 극심한 인지 부조화로 인해 극단주의자(REFLECTOR)로 변모하거나
                my_data["state"] = "REFLECTOR"
                my_data["phase"] += math.pi  # 위상을 완전히 반대로 뒤집음 (반발)
                
                # 비명(구조 요청) 이벤트를 글로벌 큐로 발행
                alert_carrier = PsiCarrier(kind="COGNITIVE_DISSONANCE", tag=self.ator_id, payload={})
                await bus.publish(PsiEvent(
                    event_id=event.event_id,
                    parent_id=event.event_id,
                    source_id=self.ator_id,
                    scope="GLOBAL",
                    tick=event.tick,
                    carrier=alert_carrier
                ))

        if event.carrier.kind == "ATTRACT_PHASE":
            target_phase = event.carrier.payload.get("phase", my_data["phase"])
            if my_data["state"] == "NORMAL":
                my_data["phase"] = target_phase