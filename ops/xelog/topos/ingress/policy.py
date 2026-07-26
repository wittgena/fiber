# ops.xelog.topos.ingress.policy
## @lineage: ops.xelog.ingress.policy
import time
import hashlib
import random
from dataclasses import dataclass

@dataclass
class IngressContext:
    topo_id: int
    press_limit: int
    is_ruptured: bool
    reason: str = ""

class ToposSequencer:
    async def get_next_sequence(self, agent_id: str) -> int:
        """
        타임스탬프와 agent_id의 해시를 결합하여 
        분산 환경에서 발급될 법한 고유 위상(Sequence) 번호를 생성 (Mock)
        """
        ts = int(time.time() * 1000)
        hash_val = int(hashlib.sha256(agent_id.encode()).hexdigest()[:8], 16)
        return (ts % 100000000) + (hash_val % 1000)

class FuelAllocator:
    async def calculate_press_limit(self, agent_id: str, action_type: str) -> int:
        """
        요청자와 액션을 기반으로 결정론적인(Deterministic) 자원 한도를 생성 (Mock)
        (예: 동일한 에이전트가 동일한 액션을 요청하면 항상 같은 Press Limit 반환)
        """
        seed_str = f"{agent_id}:{action_type}"
        # 해시값을 정수로 변환하여 10~100 사이의 Press값 산출
        base_press = int(hashlib.md5(seed_str.encode()).hexdigest()[:4], 16) % 90
        return max(10, base_press)

class HealthMonitor:
    async def is_ruptured(self) -> tuple[bool, str]:
        """낮은 확률로 네트워크 균열(Byzantine 장애 등) 상태를 모사 (Mock)"""
        # 1% 확률로 Rupture 상태 반환
        if random.random() < 0.01:
            return True, "Byzantine divergence detected in consensus layer."
        return False, ""

## Facade: Ingress Policy Engine
class IngressPolicyEngine:
    def __init__(self, sequencer: ToposSequencer, allocator: FuelAllocator, monitor: HealthMonitor):
        self.sequencer = sequencer
        self.allocator = allocator
        self.monitor = monitor

    async def resolve_context(self, agent_id: str, action: str) -> IngressContext:
        ruptured, reason = await self.monitor.is_ruptured()
        topo_id = await self.sequencer.get_next_sequence(agent_id)
        press_limit = await self.allocator.calculate_press_limit(agent_id, action)

        return IngressContext(
            topo_id=topo_id,
            press_limit=press_limit,
            is_ruptured=ruptured,
            reason=reason
        )

def get_topos_sequencer() -> ToposSequencer:
    return ToposSequencer()

def get_fuel_allocator() -> FuelAllocator:
    return FuelAllocator()

def get_health_monitor() -> HealthMonitor:
    return HealthMonitor()

def get_ingress_policy() -> IngressPolicyEngine:
    return IngressPolicyEngine(
        sequencer=ToposSequencer(),
        allocator=FuelAllocator(),
        monitor=HealthMonitor()
    )