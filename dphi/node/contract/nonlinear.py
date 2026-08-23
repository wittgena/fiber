# dphi.node.contract.nonlinear
## @lineage: phase.contract.nonlinear
## @lineage: phase.anchor.contract.nonlinear
## @lineage: phase.dphi.contract.nonlinear
## @lineage: dphi.contract.nonlinear
"""@desc: Advanced Nonlinear Dynamics Ecosystem for Tail Risk Prediction & Actuation Feedback"""
import math
from typing import List, Dict, Optional, Any
from pydantic import BaseModel

from xphi.arch.contract.interface import IDynamicsKernel, ICriticalDetector, ISystemRegime, IPhaseField, IPhaseAtor
from xphi.arch.contract.event.psi import PsiEvent, PsiCarrier
from xphi.arch.contract.registry.unified import contract
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("contract.nonlinear")

# =========================================================================
# 1. ADVANCED CONFIGURATIONS
# =========================================================================
class NonlinearConfig(BaseModel):
    global_coupling: float = 0.8
    inertia_mass: float = 1.0          # 관성 모델: 군중 심리 (오버슈팅)
    inertia_friction: float = 0.1      # 관성 모델: 시장 자정 능력
    fh_epsilon: float = 0.08           # 발화 모델: 발화 후 회복 속도
    frustration_alpha: float = 0.2     # 좌절 모델: 구조적 마찰 비용

# =========================================================================
# 2. EXTENDED DIMENSION KERNELS (Substrate)
# =========================================================================
@contract.ator("kernel.inertia", role="kernel")
class InertiaKernel(IDynamicsKernel):
    """@role: 관성 쿠라모토 모델 (군중 심리 및 3σ 오버슈팅 가속도 산출)"""
    def __init__(self, **kwargs):
        self.config = NonlinearConfig(**kwargs)

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)
        K = self.config.global_coupling
        mass = self.config.inertia_mass
        gamma = self.config.inertia_friction

        for i_id, i_data in states.items():
            # 차원 확장: 속도(velocity) 변수가 없으면 초기화
            v = i_data.get("velocity", i_data.get("omega", 0.0))
            coupling_force = sum(math.sin(j_data["phase"] - i_data["phase"]) for j_id, j_data in states.items() if i_id != j_id)

            # 가속도(Acceleration) 계산
            dv = (i_data["omega"] - (gamma * v) + (K / total_nodes) * coupling_force) / mass
            new_v = v + dv * dt
            
            states[i_id]["velocity"] = new_v # 상태 영속화
            
            # 가속도 자체가 텐션이 됨 (가속도가 클수록 시장이 급격히 쏠림)
            tension = abs(dv) * 5.0
            deltas[i_id] = {"d_phase": new_v * dt, "target_tension": tension}

        return deltas

@contract.ator("kernel.fitzhugh", role="kernel")
class FitzHughNagumoKernel(IDynamicsKernel):
    """@role: 피츠휴-나구모 모델 (유동성 포화 및 연쇄 청산 4σ 스파이크 발화)"""
    def __init__(self, **kwargs):
        self.config = NonlinearConfig(**kwargs)
        self.a, self.b = 0.7, 0.8

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)
        K = self.config.global_coupling

        for i_id, i_data in states.items():
            # 차원 확장: 막전위(Voltage)와 회복력(Recovery)
            v = i_data["phase"] - math.pi 
            w = i_data.get("recovery", 0.0)

            coupling_force = sum((j_data["phase"] - math.pi) - v for j_id, j_data in states.items() if i_id != j_id)

            dv = (v - (v**3)/3.0 - w + i_data["omega"] + (K / total_nodes) * coupling_force) * dt * 5.0
            dw = self.config.fh_epsilon * (v + self.a - self.b * w) * dt * 5.0

            states[i_id]["recovery"] = w + dw
            
            # 임계점(1.0) 돌파 시 텐션 100.0으로 폭발 (Spiking)
            is_spiking = v > 1.0
            tension = 100.0 if is_spiking else abs(dv)
            
            # 메타데이터 태그 추가 (Watcher가 읽을 수 있도록)
            states[i_id]["is_spiking"] = is_spiking

            deltas[i_id] = {"d_phase": dv, "target_tension": tension}

        return deltas


# =========================================================================
# 3. DERIVATIVE WATCHERS (Perception)
# =========================================================================
@contract.ator("watcher.kinetic", role="watcher")
class KineticWatcher(ICriticalDetector):
    """@role: 가속도(모멘텀) 급증을 감지하여 3σ 오버슈팅 전조(Pre-heating) 경고"""
    def __init__(self, accel_threshold: float = 8.0):
        self.accel_threshold = accel_threshold

    def evaluate(self, field: IPhaseField, history: List[Any], current_tick: int) -> Optional[PsiEvent]:
        # 가속도의 총합(시장의 전체 모멘텀)을 측정
        states = field.get_state()
        total_momentum = sum(data.get("tension", 0.0) for data in states.values()) / max(1, len(states))
        
        if total_momentum >= self.accel_threshold:
            log.warning(f"[KineticWatcher] ⚠️ Systemic Momentum Surge Detected: {total_momentum:.2f} (Pre-heating)")
            carrier = PsiCarrier(kind="TAIL_RISK", tag="3_SIGMA_OVERSHOOT", payload={"momentum": total_momentum})
            return PsiEvent(
                event_id=f"kinetic-{current_tick}", parent_id=None, source_id="watcher.kinetic",
                scope="SYSTEMIC", tick=current_tick, carrier=carrier, context={"risk_level": "PRE_HEATING"}
            )
        return None

@contract.ator("watcher.avalanche", role="watcher")
class AvalancheWatcher(ICriticalDetector):
    """@role: 다수 노드의 동시다발적 발화(Spike)를 감지하여 연쇄 청산(Cascading) 선고"""
    def __init__(self, cascade_ratio: float = 0.2):
        self.cascade_ratio = cascade_ratio

    def evaluate(self, field: IPhaseField, history: List[Any], current_tick: int) -> Optional[PsiEvent]:
        states = field.get_state()
        total_nodes = len(states)
        
        # Kernel이 남겨둔 'is_spiking' 차원을 읽음
        spiking_nodes = [n_id for n_id, data in states.items() if data.get("is_spiking", False)]
        current_ratio = len(spiking_nodes) / max(1, total_nodes)

        if current_ratio >= self.cascade_ratio:
            log.critical(f"[AvalancheWatcher] 🚨 CASCADING LIQUIDATION! {len(spiking_nodes)} nodes erupting.")
            carrier = PsiCarrier(kind="RUPTURE", tag="CASCADING_LIQUIDATION", payload={"infected_nodes": spiking_nodes})
            return PsiEvent(
                event_id=f"avalanche-{current_tick}", parent_id=None, source_id="watcher.avalanche",
                scope="GLOBAL", tick=current_tick, carrier=carrier, context={"risk_level": "RUPTURE"}
            )
        return None


# =========================================================================
# 4. ACTUATION FEEDBACK REGIME (Stabilization)
# =========================================================================
@contract.ator("regime.cooling", role="regime")
class CoolingRegime(ISystemRegime):
    """
    @role: Risk Daemon이 성공적으로 시장에 개입(차익/방어)했을 때 발동하여, 
           내부 물리법칙(가속도, 발화 변수)을 인위적으로 냉각(Cooling)시키는 피드백 체제
    """
    def __init__(self, **kwargs):
        self.cooling_factor = kwargs.get("cooling_factor", 0.5)

    def modify_field(self, field: IPhaseField) -> None:
        """데몬의 개입 이후, 시장의 과열된 관성과 텐션을 반감시킴"""
        states = field.get_state()
        for node_id, data in states.items():
            # 텐션 소각
            data["tension"] *= self.cooling_factor
            
            # 관성 모델의 속도 강제 감속
            if "velocity" in data:
                data["velocity"] *= self.cooling_factor
                
            # 발화 모델의 신경망 진정
            if "recovery" in data:
                data["recovery"] = 0.0
                data["is_spiking"] = False

        log.info(f"[CoolingRegime] ❄️ Actuator feedback received. Field tension & momentum cooled by {self.cooling_factor*100}%.")

    def constrain_ator(self, ator: IPhaseAtor) -> None:
        pass

    def filter_event(self, event: PsiEvent) -> Optional[PsiEvent]:
        # 시스템이 냉각되는 동안 발생하는 잡음 이벤트는 무시
        return event if event.carrier.kind != "TAIL_RISK" else None