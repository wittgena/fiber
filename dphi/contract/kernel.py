# dphi.contract.kernel
"""
@module: dphi.contract.kernel
@desc: Physical & Cognitive Dynamics Kernels for Phase Topology Evolution
"""
import math
import random
import json
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
from pydantic import BaseModel, Field

from arch.contract.interface import IDynamicsKernel
from arch.contract.registry.unified import contract

# =========================================================================
# CONFIGURATION MANIFOLDS
# =========================================================================
class TransitionPolicy(BaseModel):
    """Meta-transition rule: event → state reconfiguration"""
    rupture_to: str = "ATTRACTOR"
    reset_tension: bool = True

class KernelConfig(BaseModel):
    """Φ-dynamics parameters: global coupling / dissipation"""
    type: str = "kuramoto"
    global_coupling: float = 0.8
    dissipation_rate: float = 0.95
    # [추가됨] Advanced Kernel Parameters
    inertia_mass: float = 1.0          # 관성: 시장의 군중 심리 (오버슈팅 팩터)
    inertia_friction: float = 0.1      # 마찰: 시장의 자정 능력
    frustration_alpha: float = 0.2     # 좌절: 구조적 슬리피지 및 마찰 비용

class AtorConfig(BaseModel):
    """ψ-behavior parameters: local transduction rules"""
    base_coupling: float = 0.5
    reflector_phase_boost: float = 0.5
    attractor_gain: float = 1.5

class FieldConfig(BaseModel):
    """Φ-initial condition: phase manifold distribution"""
    size: int = 20
    init_phase_range: list = Field(default_factory=lambda: [0.0, 2 * math.pi])
    omega_range: list = Field(default_factory=lambda: [0.1, 0.3])

class WatcherConfig(BaseModel):
    """∂Φ-threshold: critical surface (rupture boundary)"""
    rupture_limit: float = 4.0

class RuntimeConfig(BaseModel):
    """τ-control: temporal resolution and execution horizon"""
    dt: float = 0.1
    max_ticks: int = 100
    sleep_interval: float = 0.1
    seed: int = 42 

class SystemConfig(BaseModel):
    """external configuration manifold (injectable structure)"""
    kernel: KernelConfig = Field(default_factory=KernelConfig)
    ator: AtorConfig = Field(default_factory=AtorConfig)
    field: FieldConfig = Field(default_factory=FieldConfig)
    watcher: WatcherConfig = Field(default_factory=WatcherConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    policy: TransitionPolicy = Field(default_factory=TransitionPolicy)

    @classmethod
    def from_json(cls, json_str: str) -> 'SystemConfig':
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SystemConfig':
        return cls(
            kernel=KernelConfig(**data.get("kernel", {})),
            ator=AtorConfig(**data.get("ator", {})),
            field=FieldConfig(**data.get("field", {})),
            watcher=WatcherConfig(**data.get("watcher", {})),
            runtime=RuntimeConfig(**data.get("runtime", {})),
            policy=TransitionPolicy(**data.get("policy", {}))
        )

    def to_json(self) -> str:
        return json.dumps(self.model_dump(), indent=2)


# =========================================================================
# STANDARD KERNELS (Legacy)
# =========================================================================
@contract.ator("sensor.ator", role="kernel")
class SensorAtor(IDynamicsKernel):
    """Φ-evolution kernel: Multi-Ator Cognitive Consensus & Clustering"""
    def __init__(self, **kwargs):
        self.config = kwargs.get("config") or KernelConfig(**kwargs)
        self.trust_radius = kwargs.get("trust_radius", 1.0)
        self.repulsion_factor = kwargs.get("repulsion_factor", 0.2)

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)

        for i_id, i_data in states.items():
            if i_data.get("state") in ["ATTRACTOR", "REFLECTOR"]:
                deltas[i_id] = {"d_phase": (i_data["omega"] * 0.1) * dt, "target_tension": 0.0}
                continue

            consensus_force = 0.0
            cognitive_dissonance = 0.0

            for j_id, j_data in states.items():
                if i_id == j_id: continue
                diff = (j_data["phase"] - i_data["phase"] + math.pi) % (2 * math.pi) - math.pi
                distance = abs(diff)

                if distance < self.trust_radius:
                    consensus_force += math.sin(diff) * self.config.global_coupling
                else:
                    consensus_force -= math.sin(diff) * self.repulsion_factor
                    cognitive_dissonance += distance 

            d_phase = (i_data["omega"] + (consensus_force / total_nodes)) * dt
            new_tension = min(cognitive_dissonance / total_nodes, 10.0)
            deltas[i_id] = {"d_phase": d_phase, "target_tension": new_tension}

        return deltas

    def render_state(self, states: Dict[str, Dict[str, Any]]) -> str:
        hypotheses = ['🟦', '🟩', '🟨', '🟥']
        visual = [hypotheses[int((s['phase'] / (2 * math.pi)) * 4) % 4] for s in states.values()]
        avg_tension = sum(s['tension'] for s in states.values()) / len(states)
        return f"Dissonance: {avg_tension:.2f} | {''.join(visual)}"

@contract.ator("sensor.kuramoto", role="kernel")
class SensorKuramoto(IDynamicsKernel):
    """Φ-evolution kernel: Standard Global Phase Coupling"""
    def __init__(self, **kwargs):
        self.config = kwargs.get("config") or KernelConfig(**kwargs)

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)

        for i_id, i_data in states.items():
            coupling_force = 0.0
            total_incoherence = 0.0

            if i_data.get("state") not in ["ATTRACTOR", "REFLECTOR"]:
                for j_id, j_data in states.items():
                    if i_id == j_id: continue
                    phase_diff = j_data["phase"] - i_data["phase"]
                    coupling_force += math.sin(phase_diff)
                    total_incoherence += abs(phase_diff)

                d_phase = (i_data["omega"] + (self.config.global_coupling / total_nodes) * coupling_force) * dt
                deltas[i_id] = {"d_phase": d_phase, "target_tension": total_incoherence / total_nodes}
            else:
                deltas[i_id] = {"d_phase": (i_data["omega"] * 1.5) * dt, "target_tension": 0.0}
                
        return deltas

    def render_state(self, states: Dict[str, Dict[str, Any]]) -> str:
        chars = ['🌑', '🌘', '🌗', '🌖', '🌕', '🌔', '🌓', '🌒']
        visual = [chars[int((s['phase'] / (2 * math.pi)) * 8) % 8] for s in states.values()]
        avg_tension = sum(s['tension'] for s in states.values()) / max(1, len(states))
        return f"Tension: {avg_tension:.2f} | {''.join(visual)}"


# =========================================================================
# ADVANCED KERNELS (Tail Risk & Structural Dynamics)
# =========================================================================
@contract.ator("sensor.kuramoto_inertia", role="kernel")
class SensorKuramotoInertia(IDynamicsKernel):
    """
    @desc: 관성 쿠라모토 모델 (Kuramoto with Inertia)
    @role: 군중 심리에 의한 오버슈팅(Overshooting) 및 3σ 이상의 극단적 폭발 텐션 예측
    """
    def __init__(self, **kwargs):
        self.config = kwargs.get("config") or KernelConfig(**kwargs)

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)
        K = self.config.global_coupling
        mass = self.config.inertia_mass
        gamma = self.config.inertia_friction

        for i_id, i_data in states.items():
            # 내부 상태에 velocity(속도) 변수 주입 및 추적
            v = i_data.get("velocity", i_data["omega"])
            coupling_force = 0.0

            for j_id, j_data in states.items():
                if i_id == j_id: continue
                coupling_force += math.sin(j_data["phase"] - i_data["phase"])

            # 가속도(dv) 계산: (자연주파수 - 마찰력 + 군중의 끌어당김) / 관성질량
            dv = (i_data["omega"] - (gamma * v) + (K / total_nodes) * coupling_force) / mass
            new_v = v + dv * dt
            
            # 속도 업데이트 (다음 틱을 위한 내부 상태 저장)
            states[i_id]["velocity"] = new_v

            # 가속도(dv)가 클수록, 즉 시장이 급격히 쏠릴 때 텐션이 폭발적으로 증가
            tension = abs(dv) * 5.0
            deltas[i_id] = {"d_phase": new_v * dt, "target_tension": tension}

        return deltas

@contract.ator("sensor.fitzhugh", role="kernel")
class SensorFitzHughNagumo(IDynamicsKernel):
    """
    @desc: 피츠휴-나구모 모델 (FitzHugh-Nagumo)
    @role: 연쇄 청산(Cascading Liquidations) 및 플래시 크래시(Flash Crash) 발화 감지
    """
    def __init__(self, **kwargs):
        self.config = kwargs.get("config") or KernelConfig(**kwargs)
        self.epsilon = 0.08  # 발화 후 회복 속도
        self.a = 0.7         # 발화 임계점 오프셋
        self.b = 0.8

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)
        K = self.config.global_coupling

        for i_id, i_data in states.items():
            # Phase를 막전위(Voltage)로 차용
            v = i_data["phase"] - math.pi  # -pi ~ pi를 -3 ~ 3 근처로 스케일링
            w = i_data.get("recovery", 0.0)
            I_ext = i_data["omega"]

            # 주변 노드들과의 확산적 결합(Diffusive Coupling)
            coupling_force = sum((j_data["phase"] - math.pi) - v for j_data in states.values() if j_data != i_data)

            # 비선형 신경망 발화 방정식
            dv = (v - (v**3)/3.0 - w + I_ext + (K / total_nodes) * coupling_force) * dt * 5.0
            dw = self.epsilon * (v + self.a - self.b * w) * dt * 5.0

            states[i_id]["recovery"] = w + dw

            # 발화(Spiking) 발생 감지: 막전위가 특정 임계(1.0)를 돌파하면 텐션이 100으로 수직 상승
            is_spiking = v > 1.0
            tension = 100.0 if is_spiking else abs(dv)

            deltas[i_id] = {"d_phase": dv, "target_tension": tension}

        return deltas

@contract.ator("sensor.sakaguchi", role="kernel")
class SensorSakaguchi(IDynamicsKernel):
    """
    @desc: 좌절된 사카구치-쿠라모토 모델 (Sakaguchi-Kuramoto with Frustration)
    @role: 시장 구조적 마찰(슬리피지, 수수료)로 인한 영구적 불완전 동기화 및 잉여 텐션 계산
    """
    def __init__(self, **kwargs):
        self.config = kwargs.get("config") or KernelConfig(**kwargs)

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)
        K = self.config.global_coupling
        alpha = self.config.frustration_alpha # 좌절 파라미터 (구조적 마찰)

        for i_id, i_data in states.items():
            coupling_force = 0.0
            frustration_tension = 0.0

            for j_id, j_data in states.items():
                if i_id == j_id: continue
                phase_diff = j_data["phase"] - i_data["phase"]
                
                # alpha만큼 위상 일치가 어긋나도록 강제 (완벽한 동기화 방해)
                coupling_force += math.sin(phase_diff - alpha)
                frustration_tension += abs(math.sin(phase_diff) - math.sin(phase_diff - alpha))

            d_phase = (i_data["omega"] + (K / total_nodes) * coupling_force) * dt
            
            # 구조적 마찰로 인해 해소되지 못하고 남은 스트레스(Frustration)
            tension = frustration_tension / total_nodes
            deltas[i_id] = {"d_phase": d_phase, "target_tension": tension}

        return deltas