# fiber.infra.observer.intent.trajectory
import os
import time
import math
import hashlib
import json
from enum import Enum
from typing import Dict, Any, List, Optional, Mapping
from dataclasses import dataclass, asdict

from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.adapter.sign import NodeSigner
from xphi.kernel.dphi.adapter.ator import AtorAdapter, NodeState, KernelDelta
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.method import DphiMethod
from xphi.watcher.plane.emitter import get_emitter

# =========================================================================
# 1. CORE ENUMS & POLICIES
# =========================================================================

class TensionPhase(Enum):
    NORMAL = 1
    PRE_HEATING = 2
    RUPTURE = 3

@dataclass
class RiskPolicy:
    base_friction_bps: float = 15.0       
    max_allocation_pct: float = 0.20      
    tension_preheat_threshold: float = 5.0   # Z-score(sigma) 대신 위상장(Phase Field) 임계값
    tension_rupture_threshold: float = 15.0  # 발작(Spike) 전조 임계값


# =========================================================================
# 2. DOMAIN DATACLASSES (State & Attestation)
# =========================================================================

@dataclass(frozen=True)
class ArbitrageIntent:
    is_actionable: bool
    optimal_long_venue: str
    optimal_short_venue: str
    expected_yield: float

@dataclass(frozen=True)
class SpreadSnapshot:
    symbol: str
    timestamp: float
    raw_states: Mapping[str, float]  
    net_spread: float

@dataclass(frozen=True)
class TrajectoryDynamics:
    velocity: float
    system_tension: float  # [INTEGRATED] System structural tension (formerly accumulated_stress)
    tension_phase: str     # [INTEGRATED] System tension phase
    is_spiking: bool       # [INTEGRATED] Non-linear systemic rupture (formerly z_score)

@dataclass(frozen=True)
class EngineEvaluation:
    engine_code_hash: str
    composite_sources: Mapping[str, str]
    individual_hashes: Mapping[str, str]
    snapshot: SpreadSnapshot
    dynamics: TrajectoryDynamics
    intent: ArbitrageIntent

@dataclass(frozen=True)
class AttestationContext:
    oracle_id: str
    timestamp: int
    signer_pubkey: str

@dataclass(frozen=True)
class AttestationRecipe:
    time_window_sec: int
    engine_code_hash: str
    composite_sources: Mapping[str, str]

@dataclass(frozen=True)
class AttestationObservation:
    snapshot: SpreadSnapshot
    dynamics: TrajectoryDynamics
    intent: ArbitrageIntent
    individual_hashes: Mapping[str, str]
    observation_root: str

@dataclass(frozen=True)
class AttestationSignature:
    canonical_root: str
    signature: str

@dataclass(frozen=True)
class SealedTrajectoryReceipt:
    context: AttestationContext
    recipe: AttestationRecipe
    observation: AttestationObservation
    attestation: AttestationSignature

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =========================================================================
# 3. UTILITIES & OBSERVERS
# =========================================================================

def _get_module_source_hash(file_path: str) -> str:
    """Extracts the SHA-256 hash of the pure source code to prove execution identity."""
    path = os.path.abspath(file_path)
    if path.endswith('.pyc'):
        path = path[:-1]
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

class FundingRateComparator:
    """Evaluates raw states to form SpreadSnapshots and actionable Intents."""
    MIN_ACTIONABLE_SPREAD = 0.0005 

    @property
    def comparator_code_hash(self) -> str:
        return _get_module_source_hash(__file__)

    @classmethod
    def evaluate(
        cls, 
        symbol: str, 
        observations: Dict[str, Dict[str, Any]]
    ) -> tuple[SpreadSnapshot, ArbitrageIntent]:
        if len(observations) < 2:
            raise ValueError("Spread comparison requires at least 2 distinct data sources.")

        raw_states = {arn: data["rate"] for arn, data in observations.items()}
        highest_funding_arn = max(raw_states, key=raw_states.get)
        lowest_funding_arn = min(raw_states, key=raw_states.get)
        
        net_spread = raw_states[highest_funding_arn] - raw_states[lowest_funding_arn]
        is_actionable = net_spread >= cls.MIN_ACTIONABLE_SPREAD
        base_timestamp = list(observations.values())[0]["time"]

        snapshot = SpreadSnapshot(
            symbol=symbol,
            timestamp=base_timestamp,
            raw_states=raw_states,
            net_spread=net_spread
        )

        intent = ArbitrageIntent(
            is_actionable=is_actionable,
            optimal_short_venue=highest_funding_arn,
            optimal_long_venue=lowest_funding_arn,
            expected_yield=net_spread
        )
        return snapshot, intent


class DynamicalTensionObserver:
    """
    Rust 기반 topos.dynamics (FitzHugh-Nagumo 등) 커널을 Broker를 통해 호출하여
    비선형 시장 스트레스(Spiking 및 시스템 텐션)를 추적하는 비동기 관찰기.
    """
    def __init__(self, broker: DphiBroker, policy: RiskPolicy):
        self.broker = broker
        self.policy = policy
        
        # 텐션 붕괴 감지를 위한 FitzHugh-Nagumo 신경망 발화 커널 사용
        self.kernel_type = "kernel.fitzhugh"
        self.params = {"global_coupling": 0.8, "a": 0.7, "b": 0.8, "fh_epsilon": 0.08}

    async def evaluate_tension_async(
        self, current_states: Dict[str, float], dt: float = 0.1
    ) -> tuple[TensionPhase, float, bool]:
        
        # 1. 펀딩비 원시 데이터를 Phase/Omega를 갖춘 NodeState 객체로 변환
        node_states: Dict[str, NodeState] = {}
        for arn, rate in current_states.items():
            node_states[arn] = AtorAdapter.build_node_state(
                phase=rate, 
                omega=rate * 10.0, 
                state="NORMAL", 
                tension=0.0
            )

        # 2. WASM 연산을 위한 Payload 구성
        payload = AtorAdapter.Dynamics.build_dynamics_ffi_payload(
            states=node_states, 
            kernel_type=self.kernel_type, 
            params=self.params, 
            dt=dt
        )
        
        # 3. 비동기 DphiBroker 호출 (SYSTEM 티어로 초고속 최우선 처리)
        res = await self.broker.invoke(
            method=DphiMethod.PROCESS_FIELD_DYNAMICS, 
            payload=json.dumps(payload), 
            tier="SYSTEM"
        )
        
        if not res.success:
            return TensionPhase.NORMAL, 0.0, False

        # 4. WASM 커널 결과(KernelDelta) 파싱 및 시스템 상태 평가
        deltas: Dict[str, KernelDelta] = AtorAdapter.Dynamics.parse_dynamics_result(json.loads(res.output))
        
        max_tension = max([d.target_tension for d in deltas.values()] + [0.0])
        is_spiking = any([d.is_spiking for d in deltas.values() if d.is_spiking is not None])
        
        # 5. 상태 페이즈 맵핑
        if is_spiking or max_tension >= self.policy.tension_rupture_threshold:
            return TensionPhase.RUPTURE, max_tension, True
        elif max_tension >= self.policy.tension_preheat_threshold:
            return TensionPhase.PRE_HEATING, max_tension, False
            
        return TensionPhase.NORMAL, max_tension, False


# =========================================================================
# 4. ORCHESTRATION ENGINES
# =========================================================================

class ProvableTrajectoryEngine:
    """
    @desc: Evaluates multi-source inputs, monitors non-linear structural tension, 
           and guarantees deterministic outputs suitable for cryptographic sealing.
    """
    def __init__(self, broker: DphiBroker):
        self.comparator = FundingRateComparator()
        self.tension_observer = DynamicalTensionObserver(broker, RiskPolicy())

    async def execute_flow_async(
        self, symbol: str, target_arns: List[str], time_window_sec: int, fetch_time: int
    ) -> EngineEvaluation:
        
        ## @desc: Placeholder for external Oracle/DB fetching logic.
        mock_observations = {
            target_arns[0]: {"rate": 0.01, "time": fetch_time},
            target_arns[1]: {"rate": -0.05, "time": fetch_time}
        }
        
        # 1. Compute Base Spread & Intent
        snapshot, intent = self.comparator.evaluate(symbol, mock_observations)
        
        # 2. Evaluate Dynamical Tension via WASM (Async)
        raw_states_for_observer = {arn: data["rate"] for arn, data in mock_observations.items()}
        tension_phase, sys_tension, is_spiking = await self.tension_observer.evaluate_tension_async(raw_states_for_observer)
        
        # 3. Assemble integrated Trajectory Dynamics
        dynamics = TrajectoryDynamics(
            velocity=0.001, 
            system_tension=sys_tension,
            tension_phase=tension_phase.name,
            is_spiking=is_spiking
        )
        
        return EngineEvaluation(
            engine_code_hash=self.comparator.comparator_code_hash,
            composite_sources={arn: "source_hash_mock" for arn in target_arns},
            individual_hashes={arn: "obs_hash_mock" for arn in target_arns},
            snapshot=snapshot,
            dynamics=dynamics,
            intent=intent
        )


class TrajectoryOracleReceptor:
    """
    @desc: Acts as the terminal boundary that physically signs the evaluated data, 
           transitioning it from local state to a globally verifiable proof.
    """
    def __init__(self, broker: DphiBroker, signer: Optional[NodeSigner] = None, logger: Optional[Any] = None):
        self.signer = signer or NodeSigner.get_instance()
        self.log = logger or get_emitter("exchange.trajectory")
        self.engine = ProvableTrajectoryEngine(broker)

    async def fetch_and_seal_async(
        self, 
        symbol: str, 
        target_arns: List[str], 
        time_window_sec: int = 28800
    ) -> SealedTrajectoryReceipt:
        
        if len(target_arns) < 2:
            raise ValueError("[Topology Error] Analysis requires at least 2 dimensions.")

        fetch_time = int(time.time())
        self.log.info(f"[Receptor] Tracking vector trajectory for {symbol} | Window: {time_window_sec}s")
        
        ## @step.1: Trigger the deterministic evaluation engine asynchronously.
        eval_result = await self.engine.execute_flow_async(symbol, target_arns, time_window_sec, fetch_time)
        
        ## @step.2: Construct the attestation metadata subtree to secure the execution context.
        context = AttestationContext(
            oracle_id="trajectory_receptor_v3.0_async",
            timestamp=fetch_time,
            signer_pubkey=getattr(self.signer, 'pubkey_hex', 'UNKNOWN')
        )
        
        recipe = AttestationRecipe(
            time_window_sec=time_window_sec,
            engine_code_hash=eval_result.engine_code_hash,
            composite_sources=eval_result.composite_sources
        )

        ## @step.3: Anchor the observation data into a single root to prevent partial data tampering.
        obs_components = {
            "snapshot": asdict(eval_result.snapshot),
            "dynamics": asdict(eval_result.dynamics),
            "intent": asdict(eval_result.intent)
        }
        observation_root = hashlib.sha256(StateAdapter.to_canonical_bytes(obs_components)).hexdigest()

        observation = AttestationObservation(
            snapshot=eval_result.snapshot,
            dynamics=eval_result.dynamics,
            intent=eval_result.intent,
            individual_hashes=eval_result.individual_hashes,
            observation_root=observation_root
        )
        
        ## @step.4: Generate the canonical root of the entire payload and attach the cryptographic signature.
        recipe_root = hashlib.sha256(StateAdapter.to_canonical_bytes(asdict(recipe))).hexdigest()
        attestation_payload = {
            "context": asdict(context),
            "recipe_root": recipe_root,
            "observation_root": observation.observation_root
        }
        
        canonical_root = StateAdapter.to_canonical_bytes(attestation_payload)
        signature = self.signer.sign_payload(canonical_root)
        attestation = AttestationSignature(
            canonical_root=hashlib.sha256(canonical_root).hexdigest(),
            signature=signature
        )

        self.log.info(f"  └─ [Trajectory Seal] Sig: {signature[:12]}... | Root: {attestation.canonical_root[:8]}")
        return SealedTrajectoryReceipt(
            context=context,
            recipe=recipe,
            observation=observation,
            attestation=attestation
        )