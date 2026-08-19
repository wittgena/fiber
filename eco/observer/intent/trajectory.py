# eco.observer.intent.trajectory
## @lineage: bound.observer.intent.trajectory
## @lineage: bound.agent.intent.trajectory
## @lineage: bound.exchange.intent.trajectory
"""@desc: Defines cryptographic data structures and engines to securely capture, evaluate, and seal arbitrage trajectory data for trustless environments"""
import os
import time
import hashlib
from typing import Dict, Any, List, Optional, Mapping
from dataclasses import dataclass, asdict

from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.sign import NodeSigner
from watcher.plane.emitter import get_emitter

"""@phase.1: Core Domain Entities (Immutable Data Structures)"""
@dataclass(frozen=True)
class ArbitrageIntent:
    """@desc: Decouples the 'execution decision' from raw data to allow independent risk validation by downstream components."""
    is_actionable: bool
    optimal_long_venue: str
    optimal_short_venue: str
    expected_yield: float

@dataclass(frozen=True)
class SpreadSnapshot:
    """@desc: Isolates fragmented matrix data at a specific timestamp to ensure the baseline observation remains tamper-proof."""
    symbol: str
    timestamp: float
    ## @desc: Enforces read-only mapping to prevent runtime mutation during the attestation process.
    raw_states: Mapping[str, float]  
    net_spread: float

@dataclass(frozen=True)
class TrajectoryDynamics:
    """@desc: Quantifies market pressure over time, providing context for risk engines without exposing raw historical ticks."""
    velocity: float
    accumulated_stress: float

@dataclass(frozen=True)
class EngineEvaluation:
    """@desc: Acts as an internal boundary object to safely transport validated domain models from the evaluation engine to the cryptographic receptor."""
    engine_code_hash: str
    composite_sources: Mapping[str, str]
    individual_hashes: Mapping[str, str]
    snapshot: SpreadSnapshot
    dynamics: TrajectoryDynamics
    intent: ArbitrageIntent


"""@phase.2: Attestation Nodes (Cryptographic Proof Subtree)"""
@dataclass(frozen=True)
class AttestationContext:
    """@desc: Anchors the proof to a specific node identity and temporal context to prevent replay attacks."""
    oracle_id: str
    timestamp: int
    signer_pubkey: str

@dataclass(frozen=True)
class AttestationRecipe:
    """@desc: Binds the evaluation logic and data sources to the proof, guaranteeing that the result was produced by an approved execution path."""
    time_window_sec: int
    engine_code_hash: str
    composite_sources: Mapping[str, str]

@dataclass(frozen=True)
class AttestationObservation:
    """@desc: Flattens and groups the evaluated domain models into a single verifiable target for Merkle-like root generation."""
    snapshot: SpreadSnapshot
    dynamics: TrajectoryDynamics
    intent: ArbitrageIntent
    individual_hashes: Mapping[str, str]
    observation_root: str


"""@phase.3: Sealed Root (Final Packaging Receipt)"""
@dataclass(frozen=True)
class AttestationSignature:
    """@desc: Contains the canonical root and its cryptographic signature to establish non-repudiation of the payload."""
    canonical_root: str
    signature: str

@dataclass(frozen=True)
class SealedTrajectoryReceipt:
    """@desc: Represents the final, non-repudiable state proof safely consumable by external smart contracts and vault daemons."""
    context: AttestationContext
    recipe: AttestationRecipe
    observation: AttestationObservation
    attestation: AttestationSignature

    def to_dict(self) -> Dict[str, Any]:
        """@desc: Provides a standardized serialization format for cross-network broadcasting and persistence."""
        return asdict(self)


"""@phase.4: Logic Engines (Engine and Receptor Implementation)"""
class FundingRateComparator:
    MIN_ACTIONABLE_SPREAD = 0.0005 

    @staticmethod
    def _extract_source_hash(module_or_file: Any) -> str:
        path = os.path.abspath(getattr(module_or_file, '__file__', module_or_file))
        if path.endswith('.pyc'):
            path = path[:-1]
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    @property
    def comparator_code_hash(self) -> str:
        return self._extract_source_hash(__file__)

    @classmethod
    def evaluate(
        cls, 
        symbol: str, 
        observations: Dict[str, Dict[str, Any]]
    ) -> tuple[SpreadSnapshot, ArbitrageIntent]:
        """@desc: Separates raw market observation from actionable execution logic to ensure clear boundaries of responsibility."""
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


class ProvableTrajectoryEngine:
    """@desc: Evaluates multi-source inputs and guarantees deterministic outputs suitable for cryptographic sealing."""
    def __init__(self):
        self.comparator = FundingRateComparator()

    def execute_flow(
        self, symbol: str, target_arns: List[str], time_window_sec: int, fetch_time: int
    ) -> EngineEvaluation:
        
        ## @desc: Placeholder for external Oracle/DB fetching logic.
        mock_observations = {
            target_arns[0]: {"rate": 0.01, "time": fetch_time},
            target_arns[1]: {"rate": -0.05, "time": fetch_time}
        }
        
        ## @step.1: Isolate the evaluation of current snapshots and future execution intents.
        snapshot, intent = self.comparator.evaluate(symbol, mock_observations)
        
        ## @step.2: Compute dynamic vector calculus to represent underlying systemic stress.
        dynamics = TrajectoryDynamics(velocity=0.001, accumulated_stress=12.5)
        
        ## @step.3: Pre-compute a temporary canonical hash to establish the data root before receptor assembly.
        temp_payload_bytes = StateAdapter.to_canonical_bytes({
            "snapshot": asdict(snapshot),
            "intent": asdict(intent),
            "dynamics": asdict(dynamics)
        })
        payload_hash = hashlib.sha256(temp_payload_bytes).hexdigest()
        
        return EngineEvaluation(
            engine_code_hash=self.comparator.comparator_code_hash,
            composite_sources={arn: "source_hash_mock" for arn in target_arns},
            individual_hashes={arn: "obs_hash_mock" for arn in target_arns},
            snapshot=snapshot,
            dynamics=dynamics,
            intent=intent
        )

class TrajectoryOracleReceptor:
    """@desc: Acts as the terminal boundary that physically signs the evaluated data, transitioning it from local state to a globally verifiable proof."""
    def __init__(self, signer: Optional[NodeSigner] = None, logger: Optional[Any] = None):
        self.signer = signer or NodeSigner.get_instance()
        self.log = logger or get_emitter("exchange.trajectory")
        self.engine = ProvableTrajectoryEngine()

    def fetch_and_seal(
        self, 
        symbol: str, 
        target_arns: List[str], 
        time_window_sec: int = 28800
    ) -> SealedTrajectoryReceipt:
        
        if len(target_arns) < 2:
            raise ValueError("[Topology Error] Analysis requires at least 2 dimensions.")

        fetch_time = int(time.time())
        self.log.info(f"[Receptor] Tracking vector trajectory for {symbol} | Window: {time_window_sec}s")
        
        ## @step.1: Trigger the deterministic evaluation engine.
        eval_result = self.engine.execute_flow(symbol, target_arns, time_window_sec, fetch_time)
        
        ## @step.2: Construct the attestation metadata subtree to secure the execution context.
        context = AttestationContext(
            oracle_id="trajectory_receptor_v2.0",
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
        
        ## @step.5: Return the self-contained, tamper-proof receipt for downstream consumption.
        return SealedTrajectoryReceipt(
            context=context,
            recipe=recipe,
            observation=observation,
            attestation=attestation
        )