# swarm.mesh.mutator
from __future__ import annotations
import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Protocol, Optional

from watcher.dphi.adapter.state import StateAdapter
from watcher.dphi.broker import WasmBroker, WasmMethod
from watcher.plane.emitter import get_emitter

log = get_emitter("swarm.mutator")

class SwarmMutator:
    """
    @desc: Generates deterministic topological mutations via the WASM kernel based on telemetry tension.
    Calling this with identical contexts across nodes yields bit-exact mutant arrays, ensuring P2P consensus.
    """
    def __init__(self, broker: WasmBroker, mutation_schema: Dict[str, Any]):
        self.broker = broker
        self.mutation_schema = mutation_schema

    async def spawn_next_generation(
        self,
        base_config: Dict[str, Any],
        tension_score: float,
        pop_size: int = 5,
    ) -> List[Dict[str, Any]]:
        
        parent_hash = base_config.get("hash", "genesis")
        
        ## 1. Construct the evolution payload for the WASM kernel
        evolution_payload = {
            "parent_hash": parent_hash,
            "tension_score": tension_score,
            "pop_size": pop_size,
            "base_config": base_config,
            "schema": self.mutation_schema
        }
        
        canonical_payload = StateAdapter.to_canonical_bytes(evolution_payload).decode('utf-8')
        
        ## 2. [EVOLUTION] Discard unstable Python RNG/float math and delegate computation to the WASM kernel
        log.info(f"[Mutator] Requesting deterministic evolution from Kernel (Parent: {parent_hash[:8]}, Tension: {tension_score})")
        res = await self.broker.invoke(WasmMethod.PROCESS_EVOLUTION, canonical_payload)
        
        if not res.success:
            log.error(f"[Mutator] Kernel rejected evolution: {res.error}")
            raise RuntimeError(f"WASM Evolution Failed: {res.error}")
            
        ## 3. Decode the mutant array returned by the kernel
        evolution_result = json.loads(res.output)
        mutants: List[Dict[str, Any]] = evolution_result.get("mutants", [])
        
        if len(mutants) != pop_size:
            log.warning(f"[Mutator] Kernel returned {len(mutants)} mutants, expected {pop_size}. Schema constraint limits?")
            
        log.info(f"[Mutator] Successfully retrieved {len(mutants)} globally consistent mutants from Kernel.")
        return mutants

class DataSharder:
    """
    @desc: Deterministically distributes data to workers during a swarm split.
    @strategy: blake2b hash modulo num_shards (crucial for trustless environments).
    """
    HASH_DIGEST_SIZE = 8

    def shard_corpus(self, corpus_path: Path, num_shards: int) -> List[Path]:
        if num_shards < 1:
            raise ValueError(f"num_shards must be >= 1, got {num_shards}")
        
        tmp_dir = Path(tempfile.mkdtemp(prefix="swarm_shard_")) 
        shard_paths = [tmp_dir / f"shard_{i:04d}.jsonl" for i in range(num_shards)]
        shard_handles = [p.open("w", encoding="utf-8") for p in shard_paths]
        
        try:
            with corpus_path.open("r", encoding="utf-8") as src:
                for line in src:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    ## Deterministic assignment via hashing
                    h = hashlib.blake2b(line.encode("utf-8"), digest_size=self.HASH_DIGEST_SIZE)
                    shard_idx = int.from_bytes(h.digest(), "big") % num_shards
                    shard_handles[shard_idx].write(line + "\n")
        finally:
            for h in shard_handles:
                h.close()

        return shard_paths

class LineageTracker(Protocol):
    """
    @desc: Tracks parent-child relationships between adapters. Serves as the basis for tombstoning.
    """
    def record_birth(self, parent_id: str, child_id: str) -> None: ...
    def find_ancestors(self, adapter_id: str, depth: int = -1) -> list[str]: ...
    def is_tombstoned(self, adapter_id: str) -> bool: ...
    def get_lineage_median_norm(self, adapter_id: str) -> float: ...

class TribunalValidator:
    """
    @desc: Cryptographically and structurally validates the generated mutations (Adapters) via the WASM kernel.
    """
    def __init__(self, broker: WasmBroker, tracker: Optional[LineageTracker] = None):
        self.broker = broker
        self.tracker = tracker

    async def validate_weight_integrity(self, adapter_payload: dict) -> bool:
        """
        [EVOLUTION] Delegates integrity validation to the WASM kernel instead of local Python computation.
        WASM verifies signatures, calculates L2 Norm spikes, and returns a deterministic outcome.
        """
        canonical_payload = StateAdapter.to_canonical_bytes(adapter_payload).decode('utf-8')
        
        log.info("[Tribunal] Delegating integrity validation to WASM Kernel...")
        res = await self.broker.invoke(WasmMethod.VERIFY_BUILD_LINEAGE, canonical_payload)
        
        if res.success:
            result_data = json.loads(res.output)
            is_valid = result_data.get("is_valid", False)
            if not is_valid:
                log.warning(f"[Tribunal] Adapter rejected by Kernel: {result_data.get('reason')}")
            return is_valid
        
        log.error(f"[Tribunal] Validation computation crashed: {res.error}")
        return False

    async def certify(self, adapter_payload: dict) -> dict:
        """
        @desc: 기존의 모의 래퍼(Validated)를 제거하고, 실제로 WASM 엔진을 통해 검증을 수행합니다.
               무결성 검증을 통과한 페이로드만 반환하며, 실패할 경우 런타임 예외를 발생시킵니다.
        """
        is_valid = await self.validate_weight_integrity(adapter_payload)
        if not is_valid:
            log.error("[Tribunal] Certification failed. Invalid mutation detected.")
            raise ValueError("Adapter certification failed due to WASM Kernel rejection.")
            
        log.info("[Tribunal] Adapter certification successful.")
        return adapter_payload