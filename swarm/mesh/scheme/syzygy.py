# swarm.mesh.scheme.syzygy
import json
import time
from typing import Optional, Dict

from arch.xor.parser.block.contract import Contract, CoherenceState
from phase.wasm.executor import TaskContext, EffectResolver
from phase.runtime.scheme import RuntimeSchemeRunner
from watcher.plane.emitter import get_emitter
from watcher.dphi.adapter.state import StateAdapter

log = get_emitter("scheme.syzygy")

class SyzygyScheme(RuntimeSchemeRunner):
    def __init__(self, broker, node_identity: str, resolvers: Optional[Dict[str, EffectResolver]] = None):
        super().__init__(broker, resolvers)
        self.node_identity = node_identity

    async def on_contract_emitted(self, contract: Contract):
        if contract.state == CoherenceState.STREAMING:
            log.trace(f"[SyzygyObserver] Node in sync. Topos: {contract.topos_id}")
            return

        if contract.kind == "divergence":
            phase_id = contract.phase_id
            log.warning(f"[SyzygyObserver] Topological Drift Detected! Parity Delta != 0 at Phase {phase_id}")
            await self._seal_void_nexus(contract)
        elif contract.kind == "coherence" and contract.payload.get("task_type") == "seal_void_epoch":
            log.info(f"[SyzygyObserver] Successfully rebased to Dominant Topos. Swarm Expansion Resumed.")

    async def _seal_void_nexus(self, drift_contract: Contract):
        log.info("--- [Reaction] Initiating Retrospective Entanglement & Void Nexus Sealing ---")
        
        payload = drift_contract.payload
        orphan_hash = payload.get("local_orphan_hash", f"orphan_drift_{self.node_identity[:8]}")
        dominant_topos = payload.get("dominant_topos_id", "macro_topos_alpha")
        
        void_parity = StateAdapter.build_parity_triplet(
            topos_id=dominant_topos, 
            phase_id=drift_contract.phase_id or 12345, 
            nexus_id=777777 # Void Nexus 고유 ID
        )
        
        sig = f"sig_{self.node_identity}_void_sealed"
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=void_parity, 
            parent_nexus_id=1000000, 
            self_parent_state=orphan_hash,
            repos={"void_sealed": True, "reason": "split_brain_convergence"}, 
            cached_states={},
            timestamp=time.time(), 
            signers=[self.node_identity], 
            signatures=[sig], 
            threshold=1, 
            allowed_signers=[self.node_identity]
        )
        
        rebase_context = TaskContext(
            task_type="seal_epoch", # Rust space.rs의 Method::SealEpoch 에 매핑되도록 정렬 (선택사항)
            payload=seal_payload,
            tier="SYSTEM"
        )
        
        log.info(f"  └─ Submitting Void Seal Task to Executor (Tier: SYSTEM)...")
        async for rebase_contract in self.executor.execute_stream(rebase_context):
            if rebase_contract.state == CoherenceState.COHERENT:
                log.info(f"  ├─ [Void Seal] Divergent history mathematically isolated.")
                break
            elif rebase_contract.state == CoherenceState.FRAGMENTED:
                reason = rebase_contract.payload.get("reason") or rebase_contract.payload.get("detail", "unknown anomaly")
                log.error(f"  ├─ [FATAL] Void Seal Rejected by Kernel: {reason}")
                break