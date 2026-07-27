# swarm.mesh.scheme.syzygy
import json
import time

from watcher.plane.emitter import get_emitter
from watcher.dphi.adapter.state import StateAdapter
from arch.xor.parser.block.contract import Contract, CoherenceState
from swarm.mesh.executor import TaskContext
from swarm.mesh.scheme.runtime import RuntimeSchemeRunner

log = get_emitter("scheme.syzygy")

class SyzygyScheme(RuntimeSchemeRunner):
    def __init__(self, broker, node_identity: str):
        super().__init__(broker)
        self.node_identity = node_identity

    async def on_contract_emitted(self, contract: Contract):
        # 1. 정상 스트리밍 상태 - 개입 없이 관측만 수행
        if contract.state == CoherenceState.STREAMING:
            log.trace(f"[SyzygyObserver] Node in sync. Topos: {contract.payload.get('topos_id')}")
            return

        # 2. 다중 진실(Multi-truth) 충돌 감지 - 망 분리 후 재결합 시 발생
        if contract.kind == "TOPOLOGY_DRIFT_DETECTED":
            phase_id = contract.payload.get("phase_id", "UNKNOWN")
            log.warning(f"[SyzygyObserver] Topological Drift Detected! Parity Delta != 0 at Phase {phase_id}")
            await self._seal_void_nexus(contract)
            
        # 3. 씰링(Rebase) 작업 완료
        elif contract.kind == "EXECUTION_COMPLETE" and contract.payload.get("task_type") == "seal_void_epoch":
            log.info(f"[SyzygyObserver] Successfully rebased to Dominant Topos. Swarm Expansion Resumed.")

    async def _seal_void_nexus(self, drift_contract: Contract):
        log.info("--- [Reaction] Initiating Retrospective Entanglement & Void Nexus Sealing ---")
        
        payload = drift_contract.payload
        ## 충돌이 발생한 로컬의 고아 상태 해시와 지배적 망의 Topos ID 추출
        orphan_hash = payload.get("local_orphan_hash", f"orphan_drift_{self.node_identity[:8]}")
        dominant_topos = payload.get("dominant_topos_id", "macro_topos_alpha")
        
        ## Void Nexus를 위한 패리티(Parity Triplet) 구성
        void_parity = StateAdapter.build_parity_triplet(
            topos_id=dominant_topos, 
            phase_id=payload.get("phase_id", 12345), 
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
            task_type="seal_void_epoch",
            payload=seal_payload,
            tier="SYSTEM"
        )
        
        log.info(f"  └─ Submitting Void Seal Task to Executor (Tier: SYSTEM)...")
        async for rebase_contract in self.executor.execute_stream(rebase_context):
            if rebase_contract.state == CoherenceState.COHERENT:
                log.info(f"  ├─ [Void Seal] Divergent history mathematically isolated.")
                break
            elif rebase_contract.state == CoherenceState.FRAGMENTED:
                log.error(f"  ├─ [FATAL] Void Seal Rejected by Kernel: {rebase_contract.payload.get('error')}")
                break