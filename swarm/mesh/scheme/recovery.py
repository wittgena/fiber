# swarm.mesh.scheme.recovery
import time
import json
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from watcher.plane.emitter import get_emitter
from watcher.dphi.adapter.state import StateAdapter
from watcher.kernel.ledger import KernelLedger, KernelCommit
from arch.xor.parser.block.contract import Contract, CoherenceState

from swarm.mesh.executor import TaskContext
from swarm.mesh.scheme.runtime import RuntimeSchemeRunner

log = get_emitter("scheme.recovery")

class RecoveryScheme(RuntimeSchemeRunner):
    def __init__(self, broker):
        super().__init__(broker)
        self.auditor_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.auditor_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.auditor_keys
        ]
        self.store = KernelLedger()

    def _sign_multisig(self, signers: list, commit_dict: dict) -> list:
        """Canonical Bytes 기반 서명 처리"""
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        return [k.sign(canonical_bytes).hex() for k in signers]

    async def on_contract_emitted(self, contract: Contract):
        # 1. 정상 스트리밍
        if contract.state == CoherenceState.STREAMING:
            log.trace(f"[RecoveryObserver] Node streaming. Topos: {contract.payload.get('topos_id', 'N/A')}")
            return
            
        # 2. 장애 감지 (OOM, Network Partition 등으로 인한 작업 유실)
        if contract.state == CoherenceState.FRAGMENTED:
            log.warning(f"[RecoveryObserver] Anomaly detected! Phase lost at Topos: {contract.payload.get('topos_id')}")
            await self._trigger_parity_recovery(contract)

    async def _trigger_parity_recovery(self, failed_contract: Contract):
        log.info(f"--- [Reaction] Initiating Parity Recovery for Nexus {failed_contract.payload.get('nexus_id')} ---")
        recovery_context = TaskContext(
            task_type="verify_parity",
            payload={
                "topos_id_low32": failed_contract.payload.get("topos_id_low32"),
                "nexus_id": failed_contract.payload.get("nexus_id")
            },
            tier="SYSTEM"
        )
        
        ## 피드백 루프: Executor에게 복구(수학적 연산)를 지시하고 그 결과를 다시 스트림으로 받음
        async for recovery_contract in self.executor.execute_stream(recovery_context):
            if recovery_contract.state == CoherenceState.COHERENT:
                recovered_phase = recovery_contract.payload.get("data", {}).get("recovered_missing")
                if recovered_phase:
                    log.info(f"  └─ [SUCCESS] Auditor mathematically recovered Phase ID: {recovered_phase}")
                    await self._step3_state_rebase_and_seal(failed_contract.payload, recovered_phase)
                break
            elif recovery_contract.state == CoherenceState.FRAGMENTED:
                log.error("  └─ [FATAL] Parity recovery mathematically failed or rejected by kernel.")
                break

    async def _step3_state_rebase_and_seal(self, crash_context: dict, recovered_phase: int):
        log.info("\n--- [Reaction] DAG Rebase & Roll-forward Sealing ---")
        restored_parity = StateAdapter.build_parity_triplet(
            topos_id=str(crash_context.get("topos_id_low32")),
            phase_id=recovered_phase,
            nexus_id=crash_context.get("nexus_id")
        )
        
        failed_hash = crash_context.get("failed_commit_hash", "orphan_hash_45_aborted")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=restored_parity,
            parent_nexus_id=crash_context.get("nexus_id"), 
            parent_commit_id=failed_hash, 
            repos={"recovery_status": "fully_healed"},
            cached_states={}
        )
        
        # 2-of-3 멀티시그 서명
        active_keys = self.auditor_keys[:2]
        active_pubs = self.auditor_pubs[:2]
        signatures = self._sign_multisig(active_keys, anchor_commit)
        
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=restored_parity,
            parent_nexus_id=crash_context.get("nexus_id"),
            self_parent_state=failed_hash,
            repos={"recovery_status": "fully_healed"},
            cached_states={},
            timestamp=time.time(),
            signers=active_pubs,
            signatures=signatures,
            threshold=2, 
            allowed_signers=self.auditor_pubs
        )

        # WASM 엔진에 씰링 검증 요청 (이 또한 Executor 파이프라인을 통과함)
        seal_context = TaskContext(task_type="seal_epoch", payload=seal_payload, tier="SYSTEM")
        async for seal_contract in self.executor.execute_stream(seal_context):
            if seal_contract.state == CoherenceState.COHERENT:
                # WASM이 수학적/암호학적 무결성을 승인함 -> Ring 0 권한으로 물리적 디스크에 확정
                sealed_data = seal_contract.payload.get("data", {})
                kernel_commit = KernelCommit(**sealed_data.get("kernel_commit", {}))
                
                try:
                    commit_hash = self.store.seal_system_epoch(
                        commit=kernel_commit, 
                        signatures=signatures, 
                        threshold=2
                    )
                    self.store.update_head("global_era_anchor", commit_hash)
                    log.info(f"  └─ [PHYSICAL SEAL SUCCESS] Ledger Head updated: {commit_hash[:8]}")
                except Exception as e:
                    log.critical(f"  └─ [FATAL] WASM validated, but physical seal to DB failed: {e}")
                break
            
            elif seal_contract.state == CoherenceState.FRAGMENTED:
                log.error(f"  └─ [FATAL] WASM rejected recovery payload: {seal_contract.payload.get('error')}")
                break