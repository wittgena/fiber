# phase.chain.recovery
"""
Module: phase.chain.recovery
Description: 체인 단편화 복구(Recovery) 및 위상 분기(Topological Drift) 해결을 담당하는 런타임 러너
"""
import time
from enum import Enum
from typing import Dict, Optional

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from arch.xor.parser.block.contract import CoherenceState, Contract
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.ledger.consensus import KernelCommit, KernelLedger
from kernel.dphi.sandbox.executor import EffectResolver, TaskContext
from kernel.dphi.method import DphiMethod

from kernel.phase.runner import RuntimeRunner, RecoveryMethod
from watcher.plane.emitter import get_emitter

log = get_emitter("chain.recovery")

class RecoveryRunner(RuntimeRunner):
    def __init__(self, broker, resolvers: Optional[Dict[str, EffectResolver]] = None):
        super().__init__(broker, resolvers)
        self.auditor_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.auditor_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.auditor_keys
        ]
        self.store = KernelLedger()

    def _sign_multisig(self, signers: list, commit_dict: dict) -> list:
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        return [k.sign(canonical_bytes).hex() for k in signers]

    async def on_contract_emitted(self, contract: Contract):
        if contract.state == CoherenceState.STREAMING:
            log.trace(f"[RecoveryRunner] Node streaming normally. Topos: {contract.topos_id}")
            return
            
        if contract.state == CoherenceState.FRAGMENTED:
            log.warning(f"[RecoveryRunner] Anomaly detected! Phase lost at Topos: {contract.topos_id}")
            await self._trigger_parity_recovery(contract)
            
        elif contract.state == CoherenceState.COHERENT and getattr(contract, "kind", None) == "coherence":
            log.info(f"[RecoveryRunner] Task finalized coherently. Nexus: {contract.nexus_id}")

    async def _trigger_parity_recovery(self, failed_contract: Contract):
        log.info(f"--- [Reaction] Initiating Parity Recovery for Nexus {failed_contract.nexus_id} ---")
        topos_id_low32 = int(failed_contract.topos_id) & 0xFFFFFFFF if failed_contract.topos_id else None
        
        recovery_context = TaskContext(
            task_type=DphiMethod.VERIFY_PARITY,
            payload={
                "topos_id_low32": topos_id_low32,
                "nexus_id": failed_contract.nexus_id
            },
            tier="SYSTEM"
        )
        
        async for recovery_contract in self.executor.execute_stream(recovery_context):
            if recovery_contract.state == CoherenceState.COHERENT:
                recovered_phase = recovery_contract.payload.get("data", {}).get("recovered_missing")
                if recovered_phase:
                    log.info(f"  └─ [SUCCESS] Auditor mathematically recovered Phase ID: {recovered_phase}")
                    await self._seal_recovered_state(topos_id_low32, failed_contract.nexus_id, recovered_phase)
                break
            elif recovery_contract.state == CoherenceState.FRAGMENTED:
                reason = recovery_contract.payload.get("reason", "math_validation_failed")
                log.error(f"  └─ [FATAL] Parity recovery rejected by kernel: {reason}")
                break

    async def _seal_recovered_state(self, topos_id: Optional[int], nexus_id: int, recovered_phase: int):
        log.info("\n--- [Reaction] DAG Rebase & Roll-forward Sealing ---")
        restored_parity = StateAdapter.build_parity_triplet(
            topos_id=str(topos_id), 
            phase_id=recovered_phase, 
            nexus_id=nexus_id
        )
        
        failed_hash = "orphan_hash_45_aborted"
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=restored_parity,
            parent_nexus_id=nexus_id, 
            parent_commit_id=failed_hash, 
            repos={"recovery_status": "fully_healed"},
            cached_states={}
        )
        
        active_keys = self.auditor_keys[:2]
        active_pubs = self.auditor_pubs[:2]
        signatures = self._sign_multisig(active_keys, anchor_commit)
        
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=restored_parity,
            parent_nexus_id=nexus_id,
            self_parent_state=failed_hash,
            repos={"recovery_status": "fully_healed"},
            cached_states={},
            timestamp=time.time(),
            signers=active_pubs,
            signatures=signatures,
            threshold=2, 
            allowed_signers=self.auditor_pubs
        )

        seal_context = TaskContext(task_type=DphiMethod.SEAL_EPOCH, payload=seal_payload, tier="SYSTEM")
        
        async for seal_contract in self.executor.execute_stream(seal_context):
            if seal_contract.state == CoherenceState.COHERENT:
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
                rejection = seal_contract.payload.get("reason") or seal_contract.payload.get("detail", "unknown anomaly")
                log.error(f"  └─ [FATAL] WASM rejected recovery payload: {rejection}")
                break

class SyzygyResonator(RuntimeRunner):
    def __init__(self, broker, node_identity: str, resolvers: Optional[Dict[str, EffectResolver]] = None):
        super().__init__(broker, resolvers)
        self.node_identity = node_identity

    async def on_contract_emitted(self, contract: Contract):
        if contract.state == CoherenceState.STREAMING:
            return

        if getattr(contract, "kind", None) == "divergence":
            phase_id = getattr(contract, "phase_id", "UNKNOWN")
            log.warning(f"[SyzygyResonator] Topological Drift Detected! Parity Delta != 0 at Phase {phase_id}")
            await self._seal_void_nexus(contract)
            
        elif getattr(contract, "kind", None) == "coherence" and contract.payload.get("task_type") == RecoveryMethod.SEAL_VOID_EPOCH:
            log.info("[SyzygyResonator] Successfully rebased to Dominant Topos. Swarm Expansion Resumed.")

    async def _seal_void_nexus(self, drift_contract: Contract):
        log.info("--- [Reaction] Initiating Retrospective Entanglement & Void Nexus Sealing ---")
        
        payload = drift_contract.payload
        orphan_hash = payload.get("local_orphan_hash", f"orphan_drift_{self.node_identity[:8]}")
        dominant_topos = payload.get("dominant_topos_id", "macro_topos_alpha")
        phase_id = getattr(drift_contract, "phase_id", 12345)
        
        void_parity = StateAdapter.build_parity_triplet(
            topos_id=dominant_topos, 
            phase_id=phase_id, 
            nexus_id=777777  # Void Nexus 고유 ID
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
            task_type=DphiMethod.SEAL_EPOCH,
            payload=seal_payload,
            tier="SYSTEM"
        )
        
        log.info("  └─ Submitting Void Seal Task to Executor (Tier: SYSTEM)...")
        async for rebase_contract in self.executor.execute_stream(rebase_context):
            if rebase_contract.state == CoherenceState.COHERENT:
                log.info("  ├─ [Void Seal] Divergent history mathematically isolated.")
                break
            elif rebase_contract.state == CoherenceState.FRAGMENTED:
                reason = rebase_contract.payload.get("reason") or rebase_contract.payload.get("detail", "unknown anomaly")
                log.error(f"  ├─ [FATAL] Void Seal Rejected by Kernel: {reason}")
                break