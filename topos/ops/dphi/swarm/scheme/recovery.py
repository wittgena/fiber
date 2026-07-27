# topos.ops.dphi.swarm.scheme.recovery
## @lineage: ops.dphi.swarm.scheme.recovery
## @lineage: ops.dphi.swarm.recovery
import time
import json
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from watcher.dphi.scheme.runner import SchemeRunner
from watcher.dphi.adapter.state import StateAdapter
from watcher.kernel.ledger import KernelLedger, KernelCommit
from watcher.plane.emitter import get_emitter

log = get_emitter("sygyzy.recovery")

class RecoveryScenarios(SchemeRunner):
    """
    @desc: Sophisticated Fault-Tolerance & State Recovery Pattern
    @domain: Stream Checkpoint Recovery & Merkle DAG Sync
    """
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
        """[EVOLUTION] 서명 규격의 통일: Canonical Bytes를 직접 서명 (Ed25519 내부에서 SHA512 처리)"""
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        return [k.sign(canonical_bytes).hex() for k in signers]

    async def run_all(self):
        log.info("\n=== [START] Executing Advanced Self-Healing & State Rebase Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        # 1. 런타임 장애 시뮬레이션 (네트워크 단절 / OOM Crash)
        crash_context = await self._step1_simulate_runtime_crash()
        
        # 2. 감시자(Watcher)의 개입 및 패리티 복원 (Parity Recovery)
        recovered_phase = await self._step2_autonomous_parity_recovery(crash_context)
        
        # 3. 인과율 재병합 및 롤포워드(Roll-forward) 확정 (DAG Rebase)
        await self._step3_state_rebase_and_seal(crash_context, recovered_phase)
        
        self.report()

    async def _step1_simulate_runtime_crash(self):
        """
        @pattern: Distributed Saga / Suspended Transaction
        @desc: 워커 노드가 연산을 수행하고 Nexus ID를 방출했으나, 
               메모리에 있던 Phase ID를 원장에 커밋하기 직전 사망(Crash)한 상황.
        """
        log.info("\n--- [Step 1] Network Partition & Node Crash Simulation ---")
        
        # 정상적인 Epoch 시작을 시도
        current_ts = int(time.time() * 1000)
        init_req = {"ts": current_ts, "topo": 101, "press": 9, "rupture": True, "injected_tick": None}
        
        res = await self.broker.invoke("init_epoch", json.dumps(init_req))
        parity_triplet = json.loads(res.output)
        
        original_phase = parity_triplet["phase_id"]
        t_id_low32 = int(parity_triplet["topos_id"].split('_')[-1]) if '_' in parity_triplet["topos_id"] else parity_triplet["topos_id"]
        nexus_id = parity_triplet["nexus_id"]
        
        log.info(f"  └─ [Intent] Node processing... (Expected Phase: {original_phase})")
        log.warning("  └─ [CRIT] Worker Node Out-of-Memory (OOM) Crash! Phase ID lost in volatile memory.")
        
        # [장애 발생] 원장에는 Topos와 Nexus만 기록되고, 정작 중요한 변화량(Phase)은 증발함.
        return {
            "topos_id_low32": int(t_id_low32),
            "nexus_id": nexus_id,
            "failed_commit_hash": "orphan_hash_45_aborted" # 유실된 작업의 고아 해시
        }

    async def _step2_autonomous_parity_recovery(self, crash_context: dict):
        """
        @pattern: Checkpoint Recovery via XOR
        @desc: Auditor가 불완전한 상태(Dangling Nexus)를 감지하고 수학적으로 유실된 Phase를 복원.
        """
        log.info("\n--- [Step 2] Auditor Intervention & XOR Parity Recovery ---")
        
        recovery_payload = {
            "topos_id_low32": crash_context["topos_id_low32"],
            "nexus_id": crash_context["nexus_id"]
            # phase_id가 고의로 누락됨
        }
        
        res = await self.broker.invoke("verify_parity", json.dumps(recovery_payload))
        recovery_data = json.loads(res.output)
        
        if recovery_data.get("is_valid") and recovery_data.get("recovered_type") == "phase_id":
            recovered_phase = recovery_data["recovered_missing"]
            log.info(f"  └─ [SUCCESS] Auditor mathematically recovered lost Phase ID: {recovered_phase}")
            return recovered_phase
        else:
            log.error("  └─ [FATAL] Parity recovery failed.")
            return None
    
    async def _step3_state_rebase_and_seal(self, crash_context: dict, recovered_phase: int):
        log.info("\n--- [Step 3] DAG Rebase & Roll-forward Sealing ---")
        
        restored_parity = StateAdapter.build_parity_triplet(
            topos_id=str(crash_context["topos_id_low32"]),
            phase_id=recovered_phase,
            nexus_id=crash_context["nexus_id"]
        )
        
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=restored_parity,
            parent_nexus_id=crash_context["nexus_id"], 
            parent_commit_id=crash_context["failed_commit_hash"], 
            repos={"recovery_status": "fully_healed", "data": "restored_payload"},
            cached_states={}
        )
        
        # 2-of-3 Multi-Sig 서명
        active_keys = self.auditor_keys[:2]
        active_pubs = self.auditor_pubs[:2]
        signatures = self._sign_multisig(active_keys, anchor_commit)
        
        payload = StateAdapter.build_seal_epoch_payload(
            parity=restored_parity,
            parent_nexus_id=crash_context["nexus_id"],
            self_parent_state=crash_context["failed_commit_hash"],
            repos={"recovery_status": "fully_healed", "data": "restored_payload"},
            cached_states={},
            timestamp=time.time(),
            signers=active_pubs,
            signatures=signatures,
            threshold=2, 
            allowed_signers=self.auditor_pubs
        )
        
        # 1. WASM 엔진에 수학적/암호학적 증명 요청
        res = await self.broker.invoke("seal_epoch", json.dumps(payload))
        
        if res.success:
            # 2. [EVOLUTION] WASM이 승인한 커밋을 물리적 디스크(KernelStore)에 확정 (Ring 0 권한)
            sealed_data = json.loads(res.output)
            kernel_commit = KernelCommit(**sealed_data.get("kernel_commit"))
            
            try:
                commit_hash = self.store.seal_system_epoch(
                    commit=kernel_commit, 
                    signatures=signatures, 
                    threshold=2
                )
                self.store.update_head("global_era_anchor", commit_hash)
                log.info(f"  └─ [PHYSICAL SEAL SUCCESS] System completely recovered. Ledger Head updated: {commit_hash[:8]}")
            except Exception as e:
                log.critical(f"  └─ [FATAL] WASM validated, but physical seal failed: {e}")
        else:
            log.error(f"  └─ [FATAL] WASM rejected recovery payload: {res.error}")