# phase.epoch.scene.anchor
import time
from typing import Any, List
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from dphi.sandbox.runner import EpochBase
from phase.epoch.config.eco import ActorIdentity
from kernel.phase.runner import SchemeRunner
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.exchange import ExchangeAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("scene.anchor")

class ActorIdentity:
    def __init__(self, name: str = "Anonymous"):
        self.name = name
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.pubkey_hex = self._generate_pub_hex()

    def _generate_pub_hex(self) -> str:
        return self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, 
            format=serialization.PublicFormat.Raw
        ).hex()

    def sign(self, commit_dict: dict) -> str:
        """StateAdapter의 JCS 규격을 사용하여 결정론적 서명을 생성합니다."""
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest()
        return self.private_key.sign(commit_hash.encode('utf-8')).hex()

class LedgerSecuritySuite(SchemeRunner):
    """@desc: Multi-sig Consensus, Ed25519 Signatures, and Sybil Defense scenarios"""
    def __init__(self, broker):
        super().__init__(broker)
        # 공통 ActorIdentity 사용
        self.committee = [ActorIdentity(f"Committee_{i}") for i in range(3)]
        self.committee_pubs = [member.pubkey_hex for member in self.committee]
        self.rogue = ActorIdentity("Rogue_Attacker")

    async def execute_suite(self):
        log.info("\n=== [PHASE 1] Executing Ledger Cryptographic Security Boundaries ===")
        await self._set_worker_policy("SYSTEM")
        await self._test_multisig_authorized()
        await self._test_multisig_threshold_fail()
        await self._test_multisig_sybil_attack()
        await self._test_multisig_acl_rejection()

    def _generate_multisig(self, commit_dict: dict, signers: List[ActorIdentity]) -> List[str]:
        return [signer.sign(commit_dict) for signer in signers]

    async def _test_multisig_authorized(self):
        log.info("\n--- Running Suite: Authorized Multi-sig (2-of-3 Consensus) ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0", repos={"repoA": "hashA"}, cached_states={}
        )
        # 3명 중 2명 서명
        active_members = self.committee[:2]
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"], parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[m.pubkey_hex for m in active_members], 
            signatures=self._generate_multisig(anchor_commit, active_members), 
            threshold=2, allowed_signers=self.committee_pubs
        )
        await self._run_case("Ledger: 2-of-3 Valid Multi-sig", "seal_epoch", payload, expected_success=True)

    async def _test_multisig_threshold_fail(self):
        log.info("\n--- Running Suite: Insufficient Signatures ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0", repos={"repoA": "hashA"}, cached_states={}
        )
        # 1명만 서명 (Threshold=2 부족)
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"], parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[self.committee[0].pubkey_hex], 
            signatures=self._generate_multisig(anchor_commit, [self.committee[0]]), 
            threshold=2, allowed_signers=self.committee_pubs
        )
        await self._run_case("Ledger: Reject Insufficient Threshold", "seal_epoch", payload, expected_success=False)

    async def _test_multisig_sybil_attack(self):
        log.info("\n--- Running Suite: Sybil Attack Defense (Duplicate Keys) ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0", repos={"repoA": "hashA"}, cached_states={}
        )
        # 1명이 2번 서명하여 조작 시도
        attacker = self.committee[0]
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"], parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[attacker.pubkey_hex, attacker.pubkey_hex], 
            signatures=self._generate_multisig(anchor_commit, [attacker, attacker]), 
            threshold=2, allowed_signers=self.committee_pubs
        )
        await self._run_case("Ledger: Reject Sybil Attack (Duplicate Signer)", "seal_epoch", payload, expected_success=False)

    async def _test_multisig_acl_rejection(self):
        log.info("\n--- Running Suite: Dynamic ACL Filtering ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0", repos={"repoA": "hashA"}, cached_states={}
        )
        # 위원 1명 + 외부 공격자(Rogue) 서명
        active_members = [self.committee[0], self.rogue]
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"], parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[m.pubkey_hex for m in active_members], 
            signatures=self._generate_multisig(anchor_commit, active_members), 
            threshold=2, allowed_signers=self.committee_pubs
        )
        await self._run_case("Ledger: Reject Unauthorized Signer via ACL", "seal_epoch", payload, expected_success=False)


# =========================================================================
# 2. Anchor Lifecycles (Swarm Consensus & Provenance Alignment)
# =========================================================================
class SwarmConsensusScene(EpochBase):
    def __init__(self, broker: Any, simulate_wallet: bool = True):
        super().__init__(broker, "AI Agent Swarm Consensus (M-of-N)", simulate_wallet)
        
    async def hook_inscribe_nodes(self, parity_triplet: dict[str, Any]) -> dict[str, str]:
        nexus_id = parity_triplet["nexus_id"]
        agent_targets = {
            "CodeAgent": "hash-code-v1",
            "SecurityAgent": "hash-sec-v1",
            "GovAgent": "hash-gov-v1"
        }
        
        for agent_name, state_hash in agent_targets.items():
            agent = ActorIdentity(agent_name)  # 공통 Identity 사용
            repo_commit = StateAdapter.build_repo_commit(nexus_id, 0, state_hash)
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id, parent_nexus_id=0, parent_commit_id=state_hash,
                signers=[agent.pubkey_hex], 
                signatures=[agent.sign(repo_commit)],
                threshold=1, allowed_signers=[agent.pubkey_hex]
            )
            await self._run_case(f"Swarm: Inscribe {agent_name}", "inscribe_actor", payload, expected_success=True)
            
        return agent_targets

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, economy_state: dict, timestamp: int) -> dict:
        cached_states = {"economy_state": economy_state} if economy_state else {}
        anchor_commit = StateAdapter.build_anchor_commit(
            parity_triplet, 0, "swarm-base", repos, cached_states
        )
        # EpochBase의 내장 멀티시그 기능 사용
        signatures = self._sign_multisig(self.committee_keys, anchor_commit)
        return StateAdapter.build_seal_epoch_payload(
            parity_triplet, 0, "swarm-base", repos, cached_states, timestamp,
            self.committee_pubs, signatures, 3, self.committee_pubs
        )

    async def hook_build_phase_root(self, commit_hash: str, repos: dict) -> dict:
        return StateAdapter.adapt_swarm_to_phase_root(commit_hash, agents_dict=repos)


class ProvAlignScene(EpochBase):
    def __init__(self, broker: Any, simulate_wallet: bool = True):
        super().__init__(broker, "Cross-Repo Provenance Alignment (M-of-N)", simulate_wallet)
        
    async def hook_inscribe_nodes(self, parity_triplet: dict[str, Any]) -> dict[str, str]:
        nexus_id = parity_triplet["nexus_id"]
        repo_targets = {
            "ml_training_code": "git-hash-code-77",
            "model_weights": "git-hash-weights-99"
        }
        
        for repo_name, state_hash in repo_targets.items():
            agent = ActorIdentity(repo_name) # 공통 Identity 사용
            repo_commit = StateAdapter.build_repo_commit(nexus_id, 907040, state_hash)
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id, parent_nexus_id=907040, parent_commit_id=state_hash,
                signers=[agent.pubkey_hex], 
                signatures=[agent.sign(repo_commit)],
                threshold=1, allowed_signers=[agent.pubkey_hex]
            )
            await self._run_case(f"Provenance: Inscribe {repo_name}", "inscribe_actor", payload, expected_success=True)
            
        return repo_targets

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, economy_state: dict, timestamp: int) -> dict:
        cached_states = {"hyperparameters": "git-hash-hyper-old"}
        if economy_state: cached_states["economy_state"] = economy_state
            
        anchor_commit = StateAdapter.build_anchor_commit(
            parity_triplet, 907040, "infra-state-v1", repos, cached_states
        )
        active_keys = self.committee_keys[:2]
        active_pubs = self.committee_pubs[:2]
        signatures = self._sign_multisig(active_keys, anchor_commit)
        
        return StateAdapter.build_seal_epoch_payload(
            parity_triplet, 907040, "infra-state-v1", repos, cached_states, timestamp,
            active_pubs, signatures, 2, self.committee_pubs
        )

    async def hook_build_phase_root(self, commit_hash: str, repos: dict) -> dict:
        return StateAdapter.adapt_provenance_to_phase_root(commit_hash, repos_dict=repos)


# =========================================================================
# 3. Global Orchestrator (Anchor Scene)
# =========================================================================
class AnchorScene(SchemeRunner):
    """실질적인 테스트 진입점으로, Ledger 보안 테스트 완료 후 5-Flow 라이프사이클을 수행합니다."""
    async def run_all(self):
        log.info("\n=== [START] Unified Anchor & Ledger Scenarios ===")
        
        # 1. 암호학적 기반 보안 규칙 검증
        ledger_suite = LedgerSecuritySuite(self.broker)
        await ledger_suite.execute_suite()
        self.success_count, self.fail_count = ledger_suite.success_count, ledger_suite.fail_count
        
        # 2. Swarm 합의 라이프사이클 
        log.info("\n=== [PHASE 2] Executing 5-Flow Complete Epoch Scenarios ===")
        swarm = SwarmConsensusScene(self.broker, simulate_wallet=True)
        swarm.success_count, swarm.fail_count = self.success_count, self.fail_count
        await swarm.execute_anchor_lifecycle(topo=1, press=3, rupture=False)
        
        # 3. Provenance 증명 라이프사이클
        prov = ProvAlignScene(self.broker, simulate_wallet=True)
        prov.success_count, prov.fail_count = swarm.success_count, swarm.fail_count
        await prov.execute_anchor_lifecycle(topo=1, press=3, rupture=True)
        
        # 4. 최종 결과 집계 리포트
        self.success_count, self.fail_count = prov.success_count, prov.fail_count
        self.report()