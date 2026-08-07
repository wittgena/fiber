# dphi.epoch.scheme.ledger
import time
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from dphi.epoch.runner import SchemeRunner
from watcher.plane.emitter import get_emitter
from kernel.dphi.adapter.state import StateAdapter

log = get_emitter("scene.ledger")

class LedgerScene(SchemeRunner):
    """@desc: Multi-sig Consensus, Ed25519 Signatures, and Sybil Defense scenarios"""
    def __init__(self, broker):
        super().__init__(broker)
        self.committee_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.committee_pubkeys = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.committee_keys
        ]
        self.rogue_key = ed25519.Ed25519PrivateKey.generate()
        self.rogue_pubkey = self.rogue_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    async def run_all(self):
        log.info("\n=== [START] Executing Ledger & Multi-sig Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        await self._test_multisig_authorized()
        await self._test_multisig_threshold_fail()
        await self._test_multisig_sybil_attack()
        await self._test_multisig_acl_rejection()
        
        self.report()

    def _generate_multisig(self, commit_dict: dict, signers_keys: list) -> list:
        """JCS 변환 후 SHA256 해시에 대해 여러 키로 다중 서명 배열을 생성합니다."""
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest().encode('utf-8')
        return [k.sign(commit_hash).hex() for k in signers_keys]

    async def _test_multisig_authorized(self):
        log.info("\n--- Running Suite: Authorized Multi-sig (2-of-3 Consensus) ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0",
            repos={"repoA": "hashA"}, cached_states={}
        )
        
        # 3명 중 2명만 서명 (2-of-3)
        active_keys = self.committee_keys[:2]
        active_pubs = self.committee_pubkeys[:2]
        signatures = self._generate_multisig(anchor_commit, active_keys)
        
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"],
            parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=active_pubs, signatures=signatures, threshold=2,
            allowed_signers=self.committee_pubkeys  # 전체 위원회 명단 주입
        )
        await self._run_case("Ledger: 2-of-3 Valid Multi-sig", "seal_epoch", payload, expected_success=True)

    async def _test_multisig_threshold_fail(self):
        log.info("\n--- Running Suite: Insufficient Signatures ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0",
            repos={"repoA": "hashA"}, cached_states={}
        )
        
        # 1명만 서명했는데 threshold는 2일 경우
        signatures = self._generate_multisig(anchor_commit, [self.committee_keys[0]])
        
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"],
            parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[self.committee_pubkeys[0]], signatures=signatures, threshold=2,
            allowed_signers=self.committee_pubkeys
        )
        await self._run_case("Ledger: Reject Insufficient Threshold", "seal_epoch", payload, expected_success=False)

    async def _test_multisig_sybil_attack(self):
        log.info("\n--- Running Suite: Sybil Attack Defense (Duplicate Keys) ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0",
            repos={"repoA": "hashA"}, cached_states={}
        )
        
        # [공격] 동일한 사람이 2번 서명해서 threshold=2를 우회하려 시도
        signatures = self._generate_multisig(anchor_commit, [self.committee_keys[0], self.committee_keys[0]])
        
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"],
            parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[self.committee_pubkeys[0], self.committee_pubkeys[0]], # 중복 키
            signatures=signatures, threshold=2,
            allowed_signers=self.committee_pubkeys
        )
        await self._run_case("Ledger: Reject Sybil Attack (Duplicate Signer)", "seal_epoch", payload, expected_success=False)

    async def _test_multisig_acl_rejection(self):
        log.info("\n--- Running Suite: Dynamic ACL Filtering ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0",
            repos={"repoA": "hashA"}, cached_states={}
        )
        
        # 1명은 정상 위원, 1명은 외부 공격자 (총 2명 서명으로 threshold 2 시도)
        active_keys = [self.committee_keys[0], self.rogue_key]
        active_pubs = [self.committee_pubkeys[0], self.rogue_pubkey]
        signatures = self._generate_multisig(anchor_commit, active_keys)
        
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"],
            parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=active_pubs, signatures=signatures, threshold=2,
            allowed_signers=self.committee_pubkeys
        )
        await self._run_case("Ledger: Reject Unauthorized Signer via ACL", "seal_epoch", payload, expected_success=False)