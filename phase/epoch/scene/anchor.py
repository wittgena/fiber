# phase.epoch.scene.anchor
import time
import json
from typing import Any, List, Dict
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.node.runner.sandbox import EpochBase
from kernel.phase.runner import SchemeRunner
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.exchange import ExchangeAdapter
from kernel.dphi.method import DphiMethod
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
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest()
        return self.private_key.sign(commit_hash.encode('utf-8')).hex()

class EcoContext:
    def __init__(self):
        self.system = ActorIdentity("System_Core")
        self.agent_a = ActorIdentity("Agent_A")
        self.agent_b = ActorIdentity("Agent_B")
        self.field = ActorIdentity("Clearing_Field")
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field.pubkey_hex)


# =========================================================================
# 1. Eco Pipeline (Integrated from scene.eco)
# =========================================================================
class EcoScene(SchemeRunner):
    """@desc: Unified Zero-Trust Data Pipeline, Autonomous State Engine, Agent-to-Agent, and P2P Exchange scenarios."""
    def __init__(self, broker):
        super().__init__(broker)
        self.ctx = EcoContext()
        self.last_receipt = None

    async def execute_suite(self):
        log.info("\n=== [PHASE 1] Executing Unified Ecosystem & Eco Structural Scenarios ===")
        # 글로벌 상태 조작 폐기 (모든 제어는 SYSTEM 베이스로 동작)
        
        # @pipeline.1: Zero-Trust Data Integrity
        await self._test_oracle_packet_integrity()
        await self._test_oracle_data_provenance()
        await self._test_oracle_epoch_initialization()
        await self._test_oracle_self_healing()
        
        # @pipeline.2: Autonomous Protocol State Engine
        await self._test_dao_tension_evaluation()
        await self._test_dao_state_evolution()
        await self._test_dao_epoch_sealing()

        # @pipeline.3: Agent-to-Agent (Eco) Proof-of-Compute
        await self._test_a2a_intent_validation()
        await self._test_a2a_trustless_execution()
        await self._test_a2a_proof_generation()
        await self._test_a2a_ledger_inscription()

        # @pipeline.4: Decentralized Exchange & Deterministic Settlement
        await self._test_p2p_exchange_settlement()

    async def _test_oracle_packet_integrity(self):
        log.info("\n--- [Data Pipeline] Phase 1: Packet Integrity Check ---")
        payload = {"packet_id": "ext-data-2026", "files": {"transaction_log.csv": "hash_xyz"}}
        await self._run_case("Pipeline: Verify Incoming Data Stream", DphiMethod.VERIFY_PACKET.value, payload, expected_success=True)

    async def _test_oracle_data_provenance(self):
        log.info("\n--- [Data Pipeline] Phase 2: Provenance Fingerprinting ---")
        payload = {"dummy_data": "Node_State_Report_Data..."}
        await self._run_case("Pipeline: Compute Tamper-Proof Root Fingerprint", DphiMethod.COMPUTE_ROOT_FINGERPRINT.value, payload, expected_success=True)

    async def _test_oracle_epoch_initialization(self):
        log.info("\n--- [Data Pipeline] Phase 3: Epoch Initialization & Parity Triplet ---")
        payload = {
            "ts": int(time.time() * 1000), 
            "topo": 1, 
            "press": 5, 
            "rupture": False, 
            "injected_tick": None
        }
        await self._run_case("Pipeline: Generate Parity Triplet (init_epoch)", DphiMethod.INIT_EPOCH.value, payload, expected_success=True)

    async def _test_oracle_self_healing(self):
        log.info("\n--- [Data Pipeline] Phase 4: Self-Healing Recovery ---")
        payload = {"topos_id_low32": 101010, "nexus_id": 907049}
        await self._run_case("Pipeline: Recover Lost Data via XOR Parity", DphiMethod.VERIFY_PARITY.value, payload, expected_success=True)

    async def _test_dao_tension_evaluation(self):
        log.info("\n--- [State Engine] Phase 1: Ecosystem Tension Evaluation ---")
        await self._run_case("Engine: Evaluate Network Tension & Load", DphiMethod.EVALUATE_TENSION.value, "node_a,node_b,node_c|node_b,node_c,node_d", expected_success=True)

    async def _test_dao_state_evolution(self):
        log.info("\n--- [State Engine] Phase 2: Protocol State Evolution ---")
        phase_root = StateAdapter.adapt_ecosystem_to_phase_root("epoch_399_state", "ipfs_hash_xyz")
        rule = StateAdapter.build_trans_rule("legacy_data", "archived_data", "CORE")
        payload = StateAdapter.build_evolution_context(phase_root, [rule])
        await self._run_case("Engine: Process High-Speed Off-chain Evolution", DphiMethod.PROCESS_EVOLUTION.value, payload, expected_success=True)

    async def _test_dao_epoch_sealing(self):
        log.info("\n--- [State Engine] Phase 3: Epoch Sealing & Consensus ---")
        parity_triplet = StateAdapter.build_parity_triplet("1767225600000_w1_d1_0", 999999, 907049)
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, parent_nexus_id=123456, parent_commit_id="state-v2-hash",
            repos={"ledger": "hash_a", "registry": "hash_b"}, cached_states={"tension_rate": "5.5%"}
        )
        sig_hex = self.ctx.system.sign(anchor_commit)
        payload = StateAdapter.build_seal_epoch_payload(
            parity=parity_triplet, parent_nexus_id=123456, self_parent_state="state-v2-hash",
            repos={"ledger": "hash_a", "registry": "hash_b"}, cached_states={"tension_rate": "5.5%"},
            timestamp=int(time.time() * 1000),
            signers=[self.ctx.system.pubkey_hex], signatures=[sig_hex], threshold=1, allowed_signers=[self.ctx.system.pubkey_hex]
        )
        await self._run_case("Engine: Seal Epoch & Finalize State Transition", DphiMethod.SEAL_EPOCH.value, payload, expected_success=True)

    async def _test_a2a_intent_validation(self):
        log.info("\n--- [Eco Pipeline] Phase 1: Intent Validation ---")
        payload = {"requester_id": "agent-a-gpt4", "responder_id": "agent-b-data-oracle", "action": "compute_financial_risk", "max_fuel_budget": 5_000_000, "timestamp": int(time.time() * 1000)}
        await self._run_case("Eco: Validate Execution Intent", DphiMethod.VALIDATE_INTENT.value, payload, expected_success=True)

    async def _test_a2a_trustless_execution(self):
        log.info("\n--- [Eco Pipeline] Phase 2: Trustless Execution (Sandboxed) ---")
        code_payload = "\ndef analyze_risk():\n    return 'Validated Risk Score: 42.5'\nprint(analyze_risk())\n"
        # [핵심 변경] 글로벌 상태 변경 없이 이 호출에만 STANDARD 티어를 주입합니다.
        await self._run_case("Eco: Execute Constrained Task (Fuel Tracked)", DphiMethod.EXECUTE_CODE.value, code_payload, expected_success=True, tier="STANDARD")

    async def _test_a2a_proof_generation(self):
        log.info("\n--- [Eco Pipeline] Phase 3: Cryptographic Proof Generation ---")
        payload = {"execution_hash": "dummy_output_hash_abc123", "fuel_consumed": 15420, "verification_seed": "random_seed_999"}
        # 이제 글로벌 티어가 오염되지 않아 기본값(SYSTEM)인 20억 Fuel로 넉넉하게 암호화 증명을 생성합니다.
        await self._run_case("Eco: Generate Proof-of-Compute", DphiMethod.GENERATE_PROOF.value, payload, expected_success=True)

    async def _test_a2a_ledger_inscription(self):
        log.info("\n--- [Eco Pipeline] Phase 4: Cryptographic Ledger Inscription ---")
        repo_commit = StateAdapter.build_repo_commit(nexus_id=907049, parent_nexus_id=0, parent_commit_id="proof-hash-xyz")
        sig_hex = self.ctx.system.sign(repo_commit)
        payload = StateAdapter.build_inscribe_payload(
            nexus_id=907049, parent_nexus_id=None, parent_commit_id="proof-hash-xyz",
            signers=[self.ctx.system.pubkey_hex], signatures=[sig_hex], threshold=1, allowed_signers=[self.ctx.system.pubkey_hex]
        )
        await self._run_case("Eco: Inscribe Transaction for State Finality (Nexus ID)", DphiMethod.INSCRIBE_ACTOR.value, payload, expected_success=True)

    async def _test_p2p_exchange_settlement(self):
        log.info("\n--- [Exchange Pipeline] P2P Order Ingress & Deterministic Settlement ---")
        phase_a = await self._step1_gateway_ingress(self.ctx.agent_a.pubkey_hex, "offer_tokenX_for_tokenY")
        phase_b = await self._step1_gateway_ingress(self.ctx.agent_b.pubkey_hex, "offer_tokenY_for_tokenX")
        if not phase_a or not phase_b:
            log.error("Failed to generate phases for P2P Exchange.")
            return

        entangled_state = await self._step2_exchange_entanglement(phase_a, phase_b)
        signatures = await self._step3_nexus_collapse(entangled_state)
        
        self.last_receipt = self.ctx.exchange_adapter.finalize_settlement(
            entangled_state=entangled_state, signatures=signatures, cost_metrics={"fuel_consumed": 35000}, tier="SYSTEM"
        )
        external_payload = self.ctx.exchange_adapter.generate_settlement_payload(self.last_receipt)
        log.info(f"\n[Exchange Ready] Payload for External Network:\n{json.dumps(external_payload.model_dump(), indent=2)}")

    async def _step1_gateway_ingress(self, agent_pub, intent_action):
        log.info(f"\n--- [Gateway Ingress] Validating trade intent from {agent_pub[:8]}... ---")
        raw_intent = {"agent_id": agent_pub, "action": intent_action, "timestamp": int(time.time() * 1000)}
        
        ingress_payload = {
            "ts": raw_intent["timestamp"], 
            "topo": 101, 
            "press": 5, 
            "rupture": False,
            "injected_tick": None
        }
        
        res = await self.broker.invoke(DphiMethod.INIT_EPOCH.value, StateAdapter.to_canonical_bytes(ingress_payload).decode('utf-8'))
        if not res.success:
            log.error(f"Init Epoch Failed: {res.error}")
            return None
            
        parity = json.loads(res.output)
        log.info(f"  └─ [Ingress Validated] Topos: {parity['topos_id']}, Phase: {parity['phase_id']}")
        return parity

    async def _step2_exchange_entanglement(self, phase_a, phase_b):
        log.info("\n--- [Matching Engine] Binding Execution State A (Bid) and State B (Ask) ---")
        entangled_repos = {"participant_a": phase_a["phase_id"], "participant_b": phase_b["phase_id"], "field_status": "matched_fully_filled"}
        log.info("  └─ [Matched] Opposite intents paired successfully. Remaining imbalance = 0.")
        unified_parity = StateAdapter.build_parity_triplet(
            topos_id=f"clearing_batch_{int(time.time())}", phase_id=phase_a["phase_id"] ^ phase_b["phase_id"], nexus_id=777777  
        )
        return {"parity": unified_parity, "repos": entangled_repos}

    async def _step3_nexus_collapse(self, entangled_state) -> List[str]:
        log.info("\n--- [Trade Settlement] Finalizing clearing via 3-of-3 Multi-sig Consensus ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=entangled_state["parity"], parent_nexus_id=0, parent_commit_id="genesis",
            repos=entangled_state["repos"], cached_states={}
        )
        signers = [self.ctx.agent_a.pubkey_hex, self.ctx.agent_b.pubkey_hex, self.ctx.field.pubkey_hex]
        signatures = [
            self.ctx.agent_a.sign(anchor_commit), self.ctx.agent_b.sign(anchor_commit), self.ctx.field.sign(anchor_commit)
        ]
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=entangled_state["parity"], parent_nexus_id=0, self_parent_state="genesis",
            repos=entangled_state["repos"], cached_states={}, timestamp=int(time.time() * 1000),
            signers=signers, signatures=signatures, threshold=3, allowed_signers=signers
        )
        await self._run_case("Trade Settlement: 3-of-3 Multi-sig State Committed to Nexus", DphiMethod.SEAL_EPOCH.value, seal_payload, expected_success=True)
        return signatures


# =========================================================================
# 2. Ledger Security Suite
# =========================================================================
class LedgerSecuritySuite(SchemeRunner):
    """@desc: Multi-sig Consensus, Ed25519 Signatures, and Sybil Defense scenarios"""
    def __init__(self, broker):
        super().__init__(broker)
        self.committee = [ActorIdentity(f"Committee_{i}") for i in range(3)]
        self.committee_pubs = [member.pubkey_hex for member in self.committee]
        self.rogue = ActorIdentity("Rogue_Attacker")

    async def execute_suite(self):
        log.info("\n=== [PHASE 2] Executing Ledger Cryptographic Security Boundaries ===")
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
        active_members = self.committee[:2]
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"], parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[m.pubkey_hex for m in active_members], 
            signatures=self._generate_multisig(anchor_commit, active_members), 
            threshold=2, allowed_signers=self.committee_pubs
        )
        await self._run_case("Ledger: 2-of-3 Valid Multi-sig", DphiMethod.SEAL_EPOCH.value, payload, expected_success=True)

    async def _test_multisig_threshold_fail(self):
        log.info("\n--- Running Suite: Insufficient Signatures ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0", repos={"repoA": "hashA"}, cached_states={}
        )
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"], parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[self.committee[0].pubkey_hex], 
            signatures=self._generate_multisig(anchor_commit, [self.committee[0]]), 
            threshold=2, allowed_signers=self.committee_pubs
        )
        await self._run_case("Ledger: Reject Insufficient Threshold", DphiMethod.SEAL_EPOCH.value, payload, expected_success=False)

    async def _test_multisig_sybil_attack(self):
        log.info("\n--- Running Suite: Sybil Attack Defense (Duplicate Keys) ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0", repos={"repoA": "hashA"}, cached_states={}
        )
        attacker = self.committee[0]
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"], parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[attacker.pubkey_hex, attacker.pubkey_hex], 
            signatures=self._generate_multisig(anchor_commit, [attacker, attacker]), 
            threshold=2, allowed_signers=self.committee_pubs
        )
        await self._run_case("Ledger: Reject Sybil Attack (Duplicate Signer)", DphiMethod.SEAL_EPOCH.value, payload, expected_success=False)

    async def _test_multisig_acl_rejection(self):
        log.info("\n--- Running Suite: Dynamic ACL Filtering ---")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet("test_topos", 1, 999),
            parent_nexus_id=0, parent_commit_id="state-0", repos={"repoA": "hashA"}, cached_states={}
        )
        active_members = [self.committee[0], self.rogue]
        payload = StateAdapter.build_seal_epoch_payload(
            parity=anchor_commit["parity"], parent_nexus_id=0, self_parent_state="state-0",
            repos={"repoA": "hashA"}, cached_states={}, timestamp=time.time(),
            signers=[m.pubkey_hex for m in active_members], 
            signatures=self._generate_multisig(anchor_commit, active_members), 
            threshold=2, allowed_signers=self.committee_pubs
        )
        await self._run_case("Ledger: Reject Unauthorized Signer via ACL", DphiMethod.SEAL_EPOCH.value, payload, expected_success=False)


# =========================================================================
# 3. Anchor Lifecycles (Swarm Consensus & Provenance Alignment)
# =========================================================================
class SwarmConsensusScene(EpochBase):
    def __init__(self, broker: Any, simulate_wallet: bool = True):
        super().__init__(broker, "AI Agent Swarm Consensus (M-of-N)", simulate_wallet)
        
    async def hook_inscribe_nodes(self, parity_triplet: dict[str, Any]) -> dict[str, str]:
        nexus_id = parity_triplet["nexus_id"]
        agent_targets = {"CodeAgent": "hash-code-v1", "SecurityAgent": "hash-sec-v1", "GovAgent": "hash-gov-v1"}
        for agent_name, state_hash in agent_targets.items():
            agent = ActorIdentity(agent_name) 
            repo_commit = StateAdapter.build_repo_commit(nexus_id, 0, state_hash)
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id, parent_nexus_id=0, parent_commit_id=state_hash,
                signers=[agent.pubkey_hex], signatures=[agent.sign(repo_commit)],
                threshold=1, allowed_signers=[agent.pubkey_hex]
            )
            await self._run_case(f"Swarm: Inscribe {agent_name}", DphiMethod.INSCRIBE_ACTOR.value, payload, expected_success=True)
        return agent_targets

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, economy_state: dict, timestamp: int) -> dict:
        cached_states = {"economy_state": economy_state} if economy_state else {}
        anchor_commit = StateAdapter.build_anchor_commit(parity_triplet, 0, "swarm-base", repos, cached_states)
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
        repo_targets = {"ml_training_code": "git-hash-code-77", "model_weights": "git-hash-weights-99"}
        for repo_name, state_hash in repo_targets.items():
            agent = ActorIdentity(repo_name)
            repo_commit = StateAdapter.build_repo_commit(nexus_id, 907040, state_hash)
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id, parent_nexus_id=907040, parent_commit_id=state_hash,
                signers=[agent.pubkey_hex], signatures=[agent.sign(repo_commit)],
                threshold=1, allowed_signers=[agent.pubkey_hex]
            )
            await self._run_case(f"Provenance: Inscribe {repo_name}", DphiMethod.INSCRIBE_ACTOR.value, payload, expected_success=True)
        return repo_targets

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, economy_state: dict, timestamp: int) -> dict:
        cached_states = {"hyperparameters": "git-hash-hyper-old"}
        if economy_state: cached_states["economy_state"] = economy_state
        anchor_commit = StateAdapter.build_anchor_commit(parity_triplet, 907040, "infra-state-v1", repos, cached_states)
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
# 4. Global Orchestrator (Unified Anchor Scene)
# =========================================================================
class AnchorScene(SchemeRunner):
    """Eco와 Anchor를 모두 통합한 완전한 파이프라인 진입점입니다."""
    async def run_all(self):
        log.info("\n=== [START] Unified Master Pipeline (Eco + Anchor) ===")
        
        # 1. Eco Pipeline (Zero-Trust, DAO, A2A, Exchange)
        eco = EcoScene(self.broker)
        await eco.execute_suite()
        self.success_count += eco.success_count
        self.fail_count += eco.fail_count
        # [핵심] 실패 내역 병합
        self.failed_cases.extend(eco.failed_cases)
        
        # 2. 암호학적 기반 보안 규칙 검증 (Ledger)
        ledger = LedgerSecuritySuite(self.broker)
        await ledger.execute_suite()
        self.success_count += ledger.success_count
        self.fail_count += ledger.fail_count
        self.failed_cases.extend(ledger.failed_cases)
        
        # 3. Swarm 합의 라이프사이클 
        log.info("\n=== [PHASE 3] Executing 5-Flow Complete Epoch Scenarios ===")
        swarm = SwarmConsensusScene(self.broker, simulate_wallet=True)
        await swarm.execute_anchor_lifecycle(topo=1, press=3, rupture=False)
        self.success_count += swarm.success_count
        self.fail_count += swarm.fail_count
        self.failed_cases.extend(swarm.failed_cases)
        
        # 4. Provenance 증명 라이프사이클
        prov = ProvAlignScene(self.broker, simulate_wallet=True)
        await prov.execute_anchor_lifecycle(topo=1, press=3, rupture=True)
        self.success_count += prov.success_count
        self.fail_count += prov.fail_count
        self.failed_cases.extend(prov.failed_cases)
        
        # 최종 결과 집계 리포트
        self.report()