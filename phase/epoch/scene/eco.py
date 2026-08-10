# phase.epoch.scene.eco
import time
import json
from typing import List

from kernel.phase.runner import SchemeRunner
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("scene.eco")

class EcoContext:
    """에코시스템 시나리오 실행에 필요한 주체들과 어댑터를 관리하는 컨텍스트"""
    def __init__(self):
        self.system = ActorIdentity("System_Core")
        self.agent_a = ActorIdentity("Agent_A")
        self.agent_b = ActorIdentity("Agent_B")
        self.field = ActorIdentity("Clearing_Field")
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field.pubkey_hex)

class EcoScene(SchemeRunner):
    """@desc: Unified Zero-Trust Data Pipeline, Autonomous State Engine, Agent-to-Agent, and P2P Exchange scenarios."""
    def __init__(self, broker):
        super().__init__(broker)
        self.ctx = EcoContext()  # 시나리오 컨텍스트 주입
        self.last_receipt = None

    async def run_all(self):
        log.info("\n=== [START] Executing Unified Ecosystem & Eco Structural Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        # @pipeline.1: Zero-Trust Data Integrity
        await self._test_oracle_packet_integrity()
        await self._test_oracle_data_provenance()
        await self._test_oracle_epoch_initialization()
        await self._test_oracle_self_healing()
        
        # @pipeline.2: Autonomous Protocol State Engine
        await self._test_dao_tension_evaluation()
        await self._test_dao_state_evolution()
        await self._test_dao_epoch_sealing()

        # @pipeline.3: Agent-to-Agent (Eco) Proof-of-Compute and Trustless Execution
        await self._test_a2a_intent_validation()
        await self._test_a2a_trustless_execution()
        await self._test_a2a_proof_generation()
        await self._test_a2a_ledger_inscription()

        # @pipeline.4: Decentralized Exchange & Deterministic Settlement
        await self._test_p2p_exchange_settlement()
        
        self.report()

    # -------------------------------------------------------------------------
    # @pipeline.1: Zero-Trust Data Integrity
    # -------------------------------------------------------------------------
    async def _test_oracle_packet_integrity(self):
        log.info("\n--- [Data Pipeline] Phase 1: Packet Integrity Check ---")
        payload = {"packet_id": "ext-data-2026", "files": {"transaction_log.csv": "hash_xyz"}}
        await self._run_case("Pipeline: Verify Incoming Data Stream", "verify_packet", payload, expected_success=True)

    async def _test_oracle_data_provenance(self):
        log.info("\n--- [Data Pipeline] Phase 2: Provenance Fingerprinting ---")
        payload = {"dummy_data": "Node_State_Report_Data..."}
        await self._run_case("Pipeline: Compute Tamper-Proof Root Fingerprint", "compute_root_fingerprint", payload, expected_success=True)

    async def _test_oracle_epoch_initialization(self):
        log.info("\n--- [Data Pipeline] Phase 3: Epoch Initialization & Parity Triplet ---")
        payload = {"ts": int(time.time() * 1000), "topo": 1, "press": 5, "rupture": False, "injected_tick": None}
        await self._run_case("Pipeline: Generate Parity Triplet (init_epoch)", "init_epoch", payload, expected_success=True)

    async def _test_oracle_self_healing(self):
        log.info("\n--- [Data Pipeline] Phase 4: Self-Healing Recovery ---")
        payload = {"topos_id_low32": 101010, "nexus_id": 907049}
        await self._run_case("Pipeline: Recover Lost Data via XOR Parity", "verify_parity", payload, expected_success=True)

    # -------------------------------------------------------------------------
    # @pipeline.2: Autonomous Protocol State Engine
    # -------------------------------------------------------------------------
    async def _test_dao_tension_evaluation(self):
        log.info("\n--- [State Engine] Phase 1: Ecosystem Tension Evaluation ---")
        await self._run_case("Engine: Evaluate Network Tension & Load", "evaluate_tension", "node_a,node_b,node_c|node_b,node_c,node_d", expected_success=True)

    async def _test_dao_state_evolution(self):
        log.info("\n--- [State Engine] Phase 2: Protocol State Evolution ---")
        phase_root = StateAdapter.adapt_ecosystem_to_phase_root("epoch_399_state", "ipfs_hash_xyz")
        rule = StateAdapter.build_trans_rule("legacy_data", "archived_data", "CORE")
        payload = StateAdapter.build_evolution_context(phase_root, [rule])
        await self._run_case("Engine: Process High-Speed Off-chain Evolution", "process_evolution", payload, expected_success=True)

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
        await self._run_case("Engine: Seal Epoch & Finalize State Transition", "seal_epoch", payload, expected_success=True)

    # -------------------------------------------------------------------------
    # @pipeline.3: Agent-to-Agent (Eco) Proof-of-Compute
    # -------------------------------------------------------------------------
    async def _test_a2a_intent_validation(self):
        log.info("\n--- [Eco Pipeline] Phase 1: Intent Validation ---")
        payload = {"requester_id": "agent-a-gpt4", "responder_id": "agent-b-data-oracle", "action": "compute_financial_risk", "max_fuel_budget": 5_000_000, "timestamp": int(time.time() * 1000)}
        await self._run_case("Eco: Validate Execution Intent", "validate_intent", payload, expected_success=True)

    async def _test_a2a_trustless_execution(self):
        log.info("\n--- [Eco Pipeline] Phase 2: Trustless Execution (Sandboxed) ---")
        await self._set_worker_policy("STANDARD")
        code_payload = "\ndef analyze_risk():\n    return 'Validated Risk Score: 42.5'\nprint(analyze_risk())\n"
        await self._run_case("Eco: Execute Constrained Task (Fuel Tracked)", "execute_code", code_payload, expected_success=True)

    async def _test_a2a_proof_generation(self):
        log.info("\n--- [Eco Pipeline] Phase 3: Cryptographic Proof Generation ---")
        payload = {"execution_hash": "dummy_output_hash_abc123", "fuel_consumed": 15420, "verification_seed": "random_seed_999"}
        await self._run_case("Eco: Generate Proof-of-Compute", "generate_proof", payload, expected_success=True)

    async def _test_a2a_ledger_inscription(self):
        log.info("\n--- [Eco Pipeline] Phase 4: Cryptographic Ledger Inscription ---")
        await self._set_worker_policy("SYSTEM")
        repo_commit = StateAdapter.build_repo_commit(nexus_id=907049, parent_nexus_id=0, parent_commit_id="proof-hash-xyz")
        
        sig_hex = self.ctx.system.sign(repo_commit)
        
        payload = StateAdapter.build_inscribe_payload(
            nexus_id=907049, parent_nexus_id=None, parent_commit_id="proof-hash-xyz",
            signers=[self.ctx.system.pubkey_hex], signatures=[sig_hex], threshold=1, allowed_signers=[self.ctx.system.pubkey_hex]
        )
        await self._run_case("Eco: Inscribe Transaction for State Finality (Nexus ID)", "inscribe_actor", payload, expected_success=True)

    # -------------------------------------------------------------------------
    # @pipeline.4: Decentralized Exchange & Settlement
    # -------------------------------------------------------------------------
    async def _test_p2p_exchange_settlement(self):
        log.info("\n--- [Exchange Pipeline] P2P Order Ingress & Deterministic Settlement ---")
        
        phase_a = await self._step1_gateway_ingress(self.ctx.agent_a.pubkey_hex, "offer_tokenX_for_tokenY")
        phase_b = await self._step1_gateway_ingress(self.ctx.agent_b.pubkey_hex, "offer_tokenY_for_tokenX")
        entangled_state = await self._step2_exchange_entanglement(phase_a, phase_b)
        signatures = await self._step3_nexus_collapse(entangled_state)
        
        self.last_receipt = self.ctx.exchange_adapter.finalize_settlement(
            entangled_state=entangled_state, signatures=signatures,
            cost_metrics={"fuel_consumed": 35000}, tier="SYSTEM"
        )
        external_payload = self.ctx.exchange_adapter.generate_settlement_payload(self.last_receipt)
        log.info(f"\n[Exchange Ready] Payload for External Network:\n{json.dumps(external_payload.model_dump(), indent=2)}")

    async def _step1_gateway_ingress(self, agent_pub, intent_action):
        log.info(f"\n--- [Gateway Ingress] Validating trade intent from {agent_pub[:8]}... ---")
        raw_intent = {"agent_id": agent_pub, "action": intent_action, "timestamp": int(time.time() * 1000)}
        ingress_payload = {"ts": raw_intent["timestamp"], "topo": 101, "press": 5, "rupture": False, "injected_intent": raw_intent}
        
        res = await self.broker.invoke("init_epoch", StateAdapter.to_canonical_bytes(ingress_payload).decode('utf-8'))
        parity = json.loads(res.output)
        log.info(f"  └─ [Ingress Validated] Topos: {parity['topos_id']}, Phase: {parity['phase_id']}")
        return parity

    async def _step2_exchange_entanglement(self, phase_a, phase_b):
        log.info("\n--- [Matching Engine] Binding Execution State A (Bid) and State B (Ask) ---")
        entangled_repos = {"participant_a": phase_a["phase_id"], "participant_b": phase_b["phase_id"], "field_status": "matched_fully_filled"}
        log.info("  └─ [Matched] Opposite intents paired successfully. Remaining imbalance = 0.")
        
        unified_parity = StateAdapter.build_parity_triplet(
            topos_id=f"clearing_batch_{int(time.time())}",
            phase_id=phase_a["phase_id"] ^ phase_b["phase_id"],
            nexus_id=777777  
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
            self.ctx.agent_a.sign(anchor_commit),
            self.ctx.agent_b.sign(anchor_commit),
            self.ctx.field.sign(anchor_commit)
        ]
        
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=entangled_state["parity"], parent_nexus_id=0, self_parent_state="genesis",
            repos=entangled_state["repos"], cached_states={}, timestamp=int(time.time() * 1000),
            signers=signers, signatures=signatures, threshold=3, allowed_signers=signers
        )
        await self._run_case("Trade Settlement: 3-of-3 Multi-sig State Committed to Nexus", "seal_epoch", seal_payload, expected_success=True)
        return signatures