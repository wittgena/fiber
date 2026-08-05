# dphi.exchange.net.anchor
import os
import time
import json
from typing import Any
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

# runner에 정의된 강력한 EpochBase와 타입 모델들을 가져옵니다.
from kernel.dphi.scheme.runner import EpochBase, SchemeRunner
from kernel.dphi.adapter.eco import (
    EcoAdapter, 
    Ap2MandateResult, 
    X402SettlementReceipt
)
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("scheme.anchor")

class SwarmScenario(EpochBase):
    def __init__(self, broker: Any):
        super().__init__(broker, "Agentic Economy Swarm Consensus (3-Tier)", simulate_wallet=True)
        self.human_owner_key = ed25519.Ed25519PrivateKey.generate()
        
    async def hook_validate_mandate(self) -> Ap2MandateResult:
        ap2_mandate = EcoAdapter.build_ap2_mandate(
            requester_id="urn:agent:gov-agent-01",
            target_action="orchestrate_task_and_pay",
            max_spend_usdc="0.10",
            signer_key=self.human_owner_key
        )
        
        await self._run_case("Economy: AP2 Mandate Verification", "validate_intent", {"intent": ap2_mandate}, expected_success=True)
        await self._set_worker_policy("STANDARD")
        log.info("  └─ [Cgroup] Policy Tier Enforced: STANDARD (Fuel: 10,000,000)")
        
        return ap2_mandate

    async def hook_inscribe_nodes(self, parity_triplet: dict[str, Any]) -> dict[str, str]:
        nexus_id = parity_triplet["nexus_id"]
        agents = {
            "CodeAgent": "hash-code-v1",       # 1. Action
            "SecurityAgent": "hash-sec-v1",    # 2. Constraint
            "GovAgent": "hash-gov-v1"          # 3. Governance
        }
        
        for agent_name, state_hash in agents.items():
            agent_key = ed25519.Ed25519PrivateKey.generate()
            pubhex = agent_key.public_key().public_bytes(encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw).hex()
            repo_commit = StateAdapter.build_repo_commit(nexus_id=nexus_id, parent_nexus_id=0, parent_commit_id=state_hash)
            
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id, parent_nexus_id=0, parent_commit_id=state_hash,
                signers=[pubhex], 
                signatures=self._sign_multisig([agent_key], repo_commit),
                threshold=1,
                allowed_signers=[pubhex]
            )
            await self._run_case(f"Swarm: Inscribe {agent_name}", "inscribe_actor", payload, expected_success=True)
            
        return agents

    async def hook_process_payment(self) -> X402SettlementReceipt:
        invoice = EcoAdapter.build_x402_invoice(
            payee_address="0xBaseNetworkTreasuryAddress",
            amount_usdc="0.05",
            resource_id="swarm_epoch_fee"
        )
        receipt = EcoAdapter.process_x402_settlement(
            invoice=invoice,
            agent_wallet_address="0xGovAgentWallet",
            wallet_adapter=self.wallet_adapter # 부모 클래스에서 상속받음
        )
        log.info(f"  └─ [Paid] Amount: {invoice['amount_usdc']} USDC, Tx Hash: {receipt['tx_hash']}")
        return receipt

    async def hook_seal_epoch(self, parity_triplet: dict[str, Any], repos: dict[str, str], economy_state: dict[str, Any], timestamp: int) -> dict[str, Any]:
        await self._set_worker_policy("SYSTEM")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, parent_nexus_id=0, parent_commit_id="swarm-base",
            repos=repos, cached_states=economy_state # 경제 트랜잭션 기록 병합
        )
        
        # 3-of-3 Full Committee Consensus (부모 클래스의 committee 정보 활용)
        signatures = self._sign_multisig(self.committee_keys, anchor_commit)
        return StateAdapter.build_seal_epoch_payload(
            parity=parity_triplet, parent_nexus_id=0, self_parent_state="swarm-base",
            repos=repos, cached_states=economy_state, timestamp=timestamp,
            signers=self.committee_pubs, signatures=signatures, threshold=3,
            allowed_signers=self.committee_pubs
        )

    async def hook_build_phase_root(self, commit_hash: str, repos: dict[str, str]) -> dict[str, Any]:
        return StateAdapter.adapt_swarm_to_phase_root(commit_hash, agents_dict=repos)


class AnchorScenarios(SchemeRunner):
    async def run_all(self):
        log.info("🌌 Executing Integrated Agentic Economy Pipeline")
        await self._set_worker_policy("SYSTEM")
        
        swarm = SwarmScenario(self.broker)
        swarm.success_count, swarm.fail_count = self.success_count, self.fail_count
        await swarm.execute_anchor_lifecycle(topo=1, press=3, rupture=False)
        
        self.success_count, self.fail_count = swarm.success_count, swarm.fail_count
        self.report()