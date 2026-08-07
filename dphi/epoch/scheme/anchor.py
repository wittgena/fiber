# dphi.epoch.scheme.anchor
import json
from typing import Any
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from dphi.epoch.runner import SchemeRunner, EpochBase
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("scene.anchor")

class SwarmConsensusScene(EpochBase):
    def __init__(self, broker: Any, simulate_wallet: bool = True):
        super().__init__(broker, "AI Agent Swarm Consensus (M-of-N)", simulate_wallet)
        
    async def hook_inscribe_nodes(self, parity_triplet: dict[str, Any]) -> dict[str, str]:
        nexus_id = parity_triplet["nexus_id"]
        agents = {
            "CodeAgent": "hash-code-v1",       # 1. Action (실행)
            "SecurityAgent": "hash-sec-v1",    # 2. Constraint (검증)
            "GovAgent": "hash-gov-v1"          # 3. Routing & Consensus (거버넌스/합의)
        }
        
        for agent_name, state_hash in agents.items():
            agent_key = ed25519.Ed25519PrivateKey.generate()
            pubhex = agent_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex()
            
            repo_commit = StateAdapter.build_repo_commit(
                nexus_id=nexus_id, parent_nexus_id=0, parent_commit_id=state_hash
            )
            
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id, parent_nexus_id=0, parent_commit_id=state_hash,
                signers=[pubhex], 
                signatures=self._sign_multisig([agent_key], repo_commit),
                threshold=1,
                allowed_signers=[pubhex]
            )
            await self._run_case(f"Swarm: Inscribe {agent_name}", "inscribe_actor", payload, expected_success=True)
            
        return agents

    async def hook_seal_epoch(self, parity_triplet: dict[str, Any], repos: dict[str, str], economy_state: dict[str, Any], timestamp: int) -> dict[str, Any]:
        cached_states = {"economy_state": economy_state} if economy_state else {}
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, parent_nexus_id=0, parent_commit_id="swarm-base",
            repos=repos, cached_states=cached_states
        )
        
        # 3-of-3 Full Committee Consensus
        signatures = self._sign_multisig(self.committee_keys, anchor_commit)
        
        return StateAdapter.build_seal_epoch_payload(
            parity=parity_triplet, parent_nexus_id=0, self_parent_state="swarm-base",
            repos=repos, cached_states=cached_states, timestamp=timestamp,
            signers=self.committee_pubs, signatures=signatures, threshold=3,
            allowed_signers=self.committee_pubs
        )

    async def hook_build_phase_root(self, commit_hash: str, repos: dict[str, str]) -> dict[str, Any]:
        return StateAdapter.adapt_swarm_to_phase_root(commit_hash, agents_dict=repos)

class ProvAlignScene(EpochBase):
    def __init__(self, broker: Any, simulate_wallet: bool = True):
        super().__init__(broker, "Cross-Repo Provenance Alignment (M-of-N)", simulate_wallet)
        
    async def hook_inscribe_nodes(self, parity_triplet: dict[str, Any]) -> dict[str, str]:
        nexus_id = parity_triplet["nexus_id"]
        repos = {
            "ml_training_code": "git-hash-code-77",
            "model_weights": "git-hash-weights-99"
        }
        
        for repo_name, state_hash in repos.items():
            agent_key = ed25519.Ed25519PrivateKey.generate()
            pubhex = agent_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex()
            repo_commit = StateAdapter.build_repo_commit(
                nexus_id=nexus_id, parent_nexus_id=907040, parent_commit_id=state_hash
            )
            
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id, parent_nexus_id=907040, parent_commit_id=state_hash,
                signers=[pubhex], 
                signatures=self._sign_multisig([agent_key], repo_commit),
                threshold=1,
                allowed_signers=[pubhex]
            )
            await self._run_case(f"Provenance: Inscribe {repo_name}", "inscribe_actor", payload, expected_success=True)
            
        return repos

    async def hook_seal_epoch(self, parity_triplet: dict[str, Any], repos: dict[str, str], economy_state: dict[str, Any], timestamp: int) -> dict[str, Any]:
        cached_states = {"hyperparameters": "git-hash-hyper-old"}
        if economy_state:
            cached_states["economy_state"] = economy_state
            
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, parent_nexus_id=907040, parent_commit_id="infra-state-v1",
            repos=repos, cached_states=cached_states
        )
        active_keys = self.committee_keys[:2]
        active_pubs = self.committee_pubs[:2]
        signatures = self._sign_multisig(active_keys, anchor_commit)
        return StateAdapter.build_seal_epoch_payload(
            parity=parity_triplet, parent_nexus_id=907040, self_parent_state="infra-state-v1",
            repos=repos, cached_states=cached_states, timestamp=timestamp,
            signers=active_pubs, signatures=signatures, threshold=2,
            allowed_signers=self.committee_pubs
        )

    async def hook_build_phase_root(self, commit_hash: str, repos: dict[str, str]) -> dict[str, Any]:
        return StateAdapter.adapt_provenance_to_phase_root(commit_hash, repos_dict=repos)


class AnchorScene(SchemeRunner):
    async def run_all(self):
        log.info("\n=== [START] Executing 5-Flow Complete Epoch Scenarios ===")
        await self._set_worker_policy("SYSTEM")

        swarm = SwarmConsensusScene(self.broker, simulate_wallet=True)
        swarm.success_count, swarm.fail_count = self.success_count, self.fail_count
        await swarm.execute_anchor_lifecycle(topo=1, press=3, rupture=False)
        
        prov = ProvAlignScene(self.broker, simulate_wallet=True)
        prov.success_count, prov.fail_count = swarm.success_count, swarm.fail_count
        await prov.execute_anchor_lifecycle(topo=1, press=3, rupture=True)
        
        self.success_count, self.fail_count = prov.success_count, prov.fail_count
        self.report()