# dphi.exchange.net.anchor
## @lineage: dphi.net.anchor
## @lineage: dphi.eco.scheme.anchor
import os
import time
import json
import hashlib
from typing import Any
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from arch.xor.secret.manager import get_secret_str
from watcher.plane.emitter import get_emitter
from kernel.dphi.scheme.runner import SchemeRunner
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.eco import EcoAdapter

log = get_emitter("scheme.anchor")

def inject_and_clear_secrets(secrets: dict[str, str], action_fn: callable):
    os.environ.update(secrets)
    try:
        return action_fn()
    finally:
        for k in secrets.keys():
            os.environ.pop(k, None)

class WalletAdapter:
    def __init__(self, network_id: str = "base-sepolia", simulate: bool = False):
        self.network_id = network_id
        self.simulate = simulate
        self.wallet = None
        
        if not self.simulate:
            self._initialize_secure_wallet()

    def _initialize_secure_wallet(self):
        try:
            from coinbase_agentkit import CdpWalletProvider
        except ImportError as e:
            log.error("[Wallet] coinbase_agentkit not installed.")
            raise RuntimeError("Missing required SDK for secure wallet") from e

        api_name = get_secret_str("CDP_API_KEY_NAME")
        api_pkey = get_secret_str("CDP_API_KEY_PRIVATE_KEY")
        
        if not api_name or not api_pkey:
            log.error("[Wallet] CDP API Keys missing in SecretManager.")
            raise ValueError("Incomplete credentials for CDP Wallet initialization")

        injected_secrets = {
            "CDP_API_KEY_NAME": api_name,
            "CDP_API_KEY_PRIVATE_KEY": api_pkey
        }
        try:
            self.wallet = inject_and_clear_secrets(
                injected_secrets, 
                lambda: CdpWalletProvider.create_wallet(network_id=self.network_id)
            )
            log.info(f"[Wallet] CDP Wallet created successfully on {self.network_id} (Secured via SecretManager)")
        except Exception as e:
            log.error(f"[Wallet] Failed to initialize CDP Wallet: {e}")
            raise

    def fund_wallet(self, asset: str = "usdc", amount: str = "0.1") -> bool:
        if self.simulate:
            log.info(f"[Wallet-Sim] Simulated funding {amount} {asset}.")
            return True
            
        log.info(f"[Wallet] Requesting faucet for {amount} {asset}...")
        try:
            self.wallet.fund(asset=asset, amount=amount)
            return True
        except Exception as e:
            log.error(f"[Wallet] Faucet funding failed: {e}")
            raise

    def transfer(self, to_address: str, amount: str, asset: str = "usdc") -> str:
        if self.simulate:
            mock_hash = f"0xsim_{int(time.time()*1000)}"
            log.info(f"[Wallet-Sim] Transferred {amount} {asset} to {to_address}. Tx: {mock_hash}")
            return mock_hash

        log.info(f"[Wallet] Transferring {amount} {asset} to {to_address}...")
        try:
            receipt = self.wallet.transfer(to_address=to_address, amount=amount, asset=asset)
            tx_hash = getattr(receipt, "transaction_hash", getattr(receipt, "hash", str(receipt)))
            log.info(f"[Wallet] Transfer success. Tx: {tx_hash}")
            return tx_hash
        except Exception as e:
            log.error(f"[Wallet] Transfer failed: {e}")
            raise

class TrustlessEpochBase(SchemeRunner):
    def __init__(self, broker: Any, scenario_name: str, simulate_wallet: bool = True):
        super().__init__(broker)
        self.scenario_name = scenario_name
        self.committee_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.committee_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.committee_keys
        ]
        self.wallet_adapter = WalletAdapter(network_id="base-sepolia", simulate=simulate_wallet)
        if not self.wallet_adapter.simulate:
            self.wallet_adapter.fund_wallet()

    def _sign_multisig(self, signers: list[ed25519.Ed25519PrivateKey], commit_dict: dict[str, Any]) -> list[str]:
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest().encode('utf-8')
        return [k.sign(commit_hash).hex() for k in signers]

    async def execute_anchor_lifecycle(self, topo: int, press: int, rupture: bool) -> None:
        log.info(f"\n=== [Lifecycle START] {self.scenario_name} ===")
        
        try:
            log.info("--- [Flow 1] Initialization: Requesting Parity Triplet ---")
            current_ts = int(time.time() * 1000)
            init_req = {"ts": current_ts, "topo": topo, "press": press, "rupture": rupture, "injected_tick": None}
            
            res = await self.broker.invoke("init_epoch", json.dumps(init_req))
            if not res.success:
                raise RuntimeError(f"init_epoch Failed: {res.error}")
                
            parity_triplet = json.loads(res.output)
            log.info(f"  └─ Generated Nexus ID: {parity_triplet.get('nexus_id')}")
            log.info("--- [Flow 1.5] Economy: AP2 Mandate Validation ---")
            
            ap2_mandate = await self.hook_validate_mandate()
            log.info("--- [Flow 2] Inscription: Gathering Local Node States ---")
            repos = await self.hook_inscribe_nodes(parity_triplet)

            log.info("--- [Flow 2.5] Economy: x402 Micropayment Settlement ---")
            x402_receipt = await self.hook_process_payment()
            
            ## 경제적 상태(Mandate & Receipt)를 로컬 상태에 임베딩
            economy_state = EcoAdapter.embed_economy_state({}, ap2_mandate, x402_receipt)
            log.info("--- [Flow 3] Sealing: Cryptographic Epoch Alignment ---")
            seal_payload = await self.hook_seal_epoch(parity_triplet, repos, economy_state, current_ts)
            
            seal_res = await self.broker.invoke("seal_epoch", json.dumps(seal_payload))
            if not seal_res.success:
                raise RuntimeError(f"seal_epoch Failed: {seal_res.error}")
                
            sealed_data = json.loads(seal_res.output)
            log.info("  └─ Epoch Sealed Successfully via Multi-sig Consensus.")

            log.info("--- [Flow 4] Transition: Validating & Applying State Evolution ---")
            anchor_result = sealed_data.get("anchor_result", sealed_data)
            commit_hash = anchor_result.get("commit_hash", "mock_fallback_hash_0x99")
            
            state_node_struct = await self.hook_build_phase_root(commit_hash, repos)
            evo_ctx = StateAdapter.build_evolution_context(phase_root=state_node_struct, external_rules=[])
            transition_payload = StateAdapter.build_transition_payload(
                intent_action="commit_era", intent_payload=anchor_result, evolution_ctx=evo_ctx
            )
            await self._run_case(f"{self.scenario_name} (Flow 4): Execute Transition", "execute_transition", transition_payload, expected_success=True)

            log.info("--- [Flow 5] Finality: Zero-Trust Parity & Recovery Verification ---")
            t_id_low32 = int(parity_triplet["topos_id"].split('_')[-1]) if '_' in parity_triplet["topos_id"] else 0
            parity_req = {
                "topos_id_low32": t_id_low32,
                "phase_id": parity_triplet["phase_id"],
                "nexus_id": parity_triplet["nexus_id"]
            }
            await self._run_case(f"{self.scenario_name} (Flow 5): Verify Parity Completeness", "verify_parity", parity_req, expected_success=True)

        except Exception as e:
            log.exception(f"[HALTED] Pipeline execution terminated at current phase. Error: {e}")
            self.fail_count += 1
            return

    async def hook_validate_mandate(self) -> dict[str, Any]: return {}
    async def hook_inscribe_nodes(self, parity_triplet: dict[str, Any]) -> dict[str, str]: raise NotImplementedError
    async def hook_process_payment(self) -> dict[str, Any]: return {}
    async def hook_seal_epoch(self, parity_triplet: dict[str, Any], repos: dict[str, str], economy_state: dict[str, Any], timestamp: int) -> dict[str, Any]: raise NotImplementedError
    async def hook_build_phase_root(self, commit_hash: str, repos: dict[str, str]) -> dict[str, Any]: raise NotImplementedError

class SwarmScenario(TrustlessEpochBase):
    def __init__(self, broker: Any):
        super().__init__(broker, "Agentic Economy Swarm Consensus (3-Tier)", simulate_wallet=True)
        self.human_owner_key = ed25519.Ed25519PrivateKey.generate()
        
    async def hook_validate_mandate(self) -> dict[str, Any]:
        ap2_mandate = EcoAdapter.build_ap2_mandate(
            requester_id="urn:agent:gov-agent-01",
            target_action="orchestrate_task_and_pay",
            max_spend_usdc="0.10",
            signer_key=self.human_owner_key
        )
        ## WASM 샌드박스로 Mandate 검증 요청
        await self._run_case("Economy: AP2 Mandate Verification", "validate_intent", {"intent": ap2_mandate}, expected_success=True)
        
        ## 권한 확인 후 안전한 STANDARD Tier 적용
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

    async def hook_process_payment(self) -> dict[str, Any]:
        invoice = EcoAdapter.build_x402_invoice(
            payee_address="0xBaseNetworkTreasuryAddress",
            amount_usdc="0.05",
            resource_id="swarm_epoch_fee"
        )
        receipt = EcoAdapter.process_x402_settlement(
            invoice=invoice,
            agent_wallet_address="0xGovAgentWallet",
            wallet_adapter=self.wallet_adapter
        )
        log.info(f"  └─ [Paid] Amount: {invoice['amount_usdc']} USDC, Tx Hash: {receipt['tx_hash']}")
        return receipt

    async def hook_seal_epoch(self, parity_triplet: dict[str, Any], repos: dict[str, str], economy_state: dict[str, Any], timestamp: int) -> dict[str, Any]:
        await self._set_worker_policy("SYSTEM")
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, parent_nexus_id=0, parent_commit_id="swarm-base",
            repos=repos, cached_states=economy_state # 경제 트랜잭션 기록 병합
        )
        
        # 3-of-3 Full Committee Consensus
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