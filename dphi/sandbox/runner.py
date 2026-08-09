# dphi.sandbox.runner
## @lineage: epoch.sandbox.runner
import time
import json
import hashlib
from typing import Any, Dict, List, Optional, Callable
import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from dphi.sandbox.script.test import ScriptDef
from dphi.adapter.eco import EcoAdapter, WalletAdapter, Ap2MandateResult, X402SettlementReceipt
from kernel.phase.runner import SchemeRunner
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("sandbox.runner")

class SandboxRunner(SchemeRunner):
    async def _assert_script(self, script: ScriptDef, context: dict = None, validator: Callable[[str], bool] = None):
        start_time = time.time()
        result = await self.broker.execute(code=script.code, tier=script.tier, context=context)
        elapsed_ms = (time.time() - start_time) * 1000
        
        output_str = str(result.output) if result.success else str(result.error)
        if result.success != script.expect_success:
            self._record_fail(elapsed_ms, f"Expected Success={script.expect_success}, Got {result.success} (Output: {output_str})", script.title)
            return
            
        if script.expected_match and script.expected_match not in output_str:
            self._record_fail(elapsed_ms, f"Expected string '{script.expected_match}' not found in output. Output: {output_str}", script.title)
            return
            
        if validator and not validator(output_str):
            self._record_fail(elapsed_ms, f"Validation failed: {output_str}", script.title)
            return
            
        self._record_success(elapsed_ms, output_str)

class EpochBase(SchemeRunner):
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

    def _sign_multisig(self, signers: List[ed25519.Ed25519PrivateKey], commit_dict: Dict[str, Any]) -> List[str]:
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

    async def hook_validate_mandate(self) -> Optional[Ap2MandateResult]: 
        return None
        
    async def hook_inscribe_nodes(self, parity_triplet: Dict[str, Any]) -> Dict[str, str]: 
        raise NotImplementedError
        
    async def hook_process_payment(self) -> Optional[X402SettlementReceipt]: 
        return None
        
    async def hook_seal_epoch(self, parity_triplet: Dict[str, Any], repos: Dict[str, str], economy_state: Dict[str, Any], timestamp: int) -> Dict[str, Any]: 
        raise NotImplementedError
        
    async def hook_build_phase_root(self, commit_hash: str, repos: Dict[str, str]) -> Dict[str, Any]: 
        raise NotImplementedError