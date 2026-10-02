# fiber.infra.e2e.epoch
import os
import time
import json
import hashlib
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from xphi.arch.bound.adapter.settlement import MandateAdapter, Ap2MandateResult, X402SettlementReceipt
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.kernel.space.sandbox.runner import SchemeRunner
from xphi.kernel.wasm.method import DphiMethod
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("infra.e2e.epoch")

class EpochBase(SchemeRunner):
    def __init__(self, broker: Any, scenario_name: str):
        super().__init__(broker)
        self.scenario_name = scenario_name
        self.committee_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.committee_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.committee_keys
        ]
        
    def _sign_multisig(self, signers: List[ed25519.Ed25519PrivateKey], commit_dict: Dict[str, Any]) -> List[str]:
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest().encode('utf-8')
        return [k.sign(commit_hash).hex() for k in signers]

    async def execute_anchor_lifecycle(self, topo: int, press: int, rupture: bool) -> None:
        log.info(f"\n=== [Lifecycle START] {self.scenario_name} ===")
        
        try:
            log.info("--- [Flow 1] Initialization: Requesting Parity Triplet ---")
            current_ts = int(time.time() * 1000)
            init_req = {
                "ts": current_ts, 
                "topo": topo, 
                "press": press, 
                "rupture": rupture,
                "injected_tick": None
            }
            init_payload = StateAdapter.to_canonical_bytes(init_req).decode('utf-8')
            res = await self.broker.invoke(DphiMethod.INIT_EPOCH.value, init_payload)
            if not res.success:
                raise RuntimeError(f"{DphiMethod.INIT_EPOCH.value} Failed: {res.error}")
                
            parity_triplet = json.loads(res.output)
            log.info(f"  └─ Generated Nexus ID: {parity_triplet.get('nexus_id')}")
            
            log.info("--- [Flow 1.5] Economy: AP2 Mandate Validation ---")
            ap2_mandate = await self.hook_validate_mandate()
            if ap2_mandate and not MandateAdapter.verify_mandate_signature(ap2_mandate):
                raise RuntimeError("Invalid Mandate Signature detected in Sandbox Validation")
            
            log.info("--- [Flow 2] Inscription: Gathering Local Node States ---")
            repos = await self.hook_inscribe_nodes(parity_triplet)

            log.info("--- [Flow 2.5] Economy: Off-chain Capability Token Issuance / Metering ---")
            x402_receipt = await self.hook_process_payment(ap2_mandate)
            economy_state = MandateAdapter.embed_economy_state({}, ap2_mandate, x402_receipt)
            
            log.info("--- [Flow 3] Sealing: Cryptographic Epoch Alignment ---")
            seal_payload_dict = await self.hook_seal_epoch(parity_triplet, repos, economy_state, current_ts)
            
            seal_payload_str = StateAdapter.to_canonical_bytes(seal_payload_dict).decode('utf-8')
            seal_res = await self.broker.invoke(DphiMethod.SEAL_EPOCH.value, seal_payload_str)
            if not seal_res.success:
                raise RuntimeError(f"{DphiMethod.SEAL_EPOCH.value} Failed: {seal_res.error}")
                
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
            await self._run_case(
                f"{self.scenario_name} (Flow 4): Execute Transition", 
                DphiMethod.EXECUTE_TRANSITION.value, 
                transition_payload, 
                expected_success=True
            )

            log.info("--- [Flow 5] Finality: Zero-Trust Parity & Recovery Verification ---")
            t_id_low32 = int(parity_triplet["topos_id"].split('_')[-1]) if '_' in parity_triplet["topos_id"] else 0
            parity_req = {
                "topos_id_low32": t_id_low32,
                "phase_id": parity_triplet["phase_id"],
                "nexus_id": parity_triplet["nexus_id"]
            }
            await self._run_case(
                f"{self.scenario_name} (Flow 5): Verify Parity Completeness", 
                DphiMethod.VERIFY_PARITY.value, 
                parity_req, 
                expected_success=True
            )

        except Exception as e:
            log.exception(f"[HALTED] Pipeline execution terminated at current phase. Error: {e}")
            self._record_fail(0, str(e), "Lifecycle Exception", title=f"Lifecycle: {self.scenario_name}")
            return

    async def hook_validate_mandate(self) -> Optional[Ap2MandateResult]: 
        return None
        
    async def hook_inscribe_nodes(self, parity_triplet: Dict[str, Any]) -> Dict[str, str]: 
        raise NotImplementedError
        
    async def hook_process_payment(self, mandate: Optional[Ap2MandateResult] = None) -> Optional[X402SettlementReceipt]: 
        if mandate:
            log.info(f"  └─ Issuing Capability Receipt based on Mandate: {mandate.mandate.constraints.max_spend_usdc} USDC")
            return MandateAdapter.issue_deferred_receipt(mandate)
            
        mock_amount = "0.01"
        if mock_amount == "0.00":
            return None

        mock_tx_hash = f"0x_mock_push_{os.urandom(8).hex()}"
        return X402SettlementReceipt(
            receipt_id=f"rcpt_push_{mock_tx_hash[2:14]}",
            receipt_type="INSTANT_PUSH",
            tx_hash=mock_tx_hash,
            network="x402/base-sepolia",
            amount_usdc=mock_amount,
            payer_wallet="0x0000000000000000000000000000000000000000",
            settled_at=int(time.time() * 1000)
        )
        
    async def hook_seal_epoch(self, parity_triplet: Dict[str, Any], repos: Dict[str, str], economy_state: Dict[str, Any], timestamp: int) -> Dict[str, Any]: 
        raise NotImplementedError
        
    async def hook_build_phase_root(self, commit_hash: str, repos: Dict[str, str]) -> Dict[str, Any]: 
        raise NotImplementedError