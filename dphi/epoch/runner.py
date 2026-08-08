# dphi.epoch.runner
import time
import json
import hashlib
from typing import Any, Dict, List, Optional
import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from dphi.adapter.eco import EcoAdapter, WalletAdapter, Ap2MandateResult, X402SettlementReceipt
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("scheme.runner")

class BaseRunner:
    def __init__(self):
        self.success_count = 0
        self.fail_count = 0
        self.last_failed_context: List[str] = []

    def report(self):
        log.info(f"\n=== [DONE] Scenarios Completed: {self.success_count} Passed, {self.fail_count} Failed ===")
        if self.fail_count > 0:
            log.warning("Review the following failed contexts:")
            for ctx in self.last_failed_context:
                log.warning(f" - {ctx}")

    def _record_success(self, elapsed_ms: float, msg: str):
        self.success_count += 1
        safe_msg = str(msg)[:150].replace('\n', ' ')
        log.info(f"  [PASS] Time: {elapsed_ms:.2f}ms | Output: {safe_msg}")

    def _record_fail(self, elapsed_ms: float, error_msg: str, context: str):
        self.fail_count += 1
        log.error(f"  [FAIL] Time: {elapsed_ms:.2f}ms | Details: {error_msg}")
        self.last_failed_context.append(context)

class SchemeRunner(BaseRunner):
    def __init__(self, broker: Any):
        super().__init__()
        self.broker = broker

    async def _set_worker_policy(self, tier_name: str):
        log.info(f"\n[Control Plane] Shifting WasmCgroup Policy Tier -> {tier_name}")
        try:
            if hasattr(self.broker, "update_policy"):
                await self.broker.update_policy(tier=tier_name)
                log.info(f"  └─ Policy successfully enforced to {tier_name}.")
            else:
                log.warning(f"  └─ Broker missing 'update_policy' API.")
        except Exception as e:
            log.error(f"  └─ Failed to update policy: {e}")

    async def _run_case(self, title: str, target_func: str, payload: Any, expected_success: bool):
        log.info(f"\n[TEST] {title} (Func: {target_func})")
        start_time = time.time()
        result = await self.broker.invoke(target_func=target_func, payload=payload)
        elapsed_ms = (time.time() - start_time) * 1000
        
        if result.success == expected_success:
            self._record_success(elapsed_ms, str(result.output) if result.success else str(result.error))
        else:
            err_msg = str(result.error) if not result.success else str(result.output)
            safe_payload_str = str(payload)[:50]
            self._record_fail(
                elapsed_ms, 
                f"Expected success={expected_success}, Got success={result.success}. {err_msg}",
                f"Function: {target_func} | Input: {safe_payload_str}... | Error: {err_msg}"
            )

class WebRunner(BaseRunner):
    def __init__(self, base_url: str, client: Optional[httpx.AsyncClient] = None):
        super().__init__()
        self.base_url = base_url
        self._is_injected_client = client is not None
        self.client = client or httpx.AsyncClient(base_url=base_url, timeout=10.0)
        self.committee_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.committee_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.committee_keys
        ]

    async def teardown(self):
        """WebRunner가 자체 생성한 클라이언트인 경우 리소스를 정리합니다."""
        if not self._is_injected_client and not self.client.is_closed:
            await self.client.aclose()

    async def _run_api_case(self, title: str, method: str, endpoint: str, payload: Dict[str, Any], expected_status: int = 200) -> Optional[httpx.Response]:
        log.info(f"\n[TEST] {title} ({method} {endpoint})")
        start_time = time.time()
        
        try:
            res = await self.client.request(method, endpoint, json=payload)
            elapsed_ms = (time.time() - start_time) * 1000
            
            if res.status_code == expected_status:
                self._record_success(elapsed_ms, res.text)
                return res
            else:
                self._record_fail(
                    elapsed_ms,
                    f"Expected: {expected_status}, Got: {res.status_code}. Response: {res.text[:100]}",
                    f"Endpoint: {endpoint} | Error: {res.text[:200]}"
                )
                return res
        except Exception as e:
            self.fail_count += 1
            log.error(f"  [CRITICAL FAIL] Network/Execution Error: {str(e)}")
            return None

    def _sign_payload(self, signers: List[ed25519.Ed25519PrivateKey], payload_dict: Dict[str, Any]) -> List[str]:
        raw_json_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        commit_hash = hashlib.sha256(raw_json_bytes).digest()
        return [k.sign(commit_hash).hex() for k in signers]

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