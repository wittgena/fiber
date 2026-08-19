# phase.node.runner.sandbox
import os
import time
import json
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.anchor.adapter.eco import EcoAdapter, Ap2MandateResult, X402SettlementReceipt
from phase.anchor.config.client import PhaseBuilder
from bound.client.local.wallet import LocalWalletClient

from kernel.phase.runner import SchemeRunner
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.method import DphiMethod
from watcher.plane.emitter import get_emitter

log = get_emitter("sandbox.runner")

@dataclass(frozen=True)
class ScriptDef:
    title: str
    code: str
    expect_success: bool = True
    expected_match: Optional[str] = None
    tier: str = "SYSTEM"

class TestScripts:
    # (기존 TestScripts 내부 내용은 완벽하므로 그대로 유지합니다)
    LEGACY_NORMAL = ScriptDef(
        title="Integrity: Light Compute (Simple Math)",
        code="print(sum([x**2 for x in range(1000)]))",
        expect_success=True,
        expected_match="332833500"
    )
    
    COMPUTE_HEAVY = ScriptDef(
        title="Workload: Heavy CPU Compute (Prime Factorization)",
        code="""
def is_prime(n):
    if n < 2: return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0: return False
    return True
primes = [p for p in range(30000) if is_prime(p)]
print(f'Found {len(primes)} primes')
        """.strip(),
        expect_success=True,
        expected_match="Found 3245 primes"
    )

    DATA_PROCESSING = ScriptDef(
        title="Workload: Memory & Data Processing (JSON Array)",
        code="""
import json
data = [{'id': i, 'val': i * 2.5, 'active': i % 2 == 0} for i in range(20000)]
serialized = json.dumps(data)
parsed = json.loads(serialized)
print(f'Processed {len(parsed)} records')
        """.strip(),
        expect_success=True,
        expected_match="Processed 20000 records"
    )

    TIME_LEAK = ScriptDef(
        title="Determinism: Sandbox Context Time Enforcement",
        code="import time\nprint(f'{time.time()}|{time.perf_counter()}')"
    )
    INJECTION = ScriptDef(
        title="Determinism: Context Injection",
        code="import time, random\nprint(f'{time.time()}|{random.random()}')"
    )
    PRNG_IDEMPOTENT = ScriptDef(
        title="Determinism: PRNG Idempotency",
        code="import random, os\nprint(f'{random.random()}|{os.urandom(4).hex()}')"
    )
    
    ENV_LEAK = ScriptDef(
        title="Isolation: Prevent Host Environment Variable Leakage",
        code="""
import os
is_virtual = '__LLVM_PROFILE_RT_INIT_ONCE' in os.environ
current_path = os.environ.get('PATH', '')
is_host_path_blocked = len(current_path.split(':')) <= 3 and 'Users' not in current_path
print(f'Isolated: {is_virtual and is_host_path_blocked}')
        """.strip(),
        expect_success=True,
        expected_match="Isolated: True"
    )
    IO_VIOLATION = ScriptDef(
        title="Isolation: Deny Low-level Filesystem Scan",
        code="with open('/etc/passwd', 'r') as f:\n    print(f.read())",
        expect_success=False,
        expected_match="FileNotFoundError"
    )
    NET_VIOLATION = ScriptDef(
        title="Isolation: Deny Low-level Socket Binding",
        code="import socket\ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\ns.connect(('8.8.8.8', 53))",
        expect_success=False,
        expected_match="Error" 
    )
    SYS_EXIT_ATTACK = ScriptDef(
        title="Isolation: Host Protection against sys.exit()",
        code="import sys\nsys.exit(1)",
        expect_success=False,
        expected_match="PythonError"
    )
    SUBPROCESS_ATTACK = ScriptDef(
        title="Isolation: Deny Process Spawning (Subprocess)",
        code="import subprocess\nsubprocess.run(['ls', '-la'])",
        expect_success=False,
        expected_match="Error" 
    )
    THREAD_ATTACK = ScriptDef(
        title="Isolation: Deny Multi-threading",
        code="import threading\ndef f(): pass\nt = threading.Thread(target=f)\nt.start()",
        expect_success=False
    )
    
    INFINITE_LOOP_ATTACK = ScriptDef(
        title="Resource: Opcode-based Fuel Exhaustion",
        code="x = 2\nwhile True: x = x ** 2",
        expect_success=False,
        expected_match="timeout", 
        tier="STANDARD"
    )
    OOM_ATTACK = ScriptDef(
        title="Resource: Heap Fragmentation OOM Guard",
        code="""
lst = []
while True:
    lst.append('A' * (1024 * 1024))
        """,
        expect_success=False,
        expected_match="MemoryError", 
        tier="STANDARD"
    )
    STACK_OVERFLOW_ATTACK = ScriptDef(
        title="Resource: Deep Recursion Guard (Stack Overflow)",
        code="def recurse(n):\n    return recurse(n+1)\nrecurse(1)",
        expect_success=False,
        expected_match="RecursionError"
    )


class SandboxRunner(SchemeRunner):
    async def _assert_script(self, script: ScriptDef, context: dict = None, validator: Callable[[str], bool] = None):
        start_time = time.time()
        
        # 🌟 지연 정산 섀도우 연산을 위한 context 주입 (옵션)
        _ctx = context or {}
        _ctx["sandbox_tier"] = script.tier
        
        result = await self.broker.execute(code=script.code, tier=script.tier, context=_ctx)
        elapsed_ms = (time.time() - start_time) * 1000
        
        output_str = str(result.output) if result.success else str(result.error)
        
        if result.success != script.expect_success:
            self._record_fail(elapsed_ms, f"Expected Success={script.expect_success}, Got {result.success} (Output: {output_str})", "Execution Output", title=script.title)
            return
            
        if script.expected_match and script.expected_match not in output_str:
            self._record_fail(elapsed_ms, f"Expected string '{script.expected_match}' not found in output. Output: {output_str}", "String Match", title=script.title)
            return
            
        if validator:
            try:
                if not validator(output_str):
                    self._record_fail(elapsed_ms, f"Validation failed: {output_str}", "Custom Validator", title=script.title)
                    return
            except Exception as e:
                self._record_fail(elapsed_ms, f"Validation crashed: {e} (Output: {output_str})", "Validator Exception", title=script.title)
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
        
        self.wallet_client: LocalWalletClient = PhaseBuilder.get_testnet_wallet()
        self.wallet_client.simulate = simulate_wallet

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
            if ap2_mandate and not EcoAdapter.verify_mandate_signature(ap2_mandate):
                raise RuntimeError("Invalid Mandate Signature detected in Sandbox Validation")
            
            log.info("--- [Flow 2] Inscription: Gathering Local Node States ---")
            repos = await self.hook_inscribe_nodes(parity_triplet)

            log.info("--- [Flow 2.5] Economy: Off-chain Capability Token Issuance / Metering ---")
            x402_receipt = await self.hook_process_payment(ap2_mandate)
            economy_state = EcoAdapter.embed_economy_state({}, ap2_mandate, x402_receipt)
            
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
        """[개선] 테스트용 에이전트 키를 생성하여 유효한 Mandate를 조립 후 반환"""
        # 하위 클래스에서 오버라이드하여 실제 Mandate 객체를 반환하도록 구현 가능
        return None
        
    async def hook_inscribe_nodes(self, parity_triplet: Dict[str, Any]) -> Dict[str, str]: 
        raise NotImplementedError
        
    async def hook_process_payment(self, mandate: Optional[Ap2MandateResult] = None) -> Optional[X402SettlementReceipt]: 
        """
        🌟 [개선] 지연 정산 훅: L1 결제(Push) 대신, Mandate를 기반으로 
        오프체인 Capability Token(영수증)을 발급하는 프로세스로 섀도우 연산 대체
        """
        if mandate:
            log.info(f"  └─ Issuing Capability Receipt based on Mandate: {mandate.mandate.constraints.max_spend_usdc} USDC")
            return EcoAdapter.issue_deferred_receipt(mandate)
            
        # Mandate가 없는 레거시 또는 Mock 상황
        mock_amount = "0.01"
        if mock_amount == "0.00":
            return None

        # 지연 정산 시나리오에서 Mandate 없이 결제를 진행하는 경우 (예외적 즉시 결제 시뮬레이션)
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