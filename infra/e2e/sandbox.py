# fiber.infra.e2e.sandbox
import os
import time
import json
import hashlib
import uuid
import random
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

import httpx
from pydantic import BaseModel, Field
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from fiber.infra.adapter.config.exchange import exchange_config

from xphi.arch.bound.adapter.settlement import (
    MandateAdapter, 
    Ap2MandateResult, 
    X402SettlementReceipt,
    TransactionReceipt
)
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.kernel.space.sandbox.runner import SchemeRunner
from xphi.kernel.wasm.method import DphiMethod
from xphi.kernel.wasm.broker import DphiBroker
from xphi.state.anchor.nexus import LedgerEventSchema, StreamAppendRequest
from xphi.arch.model.edge.receptor import (
    TradeIngressRequest,
    AnchorProposalRequest,
    ParityTripletSchema
)
from xphi.arch.model.edge.receipt import ExportLogsServiceRequest
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("infra.sandbox")

# 1. Sandbox Payload Builders (Migrated from node.builder)
class NotarySwarm:
    def __init__(self, size: int = 3):
        self.notaries = []
        for i in range(size):
            seed = hashlib.sha256(f"dphi_notary_node_{i}".encode()).digest()
            private_key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
            public_hex = private_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, 
                format=serialization.PublicFormat.Raw
            ).hex()
            self.notaries.append({"priv": private_key, "pub": public_hex})
        exchange_config.export_attestation.__class__.witness_pubkeys = property(lambda self: [node["pub"] for node in self.notaries])

    @property
    def public_keys(self) -> List[str]:
        return [node["pub"] for node in self.notaries]

    def attest_payload(self, canonical_hash: bytes) -> List[str]:
        return [node["priv"].sign(canonical_hash).hex() for node in self.notaries]


class EcoBuilder:
    __domain_metadata__ = {
        "otlp_payload": "racks LLM GenAI metrics (tokens/latency) for billing.",
        "trade_intent": "W3C DID + UniswapX/Fetch.ai. Intent-centric A2A (Agent-to-Agent) resource swap with slippage.",
        "ledger_append": "Celestia/EigenDA + RISC Zero. Immutable DA (Data Availability) & ZK-verifiable compute logs.",
        "anchor_proposal": "Ethereum L2 (OP Stack) Sequencer. Rollup of state roots (Merkle Parity) for global consensus."
    }


    @staticmethod
    def ap2_mandate_params(
        agent_pub_hex: str, 
        agent_key: ed25519.Ed25519PrivateKey,
        target_action: str = "M2M_INFERENCE_SWAP",
        max_spend_usdc: str = "0.50",
        is_expired: bool = False  
    ) -> Dict[str, Any]:
        return {
            "requester_id": agent_pub_hex,
            "target_action": target_action,
            "max_spend_usdc": max_spend_usdc,
            "signer_key": agent_key,
            "validity_ms": -3600000 if is_expired else 3600000,
            "delegation_tier": "TIER_2_ORACLE",
            "allowed_networks": ["base-mainnet", "arbitrum-one"],
            "ip_restrictions": ["192.168.1.0/24", "10.0.0.0/8"]
        }

    @staticmethod
    def trade_intent(
        action: str = "A2A_COMPUTE_LEASE",
        token: str = "USDC",
        max_fee: str = "2.50",
        slippage: int = 50,
        should_fail_policy: bool = False 
    ) -> Dict[str, Any]:
        if should_fail_policy:
            token = "DOGE"
            slippage = 5000 
            
        req = TradeIngressRequest(
            cilent_id=exchange_config.agents.alpha.did,
            action=action,
            parameters={
                "target_service": exchange_config.agents.beta.did,
                "payment_token": "USDC" if not should_fail_policy else token,
                "max_fee_amount": max_fee,          
                "slippage_tolerance_bps": slippage,      
                "deadline_ts": int(time.time()) + (10 if should_fail_policy else 300),
                "execution_environment": {
                    "hardware": "NVIDIA_H100_80GB",
                    "duration_seconds": 3600,
                    "dataset_cid": "ipfs://QmYwAPJzv5CZsnA625s3Xf2sm5DcgXU1G"
                }
            }
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def otlp_payload(
        model_name: str = None, 
        prompt_tokens: int = None,
        completion_tokens: int = None,
        latency_ms: int = None,
        is_malformed: bool = False 
    ) -> Dict[str, Any]:
        models = ["gemini-1.5-pro", "gpt-4o", "claude-3-5-sonnet", "llama-3.1-70b-instruct"]
        providers = ["gcp", "aws", "azure", "together-ai"]
        
        model_name = model_name or random.choice(models)
        prompt_tokens = prompt_tokens or random.randint(100, 150000)
        completion_tokens = completion_tokens or random.randint(10, 4096)
        latency_ms = latency_ms or random.randint(500, 15000)
        
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        agent_did = exchange_config.agents.alpha.did 
        
        req = ExportLogsServiceRequest(
            resourceLogs=[{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "xelog-agent-gateway"}},
                        {"key": "cloud.provider", "value": {"stringValue": random.choice(providers)}},
                        {"key": "cloud.region", "value": {"stringValue": "us-west-2"}},
                        {"key": "agent.did", "value": {"stringValue": agent_did}},
                        {"key": "tenant.id", "value": {"stringValue": agent_did}} 
                    ]
                },
                "scopeLogs": [{
                    "scope": {"name": "genai.instrumentation", "version": "1.2.0"},
                    "logRecords": [{
                        "timeUnixNano": str(int(time.time() * 1e9)),
                        "traceId": trace_id,
                        "spanId": span_id,
                        "severityText": "INFO",
                        "body": {"stringValue": f"[{model_name}] LLM Inference completed successfully."},
                        "attributes": [
                            {"key": "llm.model", "value": {"stringValue": model_name}},
                            {"key": "gen_ai.request.model", "value": {"stringValue": model_name}},
                            {"key": "gen_ai.response.latency_ms", "value": {"intValue": str(latency_ms)}},
                            {"key": "prompt_tokens", "value": {"intValue": str(prompt_tokens)}},
                            {"key": "completion_tokens", "value": {"intValue": str(completion_tokens)}},
                            {"key": "reasoning_tokens", "value": {"intValue": str(random.randint(0, completion_tokens))}},
                            {"key": "gen_ai.request.temperature", "value": {"doubleValue": "0.7"}},
                            {"key": "gen_ai.response.finish_reason", "value": {"stringValue": "stop"}},
                        ]
                    }]
                }]
            }]
        )
        payload = req.model_dump(exclude_none=True)
        
        if is_malformed:
            del payload["resourceLogs"] 
        else:
            estimated_cost = (prompt_tokens * 0.000001) + (completion_tokens * 0.000002)
            payload["genai_metrics"] = {
                "tenant_id": agent_did,
                "model": model_name,
                "usage": {
                    "prompt_tokens": prompt_tokens, 
                    "completion_tokens": completion_tokens,
                    "estimated_cost_usd": round(estimated_cost, 4)
                }
            }
        return payload

    @staticmethod
    def ledger_append(action_name: str, root_hash: str, event_count: int = 3) -> Dict[str, Any]:
        events = []
        for i in range(event_count):
            pii_payload = {
                "user_email": f"agent_{i}@dphi.network",
                "kyc_wallet_ip": f"192.168.1.{10+i}",
                "auth_token": f"Bearer eyJhbGci...{uuid.uuid4().hex[:8]}"
            }
            
            event = LedgerEventSchema(
                action=f"{action_name}_STEP_{i+1}",
                user_id="system_clearing_engine",
                pii_data=pii_payload, 
                details=f"State transition step {i+1} for intent hash {root_hash}."
            )
            events.append(event)
            
        req = StreamAppendRequest(
            stream_name=exchange_config.da_layer.namespace_id,
            verbose=True,
            events=events
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def anchor_proposal(
        state_roots: Dict[str, str], 
        inject_fault: bool = False 
    ) -> Dict[str, Any]:
        ledger_root = state_roots.get("ledger_root", f"0x{uuid.uuid4().hex}")
        if inject_fault:
            ledger_root = "0xBAD_HASH_CORRUPTED_STATE"
            
        parity = ParityTripletSchema(
            topos_id=f"epoch_{time.strftime('%Y%m%d')}_batch_01",
            phase_id=1,
            nexus_id=14592,
            state_hash=ledger_root
        )
        
        witnesses = exchange_config.export_attestation.witness_pubkeys
        mock_signatures = [f"{uuid.uuid4().hex}{uuid.uuid4().hex}" for _ in range(3)]
        req = AnchorProposalRequest(
            receptor_id=exchange_config.contracts.nexus_clearing,
            proposed_parity=parity,
            parent_nexus_id=14591,
            self_parent_state="genesis",
            repos={
                "exchange_merkle_root": state_roots.get("exchange_root", "0x00"),
                "otlp_telemetry_root": state_roots.get("otlp_root", "0x00")
            },
            signers=witnesses[:3], 
            signatures=mock_signatures,
            timestamp=int(time.time() * 1000)
        )
        return req.model_dump(exclude_none=True)


# ============================================================================
# 2. Sandbox Script Definitions & Isolation Tests
# ============================================================================

@dataclass(frozen=True)
class ScriptDef:
    title: str
    code: str
    expect_success: bool = True
    expected_match: Optional[str | tuple[str, ...]] = None
    tier: str = "SYSTEM"

class TestScripts:
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
        title="Isolation: Selective Gateway & Host Leak Prevention",
        code="""
import os
env = os.environ

## 1. Active Gateway Permeability: Verify that the explicitly whitelisted orchestration marker successfully penetrates the host-to-sandbox bridge
is_gateway_working = env.get('FIBER_ISOLATION_MARKER') == '1'

## 2. Host Environment Segregation: Guarantee strict absence of platform-specific or CI-injected variables to maintain absolute determinism across OS architectures.
blocked_keys = ['GITHUB_ACTIONS', 'COMPUTERNAME', 'XPC_SERVICE_NAME', 'COMMAND_MODE', 'TERM_PROGRAM']
is_host_blocked = all(k not in env for k in blocked_keys)

## 3. State Entropy Constraint: Strictly cap the total quantity of environment variables
is_minimal = len(env) <= 11

## [Assertion] A controlled closed system
isolated = is_gateway_working and is_host_blocked and is_minimal

print(f'Isolated: {isolated} | Dump: {dict(env)}')
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
    HEAP_ALLOCATION_ATTACK = ScriptDef(
        title="Resource: Heap Allocation Guard (OOM / SLA Timeout)",
        code="""
lst = []
while True:
    lst.append(bytearray(10 * 1024 * 1024))
        """.strip(),
        expect_success=False,
        expected_match=("MemoryError", "Sandbox Hard Terminated", "timeout"),
        tier="STANDARD"
    )
    STACK_OVERFLOW_ATTACK = ScriptDef(
        title="Resource: Deep Recursion Guard (Stack Overflow)",
        code="def recurse(n):\n    return recurse(n+1)\nrecurse(1)",
        expect_success=False,
        expected_match="RecursionError"
    )


# ============================================================================
# 3. Sandbox Runners
# ============================================================================

class SandboxRunner(SchemeRunner):
    async def _assert_script(self, script: ScriptDef, context: dict = None, validator: Callable[[str], bool] = None):
        start_time = time.time()
        _ctx = context or {}
        _ctx["sandbox_tier"] = script.tier
        
        result = await self.broker.execute(code=script.code, tier=script.tier, context=_ctx)
        elapsed_ms = (time.time() - start_time) * 1000
        
        output_str = str(result.output) if result.success else str(result.error)
        
        if result.success != script.expect_success:
            self._record_fail(elapsed_ms, f"Expected Success={script.expect_success}, Got {result.success} (Output: {output_str})", "Execution Output", title=script.title)
            return

        if script.expected_match is not None:
            if isinstance(script.expected_match, tuple):
                if not any(match_str in output_str for match_str in script.expected_match):
                    self._record_fail(elapsed_ms, f"Expected one of {script.expected_match} not found in output. Output: {output_str}", "String Match", title=script.title)
                    return
            else:
                if script.expected_match not in output_str:
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
        """[개선] 테스트용 에이전트 키를 생성하여 유효한 Mandate를 조립 후 반환"""
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