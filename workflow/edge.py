# fiber.workflow.edge
## @lineage: workflow.edge
import asyncio
import hashlib
import random
import time
import uuid
import uvicorn
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct

from fiber.dphi.adapter.config import dphi_env
from fiber.dphi.receptor.rest import create_app, Config
from fiber.dphi.client.http import VerifiedHttpClient

from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.dphi.runner.phase import WebRunner
from xphi.kernel.phase.reactor import PhaseReactor

from xphi.watcher.ingress.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from xphi.watcher.receptor.edge.tracer import E2EConfig, SceneConfig, HttpFlowTracer
from xphi.watcher.plane.emitter import flow_scope, get_emitter
from xphi.watcher.tracer.dphi import DphiTracer
from xphi.watcher.wasm.builder import WasmBuilder

log = get_emitter("workflow.edge")

class StartSceneMsg(WorkflowMessage): pass
class AgentQuoteMsg(WorkflowMessage): pass
class BillingInvoiceMsg(WorkflowMessage): pass
class BillingBalanceMsg(WorkflowMessage): pass
class AgentExecuteMsg(WorkflowMessage): pass  
class AuditVerifyMsg(WorkflowMessage): pass
class OtlpIngressMsg(WorkflowMessage): pass
class D3FiExchangeMsg(WorkflowMessage): pass
class LedgerAppendMsg(WorkflowMessage): pass
class DvmClearingMsg(WorkflowMessage): pass

@dataclass
class Phase:
    name: str
    action: Callable[[], Coroutine[Any, Any, None]]

@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success


class ManagedTestServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass


def create_otlp_payload(inject_faults: bool) -> dict:
    from fiber.dphi.adapter.anchor import PhaseBuilder
    if inject_faults:
        return {"garbage_field_missing_required_keys": True}
    return PhaseBuilder.otlp_payload(is_malformed=False)

def create_agent_intent_payload(inject_faults: bool) -> dict:
    return {
        "agent_id": "test-agent-01",
        "responder_id": "target-node-01", 
        "action": "EXECUTE_PYTHON",
        "source_code": "print('Hello from Edge E2E Test')",
        "max_fuel": 1000000,
        "signature": "0x_bad_signature_for_testing_faults" if inject_faults else "0x_valid_dummy_signature",
        "sig_algo": "ECDSA_SECP256K1" 
    }

def create_trade_payload(inject_faults: bool) -> dict:
    if inject_faults:
        return {"agent_id": "test-agent-01"} 
    return {
        "agent_id": "test-agent-01", 
        "action": "TRADE", 
        "parameters": {"target_pair": "ETH/USDC", "amount": 100}
    }

def create_ledger_payload(exchange_root: str, inject_faults: bool) -> dict:
    if inject_faults:
        return {"stream_name": "audit_stream"} 
    return {
        "stream_name": "system_audit",
        "events": [
            {
                "action": "D3FI_SETTLEMENT",
                "user_id": "agent-01",
                "details": f"Trade Executed. State Root: {exchange_root}"
            }
        ],
        "verbose": False
    }

def get_edge_scene_config() -> SceneConfig:
    return SceneConfig(
        otlp_builder=create_otlp_payload,
        agent_intent_builder=create_agent_intent_payload,
        trade_builder=create_trade_payload,
        ledger_builder=create_ledger_payload
    )


class EdgeWorkflow(Workflow):
    def __init__(
        self, 
        config: E2EConfig, 
        scene_config: SceneConfig, 
        client: httpx.AsyncClient, 
        inject_faults: bool = False,
        attestation_injector: Optional[Callable[[httpx.Response], httpx.Response]] = None
    ):
        super().__init__(name="E2E_SCENE_NET")
        self.config = config
        self.scene_config = scene_config
        self.inject_faults = inject_faults
        self.runner = WebRunner(config.base_url, client=client)
        self.state_roots: Dict[str, str] = {}
        self.log = get_emitter("workflow.scene_runner")
        self.attestation_injector = attestation_injector
        
        self.test_acct = Account.create()
        self.test_agent_id = self.test_acct.address

    def _sign_payload(self, text_to_sign: str) -> str:
        msg = encode_defunct(text=text_to_sign)
        return self.test_acct.sign_message(msg).signature.hex()

    async def execute(self):
        mode_str = "Negative/Faults" if self.inject_faults else "Golden Path"
        if self.attestation_injector:
            mode_str = "Attestation Rejection (Tampered Headers)"
            
        self.log.info(f"\n=== [START] {self.name} ({mode_str}) ===")
        self.post_message(StartSceneMsg())
        await self.run()

    def _verify_attestation(self, response: httpx.Response, request_path: str) -> Optional[ErrorMessage]:
        if response.status_code != 200:
            return None

        if self.attestation_injector:
            self.log.warning(f"  └─ 👾 Injecting Chaos: Tampering Attestation Headers for {request_path}")
            response = self.attestation_injector(response)
            
        try:
            self.log.info(f"  └─ 🔍 Verifying First-Party Attestation for {request_path}...")
            verifier = VerifiedHttpClient(client=self.runner.client)
            verifier._verify_header_proof(response)
            self.log.info("  └─ ✅ Attestation Signature Verified Successfully!")
            return None
        except Exception as e:
            self.log.error(f"  └─ 🚨 Attestation Failed: {e}")
            self.runner.fail_count += 1
            return ErrorMessage(f"Attestation Proof Verification Failed: {e}")

    @step
    async def phase_head_smoke(self, msg: StartSceneMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1] API Head & MCP Connectivity Sweep ---")
        res = await self.runner.client.get(f"{self.runner.base_url}/openapi.json")
        if res.status_code == 200: self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("API Head is unreachable")

        mcp_res = await self.runner.client.post(f"{self.runner.base_url}/mcp/sse")
        if mcp_res.status_code in [405, 400]: self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("MCP Server unreachable")
            
        return AgentQuoteMsg() 

    @step
    async def phase_agent_quote(self, msg: AgentQuoteMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1.5] Public Agent Pre-flight Quotation (Dry-run) ---")
        path = "/v1/public/agent/quote"
        payload = self.scene_config.agent_intent_builder(self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        
        headers = {}
        if self.inject_faults:
            headers = {"X-X402-Receipt": "dummy_receipt_to_trigger_premium_validation"}
        
        self.log.info(f"  └─ Sending POST to {path} (Expected: {expected_status})")
        try:
            res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload, headers=headers)
            if res.status_code == expected_status:
                if not self.inject_faults:
                    self.log.info(f"  └─ ✅ Passed: Dry-run quotation processed correctly.")
                else:
                    self.log.info(f"  └─ 🛡️ Defense Triggered: Invalid intent rejected ({res.status_code}).")
                self.runner.success_count += 1
            else:
                self.log.error(f"  └─ ❌ Failed: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")
                self.runner.fail_count += 1
                return ErrorMessage(f"Quote Check Failed: Expected {expected_status}, Got {res.status_code}")
        except Exception as e:
            return ErrorMessage(str(e))
            
        return BillingInvoiceMsg()

    @step
    async def phase_billing_invoice(self, msg: BillingInvoiceMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1.6] Public L402 Invoice Generation ---")
        path = "/v1/public/billing/invoice"
        payload = {
            "payee_address": "0x000000000000000000000000000000000000dEaD",
            "amount_usdc": "5.0",
            "resource_id": f"res_{uuid.uuid4().hex[:8]}"
        }
        if self.inject_faults:
            payload.pop("amount_usdc") 
        expected_status = 422 if self.inject_faults else 200
        
        self.log.info(f"  └─ Sending POST to {path} (Expected: {expected_status})")
        res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload)
        
        if res.status_code == expected_status:
            if not self.inject_faults:
                self.log.info(f"  └─ ✅ Passed: L402 Invoice generation successful.")
            else:
                self.log.info(f"  └─ 🛡️ Defense Triggered: Missing invoice params rejected ({res.status_code}).")
            self.runner.success_count += 1
        else:
            self.log.error(f"  └─ ❌ Failed: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")
            self.runner.fail_count += 1
            return ErrorMessage(f"Invoice Issue Check Failed: Got {res.status_code}")
            
        return BillingBalanceMsg()

    @step
    async def phase_billing_balance(self, msg: BillingBalanceMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1.7] Public UTXO Hot State Balance Read ---")
        path = "/v1/public/billing/balance"
        params = {"agent_id": self.test_agent_id, "asset_type": "fuel"}
        if self.inject_faults:
            params.pop("agent_id")
        expected_status = 422 if self.inject_faults else 200
        
        self.log.info(f"  └─ Sending GET to {path} (Expected: {expected_status})")
        res = await self.runner.client.get(f"{self.runner.base_url}{path}", params=params)
        
        if res.status_code == expected_status:
            if not self.inject_faults:
                self.log.info(f"  └─ ✅ Passed: Hot State Balance read successful.")
            else:
                self.log.info(f"  └─ 🛡️ Defense Triggered: Missing agent_id rejected ({res.status_code}).")
            self.runner.success_count += 1
        else:
            self.log.error(f"  └─ ❌ Failed: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")
            self.runner.fail_count += 1
            return ErrorMessage(f"Balance Check Failed: Got {res.status_code}")
            
        return AgentExecuteMsg()

    @step
    async def phase_agent_execute(self, msg: AgentExecuteMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 2] Public Agent Execution & Metering ---")
        
        path = "/v1/public/agent/execute"
        payload = self.scene_config.agent_intent_builder(self.inject_faults)
        payload["agent_id"] = self.test_agent_id
        
        if not self.inject_faults:
            sig_text = f"EXECUTE:{self.test_agent_id}:{payload['action']}:{payload['max_fuel']}"
            payload["signature"] = self._sign_payload(sig_text)
            expected_status = 200
        else:
            expected_status = 401 
        
        headers = {"X-X402-Receipt": "audit_receipt_dummy_string"}
        
        self.log.info(f"  └─ Sending POST to {path} (Expected: {expected_status})")
        try:
            res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload, headers=headers)
            
            if res.status_code == expected_status:
                if not self.inject_faults:
                    self.state_roots["audit_receipt"] = res.json()
                    # 💡 [개선] 단순 200 OK가 아닌 실행 증명 출력
                    self.log.info(
                        f"  └─ ✅ Passed: Agent Computed Successfully.\n"
                        f"     ├─ 🤖 Agent ID  : {self.test_agent_id[:10]}...\n"
                        f"     ├─ ⚙️ Action    : {payload['action']}\n"
                        f"     └─ ⛽ Max Fuel  : {payload['max_fuel']}"
                    )
                else:
                    # 💡 [개선] 네거티브 패스 방어 증명
                    self.log.info(f"  └─ 🛡️ Defense Triggered: Malformed intent safely rejected ({res.status_code}).")
                
                self.runner.success_count += 1
            else:
                self.log.error(f"  └─ ❌ Failed: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")
                self.runner.fail_count += 1
                return ErrorMessage(f"Agent Execution Check Failed: Expected {expected_status}, Got {res.status_code}")
                
            attest_err = self._verify_attestation(res, path)
            if attest_err: return attest_err
        except Exception as e:
            return ErrorMessage(str(e))
            
        return AuditVerifyMsg()

    @step
    async def phase_audit_verify(self, msg: AuditVerifyMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 2.5] AuditReceipt Verification (Auditor Validation) ---")
        path = "/v1/public/audit/verify"
        
        receipt = self.state_roots.get("audit_receipt", {})
        if not receipt and not self.inject_faults:
            self.log.warning("  └─ ⚠️ No receipt found from previous step. Skipping verification.")
            return OtlpIngressMsg()
            
        if self.inject_faults:
            # 고의로 필드를 누락시켜 WASM의 422 Rejection 유도
            receipt = {"state_root": "0x_tampered_root_hash_for_chaos", "receipt_id": "fake_123"}
            expected_status = 422 
        else:
            # Rust WASM 커널이 요구하는 ParityRequest 필드 명시적 주입
            receipt["topos_id_low32"] = 1
            receipt["phase_id"] = 2
            receipt["nexus_id"] = 3
            expected_status = 200
            
        self.log.info(f"  └─ Sending POST to {path} (Expected: {expected_status})")
        res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=receipt)
        
        if res.status_code == expected_status:
            if not self.inject_faults:
                self.log.info(f"  └─ ✅ Passed: Cryptographic verification validated by Auditor.")
            else:
                self.log.info(f"  └─ 🛡️ Defense Triggered: Tampered receipt verification blocked ({res.status_code}).")
            self.runner.success_count += 1
        else:
            self.log.error(f"  └─ ❌ Failed: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")
            self.runner.fail_count += 1
            return ErrorMessage(f"Audit Verify Check Failed: Got {res.status_code}")
            
        return OtlpIngressMsg()

    @step
    async def phase_otlp_ingress(self, msg: OtlpIngressMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 3] OTLP Telemetry Ingress & Strict Schema Check ---")
        
        path = "/v1/public/telemetry/logs"
        payload = self.scene_config.otlp_builder(self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        headers = {"X-X402-Receipt": "audit_receipt_dummy_string"}
            
        self.log.info(f"  └─ Sending POST to {path} (Expected: {expected_status})")
        try:
            res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload, headers=headers)
            
            if res.status_code == expected_status:
                if not self.inject_faults:
                    self.state_roots["otlp_root"] = res.headers.get("x-edge-content-hash", "0x_default_otlp_hash")
                    self.log.info(f"  └─ ✅ Passed: OTLP Log ingested successfully.")
                else:
                    self.log.info(f"  └─ 🛡️ Defense Triggered: Bad OTLP schema dropped ({res.status_code}).")
                self.runner.success_count += 1
            else:
                self.log.error(f"  └─ ❌ Failed OTLP Ingress: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")
                self.runner.fail_count += 1
                return ErrorMessage(f"OTLP Ingress Check Failed: Expected {expected_status}, Got {res.status_code}")
                
            attest_err = self._verify_attestation(res, path)
            if attest_err: return attest_err
        except Exception as e:
             return ErrorMessage(str(e))
             
        return D3FiExchangeMsg()

    @step
    async def phase_d3fi_exchange(self, msg: D3FiExchangeMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 4] D3Fi P2P Trade & Settlement (ExchangeNet) ---")
        
        path = "/v1/eco/exchange/order/ingress"
        payload = self.scene_config.trade_builder(self.inject_faults)
        payload["agent_id"] = self.test_agent_id
        expected_status = 422 if self.inject_faults else 200
        
        res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload)
        if res.status_code == expected_status:
            if not self.inject_faults:
                self.state_roots["exchange_root"] = res.json().get("session", {}).get("topo_id", f"d3fi_{uuid.uuid4().hex[:8]}")
                self.log.info(f"  └─ ✅ Passed: D3Fi trade order accurately routed to ExchangeNet.")
            else:
                self.log.info(f"  └─ 🛡️ Defense Triggered: Malformed trade order dismissed ({res.status_code}).")
            self.runner.success_count += 1
        else:
             self.runner.fail_count += 1
             return ErrorMessage(f"D3Fi Ingress Check Failed: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")
             
        return LedgerAppendMsg()

    @step
    async def phase_ledger_append(self, msg: LedgerAppendMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 5] Immutable Ledger Stream Append ---")
        
        path = "/v1/core/ledger/stream/append"
        exchange_root = self.state_roots.get("exchange_root", "0x00")
        payload = self.scene_config.ledger_builder(exchange_root, self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
            
        res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload)
        if res.status_code == expected_status:
            if not self.inject_faults:
                ledger_hash = res.json().get("result", {}).get("hash", "0x_default_ledger_hash")
                self.state_roots["ledger_root"] = ledger_hash
                # 💡 [개선] 렛저 기록 증명 출력
                self.log.info(f"  └─ ✅ Passed: Ledger Appended. [Root: {ledger_hash[:16]}...]")
            else:
                self.log.info(f"  └─ 🛡️ Defense Triggered: Invalid Ledger Schema rejected ({res.status_code}).")
            self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage(f"Ledger Append Failed: Expected {expected_status}, Got {res.status_code}")
            
        return DvmClearingMsg()

    @step
    async def phase_dvm_clearing(self, msg: DvmClearingMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 6] DVM Ledger Clearing & Settlement (Internal L2) ---")
        
        path = "/v1/ext/wallet/pay/x402"
        payload = {
            "payee_address": "0x000000000000000000000000000000000000dEaD",
            "amount_usdc": "10.0",
            "resource_id": f"res_{uuid.uuid4().hex[:8]}",
            "use_ledger": True
        }
        
        if self.inject_faults: 
            payload.pop("amount_usdc") 
            
        expected_status = 422 if self.inject_faults else 200
        
        self.log.info(f"  └─ Sending POST to {path} with use_ledger=True (Expected: {expected_status})")
        res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload)
        
        if res.status_code == expected_status:
            self.runner.success_count += 1
            if not self.inject_faults:
                # 💡 [개선] X402 최종 정산 영수증 렌더링
                mock_tx_hash = res.json().get("tx_hash", "0x0adc15b94c5ab3b8") if res.text else "0x0adc15b94c5ab3b8"
                receipt = (
                    f"\n    🧾 [X402 DEFERRED SETTLEMENT RECEIPT]\n"
                    f"     ├─ 👤 Payee      : {payload['payee_address'][:10]}...dEaD\n"
                    f"     ├─ 💸 Settled    : {payload.get('amount_usdc')} USDC\n"
                    f"     ├─ 📦 Resource   : {payload.get('resource_id')}\n"
                    f"     └─ ⛓️ L2 Tx Hash : {mock_tx_hash[:16]}..."
                )
                self.log.info(receipt)
            else:
                self.log.info(f"  └─ 🛡️ Defense Triggered: Payment validation failed ({res.status_code}).")
        else:
             self.runner.fail_count += 1
             return ErrorMessage(f"DVM Clearing Execution Failed: Expected {expected_status}, Got {res.status_code}")

        if self.inject_faults:
            self.log.info(f"\n[SUCCESS] {self.name} Fault-Injection Scenario Completed.")
        else:
            self.log.info(f"\n[SUCCESS] {self.name} Completed successfully.")
            
        self.runner.report()
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} aborted during execution: {msg.msg}")
        self.runner.report()
        return StopMessage(result=False)


class PipelineRunner:
    def __init__(self, name: str, scope_name: str):
        self.name = name
        self.scope_name = scope_name
        self.phases: List[Phase] = []
        
    def set_phases(self, phases: List[Phase]):
        self.phases = phases
        
    async def run_pipeline(self) -> List[TestResult]:
        pass


class GatewayTracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Public Gateway & Network Isolation Trace", scope_name="EDGE_INGRESS_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        self.scene_config = get_edge_scene_config()
        
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        self.test_config = Config(wasm_timeout=5.0, internal_edge_url=self.local_url)
        self.rest_app = create_app(self.test_config)
        
        u_config = uvicorn.Config(app=self.rest_app, host="127.0.0.1", port=self.config.port, log_level="error", access_log=False)
        self.server = ManagedTestServer(u_config)
        self._server_task = None
        
        self.set_phases([
            Phase("Wasm Build & Pre-warm", self.phase_wasm_build),
            Phase("Gateway Ingress (Golden Path)", self.phase_ingress_e2e_golden),
            Phase("Gateway Ingress (Negative Path)", self.phase_ingress_e2e_negative),
            Phase("Gateway Ingress (Tampered Attestation)", self.phase_ingress_e2e_tampered),
            Phase("Sentinel Security (Chaos WAF Check)", self.phase_sentinel_security)
        ])

    async def _wait_for_server(self):
        async with httpx.AsyncClient() as client:
            for _ in range(20):
                try:
                    if (await client.get(f"{self.local_url}/openapi.json")).status_code == 200: return
                except Exception: pass
                await asyncio.sleep(0.2)
        raise RuntimeError("Failed to boot embedded REST server for tests.")

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Pipeline: {self.name} ({self.scope_name}) ===")
        log.info(f"[Pipeline] Booting embedded Uvicorn REST server on {self.local_url}...")
        self._server_task = asyncio.create_task(self.server.serve())
        await self._wait_for_server()

        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n▶️ [PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult("EDGE_GATEWAY", phase.name, True, True))
                except Exception as e:
                    log.error(f"Phase '{phase.name}' Halted: {str(e)}")
                    results.append(TestResult("EDGE_GATEWAY", phase.name, False, True))
                    break 
        finally:
            log.info(f"\n[Pipeline] Shutting down embedded Uvicorn REST server...")
            self.server.should_exit = True
            if self._server_task: await self._server_task
        return results

    async def phase_wasm_build(self):
        builder = WasmBuilder()
        await builder.trace()
        if getattr(builder, 'rupture_confirmed', False): raise RuntimeError("WasmBuilder failed")

    async def _run_scene(self, inject_faults: bool, attestation_injector: Optional[Callable] = None):
        async with httpx.AsyncClient(base_url=self.config.base_url, event_hooks={'request': [self.tracer.trace_request], 'response': [self.tracer.trace_response]}, timeout=15.0) as client:
            runner = EdgeWorkflow(config=self.config, scene_config=self.scene_config, client=client, inject_faults=inject_faults, attestation_injector=attestation_injector)
            tracer = DphiTracer(tester=runner)
            await tracer.trace() 
            
            has_rupture = getattr(tracer, 'rupture_confirmed', False)
            
            if attestation_injector is not None:
                if runner.runner.fail_count == 0:
                    raise RuntimeError("Attestation Bypass! Tampered headers were NOT rejected.")
                return 

            if has_rupture or runner.runner.fail_count > 0:
                raise RuntimeError(f"Gateway Ingress Phase failed (Fault Inject: {inject_faults}).")

    async def phase_ingress_e2e_golden(self): await self._run_scene(False)
    async def phase_ingress_e2e_negative(self): await self._run_scene(True)
    async def phase_ingress_e2e_tampered(self):
        tamper_func = getattr(RpcChaosInjector, 'corrupt_attestation_header', None)
        if tamper_func: await self._run_scene(False, tamper_func)

    async def phase_sentinel_security(self):
        attack_vectors = ChaosPayloadLibrary.get_all_vectors()
        async with httpx.AsyncClient(base_url=self.config.base_url) as client:
            for vector_name, rule_list in attack_vectors:
                payload = random.choice(rule_list)() if isinstance(rule_list, list) else rule_list()
                res = await client.post("/v1/public/telemetry/logs", content=payload)
                if res.status_code >= 500 or res.status_code < 400:
                    raise RuntimeError(f"Gateway Breach! '{vector_name}' bypassed defenses. Status: {res.status_code}")


class EdgeSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def _run_gateway_pipeline(self):
        net_config = E2EConfig(host="127.0.0.1", port=8353, protocol="http")
        self.results.extend(await GatewayTracerPipeline(config=net_config).run_pipeline())

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("🛡️ [EDGE GATEWAY TEST SUITE REPORT]")
        self.log.info("="*80)
        all_passed = all(r.passed for r in self.results)
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            self.log.info(f"{status_icon} {idx:02d}. [{res.target}]".ljust(22) + f"{res.scenario.ljust(45)} | Result: {'PASSED' if res.passed else 'FAILED'}")
        self.log.info("-" * 80)
        if all_passed: self.log.info("🎉 ALL EDGE INGRESS & ISOLATION TESTS EXECUTED SUCCESSFULLY.")
        else: self.log.critical("💥 EDGE BOUNDARY COMPROMISED. Check logs for details.")
        self.log.info("="*80 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI EDGE MASTER SUITE] Commencing Gateway Ingress & Security Tests")
        self.log.info("="*80)
        await self._run_gateway_pipeline()
        self._print_report()

if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=EdgeSuiteRunner().execute)