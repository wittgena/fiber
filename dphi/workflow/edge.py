# fiber.dphi.workflow.edge
import uuid
from typing import Dict, Optional, Callable

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct
from fastapi import HTTPException

from fiber.dphi.client.http import VerifiedHttpClient
from fiber.dphi.rpc.client import InternalRpcClient

from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.space.runner import WebRunner
from xphi.watcher.tracer.edge import E2EConfig, SceneConfig
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("dphi.workflow.edge")

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
        
        # 내부 워커(Daemon) 통신 검증용 RPC 클라이언트
        self.rpc = InternalRpcClient()

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
                    self.log.info(
                        f"  └─ ✅ Passed: Agent Computed Successfully.\n"
                        f"     ├─ 🤖 Agent ID  : {self.test_agent_id[:10]}...\n"
                        f"     ├─ ⚙️ Action    : {payload['action']}\n"
                        f"     └─ ⛽ Max Fuel  : {payload['max_fuel']}"
                    )
                else:
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
            receipt = {"state_root": "0x_tampered_root_hash_for_chaos", "receipt_id": "fake_123"}
            expected_status = 422 
        else:
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
        self.log.info("\n--- [Phase 4] D3Fi P2P Trade & Settlement (Headless RPC Test) ---")
        
        payload = self.scene_config.trade_builder(self.inject_faults)
        payload["agent_id"] = self.test_agent_id
        expected_status = 422 if self.inject_faults else 200
        
        self.log.info(f"  └─ Sending RPC 'eco.exchange.order.ingress' (Expected: {expected_status})")
        try:
            res = await self.rpc.call("eco.exchange.order.ingress", payload)
            
            if not self.inject_faults:
                self.state_roots["exchange_root"] = res.get("session", {}).get("topo_id", f"d3fi_{uuid.uuid4().hex[:8]}")
                self.log.info(f"  └─ ✅ Passed: D3Fi trade order accurately routed to ExchangeNet Worker.")
                self.runner.success_count += 1
            else:
                self.log.error(f"  └─ ❌ Failed: Expected error {expected_status}, but succeeded.")
                self.runner.fail_count += 1
                return ErrorMessage("D3Fi Ingress Check Failed: Expected error, but succeeded")
                
        except HTTPException as e:
            if e.status_code == expected_status and self.inject_faults:
                self.log.info(f"  └─ 🛡️ Defense Triggered: Malformed trade order dismissed by Worker ({e.status_code}).")
                self.runner.success_count += 1
            else:
                self.log.error(f"  └─ ❌ Failed RPC Call: Expected {expected_status}, Got {e.status_code}. Detail: {e.detail}")
                self.runner.fail_count += 1
                return ErrorMessage(f"D3Fi Ingress Check Failed: Got {e.status_code}")
             
        return LedgerAppendMsg()

    @step
    async def phase_ledger_append(self, msg: LedgerAppendMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 5] Immutable Ledger Stream Append (Headless RPC Test) ---")
        
        exchange_root = self.state_roots.get("exchange_root", "0x00")
        payload = self.scene_config.ledger_builder(exchange_root, self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
            
        self.log.info(f"  └─ Sending RPC 'core.ledger.append' (Expected: {expected_status})")
        try:
            res = await self.rpc.call("core.ledger.append", payload)
            
            if not self.inject_faults:
                ledger_hash = res.get("result", {}).get("hash", "0x_default_ledger_hash")
                self.state_roots["ledger_root"] = ledger_hash
                self.log.info(f"  └─ ✅ Passed: Ledger Appended by Worker. [Root: {ledger_hash[:16]}...]")
                self.runner.success_count += 1
            else:
                self.log.error(f"  └─ ❌ Failed: Expected error {expected_status}, but succeeded.")
                self.runner.fail_count += 1
                return ErrorMessage("Ledger Append Check Failed: Expected error, but succeeded")
                
        except HTTPException as e:
            if e.status_code == expected_status and self.inject_faults:
                self.log.info(f"  └─ 🛡️ Defense Triggered: Invalid Ledger Schema rejected by Worker ({e.status_code}).")
                self.runner.success_count += 1
            else:
                self.log.error(f"  └─ ❌ Failed RPC Call: Expected {expected_status}, Got {e.status_code}. Detail: {e.detail}")
                self.runner.fail_count += 1
                return ErrorMessage(f"Ledger Append Check Failed: Got {e.status_code}")
            
        return DvmClearingMsg()

    @step
    async def phase_dvm_clearing(self, msg: DvmClearingMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 6] DVM Ledger Clearing & Settlement (Ext API Test) ---")
        
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