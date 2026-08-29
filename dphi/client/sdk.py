# fiber.dphi.client.sdk
"""
@desc: DPHI Public Gateway SDK Core & Integration Scenario Runner
- Provides a zero-trust computing blackbox client for autonomous systems.
- Integrates LLM edge and Enterprise MCP interfaces.
"""

import time
import asyncio
import logging
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, List
import httpx

from fiber.dphi.client.http import VerifiedHttpClient
from xphi.watcher.receptor.edge.receipt import AuditLogRequest, AuditEvent, ExportLogsServiceRequest


# =========================================================================
# @phase.1: SDK Models & Endpoints (정밀 검토 및 정렬 완료)
# =========================================================================
class Endpoints:
    """백엔드의 실제 라우터 Prefix에 맞게 엔드포인트를 분리 및 정렬했습니다."""
    
    # --- edge.public (prefix: /v1/public) ---
    KEYS            = "/v1/public/keys"
    AGENT_QUOTE     = "/v1/public/agent/quote"
    AGENT_HANDSHAKE = "/v1/public/agent/handshake"
    AGENT_EXECUTE   = "/v1/public/agent/execute"
    BILLING_INVOICE = "/v1/public/billing/invoice"
    BILLING_BALANCE = "/v1/public/billing/balance"
    TELEMETRY_LOGS  = "/v1/public/telemetry/logs"
    AUDIT_EVENT     = "/v1/public/audit/event"
    AUDIT_VERIFY    = "/v1/public/audit/verify"

    # --- edge.llm (prefix: /v1) ---
    LLM_CHAT        = "/v1/chat/completions"
    LLM_EMBEDDING   = "/v1/embeddings"
    MCP_STATE       = "/v1/mcp-gateway/state"


@dataclass
class CodebotIntent:
    agent_id: str
    action: str
    source_code: str
    max_fuel: int
    signature: str

@dataclass
class LLMIntent:
    agent_id: str
    model: str
    messages: List[Dict[str, str]]
    max_tokens: int = 512

@dataclass
class MCPStateIntent:
    """Enterprise MCP 2.0 호출을 위한 필수 헤더/페이로드 모델"""
    action: str
    handle_id: Optional[str]
    payload: Dict[str, Any]
    x_spiffe_id: str
    x_dpop_proof: str
    x_nonce: str
    x_tenant_id: str
    x_idempotency_key: str
    x_trace_id: Optional[str] = None


# =========================================================================
# @phase.2: Core SDK Client
# =========================================================================
class DphiPublicClient:
    def __init__(self, base_url: str = "http://localhost:8000", api_key: str = "test_key"):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.http_timeout = httpx.Timeout(60.0, connect=5.0)
        
        self.log = logging.getLogger("dphi.client.sdk")
        if not self.log.handlers:
            logging.basicConfig(level=logging.INFO, format="%(message)s")

    def _get_verified_client(self) -> VerifiedHttpClient:
        headers = {"X-Dphi-API-Key": self.api_key}
        base_client = httpx.AsyncClient(
            base_url=self.base_url, 
            headers=headers, 
            timeout=self.http_timeout
        )
        return VerifiedHttpClient(client=base_client, max_age_seconds=60)

    # -------------------------------------------------------------------------
    # Public Edge Methods
    # -------------------------------------------------------------------------
    async def request_handshake(self, intent: CodebotIntent) -> Dict[str, Any]:
        self.log.info(f"\n🤝 [Economy] Negotiating execution budget for {intent.agent_id}...")
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_post_verified(Endpoints.AGENT_HANDSHAKE, json=asdict(intent))
            data = response.json()
            self.log.info(f"  └─ ✅ Handshake Ready. Estimated Cost: ${data.get('estimated_cost_usd', 0):.4f}")
            return data
        except Exception as e:
            self.log.error(f"  └─ ❌ Handshake Failed: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    async def get_fuel_balance(self, agent_id: str, asset_type: str = "fuel") -> Dict[str, Any]:
        self.log.info(f"\n💰 [Economy] Checking UTXO hot state for {agent_id}...")
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_get_verified(Endpoints.BILLING_BALANCE, params={"agent_id": agent_id, "asset_type": asset_type})
            data = response.json()
            self.log.info(f"  └─ ✅ Balance: {data.get('balance')} {asset_type}")
            return data
        except Exception as e:
            self.log.error(f"  └─ ❌ Balance Check Failed: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    async def execute_agent_intent(self, intent: CodebotIntent, payment_receipt: Optional[str] = None) -> Dict[str, Any]:
        self.log.info(f"\n🚀 [Compute] Requesting isolated execution for {intent.agent_id}...")
        verifier = self._get_verified_client()
        headers = {"X-X402-Receipt": payment_receipt} if payment_receipt else {}
            
        try:
            response = await verifier.async_post_verified(Endpoints.AGENT_EXECUTE, json=asdict(intent), headers=headers)
            receipt = response.json()
            self.log.info(f"  └─ ✅ Success! Billed: ${receipt.get('metered_cost_usd', 0):.4f}")
            self.log.info(f"  └─ 📜 State Root: {receipt.get('state_root')}")
            return receipt
        except Exception as e:
            self.log.error(f"  └─ ❌ Execution Rejected: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    async def verify_audit_receipt(self, receipt: Dict[str, Any]) -> Dict[str, Any]:
        self.log.info(f"\n🔍 [Compliance] Verifying cryptographic integrity of the receipt...")
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_post_verified(Endpoints.AUDIT_VERIFY, json=receipt)
            data = response.json()
            if data.get("is_valid"):
                self.log.info("  └─ ✅ VERIFIED: Receipt is cryptographically authentic.")
            else:
                self.log.critical("  └─ 🚨 COMPROMISED: Receipt verification failed!")
            return data
        except Exception as e:
            self.log.error(f"  └─ ❌ Verification Error: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    async def run_autonomous_intent(self, intent: CodebotIntent) -> Dict[str, Any]:
        self.log.info("\n" + "="*65)
        self.log.info(f"🤖 [Auto-Orchestration] Initiating Zero-Trust Autonomous Run")
        self.log.info("="*65)
        
        hs_res = await self.request_handshake(intent)
        if "error" in hs_res:
            return {"error": "Handshake sequence failed", "details": hs_res}
            
        macaroon = hs_res.get("macaroon", "dummy_macaroon_for_internal_auth")
        await self.get_fuel_balance(intent.agent_id)
        
        exec_res = await self.execute_agent_intent(intent, payment_receipt=macaroon)
        if "error" in exec_res:
            return {"error": "Execution sequence failed", "details": exec_res}
            
        verify_res = await self.verify_audit_receipt(exec_res)
        if not verify_res.get("is_valid"):
            self.log.critical("🚨 Execution succeeded but receipt verification failed.")
            return {"error": "Receipt tampered during transit"}
            
        self.log.info("\n🎉 [Autonomous Run] All sequences completed securely.")
        return exec_res

    async def push_telemetry(self, request: ExportLogsServiceRequest) -> Dict[str, Any]:
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_post_verified(Endpoints.TELEMETRY_LOGS, json=request.model_dump(exclude_none=True))
            content_hash = response.headers.get("x-edge-content-hash", "N/A")
            return {"status": "success", "content_hash": content_hash}
        except Exception as e:
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    async def record_audit_event(self, request: AuditLogRequest) -> Dict[str, Any]:
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_post_verified(Endpoints.AUDIT_EVENT, json=request.model_dump(exclude_none=True))
            return response.json().get("result", {})
        except Exception as e:
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    # -------------------------------------------------------------------------
    # LLM Edge Methods
    # -------------------------------------------------------------------------
    async def execute_secure_llm_intent(self, intent: LLMIntent) -> Dict[str, Any]:
        self.log.info(f"\n🧠 [Intelligence] Requesting Zero-Trust LLM Compute for {intent.agent_id}...")
        verifier = self._get_verified_client()
        url = Endpoints.LLM_CHAT
        
        payload = {
            "model": intent.model,
            "messages": intent.messages,
            "max_tokens": intent.max_tokens,
            "metadata": {"agent_id": intent.agent_id}
        }

        try:
            response = await verifier._client.post(url, json=payload)
            
            headers = {}
            if response.status_code == 402:
                self.log.warning("  ├─ 🛑 402 Payment Required intercepted. Initiating auto L402 Handshake...")
                
                hs_res = await self.request_handshake(CodebotIntent(
                    agent_id=intent.agent_id, action="LLM_COMPUTE", source_code="", max_fuel=intent.max_tokens, signature="sig"
                ))
                macaroon = hs_res.get("macaroon")
                if not macaroon:
                    raise Exception("Failed to procure L402 Macaroon from Handshake")
                    
                headers["X-X402-Receipt"] = macaroon
                self.log.info("  ├─ 💸 Payment authorized. Retrying LLM Compute via WASM Kernel...")
                response = await verifier._client.post(url, json=payload, headers=headers)
                
            response.raise_for_status()
            llm_res = response.json()
            
            audit_hash = llm_res.get("system_fingerprint", "N/A")
            fuel_consumed = llm_res.get("usage", {}).get("fuel_consumed", "Unknown")
            
            self.log.info(f"  └─ ✅ LLM Response Received! (Fuel Burned: {fuel_consumed})")
            self.log.info(f"  └─ 📜 Audit Fingerprint Extracted: {audit_hash}")
            return llm_res

        except Exception as e:
            self.log.error(f"  └─ ❌ LLM Execution Failed: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    # -------------------------------------------------------------------------
    # Enterprise MCP Edge Method (신규 통합)
    # -------------------------------------------------------------------------
    async def process_mcp_state(self, intent: MCPStateIntent) -> Dict[str, Any]:
        self.log.info(f"\n🏢 [Enterprise] Processing MCP State for Tenant {intent.x_tenant_id}...")
        verifier = self._get_verified_client()
        url = Endpoints.MCP_STATE
        
        headers = {
            "x-spiffe-id": intent.x_spiffe_id,
            "x-dpop-proof": intent.x_dpop_proof,
            "x-nonce": intent.x_nonce,
            "x-tenant-id": intent.x_tenant_id,
            "x-idempotency-key": intent.x_idempotency_key
        }
        if intent.x_trace_id:
            headers["x-trace-id"] = intent.x_trace_id

        payload = {
            "action": intent.action,
            "handle_id": intent.handle_id,
            "payload": intent.payload
        }

        try:
            response = await verifier._client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            res_data = response.json()
            self.log.info(f"  └─ ✅ MCP State Processed. Result: {res_data.get('status', 'OK')}")
            return res_data
        except httpx.HTTPStatusError as he:
            self.log.error(f"  └─ ❌ MCP State Rejected (Status {he.response.status_code}): {he.response.text}")
            return {"error": he.response.text, "code": he.response.status_code}
        except Exception as e:
            self.log.error(f"  └─ ❌ MCP State Exception: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()


# =========================================================================
# @phase.3: Testing Scenario Components (Payload Builder & Runner)
# =========================================================================
class UsecasePayloadBuilder:
    @staticmethod
    def build_intent() -> CodebotIntent:
        return CodebotIntent(
            agent_id="codebot-alpha-99", action="EXECUTE_PYTHON",
            source_code="print('Verified Execution!')", max_fuel=1_500_000, signature="0xab1234567890_mock_signature"
        )

    @staticmethod
    def build_otlp() -> ExportLogsServiceRequest:
        return ExportLogsServiceRequest(
            resourceLogs=[{"resource": {"attributes": {"tenant": {"id": "tenant-corp-xyz"}}}, "scopeLogs": [{"logRecords": [{"timeUnixNano": str(time.time_ns()), "attributes": [{"key": "llm.model", "value": {"stringValue": "gpt-4"}}]}]}]}]
        )

    @staticmethod
    def build_audit() -> AuditLogRequest:
        return AuditLogRequest(event=AuditEvent(message="Accessed Sensitive Record", actor="health-agent-01", action="READ", target="P-88910"), verbose=True)

    @staticmethod
    def build_llm_intent() -> LLMIntent:
        return LLMIntent(
            agent_id="analyst-agent-01", model="inter/claude-3-opus",
            messages=[{"role": "system", "content": "You are a cyber security expert."}, {"role": "user", "content": "Explain Topological Sealing."}], max_tokens=1024
        )

    @staticmethod
    def build_mcp_intent() -> MCPStateIntent:
        return MCPStateIntent(
            action="COMMIT", handle_id="hdl-123", payload={"record": "data"},
            x_spiffe_id="spiffe://trust.domain/agent/1", x_dpop_proof="proof-123",
            x_nonce="nonce-abc", x_tenant_id="tenant-corp-xyz", x_idempotency_key="idemp-key-1"
        )

class UsecaseRunner:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.client = DphiPublicClient(base_url=base_url)
        self.log = logging.getLogger("dphi.client.sdk")

    async def run_all(self):
        self.log.info("\n=== [START] DPHI Public Usecase Scenarios ===")
        self.log.info(f"📍 Target Edge URL: {self.client.base_url}")

        ## 1. Autonomous Agent Execution
        intent_req = UsecasePayloadBuilder.build_intent()
        await self.client.run_autonomous_intent(intent_req)
        await asyncio.sleep(0.5)
        
        ## 2. OTLP Telemetry Ingress
        otlp_req = UsecasePayloadBuilder.build_otlp()
        await self.client.push_telemetry(otlp_req)
        await asyncio.sleep(0.5)
        
        ## 3. Secure Regulated Audit Logging
        audit_req = UsecasePayloadBuilder.build_audit()
        await self.client.record_audit_event(audit_req)
        await asyncio.sleep(0.5)

        ## 4. Zero-Trust LLM Compute
        llm_intent = UsecasePayloadBuilder.build_llm_intent()
        llm_res = await self.client.execute_secure_llm_intent(llm_intent)
        
        if "error" not in llm_res:
            fingerprint = llm_res.get("system_fingerprint")
            if fingerprint and fingerprint != "N/A":
                verify_payload = {"receipt_id": "llm_chat_verification", "state_root": fingerprint, "receipt_type": "Proof-of-Compute"}
                await self.client.verify_audit_receipt(verify_payload)

        ## 5. Enterprise MCP State Sync (New)
        mcp_intent = UsecasePayloadBuilder.build_mcp_intent()
        await self.client.process_mcp_state(mcp_intent)

        self.log.info("\n=== [SUCCESS] All Usecase Scenarios Completed ===")

if __name__ == "__main__":
    runner = UsecaseRunner()
    asyncio.run(runner.run_all())