# phase.client.dphi.usecase
"""
@desc: DPHI Public Gateway SDK & Usecase Runner
- Provides a zero-trust computing blackbox client for autonomous systems.
- Abstracts L402 micro-transactions and cryptographic attestations into a seamless drop-in integration.
"""

import time
import asyncio
import logging
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
import httpx

from fiber.phase.client.http import VerifiedHttpClient, ProofVerificationError, ReplayAttackError
from xphi.watcher.receptor.contract.model import AuditLogRequest, AuditEvent, ExportLogsServiceRequest


"""CONSTANTS & DATA TRANSFER OBJECTS (DTO)"""
class PublicEndpoints:
    """
    @desc: DPHI Public Gateway API Endpoints
    - Reflects the exact symmetric architecture of edge.public
    """
    AGENT_QUOTE     = "/v1/public/agent/quote"          # 사전 견적 (Dry-run)
    AGENT_HANDSHAKE = "/v1/public/agent/handshake"      # L402 견적 및 청구서 통합 발급
    AGENT_EXECUTE   = "/v1/public/agent/execute"        # 과금 기반 인텐트 실행 및 Proof-of-Action 발급
    
    BILLING_INVOICE = "/v1/public/billing/invoice"      # L402 청구서 발급
    BILLING_BALANCE = "/v1/public/billing/balance"      # 인메모리 연료 잔고 조회
    
    TELEMETRY_LOGS  = "/v1/public/telemetry/logs"       # OTLP 텔레메트리 인입 및 커널 씰링
    AUDIT_EVENT     = "/v1/public/audit/event"          # 감사 로그 암호화 기록 및 ZK/Merkle 증명
    AUDIT_VERIFY    = "/v1/public/audit/verify"         # 영수증 진위 여부 검증 (Auditor 접점)


@dataclass
class CodebotIntent:
    """@desc: Payload schema for requesting isolated agent execution"""
    agent_id: str
    action: str
    source_code: str
    max_fuel: int
    signature: str


"""PUBLIC CLIENT SDK (THE ZERO-TRUST BLACKBOX)"""
class DphiPublicClient:
    """
    @desc: Secure SDK for DPHI Public Gateway
    - Automatically verifies cryptographic signatures (X-Dphi-Signature) of server responses to defend against MitM and Replay attacks
    - Abstracts the entire L402 payment pipeline
    """
    def __init__(self, base_url: str = "http://localhost:443", api_key: str = "test_key"):
        self.base_url = base_url
        self.api_key = api_key
        self.http_timeout = httpx.Timeout(20.0, connect=5.0)
        
        self.log = logging.getLogger("dphi.client.sdk")
        if not self.log.handlers:
            logging.basicConfig(level=logging.INFO, format="%(message)s")

    def _get_verified_client(self) -> VerifiedHttpClient:
        """@desc: Instantiates a VerifiedHttpClient with a 60-second replay attack defense"""
        headers = {"X-Dphi-API-Key": self.api_key}
        base_client = httpx.AsyncClient(
            base_url=self.base_url, 
            headers=headers, 
            timeout=self.http_timeout
        )
        return VerifiedHttpClient(client=base_client, max_age_seconds=60)

    async def request_handshake(self, intent: CodebotIntent) -> Dict[str, Any]:
        """@desc: Computes the required fuel via dry-run and issues an L402 invoice"""
        self.log.info(f"\n🤝 [Economy] Negotiating execution budget for {intent.agent_id}...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.AGENT_HANDSHAKE
        try:
            response = await verifier.async_post_verified(url, json=asdict(intent))
            data = response.json()
            self.log.info(f"  └─ ✅ Handshake Ready. Estimated Cost: ${data.get('estimated_cost_usd', 0):.4f}")
            return data
        except Exception as e:
            self.log.error(f"  └─ ❌ Handshake Failed: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    async def get_fuel_balance(self, agent_id: str, asset_type: str = "fuel") -> Dict[str, Any]:
        """@desc: Retrieves the real-time hot state UTXO balance of the given agent"""
        self.log.info(f"\n💰 [Economy] Checking UTXO hot state for {agent_id}...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.BILLING_BALANCE
        try:
            response = await verifier.async_get_verified(url, params={"agent_id": agent_id, "asset_type": asset_type})
            data = response.json()
            self.log.info(f"  └─ ✅ Balance: {data.get('balance')} {asset_type}")
            return data
        except Exception as e:
            self.log.error(f"  └─ ❌ Balance Check Failed: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    async def execute_agent_intent(self, intent: CodebotIntent, payment_receipt: Optional[str] = None) -> Dict[str, Any]:
        """@desc: Transmits the intent for isolated WASM execution and retrieves a Proof-of-Action receipt"""
        self.log.info(f"\n🚀 [Compute] Requesting isolated execution for {intent.agent_id}...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.AGENT_EXECUTE
        headers = {}
        if payment_receipt:
            headers["X-X402-Receipt"] = payment_receipt
            
        try:
            response = await verifier.async_post_verified(url, json=asdict(intent), headers=headers)
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
        """@desc: Submits a receipt to the kernel to mathematically prove its authenticity (Parity Check)"""
        self.log.info(f"\n🔍 [Compliance] Verifying cryptographic integrity of the receipt...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.AUDIT_VERIFY
        try:
            response = await verifier.async_post_verified(url, json=receipt)
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
        """
        @desc: THE INTEGRATION CONTRACT (Zero-Trust Auto-Orchestration)
        - Abstracts the entire symmetric lifecycle: Handshake -> Execute -> Verify
        """
        self.log.info("\n" + "="*65)
        self.log.info(f"🤖 [Auto-Orchestration] Initiating Zero-Trust Autonomous Run")
        self.log.info("="*65)
        
        ## @step.1: Negotiate Execution Budget & Procure Invoice
        hs_res = await self.request_handshake(intent)
        if "error" in hs_res:
            return {"error": "Handshake sequence failed", "details": hs_res}
            
        macaroon = hs_res.get("macaroon", "dummy_macaroon_for_internal_auth")
        
        ## @step.2: Optional Read - Confirm available UTXO fuel
        await self.get_fuel_balance(intent.agent_id)
        
        ## @step.3: Transmit intent with authorized capability receipt
        exec_res = await self.execute_agent_intent(intent, payment_receipt=macaroon)
        if "error" in exec_res:
            return {"error": "Execution sequence failed", "details": exec_res}
            
        ## @step.4: Cross-verify the authenticity of the obtained receipt
        verify_res = await self.verify_audit_receipt(exec_res)
        if not verify_res.get("is_valid"):
            self.log.critical("🚨 Execution succeeded but receipt verification failed. Possible interception!")
            return {"error": "Receipt tampered during transit"}
            
        self.log.info("\n🎉 [Autonomous Run] All sequences completed securely.")
        return exec_res

    async def push_telemetry(self, request: ExportLogsServiceRequest) -> Dict[str, Any]:
        """@desc: Pushes OTLP metrics to the Edge Stream and verifies the kernel-sealed content hash"""
        self.log.info("\n📡 [Compliance] Pushing OTLP metrics to Edge Stream...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.TELEMETRY_LOGS
        try:
            response = await verifier.async_post_verified(url, json=request.model_dump(exclude_none=True))
            headers = response.headers
            content_hash = headers.get("x-edge-content-hash", "N/A")
            self.log.info(f"  └─ ✅ Telemetry Accepted. Content Hash: {content_hash}")
            return {"status": "success", "content_hash": content_hash}
        except Exception as e:
            self.log.error(f"  └─ ❌ Telemetry Rejected: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    async def record_audit_event(self, request: AuditLogRequest) -> Dict[str, Any]:
        """@desc: Anchors sensitive audit logs to the ledger, conditionally returning ZK/Merkle proofs"""
        self.log.info(f"\n🔒 [Compliance] Recording sensitive event: {request.event.message}...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.AUDIT_EVENT
        try:
            response = await verifier.async_post_verified(url, json=request.model_dump(exclude_none=True))
            audit_res = response.json().get("result", {})
            self.log.info(f"  └─ ✅ Audit Secured. Hash: {audit_res.get('hash')}")
            return audit_res
        except Exception as e:
            self.log.error(f"  └─ ❌ Audit Rejected: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

"""PAYLOAD BUILDERS"""
class UsecasePayloadBuilder:
    """@desc: Factory for assembling mock payloads used in end-to-end cryptographic workflow verification"""
    
    @staticmethod
    def build_intent() -> CodebotIntent:
        return CodebotIntent(
            agent_id="codebot-alpha-99", 
            action="EXECUTE_PYTHON",
            source_code="print('Verified Execution!')", 
            max_fuel=1_500_000,
            signature="0xab1234567890_mock_signature"
        )

    @staticmethod
    def build_otlp() -> ExportLogsServiceRequest:
        return ExportLogsServiceRequest(
            resourceLogs=[{
                "resource": {"attributes": {"tenant": {"id": "tenant-corp-xyz"}}},
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": str(time.time_ns()),
                        "attributes": [
                            {"key": "llm.model", "value": {"stringValue": "gpt-4"}},
                            {"key": "prompt_tokens", "value": {"intValue": 125}}
                        ]
                    }]
                }]
            }]
        )

    @staticmethod
    def build_audit() -> AuditLogRequest:
        return AuditLogRequest(
            event=AuditEvent(
                message="Accessed Sensitive Patient Record",
                actor="health-agent-01",
                action="READ",
                target="P-88910"
            ),
            verbose=True
        )

"""SCENARIO RUNNER"""
class UsecaseRunner:
    """@desc: Master execution runner that simulates an external agent integrating via the Public SDK"""
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.client = DphiPublicClient(base_url=base_url)
        self.log = logging.getLogger("dphi.client.sdk")

    async def run_all(self):
        self.log.info("\n=== [START] DPHI Public Usecase Scenarios ===")

        ## @phase.1: Autonomous Agent Execution (Full L402 Cycle) - Initiates the auto-orchestration method that encapsulates Handshake, Execute, and Verify
        intent_req = UsecasePayloadBuilder.build_intent()
        await self.client.run_autonomous_intent(intent_req)
        await asyncio.sleep(1.0)
        
        ## @phase.2: OTLP Telemetry Ingress
        otlp_req = UsecasePayloadBuilder.build_otlp()
        await self.client.push_telemetry(otlp_req)
        await asyncio.sleep(1.0)
        
        ## @phase.3: Secure Regulated Audit Logging
        audit_req = UsecasePayloadBuilder.build_audit()
        await self.client.record_audit_event(audit_req)
        self.log.info("\n=== [SUCCESS] All Usecase Scenarios Completed ===")

if __name__ == "__main__":
    runner = UsecaseRunner()
    asyncio.run(runner.run_all())