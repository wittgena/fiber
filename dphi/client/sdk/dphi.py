# fiber.dphi.client.sdk.dphi
## @lineage: dphi.client.sdk.dphi
## @lineage: phase.client.sdk.dphi
## @lineage: phase.client.dphi.sdk
"""
@desc: DPHI Public Gateway SDK Core
- Provides a zero-trust computing blackbox client for autonomous systems.
- Abstracts L402 micro-transactions and cryptographic attestations.
"""

import logging
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, List
import httpx

from fiber.dphi.client.http import VerifiedHttpClient
from xphi.watcher.receptor.edge.receipt import AuditLogRequest, ExportLogsServiceRequest

class PublicEndpoints:
    AGENT_QUOTE     = "/v1/public/agent/quote"
    AGENT_HANDSHAKE = "/v1/public/agent/handshake"
    AGENT_EXECUTE   = "/v1/public/agent/execute"
    BILLING_INVOICE = "/v1/public/billing/invoice"
    BILLING_BALANCE = "/v1/public/billing/balance"
    TELEMETRY_LOGS  = "/v1/public/telemetry/logs"
    AUDIT_EVENT     = "/v1/public/audit/event"
    AUDIT_VERIFY    = "/v1/public/audit/verify"
    LLM_CHAT        = "/v1/chat/completions"
    LLM_EMBEDDING   = "/v1/embeddings"

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

    async def request_handshake(self, intent: CodebotIntent) -> Dict[str, Any]:
        self.log.info(f"\n🤝 [Economy] Negotiating execution budget for {intent.agent_id}...")
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_post_verified(PublicEndpoints.AGENT_HANDSHAKE, json=asdict(intent))
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
            response = await verifier.async_get_verified(PublicEndpoints.BILLING_BALANCE, params={"agent_id": agent_id, "asset_type": asset_type})
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
            response = await verifier.async_post_verified(PublicEndpoints.AGENT_EXECUTE, json=asdict(intent), headers=headers)
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
            response = await verifier.async_post_verified(PublicEndpoints.AUDIT_VERIFY, json=receipt)
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

    async def execute_secure_llm_intent(self, intent: LLMIntent) -> Dict[str, Any]:
        self.log.info(f"\n🧠 [Intelligence] Requesting Zero-Trust LLM Compute for {intent.agent_id}...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.LLM_CHAT
        
        payload = {
            "model": intent.model,
            "messages": intent.messages,
            "max_tokens": intent.max_tokens,
            "metadata": {"agent_id": intent.agent_id}
        }

        try:
            # 1. 의도된 마찰: 영수증 없이 요청
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
                    
                # 2. 영수증 획득 후 재시도
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
        self.log.info("\n📡 [Compliance] Pushing OTLP metrics to Edge Stream...")
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_post_verified(PublicEndpoints.TELEMETRY_LOGS, json=request.model_dump(exclude_none=True))
            content_hash = response.headers.get("x-edge-content-hash", "N/A")
            self.log.info(f"  └─ ✅ Telemetry Accepted. Content Hash: {content_hash}")
            return {"status": "success", "content_hash": content_hash}
        except Exception as e:
            self.log.error(f"  └─ ❌ Telemetry Rejected: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()

    async def record_audit_event(self, request: AuditLogRequest) -> Dict[str, Any]:
        self.log.info(f"\n🔒 [Compliance] Recording sensitive event: {request.event.message}...")
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_post_verified(PublicEndpoints.AUDIT_EVENT, json=request.model_dump(exclude_none=True))
            audit_res = response.json().get("result", {})
            self.log.info(f"  └─ ✅ Audit Secured. Hash: {audit_res.get('hash')}")
            return audit_res
        except Exception as e:
            self.log.error(f"  └─ ❌ Audit Rejected: {e}")
            return {"error": str(e)}
        finally:
            await verifier._client.aclose()