# fiber.dphi.eco.client.sdk
"""
@desc: DPHI Public Gateway SDK Core
- Provides a zero-trust computing blackbox client for isolated sandbox workloads.
- Integrates LLM edge and Enterprise MCP interfaces.
- Intended for external developers and agentic workflows.
"""
import time
import logging
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, List
import httpx

from fiber.dphi.eco.client.http import VerifiedHttpClient
from xphi.arch.model.edge.receipt import (
    AuditLogRequest, 
    AuditEvent, 
    ExportLogsServiceRequest,
    ResourceLogs,
    ScopeLogs,
    LogRecord,
    KeyValue
)
from xphi.arch.model.dphi.receptor import EdgeHeader


# =========================================================================
# Endpoints & Models
# =========================================================================
class Endpoints:
    """Backend routing prefixes and endpoints for DPHI Gateway."""
    # --- edge.public (prefix: /v1/public) ---
    KEYS              = "/v1/public/keys"
    SANDBOX_QUOTE     = "/v1/public/sandbox/quote"
    SANDBOX_HANDSHAKE = "/v1/public/sandbox/handshake"
    SANDBOX_EXECUTE   = "/v1/public/sandbox/execute"
    BILLING_INVOICE   = "/v1/public/billing/invoice"
    BILLING_BALANCE   = "/v1/public/billing/balance"
    TELEMETRY_LOGS    = "/v1/public/telemetry/logs"
    AUDIT_EVENT       = "/v1/public/audit/event"
    AUDIT_VERIFY      = "/v1/public/audit/verify"

    # --- edge.llm (prefix: /v1) ---
    LLM_CHAT        = "/v1/chat/completions"
    LLM_EMBEDDING   = "/v1/embeddings"
    MCP_STATE       = "/v1/mcp-gateway/state"


@dataclass
class SandboxIntent:
    client_id: str
    action: str
    source_code: str
    max_fuel: int
    signature: str

@dataclass
class LLMIntent:
    client_id: str
    model: str
    messages: List[Dict[str, str]]
    max_tokens: int = 512

@dataclass
class MCPStateIntent:
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
# Strict Payload Factory (Zero-Trust Data Assurance)
# =========================================================================
class StrictPayloadFactory:
    """
    Constructs highly constrained payloads that strictly comply with 
    the DPHI Gateway's Zero-Trust validation schemas and extraction rulesets.
    """

    @staticmethod
    def create_telemetry_payload(
        tenant_id: str, 
        model_name: str, 
        prompt_tokens: int, 
        completion_tokens: int,
        message: str = "Telemetry sealed"
    ) -> ExportLogsServiceRequest:
        return ExportLogsServiceRequest(
            resourceLogs=[
                ResourceLogs(
                    resource={
                        "attributes": [
                            KeyValue(key="tenant", value={"id": tenant_id})
                        ]
                    },
                    scopeLogs=[
                        ScopeLogs(
                            logRecords=[
                                LogRecord(
                                    timeUnixNano=str(time.time_ns()), 
                                    attributes=[
                                        KeyValue(key="llm", value={"model": model_name}),
                                        KeyValue(key="prompt_tokens", value={"intValue": prompt_tokens}),
                                        KeyValue(key="completion_tokens", value={"intValue": completion_tokens})
                                    ],
                                    body={"stringValue": message}
                                )
                            ]
                        )
                    ]
                )
            ]
        )

    @staticmethod
    def create_audit_payload(actor: str, action: str, message: str, require_proof: bool = True) -> AuditLogRequest:
        return AuditLogRequest(
            event=AuditEvent(
                message=message,
                actor=actor,
                action=action,
                status="success"
            ),
            verbose=require_proof,
            sign_local=False
        )


# =========================================================================
# Core SDK Client
# =========================================================================
class DphiPublicClient:
    """
    Client for interacting with the DPHI Zero-Trust Infrastructure.
    Handles cryptographic handshakes, secure compute execution, and audit logging.
    """
    def __init__(self, base_url: str = "http://localhost:8000", api_key: str = ""):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.http_timeout = httpx.Timeout(60.0, connect=5.0)
        
        self.log = logging.getLogger("dphi.client.sdk")
        if not self.log.handlers:
            logging.basicConfig(level=logging.INFO, format="%(message)s")

    def _get_verified_client(self) -> VerifiedHttpClient:
        headers = {}
        if self.api_key:
            headers["X-Dphi-API-Key"] = self.api_key
            
        base_client = httpx.AsyncClient(
            base_url=self.base_url, 
            headers=headers, 
            timeout=self.http_timeout
        )
        return VerifiedHttpClient(client=base_client, max_age_seconds=60)

    # -------------------------------------------------------------------------
    # Public Edge (Sandbox & Economy)
    # -------------------------------------------------------------------------
    async def request_handshake(self, intent: SandboxIntent) -> Dict[str, Any]:
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_post_verified(Endpoints.SANDBOX_HANDSHAKE, json=asdict(intent))
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.log.error(f"[SDK] Handshake Failed: {e}")
            raise
        finally:
            await verifier._client.aclose()

    async def get_fuel_balance(self, client_id: str, asset_type: str = "fuel") -> Dict[str, Any]:
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_get_verified(Endpoints.BILLING_BALANCE, params={"client_id": client_id, "asset_type": asset_type})
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.log.error(f"[SDK] Balance Check Failed: {e}")
            raise
        finally:
            await verifier._client.aclose()

    async def execute_sandbox_intent(self, intent: SandboxIntent, payment_receipt: Optional[str] = None) -> Dict[str, Any]:
        verifier = self._get_verified_client()
        headers = {"X-X402-Receipt": payment_receipt} if payment_receipt else {}
        try:
            response = await verifier.async_post_verified(Endpoints.SANDBOX_EXECUTE, json=asdict(intent), headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as he:
            self.log.error(f"[SDK] Execution Rejected (Status {he.response.status_code}): {he.response.text}")
            raise
        except Exception as e:
            self.log.error(f"[SDK] Execution Failed: {e}")
            raise
        finally:
            await verifier._client.aclose()

    # -------------------------------------------------------------------------
    # Compliance & Audit Methods
    # -------------------------------------------------------------------------
    async def verify_audit_receipt(self, receipt: Dict[str, Any]) -> Dict[str, Any]:
        verifier = self._get_verified_client()
        try:
            response = await verifier.async_post_verified(Endpoints.AUDIT_VERIFY, json=receipt)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.log.error(f"[SDK] Verification Error: {e}")
            raise
        finally:
            await verifier._client.aclose()

    async def push_telemetry(self, request: ExportLogsServiceRequest, payment_receipt: Optional[str] = None) -> Dict[str, Any]:
        verifier = self._get_verified_client()
        headers = {"X-X402-Receipt": payment_receipt} if payment_receipt else {}
        try:
            response = await verifier.async_post_verified(
                Endpoints.TELEMETRY_LOGS, 
                json=request.model_dump(exclude_none=True), 
                headers=headers
            )
            response.raise_for_status()
            
            # [해결됨] xphi.arch.model.dphi.receptor.EdgeHeader 상수를 사용하여 
            # 서버가 보낸 헤더 키(X-Kernel-Fingerprint 등)와 완벽히 일치시킴. 
            # httpx.Headers는 Case-Insensitive 하므로 상수값(.value)을 그대로 써도 매칭됨.
            headers_dict = response.headers
            content_hash = headers_dict.get(EdgeHeader.CONTENT_HASH.value, "N/A")
            fingerprint = headers_dict.get(EdgeHeader.FINGERPRINT.value, "N/A")
            
            return {
                "status": "success", 
                "content_hash": content_hash, 
                "fingerprint": fingerprint
            }
        except httpx.HTTPStatusError as he:
            self.log.error(f"[SDK] Telemetry Rejected (Status {he.response.status_code}): {he.response.text}")
            raise
        except Exception as e:
            self.log.error(f"[SDK] Telemetry Exception: {e}")
            raise
        finally:
            await verifier._client.aclose()

    async def record_audit_event(self, request: AuditLogRequest, payment_receipt: Optional[str] = None) -> Dict[str, Any]:
        verifier = self._get_verified_client()
        headers = {"X-X402-Receipt": payment_receipt} if payment_receipt else {}
        try:
            response = await verifier.async_post_verified(
                Endpoints.AUDIT_EVENT, 
                json=request.model_dump(exclude_none=True), 
                headers=headers
            )
            response.raise_for_status()
            return response.json().get("result", {})
        except httpx.HTTPStatusError as he:
            self.log.error(f"[SDK] Audit Rejected (Status {he.response.status_code}): {he.response.text}")
            raise
        except Exception as e:
            self.log.error(f"[SDK] Audit Exception: {e}")
            raise
        finally:
            await verifier._client.aclose()

    # -------------------------------------------------------------------------
    # LLM & Enterprise MCP
    # -------------------------------------------------------------------------
    async def execute_secure_llm_intent(self, intent: LLMIntent) -> Dict[str, Any]:
        verifier = self._get_verified_client()
        payload = {
            "model": intent.model,
            "messages": intent.messages,
            "max_tokens": intent.max_tokens,
            "metadata": {"client_id": intent.client_id}
        }

        try:
            response = await verifier._client.post(Endpoints.LLM_CHAT, json=payload)
            
            if response.status_code == 402:
                self.log.warning("[SDK] 402 Payment Required. Initiating auto x402 Handshake...")
                hs_res = await self.request_handshake(SandboxIntent(
                    client_id=intent.client_id, action="LLM_COMPUTE", source_code="", max_fuel=intent.max_tokens, signature="sig"
                ))
                macaroon = hs_res.get("macaroon")
                if not macaroon:
                    raise Exception("Failed to procure x402 Macaroon from Handshake")
                    
                headers = {"X-X402-Receipt": macaroon}
                response = await verifier._client.post(Endpoints.LLM_CHAT, json=payload, headers=headers)
                
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.log.error(f"[SDK] LLM Execution Failed: {e}")
            raise
        finally:
            await verifier._client.aclose()

    async def process_mcp_state(self, intent: MCPStateIntent) -> Dict[str, Any]:
        verifier = self._get_verified_client()
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
            response = await verifier._client.post(Endpoints.MCP_STATE, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as he:
            self.log.error(f"[SDK] MCP State Rejected (Status {he.response.status_code}): {he.response.text}")
            raise
        finally:
            await verifier._client.aclose()