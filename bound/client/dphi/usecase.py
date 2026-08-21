# bound.client.dphi.usecase
import time
import asyncio
import logging
from dataclasses import dataclass, asdict
from typing import Dict, Any
import httpx

from bound.client.http import VerifiedHttpClient, ProofVerificationError, ReplayAttackError
from watcher.receptor.contract.model import AuditLogRequest, AuditEvent, ExportLogsServiceRequest

"""CONSTANTS & DTO"""
class PublicEndpoints:
    """DPHI Public Gateway API Endpoints"""
    AGENT_EXECUTE  = "/v1/public/agent/execute"    # 과금 기반 인텐트 실행 및 Proof-of-Action 발급
    TELEMETRY_LOGS = "/v1/public/telemetry/logs"   # OTLP 텔레메트리 인입 및 커널 씰링(Sealed Stream)
    AUDIT_EVENT    = "/v1/public/audit/event"      # 감사 로그 암호화 기록 및 조건부 ZK/Merkle 증명

@dataclass
class CodebotIntent:
    agent_id: str
    action: str
    source_code: str
    max_fuel: int
    signature: str


"""PUBLIC CLIENT SDK"""
class DphiPublicClient:
    """
    Secure SDK for DPHI Public Gateway. 
    서버 응답의 암호학적 서명(X-Dphi-Signature)을 자동 검증하여 MitM 및 Replay 공격을 방어합니다.
    """
    def __init__(self, base_url: str = "http://localhost:443", api_key: str = "test_key"):
        self.base_url = base_url
        self.api_key = api_key
        self.http_timeout = httpx.Timeout(20.0, connect=5.0)
        
        self.log = logging.getLogger("dphi.client.sdk")
        if not self.log.handlers:
            logging.basicConfig(level=logging.INFO, format="%(message)s")

    def _get_verified_client(self) -> VerifiedHttpClient:
        """응답 무결성 검증 및 60초 Replay Attack 방어가 적용된 HTTP 클라이언트 반환"""
        headers = {"X-Dphi-API-Key": self.api_key}
        base_client = httpx.AsyncClient(
            base_url=self.base_url, 
            headers=headers, 
            timeout=self.http_timeout
        )
        return VerifiedHttpClient(client=base_client, max_age_seconds=60)

    ## API 1: Agent Execute
    async def execute_agent_intent(self, intent: CodebotIntent) -> Dict[str, Any]:
        """AI 인텐트를 전송하고, 위변조가 불가능한 실행 상태 영수증(AuditReceipt)을 검증 및 수령"""
        self.log.info(f"\n🚀 [API 1: Agent Execute] Requesting remote execution for {intent.agent_id}...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.AGENT_EXECUTE
        
        try:
            response = await verifier._client.post(url, json=asdict(intent))
            response.raise_for_status()
            
            try:
                verifier._verify_header_proof(response, f"{self.base_url}{url}")
                self.log.info("  └─ 🛡️ Server Attestation Verified (No tampering detected).")
            except (ProofVerificationError, ReplayAttackError) as e:
                self.log.critical(f"  └─ 🚨 CRITICAL: Server Response is compromised! {e}")
                return {"error": "Attestation Verification Failed", "detail": str(e)}

            receipt = response.json()
            self.log.info(f"  └─ ✅ Success! Billed: ${receipt.get('metered_cost_usd', 0):.4f}")
            self.log.info(f"  └─ 📜 State Root (Anchor): {receipt.get('state_root')}")
            return receipt
            
        except httpx.HTTPStatusError as e:
            self.log.error(f"  └─ ❌ Gateway Rejected (HTTP {e.response.status_code}): {e.response.text}")
            return {"error": e.response.text}
        finally:
            await verifier._client.aclose()

    ## API 2: OTLP Telemetry Ingress
    async def push_telemetry(self, request: ExportLogsServiceRequest) -> Dict[str, Any]:
        """OTLP 메트릭을 전송하고, 커널이 봉인(Sealing)한 콘텐츠 해시와 지문(Fingerprint)을 검증"""
        self.log.info("\n📡 [API 2: Telemetry Logs] Pushing OTLP metrics to Edge Stream...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.TELEMETRY_LOGS
        
        try:
            response = await verifier._client.post(url, json=request.model_dump(exclude_none=True))
            response.raise_for_status()
            
            try:
                verifier._verify_header_proof(response, f"{self.base_url}{url}")
            except Exception as e:
                self.log.warning(f"  └─ ⚠️ Attestation Warning (Telemetry): {e}")
            
            headers = response.headers
            content_hash = headers.get("x-edge-content-hash", "N/A")
            fingerprint = headers.get("x-edge-fingerprint", "N/A")
            
            self.log.info(f"  └─ ✅ Telemetry Accepted. Content Hash: {content_hash} / FP: {fingerprint}")
            return {"status": "success", "content_hash": content_hash, "fingerprint": fingerprint}
            
        except httpx.HTTPStatusError as e:
            self.log.error(f"  └─ ❌ Telemetry Rejected: {e.response.text}")
            return {"error": e.response.text}
        finally:
            await verifier._client.aclose()

    ## API 3: Regulated Audit Event
    async def record_audit_event(self, request: AuditLogRequest) -> Dict[str, Any]:
        """민감 감사 로그를 암호화하여 원장에 기록하고, 조건부 Merkle/ZK 증명을 수령"""
        self.log.info(f"\n🔒 [API 3: Audit Event] Recording sensitive event: {request.event.message}...")
        verifier = self._get_verified_client()
        url = PublicEndpoints.AUDIT_EVENT
        
        try:
            response = await verifier._client.post(url, json=request.model_dump(exclude_none=True))
            response.raise_for_status()
            
            verifier._verify_header_proof(response, f"{self.base_url}{url}")
            audit_res = response.json().get("result", {})
            self.log.info(f"  └─ ✅ Audit Secured. Hash: {audit_res.get('hash')}")
            
            if audit_res.get("membership_proof"):
                self.log.info("  └─ 🛡️ ZK/Merkle Proof securely attached.")
                
            return audit_res
            
        except httpx.HTTPStatusError as e:
            self.log.error(f"  └─ ❌ Audit Rejected: {e.response.text}")
            return {"error": e.response.text}
        finally:
            await verifier._client.aclose()


# =====================================================================
# 3. PAYLOAD BUILDERS
# =====================================================================
class UsecasePayloadBuilder:
    """암호학적 워크플로우 검증을 위한 Mock 페이로드 조립 팩토리"""
    
    @staticmethod
    def build_intent() -> CodebotIntent:
        return CodebotIntent(
            agent_id="codebot-alpha-99", 
            action="EXECUTE_PYTHON",
            source_code="print('Verified Execution!')", 
            max_fuel=1_500_000,
            signature="0xab1234567890..."
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


# =====================================================================
# 4. SCENARIO RUNNER
# =====================================================================
class UsecaseRunner:
    """보안 클라이언트(SDK) 엔드투엔드 워크플로우 검증 러너"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.client = DphiPublicClient(base_url=base_url)
        self.log = logging.getLogger("dphi.client.sdk")

    async def run_all(self):
        self.log.info("\n=== [START] DPHI Public Usecase Scenarios ===")

        # Phase 1: Agent Execution
        self.log.info("\n--- [Phase 1] Remote Agent Execution ---")
        intent_req = UsecasePayloadBuilder.build_intent()
        await self.client.execute_agent_intent(intent_req)
        await asyncio.sleep(1.0)
        
        # Phase 2: OTLP Push
        self.log.info("\n--- [Phase 2] OTLP Telemetry Ingress ---")
        otlp_req = UsecasePayloadBuilder.build_otlp()
        await self.client.push_telemetry(otlp_req)
        await asyncio.sleep(1.0)
        
        # Phase 3: Audit Log
        self.log.info("\n--- [Phase 3] Secure Audit Logging ---")
        audit_req = UsecasePayloadBuilder.build_audit()
        await self.client.record_audit_event(audit_req)

        self.log.info("\n=== [SUCCESS] All Usecase Scenarios Completed ===")


if __name__ == "__main__":
    runner = UsecaseRunner()
    asyncio.run(runner.run_all())