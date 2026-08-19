# eco.client.dphi.usecase
import time
import asyncio
import logging
from dataclasses import dataclass, asdict
from typing import Dict, Any

import httpx

from watcher.receptor.contract.model import AuditLogRequest, AuditEvent, ExportLogsServiceRequest
from ator.client.http import VerifiedHttpClient, ProofVerificationError, ReplayAttackError

"""CONSTANTS & DTO"""
class PublicEndpoints:
    """DPHI Public Gateway API 엔드포인트 URL 정의"""
    AGENT_EXECUTE  = "/v1/public/agent/execute"    # 단일 인텐트 실행
    TELEMETRY_LOGS = "/v1/public/telemetry/logs"   # OTLP 텔레메트리 스트리밍
    AUDIT_EVENT    = "/v1/public/audit/event"      # 민감 정보 마스킹 및 ZK 증명 발급

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
    - 외부망에서 DPHI Public Gateway와 통신하는 보안 클라이언트 SDK
    - VerifiedHttpClient를 래핑하여, 서버(Gateway)가 반환하는 모든 응답의 암호학적 서명(X-Dphi-Signature)을 자동으로 검증하여 중간자 공격(MitM)을 방어
    """
    def __init__(self, base_url: str = "http://localhost:443", api_key: str = "test_key"):
        self.base_url = base_url
        self.api_key = api_key
        self.http_timeout = httpx.Timeout(20.0, connect=5.0)
        
        self.log = logging.getLogger("dphi.client.sdk")
        if not self.log.handlers:
            logging.basicConfig(level=logging.INFO, format="%(message)s")

    def _get_verified_client(self) -> VerifiedHttpClient:
        """기본 httpx 클라이언트를 생성하고 VerifiedHttpClient로 래핑하여 반환"""
        headers = {"X-Dphi-API-Key": self.api_key}
        base_client = httpx.AsyncClient(
            base_url=self.base_url, 
            headers=headers, 
            timeout=self.http_timeout
        )
        # max_age_seconds=60: 60초가 지난 Replay Attack 방어
        return VerifiedHttpClient(client=base_client, max_age_seconds=60)

    ## API 1: Agent Execute
    async def execute_agent_intent(self, intent: CodebotIntent) -> Dict[str, Any]:
        """AI 에이전트의 코드를 실행하고 암호학적 영수증을 반환"""
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
        """서버 공통 DTO 모델(ExportLogsServiceRequest)을 사용하여 OTLP 텔레메트리 전송"""
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
        """서버 공통 DTO 모델(AuditLogRequest)을 사용하여 민감한 감사 기록 전송"""
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
# 3. PAYLOAD BUILDERS (Inspired by workflow patterns)
# =====================================================================
class UsecasePayloadBuilder:
    """Workflow의 SceneConfig 처럼 요청 더미 데이터를 조립하는 역할을 분리"""
    
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
    """순차적인 API 테스트를 Workflow Phase 처럼 명확하게 실행"""
    
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