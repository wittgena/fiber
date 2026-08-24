# phase.client.sdk.usecase
"""
@desc: DPHI Usecase Simulator
- Demonstrates how external agents integrate with the DphiPublicClient SDK.
"""
import time
import asyncio
import logging
from fiber.phase.client.sdk.dphi import DphiPublicClient, CodebotIntent, LLMIntent
from xphi.watcher.receptor.contract.model import AuditLogRequest, AuditEvent, ExportLogsServiceRequest

"""PAYLOAD BUILDERS"""
class UsecasePayloadBuilder:
    """@desc: Factory for assembling mock payloads for integration testing"""
    
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

    @staticmethod
    def build_llm_intent() -> LLMIntent:
        return LLMIntent(
            agent_id="analyst-agent-01",
            model="inter/claude-3-opus",
            messages=[
                {"role": "system", "content": "You are a cyber security expert."},
                {"role": "user", "content": "Explain the concept of Topological Sealing in Zero-Trust architecture."}
            ],
            max_tokens=1024
        )

"""SCENARIO RUNNER"""
class UsecaseRunner:
    """@desc: Master execution runner that orchestrates SDK interactions"""
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.client = DphiPublicClient(base_url=base_url)
        self.log = logging.getLogger("dphi.client.sdk")

    async def run_all(self):
        self.log.info("\n=== [START] DPHI Public Usecase Scenarios ===")

        ## @phase.1: Autonomous Agent Execution (Full L402 Cycle)
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
        await asyncio.sleep(1.0)

        ## @phase.4: Zero-Trust LLM Compute (The Trojan Horse Scenario)
        llm_intent = UsecasePayloadBuilder.build_llm_intent()
        llm_res = await self.client.execute_secure_llm_intent(llm_intent)
        
        # 교차 검증 (Cross-Verification of Audit Hash)
        if "error" not in llm_res:
            fingerprint = llm_res.get("system_fingerprint")
            if fingerprint and fingerprint != "N/A":
                verify_payload = {
                    "receipt_id": "llm_chat_verification", 
                    "state_root": fingerprint,
                    "receipt_type": "Proof-of-Compute"
                }
                await self.client.verify_audit_receipt(verify_payload)
            else:
                self.log.warning("  └─ ⚠️ No fingerprint found in the LLM response. Verification skipped.")

        self.log.info("\n=== [SUCCESS] All Usecase Scenarios Completed ===")

if __name__ == "__main__":
    runner = UsecaseRunner()
    asyncio.run(runner.run_all())