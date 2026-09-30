# fiber.dev.e2e.edge.compliance
import asyncio
import random
from dataclasses import dataclass
from typing import Any, Dict, List
import httpx

from fiber.dev.ex.sdk.gateway import DphiPublicClient, StrictPayloadFactory
from xphi.arch.contract.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.state.phase.reactor import PhaseReactor
from xphi.arch.dev.transport.sentinel import ChaosPayloadLibrary
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.contract.config.env import XPHI_BASE, DPHI_ENV

log = get_emitter("e2e.edge.compliance")

"""Workflow Messages (Phase Transitions)"""
class StartComplianceMsg(WorkflowMessage): pass
class TelemetryChaosMsg(WorkflowMessage): pass
class AuditGoldenMsg(WorkflowMessage): pass
class AuditNegativeMsg(WorkflowMessage): pass

@dataclass
class ComplianceReport:
    phase: str
    status: str
    passed: bool
    details: str

class ComplianceSuiteWorkflow(Workflow):
    def __init__(self, base_url: str):
        super().__init__(name="EDGE_COMPLIANCE_SUITE")
        self.base_url = base_url
        
        # SDK 및 원시 HTTP 클라이언트 초기화 (외부 서버 타겟팅)
        self.sdk_client = DphiPublicClient(base_url=self.base_url)
        self.raw_client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0, follow_redirects=True)
        
        self.log = get_emitter("workflow.compliance", phase="AUDIT")
        self.reports: List[ComplianceReport] = []
        self.halted_by_error = False

    async def execute(self):
        self.log.info(f"\n=== [START] {self.name} (Cryptographic Compliance & Audit) ===")
        self.post_message(StartComplianceMsg())
        await self.run()

    def _record(self, phase: str, passed: bool, details: str) -> bool:
        self.reports.append(ComplianceReport(phase, "PASSED" if passed else "FAILED", passed, details))
        if passed:
            self.log.info(f"  └─ ✅ [{phase}]: {details}")
        else:
            self.log.critical(f"  └─ 🚨 BREACH [{phase}]: {details}")
        return passed

    @step
    async def phase_telemetry_golden(self, msg: StartComplianceMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1/4] Telemetry OTLP Ingress (Golden Path) ---")
        try:
            payload = StrictPayloadFactory.create_telemetry_payload(
                tenant_id="tenant-456",
                model_name="gpt-4o",
                prompt_tokens=150,
                completion_tokens=50
            )
            
            res = await self.sdk_client.push_telemetry(
                request=payload, 
                fuel_receipt="mock_valid_receipt"
            )

            if res.get("status") != "success":
                raise RuntimeError("SDK failed to confirm telemetry success.")
            if res.get("fingerprint") == "N/A":
                raise RuntimeError("Kernel Fingerprint is missing from SDK response.")
                
            self._record("Telemetry Golden", True, f"Telemetry Sealed. Fingerprint: {res.get('fingerprint')}")
            return TelemetryChaosMsg()
            
        except Exception as e:
            return ErrorMessage(f"Telemetry Golden Path Failed: {e}")

    @step
    async def phase_telemetry_chaos(self, msg: TelemetryChaosMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 2/4] Telemetry WAF Defenses (Chaos Check) ---")
        try:
            attack_vectors = ChaosPayloadLibrary.get_all_vectors()
            for vector_name, rule_list in attack_vectors:
                payload = random.choice(rule_list)() if isinstance(rule_list, list) else rule_list()
                res = await self.raw_client.post("/v1/public/telemetry/logs", content=payload)
                if res.status_code >= 500 or res.status_code < 400:
                    raise RuntimeError(f"Compliance WAF Breach! '{vector_name}' bypassed defenses. Status: {res.status_code}")
            
            self._record("Telemetry Chaos", True, "WAF Defense fully operational against raw injection payloads.")
            return AuditGoldenMsg()
            
        except Exception as e:
            return ErrorMessage(f"Telemetry WAF Defense Failed: {e}")

    @step
    async def phase_audit_golden(self, msg: AuditGoldenMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 3/4] Audit Notarization & Proof Issuance (Golden) ---")
        try:
            payload = StrictPayloadFactory.create_audit_payload(
                actor="bot-007",
                action="financial_trade",
                message="Financial trade executed for user secret@corp.com",
                require_proof=True
            )
            
            res = await self.sdk_client.record_audit_event(
                request=payload,
                fuel_receipt="mock_x402_trade"
            )
            
            # ✅ [FIX APPLIED] : API 응답의 중첩된 'result' 객체 내부의 증명(Proof) 데이터를 참조
            audit_result = res.get("result", {})
            if "membership_proof" not in audit_result or not audit_result.get("membership_proof"):
                raise RuntimeError("CRITICAL: SDK did not return Cryptographic Proof (Merkle).")
                
            self._record("Audit Golden", True, f"Audit Notarized via SDK. Proof Hash: {audit_result.get('hash')}")
            return AuditNegativeMsg()
            
        except Exception as e:
            return ErrorMessage(f"Audit Golden Path Failed: {e}")

    @step
    async def phase_audit_negative(self, msg: AuditNegativeMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 4/4] Audit Schema Enforcement (Negative) ---")
        try:
            malformed_payload = {"verbose": True, "some_data": "invalid"}
            headers = {"X-X402-Receipt": "mock_x402_trade"}
            
            res = await self.raw_client.post("/v1/public/audit/event", json=malformed_payload, headers=headers)
            
            if res.status_code != 422:
                raise RuntimeError(f"Failed to block malformed audit request. Expected 422, got {res.status_code}")
                
            self._record("Audit Negative", True, "Schema enforcement validated. Malformed requests blocked (HTTP 422).")
            return StopMessage(result=True)
            
        except Exception as e:
            return ErrorMessage(f"Audit Schema Validation Failed: {e}")

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} Critical Breach Detected: {msg.msg}")
        self.halted_by_error = True
        self._record("WORKFLOW_CRASH", False, f"Test Suite halted prematurely due to: {msg.msg}")
        return StopMessage(result=False)


class ComplianceSuiteRunner:
    def __init__(self):
        self.log = log
        self.base_url = XPHI_BASE
        self.workflow = ComplianceSuiteWorkflow(base_url=self.base_url)

    def _print_report(self):
        self.log.info("\n" + "=" * 80)
        self.log.info("📜 [EDGE COMPLIANCE TEST SUITE REPORT]")
        self.log.info("=" * 80)
        
        reports = self.workflow.reports
        failed = sum(1 for r in reports if not r.passed)
        
        for idx, r in enumerate(reports, 1):
            status_icon = "✅" if r.passed else "❌"
            prefix = f"{status_icon} {idx:02d}. [{r.phase}]".ljust(26)
            self.log.info(f"{prefix} | Result: {r.status.ljust(6)} | {r.details}")
            
        self.log.info("-" * 80)
        if failed == 0 and not self.workflow.halted_by_error:
            self.log.info("🎉 ALL COMPLIANCE NOTARIZATION & SECURITY TESTS PASSED.")
        else:
            self.log.critical(f"💥 COMPLIANCE BOUNDARY COMPROMISED! Failed: {failed}. Check logs for details.")
            exit(1)
        self.log.info("=" * 80 + "\n")

    async def execute_suite(self):
        self.log.info("\n" + "=" * 80)
        self.log.info(f"🧪 [DPHI COMPLIANCE SUITE] Commencing Cryptographic Tests against {self.base_url}")
        self.log.info("=" * 80)
        
        try:
            await self.workflow.execute()
        finally:
            await self.workflow.raw_client.aclose()
            
        self._print_report()

def main(args_list: list[str] = None):
    app = ComplianceSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute_suite)

if __name__ == "__main__":
    main()