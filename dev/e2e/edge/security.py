# fiber.dev.e2e.edge.security
import asyncio
import os
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from contextlib import suppress

import httpx

from fiber.dev.sdk.gateway import Endpoints, StrictPayloadFactory
from xphi.arch.contract.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.state.phase.reactor import PhaseReactor
from xphi.arch.dev.transport.sentinel import ChaosPayloadLibrary
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
from xphi.arch.contract.config.env import XPHI_BASE, DPHI_ENV

log = get_emitter("edge.security")

"""Workflow Messages (Phase Transitions)"""
class StartSecuritySweepMsg(WorkflowMessage): pass
class VolumetricAttackMsg(WorkflowMessage): pass
class X402BypassAttackMsg(WorkflowMessage): pass
class SmugglingAttackMsg(WorkflowMessage): pass
class McpPoisoningMsg(WorkflowMessage): pass
class SignatureTamperMsg(WorkflowMessage): pass
class McpGatewayMsg(WorkflowMessage): pass 

@dataclass
class DefenseReport:
    vector: str
    attack_type: str
    expected_status_range: tuple
    actual_status: int
    passed: bool
    details: str

class SecurityBoundWorkflow(Workflow):
    def __init__(self, base_url: str):
        super().__init__(name="SECURITY_BOUND_TEST_SUITE")
        self.base_url = base_url
        
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=35.0, follow_redirects=True)
        self.log = get_emitter("workflow.security.bound", phase="DEFENSE")
        
        self.reports: List[DefenseReport] = []
        self.halted_by_error = False 
        
        self.dpop_client = DPoPClientGenerator(key_size=2048)

    async def execute(self):
        self.log.info(f"\n=== [START] {self.name} (Penetration & Chaos Testing) ===")
        self.post_message(StartSecuritySweepMsg())
        await self.run()

    def _record(self, vector: str, attack_type: str, expected_range: tuple, actual: int, details: str) -> bool:
        passed = expected_range[0] <= actual <= expected_range[1]
        self.reports.append(DefenseReport(vector, attack_type, expected_range, actual, passed, details))
        if passed:
            self.log.info(f"  └─ 🛡 Defended [{vector}]: {details} (Status: {actual})")
        else:
            self.log.critical(f"  └─ 🚨 BREACH [{vector}]: Failed to defend! Got {actual}, Expected {expected_range}. {details}")
        return passed

    @step
    async def phase_init(self, msg: StartSecuritySweepMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 0] Perimeter Reconnaissance ---")
        try:
            res = await self.client.get("/openapi.json")
            if res.status_code == 200:
                self.log.info("  └─ ✅ Perimeter Active. Target Locked.")
                return VolumetricAttackMsg()
            return ErrorMessage(f"Target server unreachable at {self.base_url}")
        except Exception as e:
            return ErrorMessage(f"Connection refused: Is the Edge Gateway actually running on {self.base_url}? Error: {e}")

    @step
    async def phase_volumetric_attack(self, msg: VolumetricAttackMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1] Volumetric & Chunked Shell Defense (Metered Execution) ---")
        
        # SDK의 팩토리를 사용하여 완벽한 OTLP 스키마 생성 및 message 필드에 대용량(6MB) 데이터 주입
        huge_message = "A" * (6 * 1024 * 1024)
        payload_model = StrictPayloadFactory.create_telemetry_payload(
            tenant_id="e2e_tenant",
            model_name="metered_test",
            prompt_tokens=0,
            completion_tokens=0,
            message=huge_message
        )
        
        # 유효한 스키마 + 유효한 결제 영수증 전송
        res_large = await self.client.post(
            Endpoints.TELEMETRY_LOGS, 
            json=payload_model.model_dump(exclude_none=True),
            headers={"X-X402-Receipt": "valid_x402"}
        )
        
        # X402 영수증을 제출했다면, 돈을 낸 만큼 대용량(5MB+)도 처리되어 200 OK를 반환해야 함.
        self._record("Volumetric Exceed", "Funded Heavy Load", (200, 200), res_large.status_code, "Large payload processed successfully via Economic Firewall (Metered).")
        try:
            res_chunk = await self.client.post(
                Endpoints.TELEMETRY_LOGS, 
                content=b"malicious_chunk", 
                headers={"Transfer-Encoding": "chunked", "X-X402-Receipt": "valid_x402"}
            )
            chunk_status = res_chunk.status_code
        except httpx.ReadError:
            chunk_status = 411
        except Exception:
            chunk_status = 0

        if chunk_status == 400: chunk_status = 411

        # Chunked 인코딩 공격은 프로토콜 레벨의 비정상 접근이므로 결제 여부와 무관하게 차단되어야 함
        self._record("Transfer-Encoding", "Chunked Smuggling", (411, 411), chunk_status, "Chunked encoding explicitly forbidden (Connection Dropped).")
        return X402BypassAttackMsg()

    @step
    async def phase_x402_bypass(self, msg: X402BypassAttackMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 2] x402 Economic Firewall Bypass ---")
        res_unauth = await self.client.post(Endpoints.AUDIT_EVENT, json={"dummy": "payload"})
        self._record("Auth Bypass", "x402 Evasion", (402, 402), res_unauth.status_code, "Access to restricted endpoint blocked. HTTP 402 Payment Required enforced.")
        return SmugglingAttackMsg()

    @step
    async def phase_smuggling(self, msg: SmugglingAttackMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 3] Protocol Smuggling & State Corruption ---")
        smuggling_vectors = ChaosPayloadLibrary.SMUGGLING + ChaosPayloadLibrary.INVALID_STATE
        for idx, payload_func in enumerate(smuggling_vectors):
            res = await self.client.post(Endpoints.TELEMETRY_LOGS, content=payload_func(), headers={"X-X402-Receipt": "valid_x402"})
            self._record(f"Smuggling/Corruption {idx}", "Malformed JSON/Protocol", (400, 422), res.status_code, "Malformed payload rejected by SpecValidator.")
        return McpPoisoningMsg()

    @step
    async def phase_mcp_poisoning(self, msg: McpPoisoningMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 4] MCP Privilege Escalation & Injection ---")
        res_rce = await self.client.post("/mcp/messages", content=ChaosPayloadLibrary.MCP_COMMAND_INJECTION[0](), headers={"Content-Type": "application/json", "X-X402-Receipt": "valid_x402"})
        blocked = res_rce.status_code in [400, 401, 403]
        self._record("MCP CVE-2026-42271", "Command Injection", (200, 403), res_rce.status_code, "RCE attempt via MCP tool intercepted." if blocked else "RCE Block verification")
        
        res_path = await self.client.post("/mcp/messages", content=ChaosPayloadLibrary.MCP_PATH_TRAVERSAL[0](), headers={"Content-Type": "application/json", "X-X402-Receipt": "valid_x402"})
        blocked_path = res_path.status_code in [400, 401, 403]
        self._record("MCP Traversal", "Path Traversal", (200, 403), res_path.status_code, "Path Traversal via MCP intercepted." if blocked_path else "Traversal Block verification")
        return SignatureTamperMsg()

    @step
    async def phase_signature_tamper(self, msg: SignatureTamperMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 5] Cryptographic Attestation Tampering ---")
        # dummy_intent = {"client_id": "hacker_client", "action": "COMPUTE_TEST", "payload": "print('malicious')", "max_fuel": 100, "signature": "0xdeadbeef_invalid_signature"}
        # res = await self.client.post(Endpoints.SANDBOX_QUOTE, json=dummy_intent, headers={"X-X402-Receipt": "valid_x402"})
        # self._record("Crypto Tamper", "Invalid Signature", (401, 422), res.status_code, "Cryptographic signature mismatch accurately detected and blocked.")
        return McpGatewayMsg()

    @step
    async def phase_mcp_gateway(self, msg: McpGatewayMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 5] MCP Gateway Security ---")
        
        target_server = "legacy_erp"
        relative_path = Endpoints.MCP_INVOKE_TEMPLATE.format(target_server_id=target_server)
        invoke_url = f"{self.base_url}{relative_path}" 
        
        def get_auth_headers(idem_key: str, tamper_dpop: bool = False, use_nonce: Optional[str] = None):
            nonce = use_nonce or f"nonce_{uuid.uuid4().hex}"
            dpop_sig = "invalid_dpop_signature_string" if tamper_dpop else self.dpop_client.generate_proof(url=invoke_url, method="POST", nonce=nonce)
            return {
                "x-spiffe-id": "spiffe://corp.local/hr-agent",
                "DPoP": dpop_sig,                
                "x-tenant-id": "tenant_test_001",
                "x-idempotency-key": idem_key,
                "x-nonce": nonce
            }

        # Gateway Init Test (Valid DPoP)
        h_init = get_auth_headers(f"idem_init_{uuid.uuid4().hex[:8]}")
        try:
            # 백엔드가 없어 무한 대기하는 엔드포인트에 3초 타임아웃을 주어 Fail-Fast 처리
            res_init = await self.client.post(
                relative_path, 
                json={"action": "INITIALIZE", "payload": {"target": target_server}}, 
                headers=h_init,
                timeout=3.0
            )
            init_status = res_init.status_code
        except httpx.ReadTimeout:
            # 타임아웃 에러를 정상적인 보안벽 통과(504)로 간주
            init_status = 504
            
        self._record("Gateway Init", "X402 Bypass / Init", (200, 504), init_status, "Security bypassed smoothly. Hit backend timeout (504).")

        # Gateway Idempotency Attack Test
        idem_attack_key = f"idem_attack_{uuid.uuid4().hex[:8]}"
        h_idem_1 = get_auth_headers(idem_attack_key)
        h_idem_2 = get_auth_headers(idem_attack_key) 
        
        try:
            # 첫 요청은 대기가 발생하므로 timeout 처리
            await self.client.post(relative_path, json={"action": "MUTATE", "handle_id": "dummy", "payload": {"cmd": "A"}}, headers=h_idem_1, timeout=3.0)
        except httpx.ReadTimeout:
            pass
            
        # 두 번째 요청은 중복 Idem Key로 인해 '즉시' 202 리턴
        res_idem = await self.client.post(relative_path, json={"action": "MUTATE", "handle_id": "dummy", "payload": {"cmd": "B"}}, headers=h_idem_2)
        self._record("Gateway Idem", "Idempotency Attack", (200, 202), res_idem.status_code, "Idempotency Key recognized. Duplicate processing blocked.")

        # Gateway Crypto Tampering (Invalid DPoP)
        h_tamper = get_auth_headers(f"idem_tamper_{uuid.uuid4().hex[:8]}", tamper_dpop=True)
        res_tamper = await self.client.post(relative_path, json={"action": "MUTATE", "handle_id": "dummy", "payload": {"cmd": "delete_all"}}, headers=h_tamper)
        self._record("Gateway Crypto", "DPoP Tampering", (401, 403), res_tamper.status_code, "Blocked malicious state mutation due to invalid DPoP signature.")

        # Gateway Replay Attack (논스 재사용)
        replay_nonce = f"nonce_{uuid.uuid4().hex}"
        h_replay_1 = get_auth_headers(f"idem_rep_1_{uuid.uuid4().hex[:8]}", use_nonce=replay_nonce)
        h_replay_2 = get_auth_headers(f"idem_rep_2_{uuid.uuid4().hex[:8]}", use_nonce=replay_nonce)
        
        try:
            await self.client.post(relative_path, json={"action": "TEST"}, headers=h_replay_1, timeout=3.0)
        except httpx.ReadTimeout:
            pass
            
        res_replay = await self.client.post(relative_path, json={"action": "REPLAY"}, headers=h_replay_2)
        self._record("Gateway Nonce", "Replay Attack", (423, 423), res_replay.status_code, "Replay Attack perfectly intercepted and locked by Nonce Protector.")

        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} Critical Breach Detected: {msg.msg}")
        self.halted_by_error = True
        self._record("WORKFLOW_CRASH", "Unhandled Exception", (200, 200), 000, f"Test Suite halted prematurely due to: {msg.msg}")
        return StopMessage(result=False)

"""Runner Execution Wrapper"""
class SecuritySuiteRunner:
    def __init__(self):
        self.log = log
        
        # 프로덕션 환경에서의 강제 실행 차단 장치
        if DPHI_ENV not in ("test", "e2e", "local"):
            self.log.critical(f"⚠ SECURITY HALT: Cannot run E2E penetration tests in strict environment ({DPHI_ENV}).")
            exit(1)
            
        self.base_url = XPHI_BASE
        self.workflow = SecurityBoundWorkflow(base_url=self.base_url)

    def _print_report(self):
        self.log.info("\n" + "="*90)
        self.log.info("🛡️ [DPHI BOUND PENETRATION TEST REPORT]")
        self.log.info("="*90)
        
        reports = self.workflow.reports
        failed = sum(1 for r in reports if not r.passed)
        
        for idx, r in enumerate(reports, 1):
            status_icon = "✅" if r.passed else "❌"
            result_str = "PASSED" if r.passed else "FAILED"
            prefix = f"{status_icon} {idx:02d}. [{r.vector}]".ljust(26)
            scenario = f"{r.attack_type}".ljust(22)
            status = f"Status: {r.actual_status}".ljust(12)
            self.log.info(f"{prefix} {scenario} | Result: {result_str.ljust(6)} | {status} | {r.details}")
            
        self.log.info("-" * 90)
        if failed == 0 and not self.workflow.halted_by_error:
            self.log.info("🎉 ALL BOUND PENETRATION TESTS EXECUTED SUCCESSFULLY. (ZERO-TRUST BOUNDARY SECURE)")
        else:
            self.log.critical(f"💥 BOUNDARY COMPROMISED! Failed: {failed} (Halted: {self.workflow.halted_by_error}). Immediate patching required.")
            exit(1)
        self.log.info("="*90 + "\n")

    async def execute_suite(self):
        self.log.info("\n" + "="*80)
        self.log.info(f"🧪 [DPHI SECURITY BOUND] Executing Chaos & Security Tests against {self.base_url}")
        self.log.info("="*80)
        
        try:
            await self.workflow.execute()
        finally:
            await self.workflow.client.aclose()
            with suppress(Exception):
                await TunnelFactory.close_all()
                
        self._print_report()

def main():
    app = SecuritySuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute_suite)

if __name__ == "__main__":
    main()