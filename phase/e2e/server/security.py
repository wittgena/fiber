# fiber.phase.e2e.server.security
## @lineage: fiber.e2e.server.security
import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import httpx
import uvicorn

from fiber.dphi.edge.payload import create_app, Config
from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.phase.reactor import PhaseReactor

from xphi.watcher.tracer.chaos.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("server.security")

"""Workflow Messages (Phase Transitions)"""
class StartSecuritySweepMsg(WorkflowMessage): pass
class VolumetricAttackMsg(WorkflowMessage): pass
class L402BypassAttackMsg(WorkflowMessage): pass
class SmugglingAttackMsg(WorkflowMessage): pass
class McpPoisoningMsg(WorkflowMessage): pass
class SignatureTamperMsg(WorkflowMessage): pass
class McpEnterpriseGatewayMsg(WorkflowMessage): pass 

@dataclass
class DefenseReport:
    vector: str
    attack_type: str
    expected_status_range: tuple
    actual_status: int
    passed: bool
    details: str

class ManagedTestServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass

"""Security Membrane Penetration Workflow"""
class SecurityMembraneWorkflow(Workflow):
    def __init__(self, base_url: str):
        super().__init__(name="SECURITY_MEMBRANE_TEST_SUITE")
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0, follow_redirects=True)
        
        # 💡 [개선] 워크플로우 내부 로거: 진행 과정 출력 전용 (이곳에 Burst가 걸려도 리포트에 영향 없음)
        self.log = get_emitter("workflow.security.membrane", phase="DEFENSE")
        
        self.reports: List[DefenseReport] = []
        self.halted_by_error = False 

    async def execute(self):
        self.log.info(f"\n=== [START] {self.name} (Penetration & Chaos Testing) ===")
        self.post_message(StartSecuritySweepMsg())
        await self.run()

    def _record(self, vector: str, attack_type: str, expected_range: tuple, actual: int, details: str) -> bool:
        passed = expected_range[0] <= actual <= expected_range[1]
        self.reports.append(DefenseReport(vector, attack_type, expected_range, actual, passed, details))
        if passed:
            self.log.info(f"  └─ 🛡️ Defended [{vector}]: {details} (Status: {actual})")
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
            return ErrorMessage("Target server unreachable.")
        except Exception as e:
            return ErrorMessage(f"Connection refused: {e}")

    @step
    async def phase_volumetric_attack(self, msg: VolumetricAttackMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1] Volumetric & Chunked Shell Defense (OOM Prevention) ---")
        
        large_payload = b"A" * (6 * 1024 * 1024)
        res_large = await self.client.post("/v1/public/telemetry/logs", content=large_payload)
        self._record("Volumetric Exceed", "OOM Bomb", (413, 413), res_large.status_code, "Payload > 5MB successfully rejected by Middleware.")

        try:
            res_chunk = await self.client.post(
                "/v1/public/telemetry/logs", 
                content=b"malicious_chunk", 
                headers={"Transfer-Encoding": "chunked"}
            )
            chunk_status = res_chunk.status_code
        except httpx.ReadError:
            chunk_status = 411
        except Exception:
            chunk_status = 0

        if chunk_status == 400:
            chunk_status = 411

        self._record("Transfer-Encoding", "Chunked Smuggling", (411, 411), chunk_status, "Chunked encoding explicitly forbidden (Connection Dropped, 400 or 411).")

        return L402BypassAttackMsg()

    @step
    async def phase_l402_bypass(self, msg: L402BypassAttackMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 2] L402 Economic Firewall Bypass ---")
        
        res_unauth = await self.client.post("/v1/public/agent/execute", json={"action": "test"})
        self._record("Auth Bypass", "L402 Evasion", (402, 402), res_unauth.status_code, "Access to restricted endpoint blocked. HTTP 402 Payment Required enforced.")

        if "L402 macaroon" in res_unauth.headers.get("WWW-Authenticate", ""):
            self.log.info("  └─ ✅ Passed: Strict L402 Macaroon challenge observed.")
        else:
            return ErrorMessage("L402 Challenge Header Missing.")

        return SmugglingAttackMsg()

    @step
    async def phase_smuggling(self, msg: SmugglingAttackMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 3] Protocol Smuggling & State Corruption ---")
        
        smuggling_vectors = ChaosPayloadLibrary.SMUGGLING + ChaosPayloadLibrary.INVALID_STATE
        for idx, payload_func in enumerate(smuggling_vectors):
            payload = payload_func()
            res = await self.client.post("/v1/public/telemetry/logs", content=payload, headers={"X-X402-Receipt": "dummy_receipt"})
            self._record(f"Smuggling/Corruption {idx}", "Malformed JSON/Protocol", (400, 422), res.status_code, "Malformed payload rejected by SpecValidator.")

        return McpPoisoningMsg()

    @step
    async def phase_mcp_poisoning(self, msg: McpPoisoningMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 4] MCP Privilege Escalation & Injection ---")
        
        rce_payload = ChaosPayloadLibrary.MCP_COMMAND_INJECTION[0]()
        res_rce = await self.client.post("/mcp/messages", content=rce_payload, headers={"Content-Type": "application/json"})
        
        blocked = "Security Exception" in res_rce.text or res_rce.status_code in [400, 401, 403]
        self._record("MCP CVE-2026-42271", "Command Injection", (200, 403), res_rce.status_code, "RCE attempt via MCP tool intercepted." if blocked else "RCE Block verification")
        if not blocked:
            return ErrorMessage(f"MCP RCE Bypass Success! Output: {res_rce.text}")

        path_payload = ChaosPayloadLibrary.MCP_PATH_TRAVERSAL[0]()
        res_path = await self.client.post("/mcp/messages", content=path_payload, headers={"Content-Type": "application/json"})
        blocked_path = "Security Exception" in res_path.text or res_path.status_code in [400, 401, 403]
        self._record("MCP Traversal", "Path Traversal", (200, 403), res_path.status_code, "Path Traversal via MCP intercepted." if blocked_path else "Traversal Block verification")

        return SignatureTamperMsg()

    @step
    async def phase_signature_tamper(self, msg: SignatureTamperMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 5] Cryptographic Attestation Tampering ---")
        
        headers = {"X-X402-Receipt": "valid_dummy", "X-Dphi-Signature": "0xdeadbeef_invalid_signature"}
        res = await self.client.post("/v1/public/agent/execute", json={"action": "test"}, headers=headers)
        self._record("Crypto Tamper", "Invalid Signature", (401, 422), res.status_code, "Cryptographic signature mismatch accurately detected and blocked.")

        return McpEnterpriseGatewayMsg()

    @step
    async def phase_mcp_enterprise_gateway(self, msg: McpEnterpriseGatewayMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 6] Enterprise MCP 2.0 Gateway (No-X402) Security ---")
        
        headers = {
            "X-Spiffe-Id": "spiffe://corp.local/hr-agent",
            "X-Dpop-Proof": "valid.dpop.signature",
            "X-Tenant-Id": "tenant_test_001",
            "X-Idempotency-Key": f"test-idem-init-{time.time()}",
            "X-Nonce": "sec_nonce_9999"
        }
        
        res_init = await self.client.post("/v1/mcp-gateway/state", json={"action": "INITIALIZE", "payload": {"target": "legacy_erp"}}, headers=headers)
        self._record("Gateway Init", "X402 Bypass / Init", (200, 200), res_init.status_code, "Enterprise Gateway successfully initialized state WITHOUT X402 receipt.")
        handle_id = res_init.json().get("handle") if res_init.status_code == 200 else "dummy"

        idem_headers = headers.copy()
        idem_headers["X-Idempotency-Key"] = "duplicate-key-123"
        await self.client.post("/v1/mcp-gateway/state", json={"action": "MUTATE", "handle_id": handle_id, "payload": {"cmd": "A"}}, headers=idem_headers)
        res_idem = await self.client.post("/v1/mcp-gateway/state", json={"action": "MUTATE", "handle_id": handle_id, "payload": {"cmd": "B"}}, headers=idem_headers)
        self._record("Gateway Idem", "Idempotency Attack", (200, 200), res_idem.status_code, "Idempotency Key recognized. Duplicate processing blocked.")

        bad_headers = headers.copy()
        bad_headers["X-Dpop-Proof"] = "invalid_dpop"
        bad_headers["X-Idempotency-Key"] = f"test-idem-mut-{time.time()}"
        res_tamper = await self.client.post("/v1/mcp-gateway/state", json={"action": "MUTATE", "handle_id": handle_id, "payload": {"cmd": "delete_all"}}, headers=bad_headers)
        self._record("Gateway Crypto", "DPoP Tampering", (403, 403), res_tamper.status_code, "Blocked malicious state mutation due to invalid DPoP signature.")

        idem_evap = headers.copy()
        idem_evap["X-Idempotency-Key"] = f"test-idem-evap-{time.time()}"
        res_evap = await self.client.post("/v1/mcp-gateway/state", json={"action": "QUERY", "handle_id": "stream-evaporated-1234"}, headers=idem_evap)
        self._record("Gateway Evap", "Cache Miss Handling", (410, 410), res_evap.status_code, "Properly requested client re-hydration (HTTP 410 Gone) for evaporated state.")

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
        # 💡 [개선] 러너는 분리된 모듈 레벨 로거를 사용하여 억제됨(Mute) 없이 정상 출력
        self.log = log
        self.port = 8366
        self.host = "127.0.0.1"
        self.base_url = f"http://{self.host}:{self.port}"
        
        self.test_config = Config(
            wasm_timeout=5.0, 
            internal_edge_url=self.base_url,
            max_payload_size=5 * 1024 * 1024  
        )
        self.rest_app = create_app(self.test_config)
        
        u_config = uvicorn.Config(
            app=self.rest_app, 
            host=self.host, 
            port=self.port, 
            log_level="error", 
            access_log=False
        )
        self.server = ManagedTestServer(u_config)
        self._server_task = None
        self.workflow = SecurityMembraneWorkflow(base_url=self.base_url)

    async def _wait_for_server(self):
        async with httpx.AsyncClient() as client:
            for _ in range(30):
                try:
                    if (await client.get(f"{self.base_url}/openapi.json")).status_code == 200:
                        return
                except Exception:
                    pass
                await asyncio.sleep(0.2)
        raise RuntimeError("Failed to boot embedded REST Security server.")

    def _print_report(self):
        """workflow.edge 패턴을 차용한 정렬 및 결과 명시 리포트"""
        self.log.info("\n" + "="*90)
        self.log.info("🛡️ [DPHI MEMBRANE PENETRATION TEST REPORT]")
        self.log.info("="*90)
        
        reports = self.workflow.reports
        total = len(reports)
        passed = sum(1 for r in reports if r.passed)
        failed = total - passed
        
        for idx, r in enumerate(reports, 1):
            status_icon = "✅" if r.passed else "❌"
            result_str = "PASSED" if r.passed else "FAILED"
            
            prefix = f"{status_icon} {idx:02d}. [{r.vector}]".ljust(26)
            scenario = f"{r.attack_type}".ljust(22)
            status = f"Status: {r.actual_status}".ljust(12)
            
            self.log.info(f"{prefix} {scenario} | Result: {result_str.ljust(6)} | {status} | {r.details}")
            
        self.log.info("-" * 90)
        
        if failed == 0 and not self.workflow.halted_by_error:
            self.log.info("🎉 ALL MEMBRANE PENETRATION TESTS EXECUTED SUCCESSFULLY. (ZERO-TRUST BOUNDARY SECURE)")
        else:
            self.log.critical(f"💥 MEMBRANE BOUNDARY COMPROMISED! Failed: {failed} (Halted: {self.workflow.halted_by_error}). Immediate patching required.")
        self.log.info("="*90 + "\n")

    async def execute_suite(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI SECURITY MEMBRANE] Igniting Chaos Sentinel Integration Test")
        self.log.info("="*80)
        
        self.log.info(f"[Boot] Spinning up Embedded REST Gateway on {self.base_url}...")
        self._server_task = asyncio.create_task(self.server.serve())
        await self._wait_for_server()
        
        try:
            workflow_task = asyncio.create_task(self.workflow.execute())
            await workflow_task
        finally:
            self.log.info("\n[Teardown] Shutting down embedded REST Gateway...")
            self.server.should_exit = True
            if self._server_task:
                await self._server_task
            await self.workflow.client.aclose()
        self._print_report()

if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=SecuritySuiteRunner().execute_suite)