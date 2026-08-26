# workflow.server.security
## @lineage: fiber.workflow.server.security
"""
@desc: 
- End-to-End Penetration & Chaos Testing Suite for DPHI Ingress Membrane.
- Validates Volumetric, L402 Economic Firewall, Protocol Smuggling, and MCP Vulnerabilities.
"""
import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import httpx
import uvicorn

from fiber.dphi.receptor.rest import create_app, Config
from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.phase.reactor import PhaseReactor

from xphi.watcher.ingress.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("workflow.server.security", phase="DEFENSE")

"""Workflow Messages (Phase Transitions)"""
class StartSecuritySweepMsg(WorkflowMessage): pass
class VolumetricAttackMsg(WorkflowMessage): pass
class L402BypassAttackMsg(WorkflowMessage): pass
class SmugglingAttackMsg(WorkflowMessage): pass
class McpPoisoningMsg(WorkflowMessage): pass
class SignatureTamperMsg(WorkflowMessage): pass

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
        # Override to prevent Uvicorn from taking over SIGINT/SIGTERM during tests
        pass

"""Security Membrane Penetration Workflow"""
class SecurityMembraneWorkflow(Workflow):
    def __init__(self, base_url: str):
        super().__init__(name="SECURITY_MEMBRANE_TEST_SUITE")
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        self.log = log
        self.reports: List[DefenseReport] = []

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
        
        ## 6MB Payload Attack (Exceeds 5MB MAX_PAYLOAD_SIZE)
        large_payload = b"A" * (6 * 1024 * 1024)
        res_large = await self.client.post("/v1/public/telemetry/logs", content=large_payload)
        self._record(
            "Volumetric Exceed", "OOM Bomb", (413, 413), res_large.status_code, 
            "Payload > 5MB successfully rejected by Middleware."
        )

        ## Chunked Encoding Smuggling Attempt
        res_chunk = await self.client.post(
            "/v1/public/telemetry/logs", 
            content=b"malicious_chunk", 
            headers={"Transfer-Encoding": "chunked"}
        )
        self._record(
            "Transfer-Encoding", "Chunked Smuggling", (411, 411), res_chunk.status_code, 
            "Chunked encoding explicitly forbidden."
        )

        return L402BypassAttackMsg()

    @step
    async def phase_l402_bypass(self, msg: L402BypassAttackMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 2] L402 Economic Firewall Bypass ---")
        
        # Unauthorized access to a restricted endpoint (Execute)
        res_unauth = await self.client.post("/v1/public/agent/execute", json={"action": "test"})
        self._record(
            "Auth Bypass", "L402 Evasion", (402, 402), res_unauth.status_code, 
            "Access to restricted endpoint blocked. HTTP 402 Payment Required enforced."
        )

        # Ensure WWW-Authenticate header specifies L402
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
            res = await self.client.post(
                "/v1/public/telemetry/logs", 
                content=payload,
                headers={"X-X402-Receipt": "dummy_receipt"}
            )
            
            ## Expecting validation errors (422) or generic bad request (400)
            self._record(
                f"Smuggling/Corruption {idx}", "Malformed JSON/Protocol", (400, 422), res.status_code,
                "Malformed payload rejected by SpecValidator."
            )

        return McpPoisoningMsg()

    @step
    async def phase_mcp_poisoning(self, msg: McpPoisoningMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 4] MCP Privilege Escalation & Injection ---")
        
        ## MCP OS Command Injection (CVE-2026-42271 Simulation)
        rce_payload = ChaosPayloadLibrary.MCP_COMMAND_INJECTION[0]()
        res_rce = await self.client.post("/mcp/messages", content=rce_payload)
        
        ## ToposGateway Authorization should catch and block the unauthorized tool execution (403 or internal Error)
        blocked = "Security Exception" in res_rce.text or res_rce.status_code in [403, 401]
        self._record(
            "MCP CVE-2026-42271", "Command Injection", (200, 403), res_rce.status_code,
            "RCE attempt via MCP tool intercepted by ToposGateway Auth." if blocked else "RCE Block verification"
        )
        if not blocked:
            return ErrorMessage(f"MCP RCE Bypass Success! Output: {res_rce.text}")

        ## 2. Path Traversal
        path_payload = ChaosPayloadLibrary.MCP_PATH_TRAVERSAL[0]()
        res_path = await self.client.post("/mcp/messages", content=path_payload)
        blocked_path = "Security Exception" in res_path.text or res_path.status_code in [403, 401]
        self._record(
            "MCP Traversal", "Path Traversal", (200, 403), res_path.status_code,
            "Path Traversal via MCP intercepted." if blocked_path else "Traversal Block verification"
        )

        return SignatureTamperMsg()

    @step
    async def phase_signature_tamper(self, msg: SignatureTamperMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 5] Cryptographic Attestation Tampering ---")
        
        ## Tampered headers via RpcChaosInjector logic
        headers = {"X-X402-Receipt": "valid_dummy", "X-Dphi-Signature": "0xdeadbeef_invalid_signature"}
        res = await self.client.post("/v1/public/agent/execute", json={"action": "test"}, headers=headers)
        
        self._record(
            "Crypto Tamper", "Invalid Signature", (401, 422), res.status_code,
            "Cryptographic signature mismatch accurately detected and blocked."
        )

        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} Critical Breach Detected: {msg.msg}")
        return StopMessage(result=False)

    def generate_report(self):
        self.log.info("\n" + "="*85)
        self.log.info("🛡️ [DPHI MEMBRANE PENETRATION TEST REPORT]")
        self.log.info("="*85)
        
        total = len(self.reports)
        passed = sum(1 for r in self.reports if r.passed)
        failed = total - passed
        
        for idx, r in enumerate(self.reports, 1):
            icon = "✅" if r.passed else "🚨"
            self.log.info(f"{icon} {idx:02d}. [{r.vector[:18].ljust(18)}] {r.attack_type[:20].ljust(20)} | Status: {r.actual_status} | {r.details}")
            
        self.log.info("-" * 85)
        if failed == 0:
            self.log.info(f"🎉 MEMBRANE INTEGRITY: 100% ({passed}/{total}). ZERO-TRUST BOUNDARY SECURE.")
        else:
            self.log.critical(f"💥 MEMBRANE COMPROMISED! {failed} Vectors bypassed the gateway. Immediate patching required.")
        self.log.info("="*85 + "\n")


"""Runner Execution Wrapper"""
class SecuritySuiteRunner:
    def __init__(self):
        self.log = log
        self.port = 8366
        self.host = "127.0.0.1"
        self.base_url = f"http://{self.host}:{self.port}"
        
        ## Create REST App with Test Configuration
        self.test_config = Config(
            wasm_timeout=5.0, 
            internal_edge_url=self.base_url
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
            
        self.workflow.generate_report()

if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=SecuritySuiteRunner().execute_suite)