# fiber.phase.e2e.edge.compliance
import asyncio
import random
import uvicorn
import httpx
from typing import List

from fiber.dphi.edge.payload import create_app, Config
from fiber.dphi.daemon.rpc import RpcWorkerDaemon
from fiber.phase.e2e.infra.config import PipelineRunner, ManagedTestServer, TestResult, E2EConfig, Phase
from fiber.dphi.eco.client.sdk import DphiPublicClient, StrictPayloadFactory

from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.tracer.chaos.sentinel import ChaosPayloadLibrary
from xphi.watcher.plane.emitter import get_emitter

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.state.ledger.consensus import KernelLedger

log = get_emitter("e2e.compliance")

class ComplianceTracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Public Gateway - Cryptographic Compliance & Audit", scope_name="EDGE_COMPLIANCE")
        self.config = config
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        self.test_config = Config()
        
        # [SDK 인스턴스 초기화] E2E 테스트 시뮬레이션을 위한 SDK 클라이언트
        self.sdk_client = DphiPublicClient(base_url=self.local_url)
        
        self.rest_app = None
        self.server = None
        self._server_task = None
        
        self.worker_daemon = None
        self._worker_task = None
        
        self.set_phases([
            Phase("Telemetry OTLP Ingress (Golden Path)", self.phase_telemetry_golden),
            Phase("Telemetry WAF Defenses (Chaos Check)", self.phase_telemetry_chaos),
            Phase("Audit Notarization & Proof Issuance (Golden)", self.phase_audit_golden),
            Phase("Audit Schema Enforcement (Negative)", self.phase_audit_negative)
        ])

    async def _bootstrap_infrastructure(self):
        log.info(f"[{self.scope_name}] Bootstrapping Compliance Infrastructure (Tunnel & Ledger)...")
        self.tunnel = await TunnelFactory.get_default()
        self.ledger = KernelLedger()
        
        self.rest_app = create_app(
            config=self.test_config,
            tunnel=self.tunnel,
            ledger=self.ledger
        )
        
        u_config = uvicorn.Config(
            app=self.rest_app, 
            host="127.0.0.1", 
            port=self.config.port, 
            log_level="error",
            access_log=False
        )
        self.server = ManagedTestServer(u_config)
        self.worker_daemon = RpcWorkerDaemon(ctx=self.rest_app.state)

    async def _wait_for_server(self):
        async with httpx.AsyncClient() as client:
            for _ in range(20):
                try:
                    if (await client.get(f"{self.local_url}/openapi.json")).status_code == 200: return
                except Exception: pass
                await asyncio.sleep(0.2)
        raise RuntimeError("Failed to boot embedded REST server for tests.")

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Pipeline: {self.name} ({self.scope_name}) ===")
        
        await self._bootstrap_infrastructure()
        
        log.info(f"[Pipeline] Booting embedded Uvicorn REST server on {self.local_url}...")
        self._server_task = asyncio.create_task(self.server.serve())
        
        await self._wait_for_server()
        
        log.info(f"[Pipeline] Igniting RpcWorkerDaemon lifecycle...")
        self.worker_daemon.running = True
        self._worker_task = asyncio.create_task(self.worker_daemon.run())
        
        await asyncio.sleep(0.5)

        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n▶️ [PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult("EDGE_COMPLIANCE", phase.name, True, True))
                except Exception as e:
                    log.error(f"Phase '{phase.name}' Halted: {str(e)}")
                    results.append(TestResult("EDGE_COMPLIANCE", phase.name, False, True))
                    break 
        finally:
            log.info(f"\n[Pipeline] Triggering teardown sequence...")
            
            self.server.should_exit = True
            if self._server_task: await self._server_task
            
            self.worker_daemon.running = False
            if self._worker_task: 
                self._worker_task.cancel()
                try:
                    await asyncio.wait_for(self._worker_task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                    
            try:
                await TunnelFactory.close_all()
                log.info("[Pipeline] TunnelFactory closed securely.")
            except Exception as e:
                log.error(f"[Pipeline] Error closing TunnelFactory: {e}")
                
            log.info(f"[Pipeline] All daemons and servers evaporated safely.")
            
        return results

    # =========================================================================
    # Phase Implementations (SDK 기반 통합 테스트)
    # =========================================================================

    async def phase_telemetry_golden(self):
        """[Telemetry] StrictPayloadFactory와 SDK를 통한 OTLP Seal 검증"""
        payload = StrictPayloadFactory.create_telemetry_payload(
            tenant_id="tenant-456",
            model_name="gpt-4o",
            prompt_tokens=150,
            completion_tokens=50
        )
        
        res = await self.sdk_client.push_telemetry(
            request=payload, 
            payment_receipt="mock_valid_receipt"
        )
        
        if res.get("status") != "success":
            raise RuntimeError("SDK failed to confirm telemetry success.")
        if res.get("fingerprint") == "N/A":
            raise RuntimeError("Kernel Fingerprint is missing from SDK response.")
            
        log.info(f"Telemetry Sealed Successfully via SDK. Fingerprint: {res.get('fingerprint')}")

    async def phase_telemetry_chaos(self):
        """[Telemetry] Chaos WAF 테스트 (SDK 우회하여 서버 방어막 직접 타격)"""
        attack_vectors = ChaosPayloadLibrary.get_all_vectors()
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            for vector_name, rule_list in attack_vectors:
                payload = random.choice(rule_list)() if isinstance(rule_list, list) else rule_list()
                res = await client.post("/v1/public/telemetry/logs", content=payload)
                if res.status_code >= 500 or res.status_code < 400:
                    raise RuntimeError(f"Compliance WAF Breach! '{vector_name}' bypassed defenses. Status: {res.status_code}")
        log.info("WAF Defense fully operational against raw injection payloads.")

    async def phase_audit_golden(self):
        """[Audit] SDK를 통한 이벤트 Notarization(공증) 및 Merkle Proof 검증"""
        payload = StrictPayloadFactory.create_audit_payload(
            actor="bot-007",
            action="financial_trade",
            message="Financial trade executed for user secret@corp.com",
            require_proof=True
        )
        
        result = await self.sdk_client.record_audit_event(
            request=payload,
            payment_receipt="mock_x402_trade"
        )
        
        if "membership_proof" not in result or not result["membership_proof"]:
            raise RuntimeError("CRITICAL: SDK did not return Cryptographic Proof (Merkle).")
            
        log.info(f"Audit Event Notarized via SDK. Proof Hash: {result.get('hash')}")

    async def phase_audit_negative(self):
        """[Audit] 비정상적 구조 시 422 에러 강제 여부 검증 (SDK 우회)"""
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            malformed_payload = {"verbose": True, "some_data": "invalid"}
            headers = {"X-X402-Receipt": "mock_x402_trade"}
            
            res = await client.post("/v1/public/audit/event", json=malformed_payload, headers=headers)
            
            if res.status_code != 422:
                raise RuntimeError(f"Failed to block malformed audit request. Expected 422, got {res.status_code}")
                
            log.info("Schema enforcement validated on raw HTTP ingress. Malformed requests appropriately blocked.")


class ComplianceSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def _run_compliance_pipeline(self):
        net_config = E2EConfig(host="127.0.0.1", port=8354, protocol="http")
        self.results.extend(await ComplianceTracerPipeline(config=net_config).run_pipeline())

    def _print_report(self):
        # [Burst 방지] 연속 logging 호출 대신 단일 문자열 버퍼로 결합하여 1회 발행
        lines = [
            "\n" + "=" * 80,
            "📜 [EDGE COMPLIANCE TEST SUITE REPORT]",
            "=" * 80
        ]
        
        all_passed = all(r.passed for r in self.results)
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            lines.append(
                f"{status_icon} {idx:02d}. [{res.target}]".ljust(22)
                + f"{res.scenario.ljust(45)} | Result: {'PASSED' if res.passed else 'FAILED'}"
            )
            
        lines.append("-" * 80)
        if all_passed: 
            lines.append("🎉 ALL COMPLIANCE NOTARIZATION & SECURITY TESTS PASSED.")
        else: 
            lines.append("💥 COMPLIANCE BOUNDARY COMPROMISED. Check logs for details.")
        lines.append("=" * 80 + "\n")
        
        self.log.info("\n".join(lines))

    async def execute(self):
        self.log.info("\n" + "=" * 80)
        self.log.info("🧪 [DPHI COMPLIANCE SUITE] Commencing Cryptographic Audit & Telemetry Tests via SDK")
        self.log.info("=" * 80)
        await self._run_compliance_pipeline()
        self._print_report()

def main(args_list: list[str] = None):
    app = ComplianceSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()