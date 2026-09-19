# fiber.dev.trace.eco.egress
import asyncio
import httpx
import uvicorn
from typing import List
from contextlib import suppress

from fiber.dev.infra.config import PipelineRunner, ManagedTestServer, TestResult, E2EConfig, Phase
from fiber.gateway.rest.payload import create_app, Config

from xphi.state.phase.reactor import PhaseReactor
from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.state.anchor.consensus import KernelLedger
from xphi.watcher.plane.emitter import get_emitter

from fiber.dev.sdk.gateway import DphiPublicClient, StrictPayloadFactory

log = get_emitter("tracer.eco_egress")

class EcoEgressTracer(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="ECO Egress & Observability Tracer", scope_name="TRACER_EGRESS")
        self.config = config
        self.local_url = f"{self.config.protocol}://{self.config.host}:{self.config.port}"
        
        self.mock_receipt = "x402-mock-receipt-for-egress-trace"
        self.server = None
        self._server_task = None
        self.sdk_client = None
        
        self.set_phases([
            Phase("Egress Infrastructure Ignition", self.phase_ignition),
            Phase("Strict OTLP Telemetry Extractor Validation", self.phase_telemetry_egress),
            Phase("Cryptographic Audit Event Sealing", self.phase_audit_egress)
        ])

    async def phase_ignition(self):
        log.info(f"[{self.scope_name}] Bootstrapping Gateway for Egress Trace...")
        self.tunnel = await TunnelFactory.get_default()
        self.ledger = KernelLedger()
        
        self.rest_app = create_app(config=Config(), tunnel=self.tunnel, ledger=self.ledger)
        u_config = uvicorn.Config(app=self.rest_app, host=self.config.host, port=self.config.port, log_level="error", access_log=False)
        self.server = ManagedTestServer(u_config)
        self._server_task = asyncio.create_task(self.server.serve())
        
        async with httpx.AsyncClient() as client:
            for _ in range(20):
                try:
                    if (await client.get(f"{self.local_url}/openapi.json")).status_code == 200: break
                except Exception: pass
                await asyncio.sleep(0.2)

        # Connector 없이 SDK 클라이언트만 초기화
        self.sdk_client = DphiPublicClient(base_url=self.local_url)
        log.info("✅ Egress SDK Client Ready.")

    async def phase_telemetry_egress(self):
        """SDK를 통해 생성한 OTLP 페이로드가 파싱 엔진을 무사히 통과하는지 검증"""
        otlp_payload = StrictPayloadFactory.create_telemetry_payload(
            tenant_id="tenant-trace-01",
            model_name="trace-model",
            prompt_tokens=150,
            completion_tokens=50
        )
        
        res = await self.sdk_client.push_telemetry(otlp_payload, payment_receipt=self.mock_receipt)
        if res.get("status") == "success" and res.get("fingerprint") != "N/A":
            log.info(f"✅ OTLP Telemetry sealed and mapped to kernel fingerprint: {res['fingerprint']}")
        else:
            raise RuntimeError(f"OTLP Trace failed. Response: {res}")

    async def phase_audit_egress(self):
        """SDK를 통해 생성한 Audit 이벤트가 암호학적으로 기록(Sealing)되는지 검증"""
        audit_payload = StrictPayloadFactory.create_audit_payload(
            actor="egress-tracer",
            action="verify_observability_pipeline",
            message="Audit egress path trace verification."
        )
        
        res = await self.sdk_client.record_audit_event(audit_payload, payment_receipt=self.mock_receipt)
        if "hash" in res:
            log.info(f"✅ Audit Event sealed securely. Hash: {res['hash']}")
        else:
            raise RuntimeError(f"Audit Trace failed. Response: {res}")

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Tracer: {self.name} ===")
        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                try:
                    await phase.action()
                    results.append(TestResult("EGRESS", phase.name, True, True))
                except Exception as e:
                    log.error(f"Phase '{phase.name}' Halted: {str(e)}")
                    results.append(TestResult("EGRESS", phase.name, False, True))
                    break 
        finally:
            if self.server:
                self.server.should_exit = True
                if self._server_task: await self._server_task
            with suppress(Exception): await TunnelFactory.close_all()
            
        return results

def main():
    config = E2EConfig(host="127.0.0.1", port=8361, protocol="http")
    app = EcoEgressTracer(config)
    PhaseReactor.ignite(main_coro_func=app.run_pipeline)

if __name__ == "__main__":
    main()