# fiber.phase.e2e.edge
import asyncio
import random
import uvicorn
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, List, Optional

import httpx
from fiber.dphi.workflow.edge import EdgeWorkflow
from fiber.kernel.receptor.dphi.rest import create_app, Config
from fiber.kernel.daemon.rpc import RpcWorkerDaemon

from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.ingress.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from xphi.watcher.receptor.edge.tracer import E2EConfig, SceneConfig, HttpFlowTracer
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.tracer.dphi import DphiTracer
from xphi.kernel.wasm.builder import WasmBuilder

log = get_emitter("e2e.edge")

@dataclass
class Phase:
    name: str
    action: Callable[[], Coroutine[Any, Any, None]]

@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success


class ManagedTestServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass

# ==========================================
# Payload Builders (테스트용 데이터 생성기)
# ==========================================

def create_otlp_payload(inject_faults: bool) -> dict:
    from fiber.dphi.adapter.anchor import PhaseBuilder
    if inject_faults:
        return {"garbage_field_missing_required_keys": True}
    return PhaseBuilder.otlp_payload(is_malformed=False)

def create_agent_intent_payload(inject_faults: bool) -> dict:
    return {
        "agent_id": "test-agent-01",
        "responder_id": "target-node-01", 
        "action": "EXECUTE_PYTHON",
        "source_code": "print('Hello from Edge E2E Test')",
        "max_fuel": 1000000,
        "signature": "0x_bad_signature_for_testing_faults" if inject_faults else "0x_valid_dummy_signature",
        "sig_algo": "ECDSA_SECP256K1" 
    }

def create_trade_payload(inject_faults: bool) -> dict:
    if inject_faults:
        return {"agent_id": "test-agent-01"} 
    return {
        "agent_id": "test-agent-01", 
        "action": "TRADE", 
        "parameters": {"target_pair": "ETH/USDC", "amount": 100}
    }

def create_ledger_payload(exchange_root: str, inject_faults: bool) -> dict:
    if inject_faults:
        return {"stream_name": "audit_stream"} 
    return {
        "stream_name": "system_audit",
        "events": [
            {
                "action": "D3FI_SETTLEMENT",
                "user_id": "agent-01",
                "details": f"Trade Executed. State Root: {exchange_root}"
            }
        ],
        "verbose": False
    }

def get_edge_scene_config() -> SceneConfig:
    return SceneConfig(
        otlp_builder=create_otlp_payload,
        agent_intent_builder=create_agent_intent_payload,
        trade_builder=create_trade_payload,
        ledger_builder=create_ledger_payload
    )

# ==========================================
# Test Pipeline Runners
# ==========================================

class PipelineRunner:
    def __init__(self, name: str, scope_name: str):
        self.name = name
        self.scope_name = scope_name
        self.phases: List[Phase] = []
        
    def set_phases(self, phases: List[Phase]):
        self.phases = phases
        
    async def run_pipeline(self) -> List[TestResult]:
        pass


class GatewayTracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Public Gateway & Network Isolation Trace", scope_name="EDGE_INGRESS_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        self.scene_config = get_edge_scene_config()
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        
        self.test_config = Config(wasm_timeout=5.0)
        self.rest_app = create_app(self.test_config)
        
        u_config = uvicorn.Config(app=self.rest_app, host="127.0.0.1", port=self.config.port, log_level="error", access_log=False)
        self.server = ManagedTestServer(u_config)
        self._server_task = None
        
        self.worker_daemon = RpcWorkerDaemon(ctx=self.rest_app.state)
        self._worker_task = None
        
        self.set_phases([
            Phase("Wasm Build & Pre-warm", self.phase_wasm_build),
            Phase("Gateway Ingress (Golden Path)", self.phase_ingress_e2e_golden),
            Phase("Gateway Ingress (Negative Path)", self.phase_ingress_e2e_negative),
            Phase("Gateway Ingress (Tampered Attestation)", self.phase_ingress_e2e_tampered),
            Phase("Sentinel Security (Chaos WAF Check)", self.phase_sentinel_security)
        ])

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
        
        log.info(f"[Pipeline] Booting embedded Uvicorn REST server on {self.local_url}...")
        self._server_task = asyncio.create_task(self.server.serve())
        
        await self._wait_for_server()
        
        log.info(f"[Pipeline] Igniting RpcWorkerDaemon lifecycle with shared Mock Context...")
        self.worker_daemon.running = True
        self._worker_task = asyncio.create_task(self.worker_daemon.run())
        
        await asyncio.sleep(0.5)

        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n▶️ [PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult("EDGE_GATEWAY", phase.name, True, True))
                except Exception as e:
                    log.error(f"Phase '{phase.name}' Halted: {str(e)}")
                    results.append(TestResult("EDGE_GATEWAY", phase.name, False, True))
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
            log.info(f"[Pipeline] All daemons and servers evaporated safely.")
            
        return results

    async def phase_wasm_build(self):
        builder = WasmBuilder()
        await builder.trace()
        if getattr(builder, 'rupture_confirmed', False): raise RuntimeError("WasmBuilder failed")

    async def _run_scene(self, inject_faults: bool, attestation_injector: Optional[Callable] = None):
        async with httpx.AsyncClient(base_url=self.config.base_url, event_hooks={'request': [self.tracer.trace_request], 'response': [self.tracer.trace_response]}, timeout=15.0) as client:
            runner = EdgeWorkflow(config=self.config, scene_config=self.scene_config, client=client, inject_faults=inject_faults, attestation_injector=attestation_injector)
            tracer = DphiTracer(tester=runner)
            await tracer.trace() 
            
            has_rupture = getattr(tracer, 'rupture_confirmed', False)
            
            if attestation_injector is not None:
                if runner.runner.fail_count == 0:
                    raise RuntimeError("Attestation Bypass! Tampered headers were NOT rejected.")
                return 

            if has_rupture or runner.runner.fail_count > 0:
                raise RuntimeError(f"Gateway Ingress Phase failed (Fault Inject: {inject_faults}).")

    async def phase_ingress_e2e_golden(self): await self._run_scene(False)
    async def phase_ingress_e2e_negative(self): await self._run_scene(True)
    async def phase_ingress_e2e_tampered(self):
        tamper_func = getattr(RpcChaosInjector, 'corrupt_attestation_header', None)
        if tamper_func: await self._run_scene(False, tamper_func)

    async def phase_sentinel_security(self):
        attack_vectors = ChaosPayloadLibrary.get_all_vectors()
        async with httpx.AsyncClient(base_url=self.config.base_url) as client:
            for vector_name, rule_list in attack_vectors:
                payload = random.choice(rule_list)() if isinstance(rule_list, list) else rule_list()
                res = await client.post("/v1/public/telemetry/logs", content=payload)
                if res.status_code >= 500 or res.status_code < 400:
                    raise RuntimeError(f"Gateway Breach! '{vector_name}' bypassed defenses. Status: {res.status_code}")


class EdgeSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def _run_gateway_pipeline(self):
        net_config = E2EConfig(host="127.0.0.1", port=8353, protocol="http")
        self.results.extend(await GatewayTracerPipeline(config=net_config).run_pipeline())

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("🛡️ [EDGE GATEWAY TEST SUITE REPORT]")
        self.log.info("="*80)
        all_passed = all(r.passed for r in self.results)
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            self.log.info(f"{status_icon} {idx:02d}. [{res.target}]".ljust(22) + f"{res.scenario.ljust(45)} | Result: {'PASSED' if res.passed else 'FAILED'}")
        self.log.info("-" * 80)
        if all_passed: self.log.info("🎉 ALL EDGE INGRESS & ISOLATION TESTS EXECUTED SUCCESSFULLY.")
        else: self.log.critical("💥 EDGE BOUNDARY COMPROMISED. Check logs for details.")
        self.log.info("="*80 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI EDGE MASTER SUITE] Commencing Gateway Ingress & Security Tests")
        self.log.info("="*80)
        await self._run_gateway_pipeline()
        self._print_report()

if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=EdgeSuiteRunner().execute)