# dphi.phase.edge
import asyncio
import random
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, List, Optional

import httpx

from dphi.phase.workflow.edge import EdgeWorkflow
from receptor.ingress.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from receptor.rest import api as rest_app, lifespan

from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import flow_scope, get_emitter
from watcher.tracer.dphi import DphiTracer
from watcher.tracer.edge import E2EConfig, SceneConfig, HttpFlowTracer, RouteRegistry
from watcher.wasm.builder import WasmBuilder


log = get_emitter("phase.edge")

def create_otlp_payload(inject_faults: bool) -> dict:
    from dphi.adapter.config.client import PhaseBuilder
    if inject_faults:
        return {"garbage_field_missing_required_keys": True}
    return PhaseBuilder.otlp_payload(is_malformed=False)

def create_agent_intent_payload(inject_faults: bool) -> dict:
    if inject_faults:
        return {"invalid_intent": "missing_code_and_signature"}
    
    return {
        "agent_id": "test-agent-01",
        "action": "EXECUTE_PYTHON",
        "source_code": "print('Hello from Edge E2E Test')",
        "max_fuel": 1000000,
        "signature": "0x_valid_dummy_signature"
    }

def get_edge_scene_config() -> SceneConfig:
    return SceneConfig(
        otlp_builder=create_otlp_payload,
        agent_intent_builder=create_agent_intent_payload,
        ledger_builder=lambda root_hash, fault: {}
    )

@dataclass
class Phase:
    name: str
    action: Callable[[], Coroutine[Any, Any, None]]

class PipelineRunner:
    def __init__(self, name: str, scope_name: str):
        self.name = name
        self.scope_name = scope_name
        self.phases: List[Phase] = []
        
    def set_phases(self, phases: List[Phase]):
        self.phases = phases
        
    async def run_pipeline(self):
        log.info(f"\n=== Starting Pipeline: {self.name} ({self.scope_name}) ===")
        for phase in self.phases:
            log.info(f"--> Executing Phase: {phase.name}")
            await phase.action()
        log.info(f"=== Pipeline Completed: {self.name} ===\n")

class GatewayTracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Public Gateway & Network Isolation Trace", scope_name="EDGE_INGRESS_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        
        self.routes = RouteRegistry(rest_app, config.fallback_routes)
        self.scene_config = get_edge_scene_config()
        
        self.set_phases([
            Phase("Wasm Build & Pre-warm", self.phase_wasm_build),
            Phase("Gateway Ingress (Golden Path)", self.phase_ingress_e2e_golden),
            Phase("Gateway Ingress (Negative Path)", self.phase_ingress_e2e_negative),
            Phase("Gateway Ingress (Tampered Attestation)", self.phase_ingress_e2e_tampered),
            Phase("Sentinel Security (Chaos WAF Check)", self.phase_sentinel_security)
        ])

    async def phase_wasm_build(self):
        log.info("\n[Pipeline] Validating WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        if getattr(builder, 'rupture_confirmed', False):
            raise RuntimeError("WasmBuilder failed to construct valid binaries. Cannot proceed with Edge test.")

    async def _run_scene(self, inject_faults: bool, attestation_injector: Optional[Callable] = None):
        transport = httpx.ASGITransport(app=rest_app)
        async with lifespan(rest_app):
            async with httpx.AsyncClient(
                transport=transport, 
                base_url=self.config.base_url,
                event_hooks={'request': [self.tracer.trace_request], 'response': [self.tracer.trace_response]}
            ) as client:
                
                runner = EdgeWorkflow(
                    config=self.config, 
                    scene_config=self.scene_config,
                    routes=self.routes,
                    client=client, 
                    inject_faults=inject_faults,
                    attestation_injector=attestation_injector 
                )
                
                tracer = DphiTracer(tester=runner)
                await tracer.trace() 
                
                has_rupture = getattr(tracer, 'rupture_confirmed', False)
                
                if attestation_injector is not None:
                    if runner.runner.fail_count == 0:
                         raise RuntimeError("Attestation Bypass! Tampered headers were NOT rejected by Gateway.")
                    return 
                
                if has_rupture or runner.runner.fail_count > 0:
                    raise RuntimeError(f"Gateway Ingress Phase failed (Fault Inject: {inject_faults}).")

    async def phase_ingress_e2e_golden(self):
        await self._run_scene(inject_faults=False)

    async def phase_ingress_e2e_negative(self):
        await self._run_scene(inject_faults=True)

    async def phase_ingress_e2e_tampered(self):
        tamper_func = getattr(RpcChaosInjector, 'corrupt_attestation_header', None)
        if tamper_func is None:
            log.warning("⚠️ Skipping Attestation Rejection Test: Missing Chaos Injector.")
            return
        await self._run_scene(inject_faults=False, attestation_injector=tamper_func)

    async def phase_sentinel_security(self):
        log.info(f"\n[Pipeline] Initiating Sentinel Chaos Attacks against Public Gateway...")
        transport = httpx.ASGITransport(app=rest_app)
        attack_vectors = ChaosPayloadLibrary.get_all_vectors()
        target_path = self.routes.url_for("public.public_otlp_logs_export")

        async with lifespan(rest_app):
            async with httpx.AsyncClient(
                transport=transport, 
                base_url=self.config.base_url,
                event_hooks={'request': [self.tracer.trace_request], 'response': [self.tracer.trace_response]}
            ) as client:
                for vector_name, rule_list in attack_vectors:
                    payload = random.choice(rule_list)() if isinstance(rule_list, list) else rule_list()
                    with flow_scope(execution_mode="CHAOS_TEST", security_probe=vector_name):
                        response = await client.post(target_path, content=payload)
                        # Gateway/Sentinel은 악성 페이로드를 400~499 에러로 튕겨내야 정상입니다.
                        if response.status_code >= 500 or response.status_code < 400:
                            raise RuntimeError(f"Gateway Breach! '{vector_name}' bypassed defenses. Status: {response.status_code}")
            log.info("  └─ All Chaos probes successfully deflected by Gateway Sentinel Membrane.")


@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success


class EdgeSuiteRunner:
    """Orchestrates the Gateway Ingress & Network Isolation Test Suite."""
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def _run_gateway_pipeline(self):
        self.log.info("\n▶️ [PART 1] Running Public Gateway & Isolation Pipeline...")
        net_config = E2EConfig(host="localhost", port=8000, protocol="http")
        tracer_pipeline = GatewayTracerPipeline(config=net_config)
        
        net_success = True
        try:
            await tracer_pipeline.run_pipeline()
        except Exception as e:
            self.log.error(f"Gateway Pipeline Halted: {str(e)}")
            net_success = False

        self.results.append(TestResult(
            target="EDGE_GATEWAY",
            scenario="Gateway L7 Shield, Agent Execution & Internal Isolation",
            success=net_success,
            expected_success=True
        ))

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("🛡️ [EDGE GATEWAY TEST SUITE REPORT]")
        self.log.info("="*80)
        
        all_passed = True
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
                
            target_label = f"[{res.target}]"
            self.log.info(f"{status_icon} {idx:02d}. {target_label.ljust(15)} {res.scenario.ljust(50)} | Result: {status_text}")
            
        self.log.info("-" * 80)
        if all_passed:
            self.log.info("🎉 ALL EDGE INGRESS & ISOLATION TESTS EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 EDGE BOUNDARY COMPROMISED. Check the execution logs for trace details.")
        self.log.info("="*80 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI EDGE MASTER SUITE] Commencing Gateway Ingress & Security Tests")
        self.log.info("="*80)
        
        await self._run_gateway_pipeline()
        self._print_report()

def main():
    app = EdgeSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()