# phase.entry.exchange
import asyncio
import random
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional

import httpx
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

# --- Core Framework & Telemetry ---
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import flow_scope, get_emitter

# --- Domain Workflows & Tracers ---
from dphi.tracer.dphi import DphiTracer
from dphi.wasm.builder import WasmBuilder
from dphi.workflow.edge import EdgeWorkflow, E2EConfig, SceneConfig, TargetOp
from dphi.workflow.exchange import ExchangeWorkflow, ScenarioConfig

# --- Infrastructure & Mock Envs ---
from phase.epoch.config.client import PhaseBuilder
from phase.epoch.config.dphi import mock_env
from receptor.ingress.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from receptor.ingress.tracer import HttpFlowTracer, RouteRegistry
from receptor.rest import api as rest_app, lifespan 

log = get_emitter("entry.exchange")

@rest_app.exception_handler(RequestValidationError)
async def safe_validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=422,
        content={"detail": "Payload rejected: Malformed, invalid schema, or raw binary data detected."}
    )


# ==========================================
# Payload Builders
# ==========================================
def create_otlp_payload(inject_faults: bool) -> dict:
    if inject_faults:
        return {"garbage_field_missing_required_keys": True}
    return PhaseBuilder.otlp_payload(is_malformed=False)

def create_trade_payload(inject_faults: bool) -> dict:
    if inject_faults:
        return {"invalid_trade": "missing_all_required_data"}
    return PhaseBuilder.trade_intent(should_fail_policy=False)

def create_ledger_payload(root_hash: str, inject_faults: bool) -> dict:
    if inject_faults:
        return {"stream_name": "missing_events_field_test"}
    return PhaseBuilder.ledger_append("A2A_TRADE_SETTLEMENT", root_hash)

def get_default_scene_config() -> SceneConfig:
    return SceneConfig(
        otlp_builder=create_otlp_payload,
        trade_builder=create_trade_payload,
        ledger_builder=create_ledger_payload
    )


# ==========================================
# Core Pipeline Runner
# ==========================================
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
        log.info(f"=== Starting Pipeline: {self.name} ({self.scope_name}) ===")
        for phase in self.phases:
            log.info(f"--> Executing Phase: {phase.name}")
            await phase.action()
        log.info(f"=== Pipeline Completed: {self.name} ===")


# ==========================================
# Tracer Pipeline (Membrane & Wasm E2E)
# ==========================================
class TracerPipeline(PipelineRunner):
    """Pipeline for Network Membrane and WASM Build Verification"""
    def __init__(self, config: E2EConfig):
        super().__init__(name="Full E2E & Security Trace Pipeline", scope_name="GLOBAL_TRACE_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        
        # Injects fallback_routes to guarantee safety when exact name match fails
        self.routes = RouteRegistry(rest_app, config.fallback_routes)
        self.scene_config = get_default_scene_config()
        
        self.set_phases([
            Phase("Wasm Build", self.phase_wasm_build),
            Phase("Functional E2E (Golden Path)", self.phase_functional_e2e_golden),
            Phase("Functional E2E (Negative Path)", self.phase_functional_e2e_negative),
            Phase("Functional E2E (Tampered Headers)", self.phase_functional_e2e_tampered), # 🔥 새로 추가된 페이즈
            Phase("Sentinel Security (Chaos Membrane)", self.phase_sentinel_security)
        ])

    async def phase_wasm_build(self):
        log.info("\n[Pipeline] Running WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        if getattr(builder, 'rupture_confirmed', False):
            raise RuntimeError("WasmBuilder failed to construct valid binaries.")

    async def _run_scene(self, inject_faults: bool, attestation_injector: Optional[Callable] = None):
        transport = httpx.ASGITransport(app=rest_app)
        async with lifespan(rest_app):
            async with httpx.AsyncClient(
                transport=transport, 
                base_url=self.config.base_url,
                event_hooks={'request': [self.tracer.trace_request], 'response': [self.tracer.trace_response]}
            ) as client:
                
                # 🔥 EdgeWorkflow에 attestation_injector 전달
                runner = EdgeWorkflow(
                    config=self.config, 
                    scene_config=self.scene_config,
                    routes=self.routes,
                    client=client, 
                    inject_faults=inject_faults,
                    attestation_injector=attestation_injector 
                )
                
                if hasattr(rest_app.state, 'config'):
                    rest_app.state.config.committee_pubs = runner.notary_swarm.public_keys
                    
                tracer = DphiTracer(tester=runner)
                await tracer.trace() 
                
                has_rupture = getattr(tracer, 'rupture_confirmed', False)
                # 고의로 에러를 내는 Tampered Headers 시나리오의 경우, fail_count가 0보다 커야 성공(정상 차단)입니다.
                if attestation_injector is not None:
                    if runner.runner.fail_count == 0:
                         raise RuntimeError("Attestation Bypass! Tampered headers were NOT rejected.")
                    return # 정상적으로 차단되었으므로 에러를 발생시키지 않고 리턴
                
                # 일반 시나리오의 성공 여부 판단
                if has_rupture or runner.runner.fail_count > 0:
                    raise RuntimeError(f"Functional E2E Phase failed (Fault Inject: {inject_faults}).")

    async def phase_functional_e2e_golden(self):
        await self._run_scene(inject_faults=False)

    async def phase_functional_e2e_negative(self):
        await self._run_scene(inject_faults=True)

    async def phase_functional_e2e_tampered(self):
        """First-Party Oracle 응답 헤더가 훼손되었을 때 클라이언트가 거부하는지 확인합니다."""
        tamper_func = getattr(RpcChaosInjector, 'corrupt_attestation_header', None)
        if tamper_func is None:
            log.warning("⚠️ Skipping Attestation Rejection Test: 'corrupt_attestation_header' not implemented.")
            return
        await self._run_scene(inject_faults=False, attestation_injector=tamper_func)

    async def phase_sentinel_security(self):
        log.info(f"\n[Pipeline] Initiating Sentinel Chaos Attacks against XeLog Membrane...")
        transport = httpx.ASGITransport(app=rest_app)
        attack_vectors = ChaosPayloadLibrary.get_all_vectors()
        target_path = self.routes.url_for(TargetOp.OTLP_INGRESS)
        
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
                        if response.status_code >= 500 or response.status_code < 400:
                            raise RuntimeError(f"Membrane Breach! '{vector_name}' bypassed defenses. Status: {response.status_code}")
            log.info("  └─ All Chaos probes successfully deflected by Sentinel Membrane.")


# ==========================================
# Domain Scenarios & Master Orchestrator
# ==========================================
@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success


class ExchangeSuiteRunner:
    """Orchestrates the entire E2E Integration Test Suite."""
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []
        has_testnet_keys = bool(mock_env.cdp_wallet.api_name and mock_env.cdp_wallet.api_private_key)
        self.should_simulate = not has_testnet_keys

    async def _run_network_pipeline(self):
        self.log.info("\n▶️ [PART 1] Running Network & Chaos Membrane Pipeline...")
        net_config = E2EConfig(host="localhost", port=8000, protocol="http")
        tracer_pipeline = TracerPipeline(config=net_config)
        
        net_success = True
        try:
            await tracer_pipeline.run_pipeline()
        except Exception as e:
            self.log.error(f"Network Pipeline Halted: {str(e)}")
            net_success = False

        self.results.append(TestResult(
            target="NET_TEST",
            scenario="TracerPipeline (Golden, Negative, Tampered, Chaos)",
            success=net_success,
            expected_success=True
        ))

    async def _run_domain_workflows(self):
        self.log.info("\n▶️ [PART 2] Running Domain Workflow Scenarios...")
        if not self.should_simulate:
            self.log.info(f"⚡ [Notice] Testnet Keys detected. Workflows will execute LIVE on {mock_env.cdp_wallet.network_id}!")
        else:
            self.log.info("🛡️ [Notice] Workflows will execute in SIMULATION mode.")

        # 🔥 도메인 워크플로우(ExchangeWorkflow)용 시나리오
        # 여기서 signature_injector는 도메인 계층(리스트 객체)을 다룹니다.
        scenarios = [
            {
                "config": ScenarioConfig(
                    name="Golden Path (Pure Core + Valid Notaries)",
                    mandate_injector=None,
                    signature_injector=None
                ),
                "expected": True
            },
            {
                "config": ScenarioConfig(
                    name="Core Rejection: Expired AP2 Mandate",
                    mandate_injector=RpcChaosInjector.corrupt_ap2_mandate,
                    signature_injector=None
                ),
                "expected": False
            },
            {
                "config": ScenarioConfig(
                    name="Export Forgery: Invalid Notary Attestations", 
                    mandate_injector=None,
                    signature_injector=RpcChaosInjector.corrupt_consensus_signatures
                ),
                "expected": True 
            }
        ]

        for item in scenarios:
            scenario_config = item["config"]
            expected = item["expected"]
            
            workflow = ExchangeWorkflow(scenario=scenario_config, simulate_wallet=self.should_simulate)
            is_success = await workflow.start()
            
            self.results.append(TestResult(
                target="WORKFLOW",
                scenario=scenario_config.name,
                success=is_success,
                expected_success=expected
            ))
            await asyncio.sleep(0.5)

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("📊 [MASTER TEST SUITE REPORT]")
        self.log.info("="*80)
        
        all_passed = True
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
                
            target_label = f"[{res.target}]"
            self.log.info(f"{status_icon} {idx:02d}. {target_label.ljust(12)} {res.scenario.ljust(50)} | Result: {status_text}")
            
        self.log.info("-" * 80)
        if all_passed:
            self.log.info("🎉 ALL TESTS (NETWORK & WORKFLOW) EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 SOME TESTS FAILED. Check the execution logs for trace details.")
        self.log.info("="*80 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI E2E MASTER SUITE] Commencing Full System Tests (Network + Workflow)")
        self.log.info("="*80)
        
        await self._run_network_pipeline()
        await self._run_domain_workflows()
        self._print_report()

def main():
    app = ExchangeSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()