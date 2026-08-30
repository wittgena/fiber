# fiber.phase.e2e.edge
import asyncio
import random
import uvicorn
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, List, Optional

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct

from fiber.dphi.workflow.fsm.edge import EdgePhaseFSM, EdgePhaseState, StartIntentEvent
from fiber.dphi.workflow.edge import EdgeWorkflow
from fiber.dphi.client.http import VerifiedHttpClient
from fiber.kernel.receptor.edge.rest.api import create_app, Config
from fiber.kernel.daemon.rpc import RpcWorkerDaemon

from xphi.arch.wasm.builder import WasmBuilder
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.ingress.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from xphi.watcher.tracer.edge import E2EConfig, SceneConfig, HttpFlowTracer
from xphi.watcher.plane.emitter import get_emitter

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
        """
        [정합성이 회복된 E2E 시나리오 러너]
        - 클라이언트(E2E)가 직접 진짜 지갑과 서명을 생성하여 시스템에 진입합니다.
        - HTTP 통신 계층에서 응답 서명(Attestation)을 직접 검증합니다.
        - FSM의 최종 종단 상태(Terminal State)를 통해 비즈니스 흐름 전체의 완결성을 검증합니다.
        """
        async with httpx.AsyncClient(base_url=self.config.base_url, timeout=15.0) as client:
            
            # [개선] HTTP 이벤트 훅 조립 (통신 계층 방어선)
            response_hooks = [self.tracer.trace_response]
            
            if attestation_injector:
                async def apply_tamper(response: httpx.Response):
                    attestation_injector(response)
                response_hooks.append(apply_tamper)
                
            async def verify_signature(response: httpx.Response):
                # 서버가 200 OK를 반환했을 때만 응답 헤더의 Attestation 증명을 검증
                if response.status_code == 200:
                    verifier = VerifiedHttpClient(client=client)
                    verifier._verify_header_proof(response)
            response_hooks.append(verify_signature)
            
            client.event_hooks['request'] = [self.tracer.trace_request]
            client.event_hooks['response'] = response_hooks

            # 1. 클라이언트 자격 증명(지갑) 생성
            wallet = Account.create()
            agent_id = wallet.address
            action = "EXECUTE_PYTHON"
            max_fuel = 1000000
            source_code = "print('Hello from Edge E2E Test')"

            # 2. 암호학적 서명 생성 (결함 주입 시 고의로 위조 서명 사용)
            if inject_faults:
                signature = "0x_tampered_invalid_signature_for_chaos_testing"
            else:
                sig_text = f"EXECUTE:{agent_id}:{action}:{max_fuel}"
                msg = encode_defunct(text=sig_text)
                signature = wallet.sign_message(msg).signature.hex()

            # 3. 완벽한 도메인 이벤트 조립
            start_event = StartIntentEvent(
                agent_id=agent_id,
                action=action,
                max_fuel=max_fuel,
                source_code=source_code,
                signature=signature
            )
            
            # 4. 순수 FSM 및 Workflow 인스턴스화 후 실행
            fsm = EdgePhaseFSM()
            workflow = EdgeWorkflow(fsm=fsm, client=client, base_url=self.config.base_url)
            
            await workflow.execute(start_event) 
            
            # 5. FSM 거시 상태(Macro State)를 통한 엄격한 E2E 결과 검증
            if attestation_injector is not None:
                if fsm.state != EdgePhaseState.FAILED:
                    raise RuntimeError("Attestation Bypass! Tampered headers were NOT rejected.")
                return 

            if not inject_faults and fsm.state != EdgePhaseState.COMPLETED:
                raise RuntimeError(f"Golden Path Failed! Final FSM state: {fsm.state.name}")
            
            if inject_faults and fsm.state != EdgePhaseState.FAILED:
                raise RuntimeError(f"Negative Path Failed! Expected FAILED, got: {fsm.state.name}")

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