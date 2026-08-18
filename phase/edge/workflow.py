# phase.edge.workflow
import asyncio
import hashlib
import random
import time
import uuid
import uvicorn
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional

import httpx

from bound.client.http import VerifiedHttpClient
from phase.anchor.config.dphi import dphi_env
from phase.edge.tracer import E2EConfig, SceneConfig, HttpFlowTracer

from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.phase.runner import WebRunner
from kernel.phase.reactor import PhaseReactor

from watcher.plane.emitter import flow_scope, get_emitter
from watcher.tracer.dphi import DphiTracer
from watcher.wasm.builder import WasmBuilder

from receptor.ingress.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from receptor.rest import create_app, Config


log = get_emitter("phase.edge")


class StartSceneMsg(WorkflowMessage): pass
class AgentExecuteMsg(WorkflowMessage): pass  
class OtlpIngressMsg(WorkflowMessage): pass
class D3FiExchangeMsg(WorkflowMessage): pass
class LedgerAppendMsg(WorkflowMessage): pass
class EvmOperationsMsg(WorkflowMessage): pass 

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


# =====================================================================
# Uvicorn Managed Server for E2E Tests
# =====================================================================
class ManagedTestServer(uvicorn.Server):
    """테스트 러너의 이벤트 루프와 충돌하지 않도록 시그널 핸들러를 비활성화한 서버"""
    def install_signal_handlers(self):
        pass


# =====================================================================
# Payload Builders (Edge Router Pydantic 스키마에 맞춤)
# =====================================================================
def create_otlp_payload(inject_faults: bool) -> dict:
    from phase.anchor.config.client import PhaseBuilder
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

def create_trade_payload(inject_faults: bool) -> dict:
    if inject_faults:
        return {"agent_id": "test-agent-01"}  # Missing action & parameters
        
    return {
        "agent_id": "test-agent-01", 
        "action": "TRADE", 
        "parameters": {
            "target_pair": "ETH/USDC", 
            "amount": 100
        }
    }

def create_ledger_payload(exchange_root: str, inject_faults: bool) -> dict:
    if inject_faults:
        return {"stream_name": "audit_stream"}  # Missing required 'events' array
        
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


# =====================================================================
# Edge Workflow Definitions
# =====================================================================
class EdgeWorkflow(Workflow):
    def __init__(
        self, 
        config: E2EConfig, 
        scene_config: SceneConfig, 
        client: httpx.AsyncClient, 
        inject_faults: bool = False,
        attestation_injector: Optional[Callable[[httpx.Response], httpx.Response]] = None
    ):
        super().__init__(name="E2E_SCENE_NET")
        self.config = config
        self.scene_config = scene_config
        self.inject_faults = inject_faults
        self.runner = WebRunner(config.base_url, client=client)
        self.state_roots: Dict[str, str] = {}
        self.log = get_emitter("workflow.scene_runner")
        self.attestation_injector = attestation_injector

    async def execute(self):
        mode_str = "Negative/Faults" if self.inject_faults else "Golden Path"
        if self.attestation_injector:
            mode_str = "Attestation Rejection (Tampered Headers)"
            
        self.log.info(f"\n=== [START] {self.name} ({mode_str}) ===")
        
        if not self.inject_faults:
            self.log.info(f"  └─ Settlement Sink: Chain {dphi_env.network.chain_id} (Receptor: {dphi_env.contracts.nexus_clearing})")
            self.log.info(f"  └─ Exchange Agents: {dphi_env.agents.alpha.did} ⟷ {dphi_env.agents.beta.did}")

        self.post_message(StartSceneMsg())
        await self.run()

    def _verify_attestation(self, response: httpx.Response, request_path: str) -> Optional[ErrorMessage]:
        if response.status_code != 200:
            return None

        if self.attestation_injector:
            self.log.warning(f"  └─ 👾 Injecting Chaos: Tampering Attestation Headers for {request_path}")
            response = self.attestation_injector(response)
            
        try:
            self.log.info(f"  └─ 🔍 Verifying First-Party Attestation for {request_path}...")
            verifier = VerifiedHttpClient(client=self.runner.client)
            verifier._verify_header_proof(response, request_path)
            self.log.info("  └─ ✅ Attestation Signature Verified Successfully!")
            return None
        except Exception as e:
            self.log.error(f"  └─ 🚨 Attestation Failed: {e}")
            self.runner.fail_count += 1
            return ErrorMessage(f"Attestation Proof Verification Failed: {e}")

    @step
    async def phase_head_smoke(self, msg: StartSceneMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1] API Head & MCP Connectivity Sweep ---")
        res = await self.runner.client.get(f"{self.runner.base_url}/openapi.json")
        if res.status_code == 200:
            self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("API Head is unreachable")

        mcp_res = await self.runner.client.post(f"{self.runner.base_url}/mcp/sse")
        if mcp_res.status_code in [405, 400]: 
            self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("MCP Server unreachable")
            
        return AgentExecuteMsg() 

    @step
    async def phase_agent_execute(self, msg: AgentExecuteMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 2] Public Agent Execution & Metering ---")
        
        path = "/v1/public/agent/execute"
        payload = self.scene_config.agent_intent_builder(self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        test_desc = f"Agent Intent Execution ({'Invalid Intent Test' if self.inject_faults else 'Golden Path'})"
        
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
            return ErrorMessage(f"Agent Execution Check Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
            
        attest_err = self._verify_attestation(res, path)
        if attest_err: return attest_err
            
        return OtlpIngressMsg()

    @step
    async def phase_otlp_ingress(self, msg: OtlpIngressMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 3] OTLP Telemetry Ingress & WASM Fingerprint ---")
        
        path = "/v1/public/telemetry/logs"
        payload = self.scene_config.otlp_builder(self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        test_desc = f"OTLP Ingress ({'Membrane Strict Block Test' if self.inject_faults else 'Golden Path'})"
        
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
            return ErrorMessage(f"OTLP Ingress Check Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
            
        attest_err = self._verify_attestation(res, path)
        if attest_err: return attest_err
            
        if not self.inject_faults:
            self.state_roots["otlp_root"] = res.headers.get("x-edge-content-hash", "0x_default_otlp_hash")
            
        return D3FiExchangeMsg()

    @step
    async def phase_d3fi_exchange(self, msg: D3FiExchangeMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 4] D3Fi P2P Trade & Settlement (ExchangeNet) ---")
        
        path = "/v1/eco/exchange/order/ingress"
        payload = self.scene_config.trade_builder(self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        test_desc = f"D3Fi Trade Ingress ({'Membrane Strict Block Test' if self.inject_faults else 'Golden Path'})"
        
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
             return ErrorMessage(f"D3Fi Ingress Check Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
             
        attest_err = self._verify_attestation(res, path)
        if attest_err: return attest_err
             
        if not self.inject_faults:
            self.state_roots["exchange_root"] = res.json().get("session", {}).get("topo_id", f"d3fi_{uuid.uuid4().hex[:8]}")
            
        return LedgerAppendMsg()

    @step
    async def phase_ledger_append(self, msg: LedgerAppendMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 5] Immutable Ledger Stream Append ---")
        
        path = "/v1/core/ledger/stream/append"
        exchange_root = self.state_roots.get("exchange_root", "0x00")
        payload = self.scene_config.ledger_builder(exchange_root, self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        test_desc = f"Ledger Append ({'Membrane Strict Block Test' if self.inject_faults else 'Golden Path'})"
            
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
            return ErrorMessage(f"Ledger Append Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
            
        attest_err = self._verify_attestation(res, path)
        if attest_err: return attest_err
            
        if not self.inject_faults:
            self.state_roots["ledger_root"] = res.json().get("result", {}).get("hash", "0x_default_ledger_hash")

        return EvmOperationsMsg()

    @step
    async def phase_evm_operations(self, msg: EvmOperationsMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 6] External EVM & Wallet Integration ---")
        
        path = "/v1/ext/evm/wrap"
        
        caller_did = dphi_env.agents.alpha.did
        eth_address = caller_did.split(":")[-1] if "did:pkh" in caller_did else caller_did
        
        payload = {
            "caller_address": eth_address,
            "amount_wei": "10000000000000000", # 0.01 ETH for Faucet limits
            "agent_alias": "alpha"
        }
        
        if self.inject_faults:
            payload.pop("amount_wei") 
            
        expected_status = 422 if self.inject_faults else 200
        test_desc = f"EVM Wrap Execution ({'Malformed Payload Test' if self.inject_faults else 'Golden Path'})"
        
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
            return ErrorMessage(f"EVM Wrap Execution Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
            
        attest_err = self._verify_attestation(res, path)
        if attest_err: return attest_err

        if self.inject_faults:
            self.log.info(f"\n[SUCCESS] {self.name} Fault-Injection Scenario Completed.")
        else:
            self.log.info(f"\n[SUCCESS] {self.name} Completed successfully.")
            
        self.runner.report()
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} aborted during execution: {msg.msg}")
        self.runner.report()
        return StopMessage(result=False)


# =====================================================================
# Pipelines & Core Runners
# =====================================================================
class PipelineRunner:
    def __init__(self, name: str, scope_name: str):
        self.name = name
        self.scope_name = scope_name
        self.phases: List[Phase] = []
        
    def set_phases(self, phases: List[Phase]):
        self.phases = phases
        
    async def run_pipeline(self) -> List[TestResult]:
        """기본 파이프라인 런너 (오버라이드 권장)"""
        pass


class GatewayTracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Public Gateway & Network Isolation Trace", scope_name="EDGE_INGRESS_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        self.scene_config = get_edge_scene_config()
        
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        self.test_config = Config(
            wasm_timeout=5.0,
            internal_edge_url=self.local_url 
        )
        self.rest_app = create_app(self.test_config)
        
        u_config = uvicorn.Config(
            app=self.rest_app, 
            host="127.0.0.1", 
            port=self.config.port, 
            log_level="error", 
            access_log=False
        )
        self.server = ManagedTestServer(u_config)
        self._server_task = None
        
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
                    res = await client.get(f"{self.local_url}/openapi.json")
                    if res.status_code == 200:
                        return
                except Exception:
                    pass
                await asyncio.sleep(0.2)
        raise RuntimeError("Failed to boot embedded REST server for tests.")

    async def run_pipeline(self) -> List[TestResult]:
        """파이프라인 전체를 Uvicorn 서버 라이프사이클로 래핑하고 Phase별 결과 반환"""
        log.info(f"\n=== Starting Pipeline: {self.name} ({self.scope_name}) ===")
        
        log.info(f"[Pipeline] Booting embedded Uvicorn REST server on {self.local_url}...")
        self._server_task = asyncio.create_task(self.server.serve())
        await self._wait_for_server()

        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n▶️ [PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult(
                        target="EDGE_GATEWAY",
                        scenario=phase.name,
                        success=True,
                        expected_success=True
                    ))
                except Exception as e:
                    log.error(f"Phase '{phase.name}' Halted: {str(e)}")
                    results.append(TestResult(
                        target="EDGE_GATEWAY",
                        scenario=phase.name,
                        success=False,
                        expected_success=True
                    ))
                    break # Fail Fast 기조 유지 (에러 시 이후 Phase 생략)
        finally:
            log.info(f"\n[Pipeline] Shutting down embedded Uvicorn REST server...")
            self.server.should_exit = True
            if self._server_task:
                await self._server_task

        log.info(f"=== Pipeline Completed: {self.name} ===\n")
        return results

    async def phase_wasm_build(self):
        log.info("[Pipeline] Validating WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        if getattr(builder, 'rupture_confirmed', False):
            raise RuntimeError("WasmBuilder failed to construct valid binaries.")

    async def _run_scene(self, inject_faults: bool, attestation_injector: Optional[Callable] = None):
        async with httpx.AsyncClient(
            base_url=self.config.base_url,
            event_hooks={'request': [self.tracer.trace_request], 'response': [self.tracer.trace_response]},
            timeout=15.0
        ) as client:
            
            runner = EdgeWorkflow(
                config=self.config, 
                scene_config=self.scene_config,
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
        log.info("[Pipeline] Initiating Sentinel Chaos Attacks against Public Gateway...")
        attack_vectors = ChaosPayloadLibrary.get_all_vectors()
        target_path = "/v1/public/telemetry/logs"

        async with httpx.AsyncClient(
            base_url=self.config.base_url,
            event_hooks={'request': [self.tracer.trace_request], 'response': [self.tracer.trace_response]}
        ) as client:
            for vector_name, rule_list in attack_vectors:
                payload = random.choice(rule_list)() if isinstance(rule_list, list) else rule_list()
                with flow_scope(execution_mode="CHAOS_TEST", security_probe=vector_name):
                    response = await client.post(target_path, content=payload)
                    if response.status_code >= 500 or response.status_code < 400:
                        raise RuntimeError(f"Gateway Breach! '{vector_name}' bypassed defenses. Status: {response.status_code}")
        log.info("  └─ All Chaos probes successfully deflected by Gateway Sentinel Membrane.")


class EdgeSuiteRunner:
    """Orchestrates the Gateway Ingress & Network Isolation Test Suite."""
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def _run_gateway_pipeline(self):
        self.log.info("\n▶️ [PART 1] Running Public Gateway & Isolation Pipeline...")
        net_config = E2EConfig(host="127.0.0.1", port=8353, protocol="http")
        tracer_pipeline = GatewayTracerPipeline(config=net_config)
        
        # 💡 [MODIFIED] PipelineRunner가 수집한 Phase별 결과를 받아 메인 result에 추가
        phase_results = await tracer_pipeline.run_pipeline()
        self.results.extend(phase_results)

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
            # 💡 [MODIFIED] 이제 Phase의 이름이 scenario 부분에 출력됩니다.
            self.log.info(f"{status_icon} {idx:02d}. {target_label.ljust(15)} {res.scenario.ljust(45)} | Result: {status_text}")
            
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