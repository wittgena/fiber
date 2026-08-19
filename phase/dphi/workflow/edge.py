# phase.dphi.workflow.edge
import asyncio
import hashlib
import random
import time
import uuid
import uvicorn
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct

from eco.client.http import VerifiedHttpClient
from phase.dphi.config import dphi_env
from receptor.edge.tracer import E2EConfig, SceneConfig, HttpFlowTracer

from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.phase.runner import WebRunner
from kernel.phase.reactor import PhaseReactor

from watcher.plane.emitter import flow_scope, get_emitter
from watcher.tracer.dphi import DphiTracer
from watcher.wasm.builder import WasmBuilder

from receptor.ingress.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from receptor.rest import create_app, Config


log = get_emitter("workflow.edge")

class StartSceneMsg(WorkflowMessage): pass
class AgentExecuteMsg(WorkflowMessage): pass  
class OtlpIngressMsg(WorkflowMessage): pass
class D3FiExchangeMsg(WorkflowMessage): pass
class LedgerAppendMsg(WorkflowMessage): pass
class DvmClearingMsg(WorkflowMessage): pass

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


def create_otlp_payload(inject_faults: bool) -> dict:
    from phase.dphi.adapter.anchor import PhaseBuilder
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
        "sig_algo": "ECDSA_SECP256K1" # 정렬된 멀티 알고리즘 필드 주입
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
        
        self.test_acct = Account.create()
        self.test_agent_id = self.test_acct.address

    def _sign_payload(self, text_to_sign: str) -> str:
        msg = encode_defunct(text=text_to_sign)
        return self.test_acct.sign_message(msg).signature.hex()

    async def execute(self):
        mode_str = "Negative/Faults" if self.inject_faults else "Golden Path"
        if self.attestation_injector:
            mode_str = "Attestation Rejection (Tampered Headers)"
            
        self.log.info(f"\n=== [START] {self.name} ({mode_str}) ===")
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
        if res.status_code == 200: self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("API Head is unreachable")

        mcp_res = await self.runner.client.post(f"{self.runner.base_url}/mcp/sse")
        if mcp_res.status_code in [405, 400]: self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("MCP Server unreachable")
            
        return AgentExecuteMsg() 

    @step
    async def phase_agent_execute(self, msg: AgentExecuteMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 2] Public Agent Execution & Metering ---")
        
        path = "/v1/public/agent/execute"
        payload = self.scene_config.agent_intent_builder(self.inject_faults)
        payload["agent_id"] = self.test_agent_id
        
        if not self.inject_faults:
            sig_text = f"EXECUTE:{self.test_agent_id}:{payload['action']}:{payload['max_fuel']}"
            payload["signature"] = self._sign_payload(sig_text)
            expected_status = 200
        else:
            expected_status = 401 
        
        headers = {"X-X402-Receipt": "audit_receipt_dummy_string"}
        
        self.log.info(f"  └─ Sending POST to {path} (Expected: {expected_status})")
        try:
            res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload, headers=headers)
            
            if res.status_code == expected_status:
                self.log.info(f"  └─ ✅ Passed: Received {res.status_code} as expected.")
                self.runner.success_count += 1
            else:
                self.log.error(f"  └─ ❌ Failed: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")
                self.runner.fail_count += 1
                return ErrorMessage(f"Agent Execution Check Failed: Expected {expected_status}, Got {res.status_code}")
                
            attest_err = self._verify_attestation(res, path)
            if attest_err: return attest_err
        except Exception as e:
            return ErrorMessage(str(e))
            
        return OtlpIngressMsg()

    @step
    async def phase_otlp_ingress(self, msg: OtlpIngressMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 3] OTLP Telemetry Ingress & Strict Schema Check ---")
        
        path = "/v1/public/telemetry/logs"
        payload = self.scene_config.otlp_builder(self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        headers = {"X-X402-Receipt": "audit_receipt_dummy_string"}
            
        self.log.info(f"  └─ Sending POST to {path} (Expected: {expected_status})")
        try:
            res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload, headers=headers)
            
            if res.status_code == expected_status:
                self.log.info(f"  └─ ✅ Passed: Received {res.status_code} as expected.")
                self.runner.success_count += 1
            else:
                self.log.error(f"  └─ ❌ Failed OTLP Ingress: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")
                self.runner.fail_count += 1
                return ErrorMessage(f"OTLP Ingress Check Failed: Expected {expected_status}, Got {res.status_code}")
                
            attest_err = self._verify_attestation(res, path)
            if attest_err: return attest_err
            
            if not self.inject_faults:
                self.state_roots["otlp_root"] = res.headers.get("x-edge-content-hash", "0x_default_otlp_hash")
        except Exception as e:
             return ErrorMessage(str(e))
             
        return D3FiExchangeMsg()

    @step
    async def phase_d3fi_exchange(self, msg: D3FiExchangeMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 4] D3Fi P2P Trade & Settlement (ExchangeNet) ---")
        
        path = "/v1/eco/exchange/order/ingress"
        payload = self.scene_config.trade_builder(self.inject_faults)
        payload["agent_id"] = self.test_agent_id
        expected_status = 422 if self.inject_faults else 200
        
        res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload)
        if res.status_code == expected_status:
            self.runner.success_count += 1
        else:
             self.runner.fail_count += 1
             return ErrorMessage(f"D3Fi Ingress Check Failed: Expected {expected_status}, Got {res.status_code}")
             
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
            
        res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload)
        if res.status_code == expected_status:
            self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage(f"Ledger Append Failed: Expected {expected_status}, Got {res.status_code}")
            
        if not self.inject_faults:
            self.state_roots["ledger_root"] = res.json().get("result", {}).get("hash", "0x_default_ledger_hash")

        return DvmClearingMsg()

    @step
    async def phase_dvm_clearing(self, msg: DvmClearingMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 6] DVM Ledger Clearing & Settlement (Internal L2) ---")
        
        path = "/v1/ext/wallet/pay/x402"
        payload = {
            "payee_address": "0x000000000000000000000000000000000000dEaD",
            "amount_usdc": "10.0",
            "resource_id": f"res_{uuid.uuid4().hex[:8]}",
            "use_ledger": True
        }
        
        if self.inject_faults: 
            payload.pop("amount_usdc") 
            
        expected_status = 422 if self.inject_faults else 200
        
        self.log.info(f"  └─ Sending POST to {path} with use_ledger=True (Expected: {expected_status})")
        res = await self.runner.client.post(f"{self.runner.base_url}{path}", json=payload)
        
        if res.status_code == expected_status:
             self.runner.success_count += 1
        else:
             self.runner.fail_count += 1
             return ErrorMessage(f"DVM Clearing Execution Failed: Expected {expected_status}, Got {res.status_code}. Body: {res.text}")

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
        self.test_config = Config(wasm_timeout=5.0, internal_edge_url=self.local_url)
        self.rest_app = create_app(self.test_config)
        
        u_config = uvicorn.Config(app=self.rest_app, host="127.0.0.1", port=self.config.port, log_level="error", access_log=False)
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
                    if (await client.get(f"{self.local_url}/openapi.json")).status_code == 200: return
                except Exception: pass
                await asyncio.sleep(0.2)
        raise RuntimeError("Failed to boot embedded REST server for tests.")

    async def run_pipeline(self) -> List[TestResult]:
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
                    results.append(TestResult("EDGE_GATEWAY", phase.name, True, True))
                except Exception as e:
                    log.error(f"Phase '{phase.name}' Halted: {str(e)}")
                    results.append(TestResult("EDGE_GATEWAY", phase.name, False, True))
                    break 
        finally:
            log.info(f"\n[Pipeline] Shutting down embedded Uvicorn REST server...")
            self.server.should_exit = True
            if self._server_task: await self._server_task
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
            
            # [수정 구간] 변조(Attestation Injector) 시나리오 평가 로직 수정
            if attestation_injector is not None:
                # 방어벽이 변조를 잡아내서 에러(fail)가 발생했어야 정상. fail_count가 0이면 뚫린 것.
                if runner.runner.fail_count == 0:
                    raise RuntimeError("Attestation Bypass! Tampered headers were NOT rejected.")
                # 방어 성공: 테스트는 통과한 것이므로 에러를 던지지 않고 return
                return 

            # 일반 시나리오 (Golden Path 및 Negative Payload 테스트)
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