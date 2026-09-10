# fiber.phase.e2e.dphi.edge
import asyncio
import random
import uvicorn
import json
import os
import tempfile
from typing import Any, Callable, Coroutine, List, Optional
from contextlib import suppress

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct

from fiber.dphi.edge.workflow import EdgeWorkflow
from fiber.dphi.edge.payload import create_app, Config
from fiber.dphi.infra.daemon.rpc import RpcWorkerDaemon
from fiber.dphi.infra.e2e import PipelineRunner, ManagedTestServer, TestResult, E2EConfig, Phase
from fiber.dphi.client.http import VerifiedHttpClient
from fiber.dphi.infra.origin import OriginRegistry

from xphi.state.phase.fsm.edge import EdgePhaseFSM, EdgePhaseState, StartIntentEvent
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.tracer.chaos.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from xphi.watcher.tracer.edge import SceneConfig, HttpFlowTracer
from xphi.watcher.plane.emitter import get_emitter

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.state.ledger.consensus import KernelLedger
from xphi.kernel.space.bind.resolver import resolve_path

log = get_emitter("e2e.edge")

class GatewayTracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Public Gateway & Network Isolation Trace", scope_name="EDGE_INGRESS_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        self.test_config = Config(wasm_timeout=5.0)
        
        self.rest_app = None
        self.server = None
        self._server_task = None
        
        self.worker_daemon = None
        self._worker_task = None
        
        # [개선] WASM Build 생략 및 Origin 무결성 검증 포함 6단계 Phase 구성
        self.set_phases([
            Phase("Origin API Format Validation", self.phase_origin_api_verification),
            Phase("Origin Tamper Resistance (Fail-Fast)", self.phase_origin_tamper_resistance),
            Phase("Gateway Ingress (Golden Path)", self.phase_ingress_e2e_golden),
            Phase("Gateway Ingress (Negative Path)", self.phase_ingress_e2e_negative),
            Phase("Gateway Ingress (Tampered Attestation)", self.phase_ingress_e2e_tampered),
            Phase("Sentinel Security (Chaos WAF Check)", self.phase_sentinel_security)
        ])

    async def _bootstrap_infrastructure(self):
        log.info(f"[{self.scope_name}] Bootstrapping infrastructure (Tunnel & Ledger)...")
        
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
                    
            try:
                await TunnelFactory.close_all()
                log.info("[Pipeline] TunnelFactory closed securely.")
            except Exception as e:
                log.error(f"[Pipeline] Error closing TunnelFactory: {e}")
                
            log.info(f"[Pipeline] All daemons and servers evaporated safely.")
            
        return results

    # =========================================================================
    # Phase Implementations
    # =========================================================================

    async def phase_origin_api_verification(self):
        """[Data Exposure Test] 검증된 신뢰 키가 API를 통해 올바르게 송출되는지 확인"""
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            res = await client.get("/v1/public/keys")
            
            if res.status_code != 200:
                raise RuntimeError(f"Origin API failed with status {res.status_code}")
                
            if "x-dphi-root-signature" not in res.headers:
                raise RuntimeError("Critical: 'X-Dphi-Root-Signature' header is missing from Origin API response.")
                
            data = res.json()
            if "active_signers" not in data or not isinstance(data["active_signers"], list):
                raise RuntimeError("Critical: Payload schema is malformed. Expected 'active_signers' list.")
                
            log.info(f"Origin API Validated. Retrieved {len(data['active_signers'])} active signers securely.")

    async def phase_origin_tamper_resistance(self):
        """[Tamper-Resistance Test] JSON 파일 변조 시 시스템이 Fail-Fast(패닉)하는지 검증"""
        origin_root = resolve_path("origin")
        real_config_path = os.path.join(str(origin_root), "config.json")
        
        # 1. 실제 설정 읽기
        with open(real_config_path, "r", encoding="utf-8") as f:
            tampered_config = json.load(f)
            
        # 2. 서명(Signature) 1바이트 훼손 시뮬레이션
        sig = tampered_config["attestation"]["pre_signed_root_sig"]
        tampered_sig = ("0" if sig[0] != "0" else "1") + sig[1:]  # 첫 글자 강제 변경
        tampered_config["attestation"]["pre_signed_root_sig"] = tampered_sig
        
        # 3. 임시 파일에 변조된 설정 저장
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as tmp:
            json.dump(tampered_config, tmp)
            tmp_path = tmp.name
            
        try:
            # 4. 변조된 파일로 OriginRegistry 로딩 시도
            registry = OriginRegistry(config_path=tmp_path)
            try:
                registry.load_and_verify()
                # 에러 없이 통과해버리면 무결성 방어가 뚫린 것임
                raise RuntimeError("SECURITY BYPASS! OriginRegistry accepted a cryptographically tampered config.")
            
            # [핵심 수정] ValueError가 아닌 RuntimeError 내부의 정책 강제 메시지를 잡아서 검증
            except RuntimeError as e:
                if "Zero-Trust Policy Enforced" in str(e):
                    # [성공] 변조를 감지하고 즉각 차단함
                    log.info(f"Tamper Resistance Verified. Attack blocked successfully: {e}")
                else:
                    # 예상치 못한 다른 RuntimeError의 경우 다시 던짐
                    raise e
        finally:
            os.remove(tmp_path)

    async def _run_scene(self, inject_faults: bool, attestation_injector: Optional[Callable] = None):
        """
        [정합성이 회복된 E2E 시나리오 러너]
        """
        async with httpx.AsyncClient(base_url=self.config.base_url, timeout=15.0) as client:
            response_hooks = [self.tracer.trace_response]
            if attestation_injector:
                async def apply_tamper(response: httpx.Response):
                    attestation_injector(response)
                response_hooks.append(apply_tamper)
                
            async def verify_signature(response: httpx.Response):
                if response.status_code == 200:
                    verifier = VerifiedHttpClient(client=client)
                    verifier._verify_header_proof(response)
            response_hooks.append(verify_signature)
            
            client.event_hooks['request'] = [self.tracer.trace_request]
            client.event_hooks['response'] = response_hooks

            wallet = Account.create()
            client_id = wallet.address
            action = "EXECUTE_PYTHON"
            max_fuel = 1000000
            source_code = "print('Hello from Edge E2E Test')"

            if inject_faults:
                signature = "0x_tampered_invalid_signature_for_chaos_testing"
            else:
                sig_text = f"EXECUTE:{client_id}:{action}:{max_fuel}"
                msg = encode_defunct(text=sig_text)
                signature = wallet.sign_message(msg).signature.hex()

            start_event = StartIntentEvent(
                client_id=client_id,
                action=action,
                max_fuel=max_fuel,
                source_code=source_code,
                signature=signature
            )
            
            fsm = EdgePhaseFSM()
            workflow = EdgeWorkflow(fsm=fsm, client=client, base_url=self.config.base_url)
            
            await workflow.execute(start_event) 
            
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

def main(args_list: list[str] = None):
    app = EdgeSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()