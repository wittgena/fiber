# fiber.dev.e2e.edge.client
import asyncio
import random
import json
import os
import tempfile
from typing import Callable, List, Optional

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct

from fiber.infra.e2e.edge import EdgeWorkflow
from fiber.infra.e2e.config import PipelineRunner, TestResult, E2EConfig, Phase
from fiber.phase.contract.origin import OriginRegistry

from xphi.arch.bound.client.http import VerifiedHttpClient
from xphi.arch.dev.transport.sentinel import ChaosPayloadLibrary, RpcChaosInjector
from xphi.arch.dev.tracer.transport import HttpFlowTracer
from xphi.kernel.node.fsm.edge import EdgePhaseFSM, EdgePhaseState, StartIntentEvent
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter
from xphi.state.phase.reactor import PhaseReactor

log = get_emitter("e2e.edge.client")

class EdgeTracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Public Edge & Network Isolation Trace", scope_name="EDGE_INGRESS_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        self.local_url = f"{self.config.protocol}://{self.config.host}:{self.config.port}"
        
        self.set_phases([
            Phase("Origin API Format Validation", self.phase_origin_api_verification),
            Phase("Origin Tamper Resistance (Fail-Fast)", self.phase_origin_tamper_resistance),
            Phase("Gateway Ingress (Golden Path)", self.phase_ingress_e2e_golden),
            Phase("Gateway Ingress (Negative Path)", self.phase_ingress_e2e_negative),
            Phase("Gateway Ingress (Tampered Attestation)", self.phase_ingress_e2e_tampered),
            Phase("Sentinel Security (Chaos WAF Check)", self.phase_sentinel_security)
        ])

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Pure Client Pipeline: {self.name} ===")
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
            log.info(f"\n[Pipeline] Phase execution sequence completed.")
            
        return results

    async def phase_origin_api_verification(self):
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            res = await client.get("/v1/public/keys")
            if res.status_code != 200:
                raise RuntimeError(f"Origin API failed with status {res.status_code}")
            if "x-dphi-root-signature" not in res.headers:
                raise RuntimeError("Critical: 'X-Dphi-Root-Signature' header is missing.")
            data = res.json()
            log.info(f"Origin API Validated. Retrieved {len(data['active_signers'])} signers.")

    async def phase_origin_tamper_resistance(self):
        origin_root = resolve_path("origin")
        real_config_path = os.path.join(str(origin_root), "config.json")
        with open(real_config_path, "r", encoding="utf-8") as f:
            tampered_config = json.load(f)
            
        sig = tampered_config["attestation"]["pre_signed_root_sig"]
        tampered_config["attestation"]["pre_signed_root_sig"] = ("0" if sig[0] != "0" else "1") + sig[1:]
        
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as tmp:
            json.dump(tampered_config, tmp)
            tmp_path = tmp.name
            
        try:
            registry = OriginRegistry(config_path=tmp_path)
            try:
                registry.load_and_verify()
                raise RuntimeError("SECURITY BYPASS! Tampered config accepted.")
            except RuntimeError as e:
                if "Zero-Trust Policy Enforced" in str(e):
                    log.info("Tamper Resistance Verified.")
                else:
                    raise e
        finally:
            os.remove(tmp_path)

    async def _run_scene(self, inject_faults: bool, attestation_injector: Optional[Callable] = None):
        async with httpx.AsyncClient(base_url=self.local_url, timeout=15.0) as client:
            response_hooks = [self.tracer.trace_response]
            if attestation_injector:
                async def apply_tamper(response: httpx.Response):
                    attestation_injector(response)
                response_hooks.append(apply_tamper)
                
            async def verify_signature(response: httpx.Response):
                if response.status_code == 200:
                    VerifiedHttpClient(client=client)._verify_header_proof(response)
            response_hooks.append(verify_signature)
            
            client.event_hooks['request'] = [self.tracer.trace_request]
            client.event_hooks['response'] = response_hooks

            # 일회성 지갑(클라이언트 신원) 생성
            wallet = Account.create()
            
            # [추가된 로그]: 클라이언트 지갑 주소 명시적 출력
            log.info(f"🔑 [Client Identity] Generated Ephemeral Wallet: {wallet.address}")

            if inject_faults:
                signature = "0x_tampered_invalid_signature"
                # [추가된 로그]: 변조된 서명 기록
                log.info(f"✍️ [Client Signature] Intentional fault injected. Signature: {signature}")
            else:
                msg = encode_defunct(text=f"EXECUTE:{wallet.address}:EXECUTE_PYTHON:1000000")
                signature = wallet.sign_message(msg).signature.hex()
                # [추가된 로그]: 정상적으로 생성된 서명의 앞부분 기록
                log.info(f"✍️ [Client Signature] Payload signed. Signature: {signature[:16]}...")

            start_event = StartIntentEvent(
                client_id=wallet.address, action="EXECUTE_PYTHON", max_fuel=1000000,
                source_code="print('Hello CI')", signature=signature
            )
            
            fsm = EdgePhaseFSM()
            workflow = EdgeWorkflow(fsm=fsm, client=client, base_url=self.local_url)
            await workflow.execute(start_event) 
            
            # ---------------------------------------------------------
            # 결과 Assertion 로직
            # ---------------------------------------------------------
            # 1. 응답 변조 테스트를 수행했는데 방어 실패(통과) 시 에러
            if attestation_injector and fsm.state != EdgePhaseState.FAILED:
                raise RuntimeError("Attestation Bypass!")
            
            # 2. Golden Path 체크: 요청 조작도 없고 응답 조작도 없을 때만 확인
            if not inject_faults and not attestation_injector and fsm.state != EdgePhaseState.COMPLETED:
                raise RuntimeError(f"Golden Path Failed! Final state: {fsm.state.name}")
            
            # 3. 요청 변조 테스트 시, 거절되지 않고 통과되면 에러
            if inject_faults and fsm.state != EdgePhaseState.FAILED:
                raise RuntimeError(f"Negative Path Failed! Final state: {fsm.state.name}")

    async def phase_ingress_e2e_golden(self): await self._run_scene(False)
    async def phase_ingress_e2e_negative(self): await self._run_scene(True)
    async def phase_ingress_e2e_tampered(self):
        tamper_func = getattr(RpcChaosInjector, 'corrupt_attestation_header', None)
        if tamper_func: await self._run_scene(False, tamper_func)

    async def phase_sentinel_security(self):
        vectors = ChaosPayloadLibrary.get_all_vectors()
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            for name, rule in vectors:
                payload = random.choice(rule)() if isinstance(rule, list) else rule()
                res = await client.post("/v1/public/telemetry/logs", content=payload)
                if not (400 <= res.status_code < 500):
                    raise RuntimeError(f"Gateway Breach! '{name}' bypassed defenses.")

class EdgeSuiteClientRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI EDGE MASTER SUITE] Executing Pure Client Tests against CI Kernel")
        self.log.info("="*80)
        
        port = int(os.getenv("GATEWAY_PORT", 8000))
        net_config = E2EConfig(host="127.0.0.1", port=port, protocol="http")
        self.results.extend(await EdgeTracerPipeline(config=net_config).run_pipeline())
        
        self.log.info("\n" + "="*80)
        all_passed = all(r.passed for r in self.results)
        for idx, res in enumerate(self.results, 1):
            icon = "✅" if res.passed else "❌"
            self.log.info(f"{icon} {idx:02d}. [{res.target}] {res.scenario.ljust(45)} | Result: {'PASSED' if res.passed else 'FAILED'}")
        
        if not all_passed:
            self.log.critical("💥 EDGE BOUNDARY COMPROMISED.")
            exit(1)

def main(args_list: list[str] = None):
    app = EdgeSuiteClientRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()