# fiber.dev.e2e.inter.finops
from __future__ import annotations

import os
import sys
import uuid
import json
import asyncio
import time
from pathlib import Path
import httpx

from fiber.infra.e2e.config import Phase, E2EConfig
from fiber.infra.e2e.pipeline import BaseBridgePipeline, log

from fiber.llm.entry import acompletion
from fiber.gateway.llm.channel.fuel import FuelBreaker, BillingObserver
from fiber.gateway.rest.security import TokenTranslator
from fiber.gateway.mcp.connector import MCPServerConnector

from fiber.dev.trace.llm.billing.watcher import default_finops_watcher
from fiber.dev.trace.llm.vcr.manager import (
    VCRPlaybackConfig, 
    VCRIdentityRule, 
    add_vcr_arguments
)
from fiber.dev.trace.llm.vcr.injector import VCRInjector
from fiber.dev.trace.llm.base import create_e2e_parser, build_e2e_context
import fiber.dev.ex.worker.finops as worker_finops

from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
from xphi.arch.bound.event.next import next_trace_id
from xphi.arch.bound.adapter.pta import PtaAdapter, PtaTransaction, PtaOutput
from xphi.kernel.space.bind.resolver import resolve_path, get_invoker
from xphi.kernel.wasm.broker import DphiBroker
from xphi.state.phase.reactor import PhaseReactor

FIXTURE_ROOT = "fiber-compats/suites/e2e"
CURRENT_INVOKER, _ = get_invoker(Path(__file__))

class HybridFinOpsFuelPipeline(BaseBridgePipeline):
    def __init__(self, config: E2EConfig, vcr_config: VCRPlaybackConfig, vcr_manager: "VCRManager", target_model: str):
        super().__init__(
            config=config, 
            name="FinOps Hybrid VCR Integration Suite", 
            scope_name="HYBRID_FINOPS_SUITE"
        )
        self.vcr_config = vcr_config
        self.vcr_manager = vcr_manager
        self.target_model = target_model
        
        self.finops_id = "finops-poc-01"
        self.dpop_client = DPoPClientGenerator(key_size=2048)
        
        self.test_tenant = "accounting_dept"
        self.initial_fuel = 1000
        
        self.broker = DphiBroker(timeout=5.0)
        self.pta_adapter = PtaAdapter(broker=self.broker)
        
        self.set_phases([
            Phase("Phase 1: PTA Infrastructure Setup", self.phase_pta_setup),
            Phase("Phase 2: Track B - FinOps JWT Issuance & Internal AP2 Translation", self.phase_track_b_jwt),
            Phase("Phase 3: Track A - Dynamic Pub/Sub Kill Switch vs VCR Stream", self.phase_track_a_hybrid_kill),
        ])

    async def teardown_custom(self):
        await self.broker.close()

    async def setup_workers(self):
        finops_cmd = f"{sys.executable} -m {worker_finops.__name__}"
        self.connectors.append(
            MCPServerConnector(target_id=self.finops_id, execution_target=finops_cmd, mode="multiplex")
        )
        log.info(f"Registered FinOps System Worker [{self.finops_id}]")

    def _build_trace_context(self, scenario_name: str, messages: list) -> dict:
        seed = VCRIdentityRule.generate_seed(scenario_name=scenario_name, messages=messages, invoker=CURRENT_INVOKER)
        trace_id = next_trace_id(seed) if self.vcr_config.mode in ("record", "replay") else next_trace_id()
        return {
            "trace_id": trace_id,
            "metadata": {"vcr_scenario": scenario_name, "vcr_invoker": CURRENT_INVOKER}
        }

    async def _wait_for_balance(self, target_balance: int, timeout: float = 10.0) -> int:
        start_time = time.time()
        current_balance = 0
        while time.time() - start_time < timeout:
            current_balance = await self.pta_adapter.get_balance(self.test_tenant, "fuel")
            if current_balance == target_balance:
                return current_balance
            await asyncio.sleep(0.5)
        return current_balance

    # Test Phases
    async def phase_pta_setup(self):
        log.info(f"  └─ Bootstrapping PTA Ledger for '{self.test_tenant}' with {self.initial_fuel} Fuel.")
        produce_tx = PtaTransaction(
            inputs=[], 
            outputs=[PtaOutput(amount=self.initial_fuel, owner=self.test_tenant, asset_type="fuel")],
            metadata={"action": "test_produce"}
        )
        await self.pta_adapter.execute_transaction(produce_tx)
        
        balance = await self._wait_for_balance(target_balance=self.initial_fuel)
        if balance < self.initial_fuel:
            raise RuntimeError(f"Failed to produce initial fuel. Expected {self.initial_fuel}, got {balance}.")
        log.info(f"  └─ Produced Balance for '{self.test_tenant}': {balance} Fuel")

    async def phase_track_b_jwt(self):
        # 외부 FinOps Worker에게 정적 예산이 담긴 JWT 발급 요청 (MCP Tools Call)
        payload = {
            "jsonrpc": "2.0", "id": 100, "method": "tools/call", 
            "params": {"name": "issue_jwt", "arguments": {"tenant_id": self.test_tenant, "budget_usd": 50.0}}
        }
        
        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.finops_id}/invoke"
        dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": nonce, "DPoP": dpop_proof}
        
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(url, json=payload, headers=headers)
            
        if res.status_code != 200: raise RuntimeError(f"Expected 200, got {res.status_code}")
            
        result_text = res.json().get("result", {}).get("content", [])[0].get("text", "{}")
        jwt_token = json.loads(result_text).get("jwt")
        
        if not jwt_token: raise RuntimeError("Missing JWT in response")
        log.info(f"  └─ 1. Successfully received PoC JWT from FinOps Worker: {jwt_token[:30]}...")

        # 발급받은 JWT를 Core의 TokenTranslator 모듈을 통해 내부 영수증(AP2 Mandate)으로 변환 검증
        translated_receipt = await TokenTranslator.exchange(jwt_token)
        if not translated_receipt or "AP2-v1.0" not in translated_receipt:
            raise RuntimeError("JWT to AP2 Translation failed.")
            
        log.info("  └─ 2. FinOps JWT successfully translated to internal AP2 Mandate securely.")

    async def phase_track_a_hybrid_kill(self):
        original_quota_flag = os.environ.get("FIBER_ENFORCE_QUOTA")
        os.environ["FIBER_ENFORCE_QUOTA"] = "true"
        
        scenario = "fuel_phase_4_finops"
        messages = [{"role": "user", "content": "Write a long story about FinOps."}]
        trace_ctx = self._build_trace_context(scenario, messages)
        trace_ctx["metadata"]["kernel_auth"] = {"fuel_budget": 1000, "tenant_id": self.test_tenant, "is_enforced": True}
        
        # FinOps 백그라운드 캐시 데몬 가동
        default_finops_watcher.start()
        try:
            await default_finops_watcher.wait_until_ready()

            # 비동기로 LLM 코어 스트림 실행 (VCR Replay 모드)
            async def run_llm_stream():
                response_stream = await acompletion(
                    model=self.target_model,  # 동적으로 결정된 모델 파라미터 적용
                    messages=messages,
                    stream=True,
                    fuel_breaker=FuelBreaker(finops_watcher=default_finops_watcher),
                    billing_observer=BillingObserver(tenant_eco=None, pta_adapter=self.pta_adapter),
                    **trace_ctx
                )
                chunks = 0
                async for chunk in response_stream:
                    chunks += 1
                return chunks

            # Task-1: 메인 프로세스에서 LLM 스트림 병렬 실행 시작
            stream_task = asyncio.create_task(run_llm_stream())
            
            # VCR이 응답을 준비할 수 있도록 약간의 컨텍스트 스위칭 타임 허용
            await asyncio.sleep(0.1)

            # Task-2: 서브프로세스에 떠있는 FinOps Worker를 통해 외부 킬 스위치 발동 (Gateway API)
            log.info(f"  └─ Triggering asynchronous Kill-Switch via FinOps Worker...")
            payload = {
                "jsonrpc": "2.0", "id": 200, "method": "tools/call", 
                "params": {"name": "trigger_kill", "arguments": {"tenant_id": self.test_tenant}}
            }
            nonce = uuid.uuid4().hex
            url = f"{self.local_url}/v1/mcp-gateway/{self.finops_id}/invoke"
            dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
            headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": nonce, "DPoP": dpop_proof}
            
            async with httpx.AsyncClient(base_url=self.local_url) as client:
                res = await client.post(url, json=payload, headers=headers)
                if res.status_code != 200: raise RuntimeError("Kill switch trigger failed.")
                
            received_chunks = await stream_task
            
            MAX_TOLERABLE_CHUNKS = 5
            if received_chunks <= MAX_TOLERABLE_CHUNKS:
                log.info(f"  └─ Hybrid Integration Success: FuelTrap dynamically intercepted and killed the VCR stream at chunk {received_chunks}.")
            else:
                raise RuntimeError(
                    f"FinOps Intervention FAILED! VCR Stream bypassed the kill switch. "
                    f"Expected early cutoff (<= {MAX_TOLERABLE_CHUNKS}), but received {received_chunks} chunks."
                )

        finally:
            default_finops_watcher.stop()
            if original_quota_flag is not None: os.environ["FIBER_ENFORCE_QUOTA"] = original_quota_flag
            else: os.environ.pop("FIBER_ENFORCE_QUOTA", None)

class HybridSuiteRunner:
    def __init__(self, vcr_config: VCRPlaybackConfig, vcr_manager: "VCRManager", target_model: str):
        self.vcr_config = vcr_config
        self.vcr_manager = vcr_manager
        self.target_model = target_model

    async def execute(self):
        net_config = E2EConfig(host="127.0.0.1", port=8355, protocol="http")
        results = await HybridFinOpsFuelPipeline(
            config=net_config, 
            vcr_config=self.vcr_config, 
            vcr_manager=self.vcr_manager,
            target_model=self.target_model
        ).run_pipeline()
        
        if all(r.passed for r in results):
            log.info("FINOPS SYSTEM VALIDATED SUCCESSFULLY.")
        else:
            log.critical("FINOPS FAILURE DETECTED.")

def main(args_list: list[str] = None):
    parser = create_e2e_parser("FinOps Integration Test Runner")
    add_vcr_arguments(parser)
    parsed_args, _ = parser.parse_known_args(args_list)
    _, run_context = build_e2e_context(parsed_args)
    resolved_model = run_context["target_model"]
    
    vcr_config = VCRPlaybackConfig.from_args(parsed_args)
    manager = VCRInjector.apply(config=vcr_config, fixture_dir=FIXTURE_ROOT)
    app = HybridSuiteRunner(
        vcr_config=vcr_config, 
        vcr_manager=manager, 
        target_model=resolved_model
    )
    
    try:
        PhaseReactor.ignite(main_coro_func=app.execute)
    finally:
        VCRInjector.remove()

if __name__ == "__main__":
    main()