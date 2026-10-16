# fiber.dev.e2e.inter.finops
"""
FuelBreaker operates upstream of VCRAdapterProxy to enforce physical stream termination via aclose().
During VCR record, the live stream is intentionally severed upon Kill-Switch activation, persisting only pre-cutoff chunks.
During VCR replay, the engine deterministically reproduces this exact truncated state to preserve strict idempotency.
"""
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

from fiber.dev.trace.vcr.manager import (
    VCRPlaybackConfig, 
    VCRIdentityRule, 
    add_vcr_arguments
)
from fiber.dev.trace.vcr.injector import VCRInjector
from fiber.dev.trace.llm.base import create_e2e_parser, build_e2e_context
import fiber.dev.worker.finops as worker_finops
from fiber.infra.watcher.finops import default_finops_watcher

from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
from xphi.arch.bound.event.next import next_trace_id
from xphi.arch.bound.adapter.pta import PtaAdapter, PtaTransaction, PtaOutput
from xphi.kernel.space.bind.resolver import resolve_path, get_invoker
from xphi.kernel.wasm.broker import DphiBroker
from xphi.state.phase.reactor import PhaseReactor

FIXTURE_ROOT = "fiber-compats/suites/e2e"
CURRENT_INVOKER, _ = get_invoker(Path(__file__))

LONG_FINOPS_PROMPT = (
    "Write a comprehensive, deeply technical 8-section architectural whitepaper on Enterprise AI FinOps, "
    "Real-Time Token Metering, and Deterministic Stream Governance. "
    "Section 1: Distributed Token Ledger Topologies and Atomic Quota Enforcement. "
    "Section 2: Asynchronous Stream Interception and Sub-Millisecond Kill-Switch Mechanics. "
    "Section 3: Cryptographic Mandate Translation (JWT to AP2) and Zero-Trust Identity Binding. "
    "Section 4: Time-Window Chunk Coalescing and Application-Layer VCR Replay Determinism. "
    "Section 5: Multi-Tenant Cost Attribution, Dynamic Tier Fallback, and Budget Circuit Breakers. "
    "Section 6: Edge Caching, Semantic Routing, and Pre-Flight Payload Sanitization. "
    "Section 7: Failure Modes, Network Partition Recovery, and Idempotent Billing Reconciliation. "
    "Section 8: Full Reference Architecture and Production Deployment Checklist. "
    "Provide exhaustive explanations, concrete architectural trade-offs, and detailed step-by-step "
    "walkthroughs for every single section without summarizing or skipping any details."
)

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
            
        if res.status_code != 200:
            raise RuntimeError(f"Expected 200, got {res.status_code}")
            
        result_text = res.json().get("result", {}).get("content", [])[0].get("text", "{}")
        jwt_token = json.loads(result_text).get("jwt")
        
        if not jwt_token:
            raise RuntimeError("Missing JWT in response")
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
        messages = [{"role": "user", "content": LONG_FINOPS_PROMPT}]
        trace_ctx = self._build_trace_context(scenario, messages)
        trace_ctx["metadata"]["kernel_auth"] = {
            "fuel_budget": 1000,
            "tenant_id": self.test_tenant,
            "is_enforced": True
        }
        
        first_chunk_arrived = asyncio.Event()

        # FinOps 백그라운드 캐시 데몬 가동
        default_finops_watcher.start()
        try:
            await default_finops_watcher.wait_until_ready()

            # 비동기로 LLM 코어 장문 스트림 실행
            async def run_llm_stream() -> tuple[int, int]:
                response_stream = await acompletion(
                    model=self.target_model,
                    messages=messages,
                    max_tokens=2048,
                    stream=True,
                    fuel_breaker=FuelBreaker(finops_watcher=default_finops_watcher),
                    billing_observer=BillingObserver(tenant_eco=None, pta_adapter=self.pta_adapter),
                    **trace_ctx
                )
                chunks = 0
                char_len = 0
                async for chunk in response_stream:
                    chunks += 1
                    try:
                        content = chunk.choices[0].delta.content or ""
                        char_len += len(content)
                    except (AttributeError, IndexError, KeyError, TypeError):
                        pass

                    if chunks == 1:
                        first_chunk_arrived.set()

                    # Replay(max speed) 환경에서도 외부 HTTP Kill-Switch 및 Pub/Sub 이벤트가
                    # 스트림 도중에 개입할 수 있도록 청크 간 미세 페이싱(50ms) 부여
                    await asyncio.sleep(0.05)

                return chunks, char_len

            # Task-1: 메인 프로세스에서 장문 LLM 스트림 병렬 실행 시작
            stream_task = asyncio.create_task(run_llm_stream())
            
            # 첫 번째 스트림 청크가 실제로 파이프라인을 통과할 때까지 대기 (최대 15초)
            await asyncio.wait_for(first_chunk_arrived.wait(), timeout=15.0)

            # Task-2: 스트림이 한창 흐르는 도중 FinOps Worker를 통해 외부 킬 스위치 발동
            log.info("  └─ Stream active (chunk #1 verified). Triggering asynchronous Kill-Switch via FinOps Worker...")
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
                if res.status_code != 200:
                    raise RuntimeError("Kill switch trigger failed.")
                
            received_chunks, received_chars = await stream_task
            
            MAX_TOLERABLE_CHUNKS = 8
            if 1 <= received_chunks <= MAX_TOLERABLE_CHUNKS:
                log.info(
                    f"  └─ Hybrid Integration Success: FuelTrap dynamically intercepted and killed "
                    f"the long VCR stream at chunk {received_chunks} ({received_chars} chars)."
                )
            else:
                raise RuntimeError(
                    f"FinOps Intervention FAILED! VCR Stream bypassed the kill switch. "
                    f"Expected early cutoff (1 <= chunks <= {MAX_TOLERABLE_CHUNKS}), "
                    f"but received {received_chunks} chunks ({received_chars} chars)."
                )

        finally:
            default_finops_watcher.stop()
            if original_quota_flag is not None:
                os.environ["FIBER_ENFORCE_QUOTA"] = original_quota_flag
            else:
                os.environ.pop("FIBER_ENFORCE_QUOTA", None)


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