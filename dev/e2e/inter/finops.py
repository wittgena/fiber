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
from typing import Optional
import httpx

from fiber.dev.base.workflow.config import Phase, E2EConfig
from fiber.dev.base.workflow.pipeline import MockPipeline, log

from fiber.llm.entry import acompletion
from fiber.llm.pipeline.channel.fuel import FuelBreaker, BillingObserver
from fiber.gateway.security import TokenTranslator
from fiber.gateway.contract.token.gen import FinOpsTokenGenerator
from fiber.infra.mcp.connector import MCPServerConnector

from fiber.infra.vcr.manager import (
    VCRPlaybackConfig, 
    VCRIdentityRule, 
    add_vcr_arguments
)
from fiber.infra.vcr.injector import VCRInjector
from fiber.dev.base.workflow.builder import create_e2e_parser, build_e2e_context
import fiber.infra.mcp.worker.finops as worker_finops
from fiber.infra.ops.finops.watcher import default_finops_watcher, default_tenant_eco

from xphi.arch.model.auth import NodeAuthPayload
from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
from xphi.arch.contract.event.next import next_trace_id
from xphi.arch.bound.adapter.pta import PtaAdapter, PtaTransaction, PtaOutput
from xphi.node.space.bind.resolver import resolve_path
from xphi.node.runtime.wasm.broker import DphiBroker
from xphi.state.phase.reactor import PhaseReactor

FIXTURE_ROOT = "fiber-compats/suites/e2e"
CURRENT_INVOKER = "fiber.dev.e2e.inter.finops"

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

class HybridFinOpsFuelPipeline(MockPipeline):
    def __init__(self, config: E2EConfig, vcr_config: VCRPlaybackConfig, vcr_manager: "VCRManager", target_model: str):
        super().__init__(
            config=config, 
            name="FinOps Hybrid VCR Integration Suite", 
            scope_name="HYBRID_FINOPS_SUITE"
        )
        self.vcr_config = vcr_config
        self.vcr_manager = vcr_manager
        self.target_model = target_model
        
        self.finops_tenant_id = "finops-local-01"
        self.dpop_client = DPoPClientGenerator(key_size=2048)
        self.token_gen = FinOpsTokenGenerator()
        
        self.test_tenant = "accounting_dept"
        self.initial_fuel = 1000
        self.issued_jwt: Optional[str] = None
        
        self.broker = DphiBroker(timeout=5.0)
        self.pta_adapter = PtaAdapter(broker=self.broker)
        
        self.set_phases([
            Phase("Phase 1: PTA Infrastructure Setup", self.phase_pta_setup),
            Phase("Phase 2: Track B - FinOps JWT Issuance, Idempotency & Lifecycle Translation", self.phase_track_b_jwt),
            Phase("Phase 3: Track A - Dynamic Pub/Sub Kill Switch vs VCR Stream", self.phase_track_a_hybrid_kill),
        ])

    async def teardown_custom(self):
        await self.broker.close()

    async def setup_workers(self):
        finops_cmd = f"{sys.executable} -m {worker_finops.__name__}"
        self.connectors.append(
            MCPServerConnector(target_id=self.finops_tenant_id, execution_target=finops_cmd, mode="multiplex")
        )
        log.info(f"Registered FinOps System Worker [{self.finops_tenant_id}]")

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
        payload = {
            "jsonrpc": "2.0", "id": 100, "method": "tools/call", 
            "params": {"name": "issue_jwt", "arguments": {"tenant_id": self.test_tenant, "budget_usd": 50.0}}
        }
        
        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.finops_tenant_id}/invoke"
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
        self.issued_jwt = jwt_token
        log.info(f"  └─ 1. Successfully received PoC JWT from FinOps Worker: {jwt_token[:30]}...")

        # 2. 발급받은 JWT를 TokenTranslator를 통해 AP2 Mandate로 변환 및 Watcher 예산 동기화 검증
        translated_receipt = await TokenTranslator.exchange(jwt_token)
        if not translated_receipt or "AP2-v1.0" not in translated_receipt:
            raise RuntimeError("JWT to AP2 Translation failed.")
            
        synced_fuel = default_finops_watcher.local_budget_cache.get(self.test_tenant, 0.0)
        if synced_fuel != 50000.0:
            raise RuntimeError(f"Watcher budget sync mismatch: expected 50000.0 Fuel, got {synced_fuel}")
        log.info(f"  └─ 2. FinOps JWT translated to AP2 Mandate & synced budget ({synced_fuel} Fuel).")

        # 3. [중복 생성 방지 검증] 동일 JWT(동일 JTI)로 TOP_UP이나 재호출 시 예산이 중복 합산되지 않는지 확인
        await TokenTranslator.exchange(jwt_token)
        if default_finops_watcher.local_budget_cache.get(self.test_tenant) != 50000.0:
            raise RuntimeError("JTI Idempotency failed: duplicate token exchange mutated budget.")
        log.info("  └─ 3. JTI Idempotency verified (duplicate token exchange safely ignored).")

        # 4. [변경(TOP_UP) 검증] 상위 버전(policy_ver=2) TOP_UP 토큰 주입 시 정상 합산되는지 확인
        topup_jwt = self.token_gen.generate_token(
            tenant_id=self.test_tenant, budget_usd=10.0, op="TOP_UP", policy_ver=2
        )
        await TokenTranslator.exchange(topup_jwt)
        updated_fuel = default_finops_watcher.local_budget_cache.get(self.test_tenant, 0.0)
        if updated_fuel != 60000.0:
            raise RuntimeError(f"TOP_UP transition failed: expected 60000.0 Fuel, got {updated_fuel}")
        log.info(f"  └─ 4. Policy version upgrade (v2 TOP_UP) verified: Budget -> {updated_fuel} Fuel.")

        # 5. [구버전 차단 검증] v2가 적용된 상태에서 구버전(v1) 토큰 재사용 시 거부(None)되는지 확인
        stale_res = await TokenTranslator.exchange(jwt_token)
        if stale_res is not None:
            raise RuntimeError("Stale policy version (v1) was not rejected after v2 upgrade.")
        log.info("  └─ 5. Stale token version (v1) replay strictly rejected.")

    async def phase_track_a_hybrid_kill(self):
        original_quota_flag = os.environ.get("FIBER_ENFORCE_QUOTA")
        os.environ["FIBER_ENFORCE_QUOTA"] = "true"
        
        scenario = "fuel_phase_4_finops"
        messages = [{"role": "user", "content": LONG_FINOPS_PROMPT}]
        trace_ctx = self._build_trace_context(scenario, messages)
        
        # Phase 2에서 동기화된 예산(없을 경우 initial_fuel)을 동적으로 연결
        active_budget = int(default_finops_watcher.local_budget_cache.get(self.test_tenant, self.initial_fuel))
        trace_ctx["metadata"]["node_auth"] = NodeAuthPayload(
            fuel_budget=active_budget,
            tenant_id=self.test_tenant,
            is_enforced=True
        )
        
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
                    billing_observer=BillingObserver(tenant_eco=default_tenant_eco, pta_adapter=self.pta_adapter),
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
            url = f"{self.local_url}/v1/mcp-gateway/{self.finops_tenant_id}/invoke"
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