# fiber.dev.e2e.llm.fuel
from __future__ import annotations

import os
import time
import uuid
import json
import asyncio
from typing import Optional

from fiber.gateway.llm.pipeline import PipelineSlot
from fiber.llm.entry import acompletion
from fiber.llm.response import ModelResponse

from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.arch.contract.workflow import StopMessage, WorkflowMessage, step
from xphi.kernel.wasm.broker import DphiBroker
from xphi.arch.bound.adapter.pta import PtaAdapter, PtaTransaction, PtaOutput
from xphi.kernel.space.tunnel.factory import TunnelFactory

from fiber.gateway.llm.channel.fuel import BillingObserver, FuelBreaker
from fiber.dev.trace.llm.billing.watcher import default_finops_watcher
from fiber.dev.trace.llm.base import (
    E2EBaseWorkflow, 
    E2EBaseApplication, 
    create_e2e_parser, 
    build_e2e_context, 
    ReportMsg
)
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.llm.fuel")

# =====================================================================
# External Boundary Mocks
# =====================================================================

class MockBillingTransport(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE

    async def write(self, ctx: ChannelContext, msg: dict):
        log.info("🎯 [MockBillingTransport] Intercepted outbound request. Simulating 42 tokens usage.")
        mock_response = ModelResponse(
            id=f"chatcmpl-mock-{uuid.uuid4().hex[:8]}",
            model=msg.get("model", "mock-billing-model"),
            choices=[{
                "index": 0, 
                "message": {"role": "assistant", "content": "This is a billed mock response."}, 
                "finish_reason": "stop"
            }],
            usage={"prompt_tokens": 10, "completion_tokens": 32, "total_tokens": 42}
        )
        await ctx.fire_channel_read(mock_response)


class MockStreamTransport(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE

    async def write(self, ctx: ChannelContext, msg: dict):
        log.info("🎯 [MockStreamTransport] Intercepted outbound stream request. Generating 10 chunks.")
        async def _stream_mock():
            for i in range(10):
                await asyncio.sleep(0.05)
                yield {"choices": [{"delta": {"content": f"chunk_{i} "}}]}
        
        await ctx.fire_channel_read(_stream_mock())


# =====================================================================
# Workflow Messages 
# =====================================================================
class SetupProduceMsg(WorkflowMessage): pass
class ObserverConsumeMsg(WorkflowMessage): pass
class StrictEnforceMsg(WorkflowMessage): pass
class FinOpsInterventionMsg(WorkflowMessage): pass


# =====================================================================
# Workflow Engine (E2EBaseWorkflow 상속으로 통일)
# =====================================================================

class LlmFuelWorkflow(E2EBaseWorkflow):
    def __init__(self, name: str, run_context: dict, **kwargs):
        super().__init__(name=name, run_context=run_context, **kwargs)
        # self.target_model은 E2EBaseWorkflow에서 파싱하여 주입해 줍니다.
        
        self.test_tenant = "accounting_dept"
        self.initial_fuel = 1000
        
        self.broker = DphiBroker(timeout=5.0)
        self.pta_adapter = PtaAdapter(broker=self.broker)

    async def _wait_for_balance(self, target_balance: int, timeout: float = 10.0) -> int:
        start_time = time.time()
        current_balance = 0
        while time.time() - start_time < timeout:
            current_balance = await self.pta_adapter.get_balance(self.test_tenant, "fuel")
            if current_balance == target_balance:
                return current_balance
            await asyncio.sleep(0.5)
        return current_balance

    async def execute(self) -> None:
        self.log.info(f"[{self.name}] 🚀 Igniting Fuel, Billing & FinOps Isolation Test Suite (Model: {self.target_model})")
        self.post_message(SetupProduceMsg())
        await self.run()

    @step
    async def phase_setup_produce(self, msg: SetupProduceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 1] SETUP: PTA Infrastructure & Initial Fuel Production")
        t0 = time.perf_counter()
        is_success = False
        
        try:
            produce_tx = PtaTransaction(
                inputs=[], 
                outputs=[PtaOutput(amount=self.initial_fuel, owner=self.test_tenant, asset_type="fuel")],
                metadata={"action": "test_produce"}
            )
            await self.pta_adapter.execute_transaction(produce_tx)
            
            balance = await self._wait_for_balance(target_balance=self.initial_fuel)
            self.log.info(f"[{self.name}] 💰 Produced Balance for '{self.test_tenant}': {balance} Fuel")
            
            if balance >= self.initial_fuel:
                is_success = True
            else:
                self.log.error(f"Failed to produce initial fuel. Expected {self.initial_fuel}, got {balance}.")
                
        except Exception as e:
            self.log.error(f"Phase 1 Error: {e}", exc_info=True)
            
        self.record_result(1, "PTA Setup & Fuel Production", is_success, t0)
        if not is_success: return StopMessage(result=False)
        return ObserverConsumeMsg()

    @step
    async def phase_observer_consume(self, msg: ObserverConsumeMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 2] EXECUTION: Auto-Consumption in Legacy Fallback Mode")
        t0 = time.perf_counter()
        is_success = False
        usage_tokens = "N/A"
        original_quota_flag = os.environ.get("FIBER_ENFORCE_QUOTA")
        
        try:
            os.environ["FIBER_ENFORCE_QUOTA"] = "false"
            balance_before = await self.pta_adapter.get_balance(self.test_tenant, "fuel")
            response = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Test consumption"}],
                metadata={"kernel_auth": {"fuel_budget": 1000, "tenant_id": self.test_tenant, "is_enforced": True}},
                pipeline_hooks={
                    PipelineSlot.PRE_TRANSLATE: [MockBillingTransport()]
                },
                fuel_breaker=FuelBreaker(),
                billing_observer=BillingObserver(tenant_eco=None, pta_adapter=self.pta_adapter)
            )
            
            used_tokens = getattr(response.usage, "total_tokens", 0) if hasattr(response, "usage") else 0
            usage_tokens = str(used_tokens)
            expected_balance = balance_before - used_tokens
            
            balance_after = await self._wait_for_balance(target_balance=expected_balance)
            self.log.info(f"[{self.name}] 📉 Balance Check: {balance_before} - {used_tokens}(Consumed) = {balance_after} Fuel")
            
            if balance_after == expected_balance:
                is_success = True
            else:
                self.log.error(f"Consumption Mismatch! Expected {expected_balance}, got {balance_after}.")
                
        except Exception as e:
            self.log.error(f"Phase 2 Error: {e}", exc_info=True)
        finally:
            if original_quota_flag is not None: os.environ["FIBER_ENFORCE_QUOTA"] = original_quota_flag
            else: os.environ.pop("FIBER_ENFORCE_QUOTA", None)
            
        self.record_result(2, "PTA Ledger Auto-Consumption", is_success, t0, usage_tokens=usage_tokens)
        if not is_success: return StopMessage(result=False)
        return StrictEnforceMsg()

    @step
    async def phase_strict_enforcement(self, msg: StrictEnforceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 3] SECURITY: Strict Quota Enforcement Shield")
        t0 = time.perf_counter()
        is_success = False
        original_quota_flag = os.environ.get("FIBER_ENFORCE_QUOTA")
        
        try:
            os.environ["FIBER_ENFORCE_QUOTA"] = "true"
            try:
                await acompletion(
                    model=self.target_model,
                    messages=[{"role": "user", "content": "Hack the planet"}],
                    metadata={"kernel_auth": {"fuel_budget": 999999, "tenant_id": "hacker"}},
                    fuel_breaker=FuelBreaker(),
                    billing_observer=BillingObserver(tenant_eco=None, pta_adapter=self.pta_adapter)
                )
                self.log.error("FuelBreaker failed to block unauthorized payload!")
            except PermissionError as pe:
                if "Strict Quota Enforcement is active" in str(pe):
                    is_success = True
                    self.log.info(f"[{self.name}] ✅ Blocked successfully: {pe}")
                else:
                    self.log.error(f"Blocked with unexpected error: {pe}")
        except Exception as e:
            self.log.error(f"Phase 3 Error: {e}")
        finally:
            if original_quota_flag is not None: os.environ["FIBER_ENFORCE_QUOTA"] = original_quota_flag
            else: os.environ.pop("FIBER_ENFORCE_QUOTA", None)
            
        self.record_result(3, "Quota Enforcement Firewall", is_success, t0)
        if not is_success: return StopMessage(result=False)
        return FinOpsInterventionMsg()

    @step
    async def phase_finops_intervention(self, msg: FinOpsInterventionMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 4] FINOPS: Real-Time Pub/Sub Dynamic Intervention")
        t0 = time.perf_counter()
        is_success = False
        
        default_finops_watcher.start()
        try:
            tunnel = await TunnelFactory.get_default()
            
            # 💡 [핵심 개선] 백그라운드 Watcher가 Redis 채널 구독(Subscribe)을 완전히 마칠 때까지 대기
            await default_finops_watcher.wait_until_ready()
            
            self.log.info(f"[{self.name}] 📡 Emitting Pub/Sub 'update_budget' to Kill tenant '{self.test_tenant}'")
            kill_payload = {"type": "update_budget", "tenant_id": self.test_tenant, "fuel_budget": 0}
            await tunnel.publish("finops:control_plane", json.dumps(kill_payload))
            
            # 발송된 메시지가 Watcher 캐시에 안전하게 업데이트될 아주 찰나의 시간 확보
            await asyncio.sleep(0.1)
            
            response_stream = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Generate story"}],
                stream=True,
                metadata={"kernel_auth": {"fuel_budget": 1000, "tenant_id": self.test_tenant, "is_enforced": True}},
                pipeline_hooks={PipelineSlot.PRE_TRANSLATE: [MockStreamTransport()]},
                fuel_breaker=FuelBreaker(finops_watcher=default_finops_watcher),
                billing_observer=BillingObserver(tenant_eco=None, pta_adapter=self.pta_adapter)
            )
            
            received_chunks = 0
            async for chunk in response_stream:
                received_chunks += 1
                
            if received_chunks == 0:
                is_success = True
                self.log.info(f"[{self.name}] ✅ FuelTrap dynamically intercepted and killed the stream.")
            else:
                self.log.error(f"FinOps Intervention FAILED! Received {received_chunks} chunks.")

        except Exception as e:
            self.log.error(f"Phase 4 Error: {e}", exc_info=True)
        finally:
            default_finops_watcher.stop()
            
        self.record_result(4, "Dynamic FinOps Pub/Sub Intervention", is_success, t0)
        
        # Base 클래스의 통합 리포트 생성기로 바톤 터치
        return ReportMsg()

    @step
    async def generate_report(self, msg: ReportMsg) -> WorkflowMessage:
        # 종료 전 PTA Broker 자원 회수
        await self.broker.close()
        # 부모 클래스의 통일된 대시보드 리포팅 로직 실행
        return await super().generate_report(msg)


# =====================================================================
# Unified Application & Main Entrypoint
# =====================================================================

def main(args: list[str] = None):
    # Base의 공용 파서 활용
    parser = create_e2e_parser("LLM Fuel & FinOps Isolation Test Runner")
    parsed_args, _ = parser.parse_known_args(args)
    scope_kwargs, run_context = build_e2e_context(parsed_args)
    
    # Base의 공용 어플리케이션 활용
    app = E2EBaseApplication(
        workflow_cls=LlmFuelWorkflow,
        workflow_name="FuelSuiteApp",
        scope_kwargs=scope_kwargs,
        run_context=run_context
    )
    app.execute()

if __name__ == "__main__":
    main()