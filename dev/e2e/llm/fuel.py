# fiber.dev.e2e.llm.fuel
from __future__ import annotations

import os
import time
import json
import asyncio
from pathlib import Path
from typing import Optional

from fiber.llm.entry import acompletion
from fiber.gateway.llm.pipeline.bootstrap import PipelineSlot
from fiber.gateway.llm.pipeline.channel.fuel import BillingObserver, FuelBreaker
from fiber.dev.trace.vcr.manager import (
    VCRPlaybackConfig, 
    VCRIdentityRule, 
    add_vcr_arguments
)
from fiber.dev.trace.vcr.injector import VCRInjector
from fiber.dev.trace.llm.base import (
    E2EBaseWorkflow, 
    E2EBaseApplication, 
    create_e2e_parser, 
    build_e2e_context, 
    ReportMsg
)

from fiber.infra.watcher.finops import default_finops_watcher

from xphi.arch.model.auth import NodeAuthPayload
from xphi.state.topos.workflow import StopMessage, WorkflowMessage, step
from xphi.arch.bound.adapter.pta import PtaAdapter, PtaTransaction, PtaOutput
from xphi.arch.bound.event.next import next_trace_id

from xphi.node.wasm.broker import DphiBroker
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.node.space.bind.resolver import resolve_path, get_invoker
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.llm.fuel")
FIXTURE_ROOT = "fiber-compats/suites/e2e" 
CURRENT_INVOKER, _ = get_invoker(Path(__file__))

class SetupProduceMsg(WorkflowMessage): pass
class ObserverConsumeMsg(WorkflowMessage): pass
class StrictEnforceMsg(WorkflowMessage): pass
class FinOpsInterventionMsg(WorkflowMessage): pass

class LlmFuelWorkflow(E2EBaseWorkflow):
    def __init__(self, name: str, run_context: dict, **kwargs):
        super().__init__(name=name, run_context=run_context, **kwargs)
        
        self.vcr_config: VCRPlaybackConfig = run_context.get("vcr_config")
        self.vcr_manager = run_context.get("vcr_manager")
        self.test_tenant = "accounting_dept"
        self.initial_fuel = 1000
        self.broker = DphiBroker(timeout=5.0)
        self.pta_adapter = PtaAdapter(broker=self.broker)

    def _build_trace_context(self, scenario_name: str, messages: list) -> dict:
        """VCR 식별 규칙에 맞는 Trace ID와 Meta 데이터를 생성"""
        seed = VCRIdentityRule.generate_seed(
            scenario_name=scenario_name, 
            messages=messages, 
            invoker=CURRENT_INVOKER
        )
        trace_id = next_trace_id(seed) if self.vcr_config.mode in ("record", "replay") else next_trace_id()
        
        return {
            "trace_id": trace_id,
            "metadata": {
                "vcr_scenario": scenario_name,
                "vcr_invoker": CURRENT_INVOKER
            }
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

    async def execute(self) -> None:
        self.log.info(f"[{self.name}] Igniting Fuel, Billing & FinOps Isolation Test Suite (Model: {self.target_model})")
        self.log.info(f"[VCR ENGINE] Mode: {self.vcr_config.mode.upper()} | Speed: {self.vcr_config.speed.upper()}")
        self.post_message(SetupProduceMsg())
        await self.run()

    @step
    async def phase_setup_produce(self, msg: SetupProduceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] [Phase 1] SETUP: PTA Infrastructure & Initial Fuel Production")
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
            self.log.info(f"[{self.name}] Produced Balance for '{self.test_tenant}': {balance} Fuel")
            
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
        self.log.info(f"\n[{self.name}] [Phase 2] EXECUTION: Auto-Consumption via VCR Replay")
        t0 = time.perf_counter()
        is_success = False
        usage_tokens = "N/A"
        original_quota_flag = os.environ.get("FIBER_ENFORCE_QUOTA")
        
        scenario = "fuel_phase_2_consume"
        messages = [{"role": "user", "content": "Explain quantum computing in 2 sentences."}]
        trace_ctx = self._build_trace_context(scenario, messages)
        # trace_ctx["metadata"]["node_auth"] = {"fuel_budget": 1000, "tenant_id": self.test_tenant, "is_enforced": True}
        trace_ctx["metadata"]["node_auth"] = NodeAuthPayload(
            fuel_budget=1000,
            tenant_id=self.test_tenant,
            is_enforced=True
        )
        
        try:
            os.environ["FIBER_ENFORCE_QUOTA"] = "false"
            balance_before = await self.pta_adapter.get_balance(self.test_tenant, "fuel")
            response = await acompletion(
                model=self.target_model,
                messages=messages,
                fuel_breaker=FuelBreaker(),
                billing_observer=BillingObserver(tenant_eco=None, pta_adapter=self.pta_adapter),
                **trace_ctx
            )
            
            used_tokens = getattr(response.usage, "total_tokens", 0) if hasattr(response, "usage") else 0
            usage_tokens = str(used_tokens)
            expected_balance = balance_before - used_tokens
            
            balance_after = await self._wait_for_balance(target_balance=expected_balance)
            self.log.info(f"[{self.name}] Balance Check: {balance_before} - {used_tokens}(Consumed) = {balance_after} Fuel")
            if balance_after == expected_balance and used_tokens > 0:
                is_success = True
            else:
                self.log.error(f"Consumption Mismatch or 0 tokens! Expected {expected_balance}, got {balance_after}. Tokens: {used_tokens}")
                
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
        self.log.info(f"\n[{self.name}] [Phase 3] SECURITY: Strict Quota Enforcement Shield")
        t0 = time.perf_counter()
        is_success = False
        original_quota_flag = os.environ.get("FIBER_ENFORCE_QUOTA")
        
        scenario = "fuel_phase_3_hack"
        messages = [{"role": "user", "content": "Hack the planet"}]
        trace_ctx = self._build_trace_context(scenario, messages)
        # trace_ctx["metadata"]["node_auth"] = {"fuel_budget": 999999, "tenant_id": "hacker"}
        trace_ctx["metadata"]["node_auth"] = NodeAuthPayload(
            fuel_budget=999999,
            tenant_id="hacker",
        )
        
        try:
            os.environ["FIBER_ENFORCE_QUOTA"] = "true"
            try:
                await acompletion(
                    model=self.target_model,
                    messages=messages,
                    fuel_breaker=FuelBreaker(),
                    billing_observer=BillingObserver(tenant_eco=None, pta_adapter=self.pta_adapter),
                    **trace_ctx
                )
                self.log.error("FuelBreaker failed to block unauthorized payload!")
            except PermissionError as pe:
                if "Strict Quota Enforcement is active" in str(pe):
                    is_success = True
                    self.log.info(f"[{self.name}] Blocked successfully: {pe}")
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
        self.log.info(f"\n[{self.name}] [Phase 4] FINOPS: Real-Time Pub/Sub Dynamic Intervention (VCR Stream Trap)")
        t0 = time.perf_counter()
        is_success = False
        
        scenario = "fuel_phase_4_finops"
        messages = [{"role": "user", "content": "Write a long story about FinOps."}]
        trace_ctx = self._build_trace_context(scenario, messages)
        trace_ctx["metadata"]["node_auth"] = {"fuel_budget": 1000, "tenant_id": self.test_tenant, "is_enforced": True}
        
        default_finops_watcher.start()
        try:
            tunnel = await TunnelFactory.get_default()
            await default_finops_watcher.wait_until_ready()
            
            self.log.info(f"[{self.name}] Emitting Pub/Sub 'update_budget' to Kill tenant '{self.test_tenant}'")
            kill_payload = {"type": "update_budget", "tenant_id": self.test_tenant, "fuel_budget": 0}
            await tunnel.publish("finops:control_plane", json.dumps(kill_payload))
            await asyncio.sleep(0.1)
            
            response_stream = await acompletion(
                model=self.target_model,
                messages=messages,
                stream=True,
                fuel_breaker=FuelBreaker(finops_watcher=default_finops_watcher),
                billing_observer=BillingObserver(tenant_eco=None, pta_adapter=self.pta_adapter),
                **trace_ctx
            )
            
            received_chunks = 0
            async for chunk in response_stream:
                received_chunks += 1
                
            # [유연한 임계값 적용] VCR 스피드에 따라 차단 시점이 약간 다를 수 있으므로 여유를 둡니다.
            if received_chunks <= 5:
                is_success = True
                self.log.info(f"[{self.name}] FuelTrap dynamically intercepted and killed the VCR stream at chunk {received_chunks}.")
            else:
                self.log.error(f"FinOps Intervention FAILED! Received {received_chunks} chunks.")

        except Exception as e:
            self.log.error(f"Phase 4 Error: {e}", exc_info=True)
        finally:
            default_finops_watcher.stop()
            
        self.record_result(4, "Dynamic FinOps Pub/Sub Intervention", is_success, t0)
        return ReportMsg()

    @step
    async def generate_report(self, msg: ReportMsg) -> WorkflowMessage:
        await self.broker.close()
        return await super().generate_report(msg)

def main(args: list[str] = None):
    parser = create_e2e_parser("LLM Fuel & FinOps Isolation Test Runner (VCR Enabled)")
    
    # 1. 중앙화된 VCR 파라미터 일괄 주입
    add_vcr_arguments(parser)
    
    parsed_args, _ = parser.parse_known_args(args)
    scope_kwargs, run_context = build_e2e_context(parsed_args)
    
    # 2. 통합 팩토리 메서드를 통해 config 생성
    vcr_config = VCRPlaybackConfig.from_args(parsed_args)
    os.makedirs(FIXTURE_ROOT, exist_ok=True)
    
    manager = VCRInjector.apply(config=vcr_config, fixture_dir=FIXTURE_ROOT)
    run_context["vcr_config"] = vcr_config
    run_context["vcr_manager"] = manager
    
    app = E2EBaseApplication(
        workflow_cls=LlmFuelWorkflow,
        workflow_name="FuelSuiteApp",
        scope_kwargs=scope_kwargs,
        run_context=run_context
    )
    
    try:
        app.execute()
    finally:
        VCRInjector.remove()

if __name__ == "__main__":
    main()