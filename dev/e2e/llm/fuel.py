# fiber.dev.e2e.llm.fuel
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from typing import Dict, Any, Optional, Tuple

from fiber.gateway.llm.pipeline import PipelineSlot
from fiber.llm.entry import acompletion
from fiber.llm.param import ModelResponse
from fiber.phase.scope.manager import managed_scope

from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.arch.contract.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.wasm.broker import DphiBroker
from xphi.arch.bound.adapter.pta import PtaAdapter, PtaTransaction, PtaOutput

log = get_emitter("e2e.llm.fuel")

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
            usage={"prompt_tokens": 10, "completion_tokens": 32, "total_tokens": 42} # 👈 차감 테스트의 핵심
        )
        await ctx.fire_channel_read(mock_response)

class SetupMintMsg(WorkflowMessage): pass
class ObserverDeductMsg(WorkflowMessage): pass
class StrictEnforceMsg(WorkflowMessage): pass

class LlmFuelWorkflow(Workflow):
    class Meta:
        trans_rules = {"error": ErrorMessage}

    def __init__(self, name: str, run_context: dict, **kwargs):
        super().__init__(name=name, timeout=60.0, **kwargs)
        self.log = log
        self.test_tenant = "accounting_dept"
        self.initial_fuel = 1000
        self.mock_usage = 42  # MockBillingTransport가 소모할 토큰 수
        
        self.broker = DphiBroker(timeout=5.0)
        self.pta_adapter = PtaAdapter(broker=self.broker)

    async def execute(self) -> None:
        self.log.info(f"[{self.name}] 🚀 Igniting Fuel & Billing Isolation Test Suite")
        self.post_message(SetupMintMsg())
        await self.run()

    @step
    async def phase_setup_minting(self, msg: SetupMintMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 1] SETUP: PTA Infrastructure & Stealth Minting")
        try:
            mint_tx = PtaTransaction(
                inputs=[], 
                outputs=[PtaOutput(amount=self.initial_fuel, owner=self.test_tenant, asset_type="fuel")],
                metadata={"action": "test_minting"}
            )
            await self.pta_adapter.execute_transaction(mint_tx)
            
            balance = await self.pta_adapter.get_balance(self.test_tenant, "fuel")
            self.log.info(f"[{self.name}] 💰 Minted Balance for '{self.test_tenant}': {balance} Fuel")
            
            if balance >= self.initial_fuel:
                self.log.info(f"[{self.name}] ✅ Passed: Initial PTA Minting successful.")
            else:
                raise ValueError("Failed to mint initial fuel.")
                
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Phase 1 Failed: {e}")
            return StopMessage(result=False)
            
        return ObserverDeductMsg()

    @step
    async def phase_observer_deduction(self, msg: ObserverDeductMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 2] EXECUTION: Auto-Deduction in Observer Mode")
        original_quota_flag = os.environ.get("FIBER_ENFORCE_QUOTA")
        try:
            os.environ["FIBER_ENFORCE_QUOTA"] = "false"
            balance_before = await self.pta_adapter.get_balance(self.test_tenant, "fuel")
            response = await acompletion(
                model="test-model",
                messages=[{"role": "user", "content": "Test deduction"}],
                tenant_id=self.test_tenant, 
                interceptors=[MockBillingTransport()]
            )
            
            await asyncio.sleep(1.0) 
            
            used_tokens = getattr(response.usage, "total_tokens", 0) if hasattr(response, "usage") else 0
            balance_after = await self.pta_adapter.get_balance(self.test_tenant, "fuel")
            
            self.log.info(f"[{self.name}] 📉 Balance Check: {balance_before} - {used_tokens}(Used) = {balance_after} Fuel")
            
            if used_tokens == self.mock_usage and balance_after == (balance_before - self.mock_usage):
                self.log.info(f"[{self.name}] ✅ Passed: Deterministic Deduction Logic verified perfectly.")
            else:
                raise ValueError(f"Deduction Math Mismatch! Expected {balance_before - self.mock_usage}, got {balance_after}")
                
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Phase 2 Failed: {e}", exc_info=True)
            return StopMessage(result=False)
        finally:
            if original_quota_flag is not None: os.environ["FIBER_ENFORCE_QUOTA"] = original_quota_flag
            else: os.environ.pop("FIBER_ENFORCE_QUOTA", None)
            
        return StrictEnforceMsg()

    @step
    async def phase_strict_enforcement(self, msg: StrictEnforceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 3] SECURITY: Strict Quota Enforcement Shield")
        original_quota_flag = os.environ.get("FIBER_ENFORCE_QUOTA")
        try:
            os.environ["FIBER_ENFORCE_QUOTA"] = "true"
            
            try:
                await acompletion(
                    model="test-model",
                    messages=[{"role": "user", "content": "Hack the planet"}],
                    metadata={"kernel_auth": {"fuel_budget": 999999, "tenant_id": "hacker"}} 
                )
                raise RuntimeError("FuelInterceptor failed to block unauthorized payload!")
            except PermissionError as pe:
                if "Strict Quota Enforcement is active" in str(pe):
                    self.log.info(f"[{self.name}] ✅ Passed: FuelInterceptor successfully BLOCKED unauthorized access.")
                else:
                    raise ValueError(f"Blocked, but with unexpected error: {pe}")
                    
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Phase 3 Failed: {e}")
            return StopMessage(result=False)
        finally:
            if original_quota_flag is not None: os.environ["FIBER_ENFORCE_QUOTA"] = original_quota_flag
            else: os.environ.pop("FIBER_ENFORCE_QUOTA", None)
            
        return StopMessage(result=True)

    @step
    async def settle_and_terminate(self, msg: StopMessage) -> None:
        await self.broker.close()
        self.log.info("\n" + "="*60)
        self.log.info(f"🌌 [FUEL TEST TOPOLOGY FINALIZED] Result: {'SUCCESS ✅' if msg.result else 'FAILED ❌'}")
        self.log.info("="*60 + "\n")

class LlmFuelApplication:
    def __init__(self, scope_kwargs: dict, run_context: dict):
        self.scope_kwargs = scope_kwargs
        self.run_context = run_context
        self.workflow: Optional[LlmFuelWorkflow] = None

    async def _startup_hook(self):
        async with managed_scope(**self.scope_kwargs):
            self.workflow = LlmFuelWorkflow("FuelSuiteApp", self.run_context)
            workflow_task = asyncio.create_task(self.workflow.run())
            await self.workflow.execute()
            await workflow_task

    async def _teardown_hook(self):
        if self.workflow:
            self.workflow.stop()

    def execute(self):
        log.info("🚀 Igniting Launcher Workflow via KernelReactor...")
        PhaseReactor.ignite(main_coro_func=self._startup_hook, teardown_hook=self._teardown_hook)

def main():
    parser = argparse.ArgumentParser(description="LLM Fuel & Deduction Isolation Test Runner")
    args, _ = parser.parse_known_args()
    
    scope_kwargs = {"use_proxy": False, "show_logs": True}
    run_context = {}
    
    app = LlmFuelApplication(scope_kwargs=scope_kwargs, run_context=run_context)
    app.execute()

if __name__ == "__main__":
    main()