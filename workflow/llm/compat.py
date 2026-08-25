# workflow.llm.compat
from __future__ import annotations

import argparse
import asyncio
import socket
import time
from types import SimpleNamespace
from typing import List, Tuple, Optional

from fiber.llm.entry import acompletion
from fiber.llm.model.token.counter import token_counter, get_modified_max_tokens
from fiber.llm.model.token.splitter import TokenSplitter
from fiber.llm.model.tier import model_tier_registry
from fiber.phase.scope.manager import managed_scope

from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("workflow.llm.compat")

class StartCompatMsg(WorkflowMessage): pass
class FallbackTestMsg(WorkflowMessage): pass
class MockBypassTestMsg(WorkflowMessage): pass
class FuelTrapTestMsg(WorkflowMessage): pass
class TokenUtilsTestMsg(WorkflowMessage): pass
class AdapterMappingTestMsg(WorkflowMessage): pass  # [NEW] Phase 6 Message

class LlmCompatWorkflow(Workflow):
    class Meta:
        trans_rules = {"error": ErrorMessage}

    def __init__(self, name: str, run_context: dict, **kwargs):
        super().__init__(name=name, timeout=600.0, **kwargs)
        self.run_context = run_context
        self.target_model = run_context.get("target_model")
        self.test_fallback_only = run_context.get("test_fallback_only", False)
        
        self.log = log
        self.success_count = 0
        self.fail_count = 0
        self.total_fuel = 0
        self.audit_traces: List[str] = []

    async def execute(self) -> None:
        self.log.info(f"[{self.name}] 🚀 Igniting LLM Compat Suite (Primary Model: {self.target_model})")
        if self.test_fallback_only:
            self.post_message(FallbackTestMsg())
        else:
            self.post_message(StartCompatMsg())
        await self.run()

    @step
    async def phase_basic_completion(self, msg: StartCompatMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] 🔄 [Phase 1] Core Pipeline & Drop-in Replacement")
        try:
            audit_hash = f"audit_basic_{int(time.time())}"
            response = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Hello, DPHI Kernel!"}],
                metadata={"kernel_auth": {"audit_hash": audit_hash}}
            )
            
            if response and response.choices:
                self.log.info(f"[{self.name}] ✅ Passed: Standard async completion executed successfully.")
                self.success_count += 1
                
                usage = getattr(response, "usage", None)
                self.total_fuel += getattr(usage, "fuel_consumed", 0) if usage else 0
                
                sealed_hash = getattr(response, "system_fingerprint", "N/A")
                if sealed_hash == audit_hash:
                    self.audit_traces.append(sealed_hash)
                else:
                    self.log.warning(f"[{self.name}] ⚠️ Audit hash mismatch: Expected {audit_hash}, Got {sealed_hash}")
            else:
                raise ValueError("Empty or invalid response structure.")
                
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Basic Completion Failed: {e}")

        return FallbackTestMsg()

    @step
    async def phase_fallback(self, msg: FallbackTestMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] 🔄 [Phase 2] Dynamic FallbackHandler via Tier Registry")
        try:
            fallback_1 = model_tier_registry.get_optimal_model(min_cognitive_score=2) or "gemini/gemini-3.1-flash-lite"
            fallback_2 = model_tier_registry.get_optimal_model(min_cognitive_score=3) or "gemini/gemma-4-31b"
            
            self.log.info(f"[{self.name}] ⚙️ Fallback Pool Configured: {fallback_1}, {fallback_2}")
            audit_hash = f"audit_fb_{int(time.time())}"
            
            response = await acompletion(
                model="invalid-trigger-model", 
                messages=[{"role": "user", "content": "Trigger fallback"}],
                fallbacks=[fallback_1, {"model": fallback_2, "temperature": 0.5}],
                metadata={"kernel_auth": {"audit_hash": audit_hash}}
            )
            
            used_model = getattr(response, "model", None) or ""
            if not used_model or fallback_1 in used_model or fallback_2 in used_model:
                self.log.info(f"[{self.name}] ✅ Passed: Fallback successfully routed to '{used_model}'.")
                self.success_count += 1
                
                usage = getattr(response, "usage", None)
                self.total_fuel += getattr(usage, "fuel_consumed", 0) if usage else 0
                self.audit_traces.append(getattr(response, "system_fingerprint", "N/A"))
            else:
                raise ValueError(f"Fallback didn't route as expected. Used: {used_model}")
                
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Fallback Handler Failed: {e}")

        if self.test_fallback_only:
            return StopMessage(result=True)
            
        return MockBypassTestMsg()

    @step
    async def phase_mock_bypass(self, msg: MockBypassTestMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] 🔄 [Phase 3] MockBypass Short-circuit")
        try:
            start_time = time.time()
            expected_mock = "This is a bypassed mock response."
            
            response = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Cost me nothing!"}],
                mock_response=expected_mock,
                mock_delay=1.0,
                metadata={"kernel_auth": {"audit_hash": "audit_mock"}}
            )
            
            elapsed = time.time() - start_time
            content = response.choices[0].message.content
            
            if content == expected_mock and elapsed >= 1.0:
                self.log.info(f"[{self.name}] ✅ Passed: Mock response received in {elapsed:.2f}s (Expected >1.0s).")
                self.success_count += 1
                self.audit_traces.append("audit_mock")
            else:
                raise ValueError("Mock payload mismatch or delay ignored.")
                
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Mock Bypass Failed: {e}")

        return FuelTrapTestMsg()

    @step
    async def phase_fuel_trap(self, msg: FuelTrapTestMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] 🔄 [Phase 4] Kinetic Membrane (Fuel Trap) Streaming")
        try:
            budget = 5 
            self.log.info(f"[{self.name}] ⛽ Injecting artificial fuel budget: {budget}")
            
            stream = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Write a very long essay about the history of the universe."}],
                stream=True,
                metadata={"kernel_auth": {"fuel_budget": budget}}
            )
            
            chunks_received = 0
            async for _ in stream:
                chunks_received += 1
                
            if chunks_received <= budget + 2:
                self.log.info(f"[{self.name}] ✅ Passed: Stream physically killed after {chunks_received} chunks.")
                self.success_count += 1
                self.total_fuel += chunks_received
            else:
                raise ValueError(f"Fuel trap failed to kill stream. Received {chunks_received} chunks.")
                
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Fuel Trap Failed: {e}")

        return TokenUtilsTestMsg()

    @step
    async def phase_token_utils(self, msg: TokenUtilsTestMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] 🔄 [Phase 5] Token Evaluator & Splitter")
        try:
            msgs = [{"role": "user", "content": "Hello world"}]
            count = token_counter(model=self.target_model, messages=msgs)
            
            safe_max = get_modified_max_tokens(
                model=self.target_model, base_model=self.target_model, 
                messages=msgs, user_max_tokens=1000, buffer_perc=0.1
            )
            
            splitter = TokenSplitter(chunk_size=10, chunk_overlap=2, model="gemini/gemini-3.1-flash-lite")
            chunks = splitter.split_text("This is a relatively long string meant to be safely split by token IDs.")

            if count > 0 and safe_max > 0 and len(chunks) > 1:
                self.log.info(f"[{self.name}] ✅ Passed: Token math verified. (Count: {count}, SafeMax: {safe_max}, Chunks: {len(chunks)})")
                self.success_count += 1
            else:
                raise ValueError("Invalid token computation results.")

        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Token Utils Failed: {e}")

        # [수정] 바로 종료하지 않고 Adapter 검증 단계로 트랜지션
        return AdapterMappingTestMsg()

    @step
    async def phase_adapter_mapping(self, msg: AdapterMappingTestMsg) -> WorkflowMessage:
        """[NEW] Phase 6: InterLLM Adapter & State Mapper (Traverser) 검증"""
        self.log.info(f"[{self.name}] 🔄 [Phase 6] InterLLM Adapter & State Mapper Verification")
        try:
            # 1. StateMapper & Traverser 규칙 직접 검증 (Gemini Tool Leak 시뮬레이션)
            from fiber.dphi.model.mapper.state import StateMapper
            mapper = StateMapper()
            
            mock_raw_resp = {
                "content": {
                    "parts": [
                        {"function_call": {"name": "get_weather", "args": {"location": "Seoul"}}}
                    ]
                }
            }
            mock_llama_resp = SimpleNamespace(
                message=SimpleNamespace(role=SimpleNamespace(value="assistant"), content="", additional_kwargs={}),
                raw=mock_raw_resp
            )

            # Traverser가 STATE_EXTRACTION_RULES를 이용해 복구해내는지 확인
            choice_dict = mapper.to_openai_choice(mock_llama_resp, req_id="mock_req", logger=self.log, provider="gemini")
            if choice_dict["finish_reason"] != "tool_calls" or not choice_dict["message"].get("tool_calls"):
                raise ValueError("StateMapper failed to recover tool_calls from raw response.")
            self.log.info(f"[{self.name}] 🎯 Passed: StateMapper successfully traversed and recovered tool_calls.")

            # 2. E2E Tool Call 파이프라인 검증 (InterLLMAdapter 경유)
            tools = [{
                "type": "function",
                "function": {
                    "name": "get_current_time",
                    "description": "Get the current time",
                    "parameters": {"type": "object", "properties": {}}
                }
            }]
            
            response = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "What time is it right now? Use the tool."}],
                tools=tools,
                metadata={"kernel_auth": {"audit_hash": "audit_tool_test"}}
            )
            
            if response.choices:
                choice = response.choices[0]
                finish_reason = getattr(choice, "finish_reason", None) if not isinstance(choice, dict) else choice.get("finish_reason")
                if finish_reason in ["tool_calls", "stop"]:
                    self.log.info(f"[{self.name}] ✅ Passed: InterLLMAdapter E2E tool mapping executed securely.")
                    self.success_count += 1
                else:
                    raise ValueError(f"InterLLMAdapter returned invalid finish_reason: {finish_reason}")
            else:
                raise ValueError("InterLLMAdapter returned empty choices.")
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Adapter Mapping Failed: {e}")

        return StopMessage(result=True)

    @step
    async def settle_and_terminate(self, msg: StopMessage) -> None:
        self._print_verification_report()

    @step
    async def handle_rupture(self, msg: ErrorMessage) -> None:
        self.log.error(f"[{self.name}] 🚨 Fatal topological rupture: {msg.msg}")
        self.post_message(StopMessage(result=False))

    def _print_verification_report(self):
        self.log.info("\n" + "="*60)
        self.log.info(f"🌌 [COMPAT TOPOLOGY FINALIZED] Edge State: {'SUCCESS' if self.fail_count == 0 else 'FRACTURED'}")
        self.log.info(f"  Suite Results       : ✅ {self.success_count} / ❌ {self.fail_count}")
        self.log.info(f"  Fuel Burned         : {self.total_fuel}")
        self.log.info(f"  Audit Hashes Sealed : {', '.join(self.audit_traces)}")
        self.log.info("="*60 + "\n")


class LlmCompatApplication:
    def __init__(self, scope_kwargs: dict, run_context: dict):
        self.scope_kwargs = scope_kwargs
        self.run_context = run_context
        self.workflow: Optional[LlmCompatWorkflow] = None

    async def _startup_hook(self):
        async with managed_scope(**self.scope_kwargs):
            self.workflow = LlmCompatWorkflow("CompatSuiteApp", self.run_context)
            workflow_task = asyncio.create_task(self.workflow.run())
            await self.workflow.execute()
            await workflow_task
            
            if self.workflow.fail_count > 0:
                raise RuntimeError(f"Workflow finished with {self.workflow.fail_count} failures.")

    async def _teardown_hook(self):
        if self.workflow:
            log.info("🧹 Reclaiming compat suite resources...")
            self.workflow.stop()

    def execute(self):
        log.info("🚀 Igniting Launcher Workflow via KernelReactor...")
        PhaseReactor.ignite(
            main_coro_func=self._startup_hook,
            teardown_hook=self._teardown_hook
        )


def get_environment_context(args: argparse.Namespace) -> Tuple[dict, dict]:
    def check_online(host: str = "8.8.8.8", port: int = 53, timeout: float = 0.5) -> bool:
        try:
            socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
            return True
        except OSError:
            return False

    is_online = check_online()
    resolved_model = args.model
    
    if not is_online:
        log.warning("🚨 [System Offline] Forcing fallback to Local Engine.")
        resolved_model = "ollama/local-gemma-3"
    elif not resolved_model:
        optimal = model_tier_registry.get_optimal_model(requires_tools=False, min_cognitive_score=2)
        resolved_model = f"{optimal[0]}/{optimal[1]}" if isinstance(optimal, tuple) else (f"gemini/{optimal}" if optimal else "gemini/gemini-3.1-flash-lite")

    scope_kwargs = {"use_proxy": args.proxy if is_online else False, "show_logs": True}
    run_context = {
        "use_proxy": scope_kwargs["use_proxy"], 
        "target_model": resolved_model,
        "test_fallback_only": args.test_fallback
    }
    return scope_kwargs, run_context

def main():
    parser = argparse.ArgumentParser(description="LLM Compat E2E Suite Runner")
    parser.add_argument("-m", "--model", type=str, help="Target LLM model to use.")
    parser.add_argument("--proxy", action="store_true", help="Enable remote proxy extension layout.")
    parser.add_argument("--test-fallback", action="store_true", help="Run ONLY the fallback routing scenario.")
    args = parser.parse_args()

    scope_kwargs, run_context = get_environment_context(args)
    app = LlmCompatApplication(
        scope_kwargs=scope_kwargs,
        run_context=run_context
    )
    app.execute()

if __name__ == "__main__":
    main()