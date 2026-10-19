# fiber.dev.e2e.llm.compat
from __future__ import annotations

import argparse
import asyncio
import os
import socket
import time
from types import SimpleNamespace
from typing import List, Tuple, Optional, Any, Dict

from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.llm.entry import acompletion
from fiber.llm.model.token.counter import token_counter, get_modified_max_tokens
from fiber.llm.model.token.splitter import TokenSplitter
from fiber.gateway.llm.context.metadata import ExecutionMetadata
from xphi.arch.bound.scope.manager import managed_scope

from xphi.state.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.llm.compat")

class DummyTestTracer(BaseLLMTracer):
    def __init__(self):
        self.started = False
        self.ended = False
        self.error = False
        self.recorded_model = None
        self.duration = 0.0

    async def on_llm_start(self, meta: ExecutionMetadata, kwargs: Dict[str, Any]):
        self.started = True
        self.recorded_model = meta.base_model

    async def on_llm_end(self, meta: ExecutionMetadata, response: Any, duration_ms: float):
        self.ended = True
        self.duration = duration_ms

    async def on_llm_error(self, meta: ExecutionMetadata, exc: Exception, duration_ms: float):
        self.error = True

class StartCompatMsg(WorkflowMessage): pass
class MockBypassTestMsg(WorkflowMessage): pass
class FuelBreakerTestMsg(WorkflowMessage): pass
class TokenUtilsTestMsg(WorkflowMessage): pass
class AdapterMappingTestMsg(WorkflowMessage): pass
class InterceptorTestMsg(WorkflowMessage): pass

class LlmCompatWorkflow(Workflow):
    class Meta:
        trans_rules = {"error": ErrorMessage}

    def __init__(self, name: str, run_context: dict, **kwargs):
        super().__init__(name=name, timeout=600.0, **kwargs)
        self.run_context = run_context
        self.target_model = run_context.get("target_model")

        self.log = log
        self.success_count = 0
        self.fail_count = 0
        self.total_fuel = 0
        self.audit_traces: List[str] = []

    async def execute(self) -> None:
        self.log.info(f"[{self.name}] Igniting LLM Compat Suite (Primary Model: {self.target_model})")
        self.post_message(StartCompatMsg())
        await self.run()

    @step
    async def phase_basic_completion(self, msg: StartCompatMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] [Phase 1] Core Pipeline & Drop-in Replacement")
        try:
            audit_hash = f"audit_basic_{int(time.time())}"
            response = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Hello, DPHI Kernel!"}],
                metadata={"node_auth": {"audit_hash": audit_hash}}
            )

            if response and response.choices:
                self.log.info(f"[{self.name}] Passed: Standard async completion executed successfully.")
                self.success_count += 1

                usage = getattr(response, "usage", None)
                self.total_fuel += getattr(usage, "fuel_consumed", 0) if usage else 0

                sealed_hash = getattr(response, "system_fingerprint", "N/A")
                if sealed_hash == audit_hash:
                    self.audit_traces.append(sealed_hash)
                else:
                    self.log.warning(f"[{self.name}] Audit hash mismatch: Expected {audit_hash}, Got {sealed_hash}")
            else:
                raise ValueError("Empty or invalid response structure.")

        except Exception as e:
            self.log.error(f"[{self.name}] Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Basic Completion Failed: {e}")

        return MockBypassTestMsg()

    @step
    async def phase_mock_bypass(self, msg: MockBypassTestMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] [Phase 2] MockBypass Short-circuit")
        try:
            start_time = time.time()
            expected_mock = "This is a bypassed mock response."

            response = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Cost me nothing!"}],
                mock_response=expected_mock,
                mock_delay=1.0,
                metadata={"node_auth": {"audit_hash": "audit_mock"}}
            )

            elapsed = time.time() - start_time
            content = response.choices[0].message.content

            if content == expected_mock and elapsed >= 1.0:
                self.log.info(f"[{self.name}] Passed: Mock response received in {elapsed:.2f}s (Expected >1.0s).")
                self.success_count += 1
                self.audit_traces.append("audit_mock")
            else:
                raise ValueError("Mock payload mismatch or delay ignored.")

        except Exception as e:
            self.log.error(f"[{self.name}] Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Mock Bypass Failed: {e}")

        return FuelBreakerTestMsg()

    @step
    async def phase_fuel_breaker(self, msg: FuelBreakerTestMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] [Phase 3] Fuel Breaker Streaming")
        try:
            budget = 5 
            self.log.info(f"[{self.name}] Injecting artificial fuel budget: {budget}")

            stream = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Write a very long essay about the history of the universe."}],
                stream=True,
                metadata={"node_auth": {"fuel_budget": budget}}
            )

            chunks_received = 0
            async for _ in stream:
                chunks_received += 1

            if chunks_received <= budget + 2:
                self.log.info(f"[{self.name}] Passed: Stream physically killed after {chunks_received} chunks.")
                self.success_count += 1
                self.total_fuel += chunks_received
            else:
                raise ValueError(f"Fuel breaker failed to kill stream. Received {chunks_received} chunks.")

        except Exception as e:
            self.log.error(f"[{self.name}] Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Fuel Breaker Failed: {e}")

        return TokenUtilsTestMsg()

    @step
    async def phase_token_utils(self, msg: TokenUtilsTestMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] [Phase 4] Token Evaluator & Splitter")
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
                self.log.info(f"[{self.name}] Passed: Token math verified. (Count: {count}, SafeMax: {safe_max}, Chunks: {len(chunks)})")
                self.success_count += 1
            else:
                raise ValueError("Invalid token computation results.")

        except Exception as e:
            self.log.error(f"[{self.name}] Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Token Utils Failed: {e}")

        return AdapterMappingTestMsg()

    @step
    async def phase_adapter_mapping(self, msg: AdapterMappingTestMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] [Phase 5] InterLLM Adapter & State Mapper Verification")
        try:
            from fiber.gateway.llm.state.traverser import StateMapper
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

            choice_dict = mapper.to_openai_choice(mock_llama_resp, req_id="mock_req", logger=self.log, provider="gemini")
            if choice_dict["finish_reason"] != "tool_calls" or not choice_dict["message"].get("tool_calls"):
                raise ValueError("StateMapper failed to recover tool_calls from raw response.")
            self.log.info(f"[{self.name}] Passed: StateMapper successfully traversed and recovered tool_calls.")

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
                metadata={"node_auth": {"audit_hash": "audit_tool_test"}}
            )

            if response.choices:
                choice = response.choices[0]
                finish_reason = getattr(choice, "finish_reason", None) if not isinstance(choice, dict) else choice.get("finish_reason")
                if finish_reason in ["tool_calls", "stop"]:
                    self.log.info(f"[{self.name}] Passed: InterLLMAdapter E2E tool mapping executed securely.")
                    self.success_count += 1
                else:
                    raise ValueError(f"InterLLMAdapter returned invalid finish_reason: {finish_reason}")
            else:
                raise ValueError("InterLLMAdapter returned empty choices.")
        except Exception as e:
            self.log.error(f"[{self.name}] Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Adapter Mapping Failed: {e}")

        return InterceptorTestMsg()

    @step
    async def phase_custom_interceptor(self, msg: InterceptorTestMsg) -> WorkflowMessage:
        self.log.info(f"[{self.name}] [Phase 6] Flat List Interceptor (UX Facade) Injection Verification")
        try:
            test_tracer = DummyTestTracer()
            audit_hash = f"audit_tracer_{int(time.time())}"
            
            response = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Say 'hello interceptor'"}],
                interceptors=[test_tracer],
                metadata={"node_auth": {"audit_hash": audit_hash}}
            )
            
            await asyncio.sleep(0.1) 
            
            if test_tracer.started and test_tracer.ended and (test_tracer.recorded_model == self.target_model):
                self.log.info(f"[{self.name}] Passed: Custom Interceptor successfully hooked into lifecycle via entry Facade (Duration: {test_tracer.duration:.2f}ms).")
                self.success_count += 1
                self.audit_traces.append(getattr(response, "system_fingerprint", "N/A"))
            else:
                raise ValueError(f"Interceptor hooks not fired properly! Started: {test_tracer.started}, Ended: {test_tracer.ended}")
                
        except Exception as e:
            self.log.error(f"[{self.name}] Failed: {e}")
            self.fail_count += 1
            return ErrorMessage(f"Custom Interceptor Failed: {e}")

        return StopMessage(result=True)

    @step
    async def settle_and_terminate(self, msg: StopMessage) -> None:
        self._print_verification_report()

    @step
    async def handle_rupture(self, msg: ErrorMessage) -> None:
        self.log.error(f"[{self.name}] Fatal topological rupture: {msg.msg}")
        self.post_message(StopMessage(result=False))

    def _print_verification_report(self):
        self.log.info("\n" + "="*60)
        self.log.info(f"[COMPAT TOPOLOGY FINALIZED] Edge State: {'SUCCESS' if self.fail_count == 0 else 'FRACTURED'}")
        self.log.info(f"  Suite Results       : Passed: {self.success_count} / Failed: {self.fail_count}")
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
            log.info("Reclaiming compat suite resources...")
            self.workflow.stop()

    def execute(self):
        log.info("Igniting Launcher Workflow via KernelReactor...")
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
    resolved_model = getattr(args, 'model', None) or os.environ.get("LLM_COMPAT_MODEL")
    use_proxy = getattr(args, 'proxy', False) or os.environ.get("LLM_COMPAT_PROXY", "false").lower() == "true"

    if not is_online:
        log.warning("[System Offline] Forcing fallback to Local Engine.")
        resolved_model = "ollama/gemma_2b"
    elif not resolved_model:
        resolved_model = "gemini/gemini-3.1-flash-lite"

    scope_kwargs = {"use_proxy": use_proxy if is_online else False, "show_logs": True}
    run_context = {
        "use_proxy": scope_kwargs["use_proxy"], 
        "target_model": resolved_model
    }
    return scope_kwargs, run_context

def main(args: list[str] = None):
    parser = argparse.ArgumentParser(description="LLM Compat E2E Suite Runner")
    parser.add_argument("-m", "--model", type=str, help="Target LLM model to use.")
    parser.add_argument("--proxy", action="store_true", help="Enable remote proxy extension layout.")

    args, _ = parser.parse_known_args(args)
    scope_kwargs, run_context = get_environment_context(args)
    app = LlmCompatApplication(
        scope_kwargs=scope_kwargs,
        run_context=run_context
    )
    app.execute()

if __name__ == "__main__":
    main()