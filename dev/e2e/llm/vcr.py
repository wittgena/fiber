# fiber.dev.e2e.llm.vcr
from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from fiber.llm.entry import acompletion
from fiber.gateway.llm.mapper.traverser import StateTraverser

from fiber.dev.trace.llm.debugger import DebugTracer
from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig, VCRIdentityRule
from fiber.dev.trace.llm.vcr.proxy import VCRInjector
from fiber.dev.trace.llm.base import (
    E2EBaseWorkflow, 
    E2EBaseApplication, 
    create_e2e_parser, 
    build_e2e_context, 
    ReportMsg
)

from xphi.arch.contract.workflow import WorkflowMessage, step
from xphi.kernel.space.bind.resolver import resolve_path, get_invoker
from xphi.arch.bound.event.next import next_trace_id

FIXTURE_ROOT = resolve_path("fixture")
CURRENT_INVOKER, _ = get_invoker(Path(__file__))

class StartTraceMsg(WorkflowMessage): pass
class StreamTraceMsg(WorkflowMessage): pass
class ErrorTraceMsg(WorkflowMessage): pass
class FallbackTraceMsg(WorkflowMessage): pass

class LlmVcrWorkflow(E2EBaseWorkflow):
    def __init__(self, name: str, run_context: dict, **kwargs):
        super().__init__(name=name, run_context=run_context, **kwargs)
        self.config: VCRPlaybackConfig = run_context.get("vcr_config")
        self.vcr_manager = run_context.get("vcr_manager")

    async def execute(self) -> None:
        self.log.info("=" * 120)
        self.log.info(f"🧪 [MASTER SUITE] Igniting LLM VCR Trace Suite ({self.target_model})")
        self.log.info(f"⚙️  [VCR ENGINE] Mode: {self.config.mode.upper()} | Speed: {self.config.speed.upper()} | Chaos Jitter: {self.config.chaos_latency_ms}ms")
        self.log.info("=" * 120)
        self.post_message(StartTraceMsg())
        await self.run()

    def _build_trace_context(self, scenario_name: str, messages: list) -> dict:
        """VCR 식별 규칙에 Trace ID를 생성"""
        seed = VCRIdentityRule.generate_seed(
            scenario_name=scenario_name, 
            messages=messages, 
            invoker=CURRENT_INVOKER
        )
        trace_id = next_trace_id(seed) if self.config.mode in ("record", "replay") else next_trace_id()
        
        return {
            "trace_id": trace_id,
            "metadata": {
                "vcr_scenario": scenario_name,
                "vcr_invoker": CURRENT_INVOKER
            }
        }

    def record_vcr_result(self, phase: int, scenario: str, success: bool, t_start: float, trace_id: str, ctx_hints: Any = None, usage_tokens: Any = "N/A"):
        llm_latency_ms = 0.0
        if self.vcr_manager and self.config.mode in ("record", "replay"):
            try:
                fixture = self.vcr_manager.get_fixture(trace_id, ctx_hints, raise_on_missing=False)
                if fixture:
                    llm_latency_ms = fixture["network_metrics"].get("total_duration_ms", 0.0)
                    if self.config.mode == "replay":
                        llm_latency_ms += self.config.chaos_latency_ms
            except Exception:
                pass
                
        self.record_result(phase, scenario, success, t_start, llm_latency_ms=llm_latency_ms, usage_tokens=usage_tokens)

    @step
    async def phase_tracer_only(self, msg: StartTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 1] SINGULAR TRACING: Synchronous I/O Isolation")
        t0 = time.perf_counter()
        
        scenario = "phase_1_singular"
        messages = [{"role": "user", "content": "Hello, VCR."}]
        trace_ctx = self._build_trace_context(scenario, messages)
        usage_tokens = "N/A"
        is_success = False
        
        try:
            test_tracer = DebugTracer()
            class MockCtx:
                model = self.target_model
                system_meta = type('Meta', (), {'metadata': trace_ctx['metadata']})()
            ctx_hints = MockCtx()
            
            response = await acompletion(
                model=self.target_model, 
                messages=messages,
                interceptors=[test_tracer], 
                **trace_ctx
            )
            
            usage_tokens = StateTraverser.resolve(response, "usage.total_tokens", "N/A")
            await asyncio.sleep(0.01)
            is_success = test_tracer.started and test_tracer.ended
        except Exception as e:
            self.log.error(str(e))
            
        self.record_vcr_result(1, "Singular Tracing Isolation", is_success, t0, trace_ctx["trace_id"], ctx_hints, usage_tokens)
        return StreamTraceMsg()

    @step
    async def phase_stream_trace(self, msg: StreamTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 2] STREAM TRACING: Asynchronous Chunk Tracking")
        t0 = time.perf_counter()
        
        scenario = "phase_2_stream"
        messages = [
            {
                "role": "user", 
                "content": (
                    "Please write a detailed, 3-paragraph explanation about how VCR "
                    "(Video Cassette Recorder) technology works, including its history "
                    "and mechanical components. This is for testing a streaming chunk accumulation."
                )
            }
        ]
        
        trace_ctx = self._build_trace_context(scenario, messages)
        usage_tokens = "N/A"
        is_success = False
        
        try:
            stream_tracer = DebugTracer()
            class MockCtx:
                model = self.target_model
                system_meta = type('Meta', (), {'metadata': trace_ctx['metadata']})()
            ctx_hints = MockCtx()
            
            response_stream = await acompletion(
                model=self.target_model, 
                messages=messages,
                interceptors=[stream_tracer], 
                stream=True, 
                stream_options={"include_usage": True},
                **trace_ctx
            )
            
            # 스트림을 끝까지 소진하여 청크 데이터를 모두 소비
            async for _ in response_stream: pass 
            
            if hasattr(response_stream, "accumulator"):
                complete_res = response_stream.accumulator.get_complete_response()
                usage_tokens = StateTraverser.resolve(complete_res, "usage.total_tokens", "N/A")
            
            await asyncio.sleep(0.01)
            is_success = stream_tracer.started and stream_tracer.ended
        except Exception as e:
            self.log.error(str(e))
            
        self.record_vcr_result(2, "Asynchronous Stream Tracking", is_success, t0, trace_ctx["trace_id"], ctx_hints, usage_tokens)
        return ErrorTraceMsg()

    @step
    async def phase_error_trace(self, msg: ErrorTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 3] ERROR TRACING: Exception Boundary Verification")
        t0 = time.perf_counter()
        
        scenario = "phase_3_error"
        messages = [{"role": "user", "content": "Trigger an error!"}]
        trace_ctx = self._build_trace_context(scenario, messages)
        is_success = False
        
        try:
            error_tracer = DebugTracer()
            class MockCtx:
                model = "invalid/fake"
                system_meta = type('Meta', (), {'metadata': trace_ctx['metadata']})()
            ctx_hints = MockCtx()
            
            try:
                await acompletion(
                    model="invalid/fake", 
                    messages=messages, 
                    interceptors=[error_tracer], 
                    **trace_ctx
                )
            except Exception:
                pass
            
            await asyncio.sleep(0.01)
            is_success = error_tracer.started and not error_tracer.ended and error_tracer.error
        except Exception as e:
            self.log.error(str(e))
            
        self.record_vcr_result(3, "Exception Boundary Verification", is_success, t0, trace_ctx["trace_id"], ctx_hints, "N/A")
        return FallbackTraceMsg()

    @step
    async def phase_fallback_trace(self, msg: FallbackTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 4] FALLBACK TRACING: Idempotent Retry & Deepcopy Shield")
        t0 = time.perf_counter()
        
        scenario = "phase_4_fallback"
        messages = [{"role": "user", "content": "Fallback test via VCR"}]
        trace_ctx = self._build_trace_context(scenario, messages)
        usage_tokens = "N/A"
        is_success = False
        
        try:
            fallback_tracer = DebugTracer()
            class MockCtx:
                model = self.target_model 
                system_meta = type('Meta', (), {'metadata': trace_ctx['metadata']})()
            ctx_hints = MockCtx()
            
            response = await acompletion(
                model="invalid/will-fail", 
                messages=messages,
                fallbacks=[self.target_model], 
                interceptors=[fallback_tracer],
                **trace_ctx
            )
            
            usage_tokens = StateTraverser.resolve(response, "usage.total_tokens", "N/A")
            await asyncio.sleep(0.01)
            content = StateTraverser.resolve(response, "choices.0.message.content", "")
            is_shielded = fallback_tracer.started and fallback_tracer.ended and not fallback_tracer.error
            is_success = bool(content) and is_shielded
        except Exception as e:
            self.log.error(str(e))
            
        self.record_vcr_result(4, "Fallback & Deepcopy Shielding", is_success, t0, trace_ctx["trace_id"], ctx_hints, usage_tokens)
        return ReportMsg()

def main(args: list[str] = None):
    parser = create_e2e_parser("LLM VCR & Interceptor Suite Runner")
    parser.add_argument("--vcr", type=str, choices=["live", "record", "replay"], default="live", help="VCR mode for API Mocking")
    parser.add_argument("--vcr-speed", type=str, choices=["max", "real"], default="max", help="Replay speed control (max or real-time)")
    parser.add_argument("--vcr-chaos", type=float, default=0.0, help="Inject artificial latency jitter (in ms) during replay")
    parser.add_argument("--vcr-traces", type=str, default="", help="Specific trace IDs to replay (e.g. 'phase_1:abc, def')")
    parser.add_argument("--vcr-tick", type=float, default=100.0, help="Time-window (ms) to coalesce chunks during record mode (0 for raw)")
    
    parsed_args, _ = parser.parse_known_args(args)
    scope_kwargs, run_context = build_e2e_context(parsed_args)
    vcr_config = VCRPlaybackConfig(
        mode=parsed_args.vcr, 
        speed=parsed_args.vcr_speed, 
        chaos_latency_ms=parsed_args.vcr_chaos,
        target_traces=parsed_args.vcr_traces,
        record_tick_ms=parsed_args.vcr_tick
    )
    manager = VCRInjector.apply(config=vcr_config, fixture_dir=FIXTURE_ROOT)
    
    run_context["vcr_config"] = vcr_config
    run_context["vcr_manager"] = manager
    
    app = E2EBaseApplication(
        workflow_cls=LlmVcrWorkflow,
        workflow_name="VcrSuiteApp",
        scope_kwargs=scope_kwargs,
        run_context=run_context
    )
    app.execute()

if __name__ == "__main__":
    main()