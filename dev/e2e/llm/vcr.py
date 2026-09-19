# fiber.dev.e2e.llm.vcr
from __future__ import annotations

import argparse
import asyncio
import os
import socket
import sys
import time
import uuid
from dataclasses import dataclass
from typing import List, Tuple, Any, Dict

from fiber.llm.entry import acompletion
from fiber.llm.model.tier import model_tier_registry
from fiber.phase.scope.manager import managed_scope
from fiber.gateway.llm.mapper.traverser import StateTraverser

from fiber.dev.trace.llm.debugger import DebugTracer
from fiber.dev.trace.llm.vcr import VCRInjector, VCRPlaybackConfig

from xphi.arch.contract.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

FIXTURE_ROOT = resolve_path("fixture")
log = get_emitter("e2e.llm.vcr")

"""[REPORT DTO] 명확한 가시성을 위한 결과 객체 (지연 시간 정밀 분리)"""
@dataclass
class TraceTestResult:
    phase_num: int
    scenario: str
    success: bool
    total_latency_ms: float = 0.0
    llm_latency_ms: float = 0.0  # VCRAdapterProxy가 측정한 순수 I/O(또는 에뮬레이션) 시간

    @property
    def overhead_ms(self) -> float:
        # [핵심] 전체 수행 시간에서 외부 I/O 대기시간을 뺀 100% 순수 프레임워크 오버헤드
        return max(0.0, self.total_latency_ms - self.llm_latency_ms)

    @property
    def status_icon(self) -> str:
        return "✅ PASS" if self.success else "❌ FAIL"

"""[WORKFLOW] 테스트 메시지 및 위상(Phase) 정의"""
class StartTraceMsg(WorkflowMessage): pass
class StreamTraceMsg(WorkflowMessage): pass
class ErrorTraceMsg(WorkflowMessage): pass
class FallbackTraceMsg(WorkflowMessage): pass
class ReportMsg(WorkflowMessage): pass

class LlmTraceWorkflow(Workflow):
    class Meta:
        trans_rules = {"error": ErrorMessage}

    def __init__(self, name: str, run_context: dict, **kwargs):
        super().__init__(name=name, timeout=300.0, **kwargs)
        self.target_model = run_context.get("target_model")
        self.config: VCRPlaybackConfig = run_context.get("vcr_config")
        
        # [변경점] VCRManager는 더 이상 e2e에서 직접 생성/관리하지 않고, VCRInjector를 통해 활성화된 객체를 참조합니다.
        self.vcr_manager = run_context.get("vcr_manager") 
        
        self.log = log
        self.all_results: List[TraceTestResult] = []

    async def execute(self) -> None:
        self.log.info("=" * 110)
        self.log.info(f"🧪 [MASTER SUITE] Igniting LLM VCR Trace Suite ({self.target_model})")
        self.log.info(f"⚙️  [VCR ENGINE] Mode: {self.config.mode.upper()} | Speed: {self.config.speed.upper()} | Chaos Jitter: {self.config.chaos_latency_ms}ms")
        self.log.info("=" * 110)
        self.post_message(StartTraceMsg())
        await self.run()

    def _record(self, phase: int, scenario: str, success: bool, t_start: float, trace_id: str):
        total_latency = (time.perf_counter() - t_start) * 1000
        
        llm_latency = 0.0
        # vcr_manager가 존재하고(live 모드가 아닐 때), 레코드/리플레이 모드인 경우 지연 시간 추출
        if self.vcr_manager and self.config.mode in ("record", "replay"):
            fixture = self.vcr_manager.get_fixture(trace_id)
            if fixture:
                llm_latency = fixture["network_metrics"].get("total_duration_ms", 0.0)
                if self.config.mode == "replay":
                    llm_latency += self.config.chaos_latency_ms

        self.all_results.append(TraceTestResult(phase, scenario, success, total_latency, llm_latency))
        
        if success:
            self.log.info(f"[{self.name}] ✅ Passed: {scenario} (Total: {total_latency:.2f}ms)")
        else:
            self.log.error(f"[{self.name}] ❌ Failed: {scenario}")

    @step
    async def phase_tracer_only(self, msg: StartTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 1] SINGULAR TRACING: Synchronous I/O Isolation")
        t0 = time.perf_counter()
        trace_id = f"e2e_singular_{uuid.uuid4().hex[:8]}"
        
        try:
            test_tracer = DebugTracer()
            await acompletion(
                model=self.target_model, 
                messages=[{"role": "user", "content": "Hello, VCR."}],
                interceptors=[test_tracer], 
                trace_id=trace_id
            )
            await asyncio.sleep(0.01)
            is_success = test_tracer.started and test_tracer.ended
        except Exception as e:
            self.log.error(str(e)); is_success = False
            
        self._record(1, "Singular Tracing Isolation", is_success, t0, trace_id)
        return StreamTraceMsg()

    @step
    async def phase_stream_trace(self, msg: StreamTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 2] STREAM TRACING: Asynchronous Chunk Tracking")
        t0 = time.perf_counter()
        trace_id = f"e2e_stream_{uuid.uuid4().hex[:8]}"
        
        try:
            stream_tracer = DebugTracer()
            response_stream = await acompletion(
                model=self.target_model, 
                messages=[{"role": "user", "content": "Stream test for VCR."}],
                interceptors=[stream_tracer], 
                stream=True, 
                trace_id=trace_id
            )
            async for _ in response_stream: pass 
            
            await asyncio.sleep(0.01)
            is_success = stream_tracer.started and stream_tracer.ended
        except Exception as e:
            self.log.error(str(e)); is_success = False
            
        self._record(2, "Asynchronous Stream Tracking", is_success, t0, trace_id)
        return ErrorTraceMsg()

    @step
    async def phase_error_trace(self, msg: ErrorTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 3] ERROR TRACING: Exception Boundary Verification")
        t0 = time.perf_counter()
        trace_id = f"e2e_error_{uuid.uuid4().hex[:8]}"
        
        try:
            error_tracer = DebugTracer()
            try:
                await acompletion(
                    model="invalid/fake", 
                    messages=[{"role": "user", "content": "Trigger an error!"}], 
                    interceptors=[error_tracer], 
                    trace_id=trace_id
                )
            except Exception:
                pass
            
            await asyncio.sleep(0.01)
            is_success = error_tracer.started and not error_tracer.ended and error_tracer.error
        except Exception as e:
            self.log.error(str(e)); is_success = False
            
        self._record(3, "Exception Boundary Verification", is_success, t0, trace_id)
        return FallbackTraceMsg()

    @step
    async def phase_fallback_trace(self, msg: FallbackTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 4] FALLBACK TRACING: Idempotent Retry & Deepcopy Shield")
        t0 = time.perf_counter()
        trace_id = f"e2e_fallback_{uuid.uuid4().hex[:8]}"
        
        try:
            fallback_tracer = DebugTracer()
            response = await acompletion(
                model="invalid/will-fail", 
                messages=[{"role": "user", "content": "Fallback test via VCR"}],
                fallbacks=[self.target_model], 
                interceptors=[fallback_tracer],
                trace_id=trace_id
            )
            await asyncio.sleep(0.01)
            content = StateTraverser.resolve(response, "choices.0.message.content", "")
            is_shielded = fallback_tracer.started and fallback_tracer.ended and not fallback_tracer.error
            is_success = bool(content) and is_shielded
        except Exception as e:
            self.log.error(str(e)); is_success = False
            
        self._record(4, "Fallback & Deepcopy Shielding", is_success, t0, trace_id)
        return ReportMsg()

    @step
    async def generate_report(self, msg: ReportMsg) -> WorkflowMessage:
        self.log.info("\n" + "=" * 110)
        self.log.info("📊 [LLM TRACE PIPELINE] E2E VCR LATENCY BREAKDOWN REPORT")
        self.log.info("-" * 110)
        
        all_passed = True
        for res in self.all_results:
            if not res.success: all_passed = False
            target_label = f"[PHASE {res.phase_num}]".ljust(10)
            
            if self.config.mode in ("record", "replay") and res.llm_latency_ms > 0:
                breakdown = f"(I/O: {res.llm_latency_ms:7.2f}ms | Framework Overhead: {res.overhead_ms:7.2f}ms)"
            else:
                breakdown = f"(Overhead Only / Live: {res.overhead_ms:7.2f}ms)"

            self.log.info(
                f"{res.phase_num:02d}. {res.status_icon} | {target_label} | "
                f"{res.scenario.ljust(40)} | Total: {res.total_latency_ms:7.2f}ms {breakdown}"
            )
            
        self.log.info("-" * 110)
        if all_passed:
            self.log.info("🎉 ALL LLM TRACE PIPELINE SCENARIOS EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 E2E PIPELINE FAILED. Inspect structural logs for deviations.")
        self.log.info("=" * 110 + "\n")
        
        return StopMessage(result=all_passed)

    @step
    async def handle_rupture(self, msg: ErrorMessage) -> None:
        self.log.error(f"[{self.name}] 🚨 Fatal topological rupture: {msg.msg}")
        self.post_message(StopMessage(result=False))


"""[APP & SYSTEM ENTRY] 애플리케이션 바인딩 및 VCR 초기화"""
class LlmTraceApplication:
    def __init__(self, scope_kwargs: dict, run_context: dict):
        self.scope_kwargs = scope_kwargs
        self.run_context = run_context
        
        self.vcr_config: VCRPlaybackConfig = run_context.get("vcr_config")
        
        # [변경점] VCRInjector 유틸리티를 호출하여 프레임워크 전체에 VCR을 단 한 줄로 적용합니다.
        # 내부적으로 AdapterRegistry를 래핑하고 관리자(manager) 인스턴스를 반환합니다.
        manager = VCRInjector.apply(config=self.vcr_config, fixture_dir=FIXTURE_ROOT)
        
        # Workflow에서 결과를 분석할 수 있도록 manager를 컨텍스트에 담아 넘깁니다.
        self.run_context["vcr_manager"] = manager

    async def _startup_hook(self):
        async with managed_scope(**self.scope_kwargs):
            workflow = LlmTraceWorkflow("TraceSuiteApp", self.run_context)
            workflow_task = asyncio.create_task(workflow.run())
            await workflow.execute()
            await workflow_task
            
            if any(not res.success for res in workflow.all_results):
                log.error("🚨 Workflow finished with failures.")
                sys.exit(1)

    async def _teardown_hook(self):
        log.info("🧹 Reclaiming trace suite resources...")

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
    resolved_model = getattr(args, 'model', None) or os.environ.get("LLM_COMPAT_MODEL")
    use_proxy = getattr(args, 'proxy', False) or os.environ.get("LLM_COMPAT_PROXY", "false").lower() == "true"
    
    # CLI VCR 제어 인수 파싱
    vcr_mode = getattr(args, 'vcr', "live")
    vcr_speed = getattr(args, 'vcr_speed', "max")
    vcr_chaos = getattr(args, 'vcr_chaos', 0.0)
    
    if not is_online and vcr_mode != "replay":
        log.warning("🚨 [System Offline] Forcing fallback to Local Engine.")
        resolved_model = "ollama/local-gemma-3"
    elif not resolved_model:
        optimal = model_tier_registry.get_optimal_model(requires_tools=False, min_cognitive_score=2)
        resolved_model = f"{optimal[0]}/{optimal[1]}" if isinstance(optimal, tuple) else (f"gemini/{optimal}" if optimal else "gemini/gemini-3.1-flash-lite")

    scope_kwargs = {"use_proxy": use_proxy if is_online else False, "show_logs": True}
    
    # VCR Config 객체 생성
    vcr_config = VCRPlaybackConfig(
        mode=vcr_mode, 
        speed=vcr_speed, 
        chaos_latency_ms=vcr_chaos
    )
    
    return scope_kwargs, {
        "use_proxy": scope_kwargs["use_proxy"], 
        "target_model": resolved_model,
        "vcr_config": vcr_config
    }

def main(args: list[str] = None):
    parser = argparse.ArgumentParser(description="LLM Trace & Interceptor Suite Runner")
    parser.add_argument("-m", "--model", type=str, help="Target LLM model to use.")
    parser.add_argument("-p", "--proxy", action="store_true", help="Enable remote proxy extension layout.")
    parser.add_argument("--vcr", type=str, choices=["live", "record", "replay"], default="live", help="VCR mode for API Mocking")
    parser.add_argument("--vcr-speed", type=str, choices=["max", "real"], default="max", help="Replay speed control (max or real-time)")
    parser.add_argument("--vcr-chaos", type=float, default=0.0, help="Inject artificial latency jitter (in ms) during replay")
    
    args, _ = parser.parse_known_args(args)
    scope_kwargs, run_context = get_environment_context(args)
    app = LlmTraceApplication(scope_kwargs, run_context)
    app.execute()

if __name__ == "__main__":
    main()