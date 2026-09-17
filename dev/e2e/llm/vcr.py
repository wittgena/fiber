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
from typing import List, Tuple, Optional, Any, Dict

from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.llm.pipeline import PipelineSlot
from fiber.llm.entry import acompletion
from fiber.llm.param import ModelResponse
from fiber.llm.context.metadata import ExecutionMetadata
from fiber.llm.model.tier import model_tier_registry
from fiber.phase.scope.manager import managed_scope
from fiber.dev.trace.llm.debugger import DebugTracer, DummySemanticCache, DummyPIIGuardrail
from fiber.dev.trace.llm.vcr import VCRManager, apply_vcr_patch
from fiber.llm.router.mapper.traverser import StateTraverser

from xphi.arch.contract.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

FIXTURE_ROOT = resolve_path("fixture")
log = get_emitter("e2e.llm.vcr")
tracer_log = get_emitter("plugin.tracer")

# =====================================================================
# [REPORT DTO] 명확한 가시성을 위한 결과 객체 (지연 시간 분리)
# =====================================================================
@dataclass
class TraceTestResult:
    phase_num: int
    scenario: str
    success: bool
    total_latency_ms: float = 0.0
    llm_latency_ms: float = 0.0

    @property
    def overhead_ms(self) -> float:
        # 전체 시간에서 LLM I/O 시간을 뺀 나머지 (VCR, 프레임워크, 강제 대기시간 등)
        return max(0.0, self.total_latency_ms - self.llm_latency_ms)

    @property
    def status_icon(self) -> str:
        return "✅ PASS" if self.success else "❌ FAIL"


# =====================================================================
# [WORKFLOW] 테스트 메시지 및 위상(Phase) 정의
# =====================================================================
class StartTraceMsg(WorkflowMessage): pass
class SemanticCacheMsg(WorkflowMessage): pass
class GuardrailMsg(WorkflowMessage): pass
class UnifiedFacadeMsg(WorkflowMessage): pass
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
        self.vcr_mode = run_context.get("vcr_mode", "live")
        self.log = log
        self.all_results: List[TraceTestResult] = []

    async def execute(self) -> None:
        self.log.info("=" * 100)
        self.log.info(f"🧪 [MASTER SUITE] Igniting LLM Trace & Facade Suite ({self.target_model}) [VCR: {self.vcr_mode.upper()}]")
        self.log.info("=" * 100)
        self.post_message(StartTraceMsg())
        await self.run()

    def _record(self, phase: int, scenario: str, success: bool, start_time: float, llm_latency: float = 0.0):
        total_latency = (time.time() - start_time) * 1000
        self.all_results.append(TraceTestResult(phase, scenario, success, total_latency, llm_latency))
        if success:
            self.log.info(f"[{self.name}] ✅ Passed: {scenario} (Total: {total_latency:.2f}ms)")
        else:
            self.log.error(f"[{self.name}] ❌ Failed: {scenario}")

    @step
    async def phase_tracer_only(self, msg: StartTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 1] PRE_OBSERVER: Async Tracer Isolation")
        t0 = time.time(); llm_latency = 0.0
        try:
            test_tracer = DebugTracer()
            
            llm_t0 = time.time()
            await acompletion(
                model=self.target_model, messages=[{"role": "user", "content": "Hello, tracer."}],
                interceptors=[test_tracer], metadata={"kernel_auth": {"audit_hash": "audit_trace_01"}, "vcr_phase": "phase_1"}
            )
            llm_latency = (time.time() - llm_t0) * 1000
            
            await asyncio.sleep(0.1)
            is_success = test_tracer.started and test_tracer.ended
        except Exception as e:
            self.log.error(str(e)); is_success = False
        self._record(1, "Async Tracer Isolation", is_success, t0, llm_latency)
        return SemanticCacheMsg()

    @step
    async def phase_semantic_cache(self, msg: SemanticCacheMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 2] PRE_TRANSLATE: Semantic Cache Short-circuit")
        t0 = time.time(); llm_latency = 0.0
        try:
            test_cache = DummySemanticCache()
            
            llm_t0 = time.time()
            response = await acompletion(
                model=self.target_model, messages=[{"role": "user", "content": "Hello, USE_CACHE."}],
                interceptors=[test_cache], metadata={"kernel_auth": {"audit_hash": "audit_cache_01"}, "vcr_phase": "phase_2"}
            )
            llm_latency = (time.time() - llm_t0) * 1000
            
            content = StateTraverser.resolve(response, "choices.0.message.content", "")
            is_success = content == "[CACHED] Hit!"
        except Exception as e:
            self.log.error(str(e)); is_success = False
        self._record(2, "Semantic Cache Short-circuit", is_success, t0, llm_latency)
        return GuardrailMsg()

    @step
    async def phase_pii_guardrail(self, msg: GuardrailMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 3] POST_TRANSLATE: PII Security Guardrail")
        t0 = time.time(); llm_latency = 0.0
        is_success = False
        try:
            test_guardrail = DummyPIIGuardrail()
            
            llm_t0 = time.time()
            await acompletion(
                model=self.target_model, messages=[{"role": "user", "content": "My secret is SECRET-SSN."}],
                interceptors=[test_guardrail], metadata={"kernel_auth": {"audit_hash": "audit_guard_01"}, "vcr_phase": "phase_3"}
            )
            llm_latency = (time.time() - llm_t0) * 1000
        except PermissionError:
            is_success = True
            llm_latency = (time.time() - llm_t0) * 1000  # 예외 발생까지 걸린 시간
        except Exception as e:
            self.log.error(str(e))
        self._record(3, "PII Security Guardrail Blocking", is_success, t0, llm_latency)
        return UnifiedFacadeMsg()

    @step
    async def phase_unified_facade(self, msg: UnifiedFacadeMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 4] UNIFIED FACADE: Auto-Routing Flat List Injection")
        t0 = time.time(); llm_latency = 0.0
        try:
            tracer = DebugTracer()
            
            llm_t0 = time.time()
            await acompletion(
                model=self.target_model, messages=[{"role": "user", "content": "Process normal data."}],
                interceptors=[tracer, DummySemanticCache(), DummyPIIGuardrail()], 
                metadata={"kernel_auth": {"audit_hash": "audit_unified_01"}, "vcr_phase": "phase_4"}
            )
            llm_latency = (time.time() - llm_t0) * 1000
            
            await asyncio.sleep(0.1)
            is_success = tracer.started and tracer.ended
        except Exception as e:
            self.log.error(str(e)); is_success = False
        self._record(4, "Unified Facade Routing", is_success, t0, llm_latency)
        return StreamTraceMsg()

    @step
    async def phase_stream_trace(self, msg: StreamTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 5] STREAM TRACING: Asynchronous Chunk Tracking")
        t0 = time.time(); llm_latency = 0.0
        try:
            stream_tracer = DebugTracer()
            
            llm_t0 = time.time()
            response_stream = await acompletion(
                model=self.target_model, messages=[{"role": "user", "content": "Stream test."}],
                interceptors=[stream_tracer], stream=True, 
                metadata={"kernel_auth": {"audit_hash": "audit_stream_01"}, "vcr_phase": "phase_5"}
            )
            # 스트림 전체 소비 완료 시점까지를 LLM 소요 시간으로 산정
            async for _ in response_stream: pass 
            llm_latency = (time.time() - llm_t0) * 1000
            
            await asyncio.sleep(0.1)
            is_success = stream_tracer.started and stream_tracer.ended
        except Exception as e:
            self.log.error(str(e)); is_success = False
        self._record(5, "Asynchronous Stream Tracking", is_success, t0, llm_latency)
        return ErrorTraceMsg()

    @step
    async def phase_error_trace(self, msg: ErrorTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 6] ERROR TRACING: Exception Boundary Verification")
        t0 = time.time(); llm_latency = 0.0
        try:
            error_tracer = DebugTracer()
            
            try:
                llm_t0 = time.time()
                await acompletion(
                    model="invalid/fake", messages=[{"role": "user", "content": "Error!"}], 
                    interceptors=[error_tracer], metadata={"vcr_phase": "phase_6"}
                )
            except Exception:
                llm_latency = (time.time() - llm_t0) * 1000
            
            await asyncio.sleep(0.1)
            is_success = error_tracer.started and not error_tracer.ended and error_tracer.error
        except Exception as e:
            self.log.error(str(e)); is_success = False
        self._record(6, "Exception Boundary Verification", is_success, t0, llm_latency)
        return FallbackTraceMsg()

    @step
    async def phase_fallback_trace(self, msg: FallbackTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 7] FALLBACK TRACING: Idempotent Retry & Deepcopy Shield")
        t0 = time.time(); llm_latency = 0.0
        try:
            fallback_tracer = DebugTracer()
            
            llm_t0 = time.time()
            response = await acompletion(
                model="invalid/will-fail", messages=[{"role": "user", "content": "Fallback test"}],
                fallbacks=[self.target_model], interceptors=[fallback_tracer],
                metadata={"kernel_auth": {"audit_hash": "audit_fallback_01"}, "vcr_phase": "phase_7"}
            )
            llm_latency = (time.time() - llm_t0) * 1000
            
            await asyncio.sleep(0.1)
            
            # [REFACTOR] StateTraverser를 이용한 우아한 데이터 추출 (Dict/Object 다형성 충돌 원천 차단)
            content = StateTraverser.resolve(response, "choices.0.message.content", "")

            is_shielded = fallback_tracer.started and fallback_tracer.ended and not fallback_tracer.error
            is_success = bool(content) and is_shielded
            
        except Exception as e:
            self.log.error(str(e)); is_success = False
            
        self._record(7, "Fallback & Deepcopy Shielding", is_success, t0, llm_latency)
        return ReportMsg()

    @step
    async def generate_report(self, msg: ReportMsg) -> WorkflowMessage:
        self.log.info("\n" + "=" * 100)
        self.log.info("📊 [LLM TRACE PIPELINE] E2E INTEGRATION REPORT (LATENCY BREAKDOWN)")
        self.log.info("-" * 100)
        
        all_passed = True
        for res in self.all_results:
            if not res.success: all_passed = False
            target_label = f"[PHASE {res.phase_num}]".ljust(10)
            
            # 레이턴시 분해 텍스트 조립
            if res.llm_latency_ms > 0:
                breakdown = f"(LLM: {res.llm_latency_ms:7.2f}ms + Overhead: {res.overhead_ms:7.2f}ms)"
            else:
                breakdown = f"(Overhead Only: {res.overhead_ms:7.2f}ms)"

            self.log.info(
                f"{res.phase_num:02d}. {res.status_icon} | {target_label} | "
                f"{res.scenario.ljust(35)} | Total: {res.total_latency_ms:7.2f}ms {breakdown}"
            )
            
        self.log.info("-" * 100)
        if all_passed:
            self.log.info("🎉 ALL LLM TRACE PIPELINE SCENARIOS EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 E2E PIPELINE FAILED. Inspect structural logs for deviations.")
        self.log.info("=" * 100 + "\n")
        
        return StopMessage(result=all_passed)

    @step
    async def handle_rupture(self, msg: ErrorMessage) -> None:
        self.log.error(f"[{self.name}] 🚨 Fatal topological rupture: {msg.msg}")
        self.post_message(StopMessage(result=False))


## [APP & SYSTEM ENTRY] 애플리케이션 바인딩 및 VCR 초기화
class LlmTraceApplication:
    def __init__(self, scope_kwargs: dict, run_context: dict):
        self.scope_kwargs = scope_kwargs
        self.run_context = run_context
        
        # [MODIFIED] FIXTURE_ROOT 하위로 경로 지정 및 fixture 파일명 동적 반영
        fixture_filename = run_context.get("fixture_filename", "vcr_fixtures.json")
        fixture_file = os.path.join(FIXTURE_ROOT, fixture_filename)
        
        self.vcr_manager = VCRManager(
            mode=run_context.get("vcr_mode", "live"),
            fixture_path=fixture_file
        )
        apply_vcr_patch(self.vcr_manager)

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
    vcr_mode = getattr(args, 'vcr', "live")
    
    # [MODIFIED] argparse에서 fixture 값을 가져옴
    fixture_filename = getattr(args, 'fixture', "vcr_fixtures.json")

    # Replay 모드일 때는 오프라인이어도 통과
    if not is_online and vcr_mode != "replay":
        log.warning("🚨 [System Offline] Forcing fallback to Local Engine.")
        resolved_model = "ollama/local-gemma-3"
    elif not resolved_model:
        optimal = model_tier_registry.get_optimal_model(requires_tools=False, min_cognitive_score=2)
        resolved_model = f"{optimal[0]}/{optimal[1]}" if isinstance(optimal, tuple) else (f"gemini/{optimal}" if optimal else "gemini/gemini-3.1-flash-lite")

    scope_kwargs = {"use_proxy": use_proxy if is_online else False, "show_logs": True}
    return scope_kwargs, {
        "use_proxy": scope_kwargs["use_proxy"], 
        "target_model": resolved_model,
        "vcr_mode": vcr_mode,
        "fixture_filename": fixture_filename  # [MODIFIED] run_context에 추가
    }

def main(args: list[str] = None):
    parser = argparse.ArgumentParser(description="LLM Trace & Interceptor Suite Runner")
    parser.add_argument("-m", "--model", type=str, help="Target LLM model to use.")
    parser.add_argument("--proxy", action="store_true", help="Enable remote proxy extension layout.")
    parser.add_argument("--vcr", type=str, choices=["live", "record", "replay"], default="live", help="VCR mode for API Mocking")
    parser.add_argument("--fixture", type=str, default="vcr_fixtures.json", help="Filename for the VCR fixture (stored under FIXTURE_ROOT)")
    
    args, _ = parser.parse_known_args(args)
    scope_kwargs, run_context = get_environment_context(args)
    app = LlmTraceApplication(scope_kwargs, run_context)
    app.execute()

if __name__ == "__main__":
    main()