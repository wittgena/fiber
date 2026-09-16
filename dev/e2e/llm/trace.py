# fiber.dev.e2e.llm.trace
from __future__ import annotations

import argparse
import asyncio
import os
import socket
import sys
import time
import uuid
from typing import List, Tuple, Optional, Any, Dict

from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.llm.pipeline import PipelineSlot
from fiber.llm.entry import acompletion
from fiber.llm.param import ModelResponse
from fiber.llm.context.metadata import ExecutionMetadata
from fiber.llm.model.tier import model_tier_registry
from fiber.phase.scope.manager import managed_scope

from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.llm.trace")
tracer_log = get_emitter("plugin.tracer")

class DebugTracer(BaseLLMTracer):
    """[Slot: PRE_OBSERVER] 실전형 비동기 방출 트레이서 (상세 시각화 로깅 지원)"""
    def __init__(self):
        self.started = False
        self.ended = False
        self.error = False
        self.duration = 0.0

    async def on_llm_start(self, meta: ExecutionMetadata, kwargs: Dict[str, Any]):
        self.started = True
        # 민감하지 않은(API 키 제외) 파라미터만 추출
        safe_kwargs = {k: v for k, v in kwargs.items() if k not in ["api_key", "headers", "interceptors", "pipeline_hooks"]}
        
        tracer_log.info(
            f"\n[🔍 LLM CALL INITIATED] \n"
            f" ├─ Trace ID : {meta.trace_id}\n"
            f" ├─ Model    : {meta.base_model}\n"
            f" ├─ Config   : {safe_kwargs.get('temperature', 0.7)} Temp, {safe_kwargs.get('max_tokens', 'Auto')} MaxTokens\n"
            f" └─ Messages : {len(kwargs.get('messages', []))} items"
        )

    async def on_llm_end(self, meta: ExecutionMetadata, response: Any, duration_ms: float):
        self.ended = True
        self.duration = duration_ms
        
        # [수정 1] dict 및 object(Pydantic) 타입 모두 안전하게 처리
        if isinstance(response, dict):
            usage = response.get("usage", {})
            total_tokens = usage.get("total_tokens", "N/A") if usage else "N/A"
            choices = response.get("choices", [])
        else:
            usage = getattr(response, "usage", None)
            total_tokens = getattr(usage, "total_tokens", "N/A") if usage else "N/A"
            choices = getattr(response, "choices", [])
        
        # 첫 번째 선택지(Choice)의 결과물 일부 노출 (스트리밍 방어 처리)
        content_preview = "[Streaming Content or Empty]"
        if choices and len(choices) > 0:
            first_choice = choices[0]
            if isinstance(first_choice, dict):
                content = first_choice.get("message", {}).get("content", "")
            else:
                msg_obj = getattr(first_choice, "message", None)
                if isinstance(msg_obj, dict):
                    content = msg_obj.get("content", "")
                else:
                    content = getattr(msg_obj, "content", "") if msg_obj else ""
            
            if content:
                content_preview = str(content)[:100].replace('\n', ' ') + "..."

        tracer_log.info(
            f"\n[✅ LLM CALL COMPLETED] \n"
            f" ├─ Trace ID : {meta.trace_id}\n"
            f" ├─ Latency  : {duration_ms:.2f} ms\n"
            f" ├─ Usage    : {total_tokens} tokens\n"
            f" └─ Response : {content_preview}"
        )

    async def on_llm_error(self, meta: ExecutionMetadata, exc: Exception, duration_ms: float):
        self.error = True
        tracer_log.error(
            f"\n[🚨 LLM CALL FAILED] \n"
            f" ├─ Trace ID : {meta.trace_id}\n"
            f" ├─ Latency  : {duration_ms:.2f} ms\n"
            f" └─ Error    : {type(exc).__name__} - {str(exc)}"
        )


class DummySemanticCache(DuplexChannel):
    """[Slot: PRE_TRANSLATE] I/O 숏서킷을 수행하는 모의 캐시"""
    target_slot = PipelineSlot.PRE_TRANSLATE

    async def write(self, ctx: ChannelContext, msg: dict):
        meta = ctx.get_attr("system_meta")
        tracer_log.info(f"💾 [CACHE HOOK] Validating Trace ID: {meta.trace_id if meta else 'N/A'}")
        
        prompt = msg.get("messages", [{}])[-1].get("content", "")
        
        # 특정 키워드가 있으면 실제 LLM을 타지 않고 가짜 응답을 즉시 반환 (Short-circuit)
        if "USE_CACHE" in prompt:
            tracer_log.info("🎯 [CACHE HIT] Short-circuiting physical I/O...")
            cached_response = ModelResponse(
                id=f"cache-{uuid.uuid4()}",
                model=msg.get("model", "cached-model"),
                choices=[{"index": 0, "message": {"role": "assistant", "content": "[CACHED] Hit!"}, "finish_reason": "stop"}],
                usage={"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            )
            await ctx.fire_channel_read(cached_response)
            return
            
        await ctx.fire_write(msg)


class DummyPIIGuardrail(DuplexChannel):
    """[Slot: POST_TRANSLATE] 검증된 객체 상태에서 민감 정보를 차단하는 가드레일"""
    target_slot = PipelineSlot.POST_TRANSLATE

    async def write(self, ctx: ChannelContext, processed_msg: Any):
        meta = ctx.get_attr("system_meta")
        tracer_log.info(f"🛡️ [GUARDRAIL HOOK] Inspecting Payload for Trace ID: {meta.trace_id if meta else 'N/A'}")
        
        original_kwargs = getattr(processed_msg, "original_kwargs", {})
        messages = original_kwargs.get("messages", [])
        
        for msg in messages:
            content = msg.get("content", "")
            if "SECRET-SSN" in content:
                tracer_log.error("🛑 [GUARDRAIL BLOCK] Sensitive Information (PII) Detected!")
                # 파이프라인 멈춤(Hang)을 방지하기 위해 명시적으로 에러 전파 후 즉시 리턴
                err = PermissionError("Guardrail Triggered: PII (SSN) detected.")
                await ctx.fire_exception_caught(err)
                return  
                
        await ctx.fire_write(processed_msg)


# =====================================================================
# [WORKFLOW] 테스트 메시지 및 위상 정의
# =====================================================================

class StartTraceMsg(WorkflowMessage): pass
class SemanticCacheMsg(WorkflowMessage): pass
class GuardrailMsg(WorkflowMessage): pass
class UnifiedFacadeMsg(WorkflowMessage): pass
class StreamTraceMsg(WorkflowMessage): pass
class ErrorTraceMsg(WorkflowMessage): pass


class LlmTraceWorkflow(Workflow):
    class Meta:
        trans_rules = {"error": ErrorMessage}

    def __init__(self, name: str, run_context: dict, **kwargs):
        super().__init__(name=name, timeout=300.0, **kwargs)
        self.target_model = run_context.get("target_model")
        self.log = log
        self.success_count = 0
        self.fail_count = 0

    async def execute(self) -> None:
        self.log.info(f"[{self.name}] 🚀 Igniting LLM Trace & Facade Suite (Primary Model: {self.target_model})")
        self.post_message(StartTraceMsg())
        await self.run()

    @step
    async def phase_tracer_only(self, msg: StartTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 1] PRE_OBSERVER: Async Tracer Isolation")
        try:
            test_tracer = DebugTracer()
            await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Hello, tracer."}],
                interceptors=[test_tracer],
                metadata={"kernel_auth": {"audit_hash": "audit_trace_01"}}
            )
            
            await asyncio.sleep(0.1) # Fire-and-forget 대기
            if test_tracer.started and test_tracer.ended:
                self.log.info(f"[{self.name}] ✅ Passed: Tracer executed asynchronously (Duration: {test_tracer.duration:.2f}ms).")
                self.success_count += 1
            else:
                raise ValueError("Tracer lifecycle hooks not fired.")
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1

        return SemanticCacheMsg()

    @step
    async def phase_semantic_cache(self, msg: SemanticCacheMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 2] PRE_TRANSLATE: Semantic Cache Short-circuit")
        try:
            start_time = time.time()
            test_cache = DummySemanticCache()
            
            response = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Hello, USE_CACHE."}],
                interceptors=[test_cache],
                metadata={"kernel_auth": {"audit_hash": "audit_cache_01"}}
            )
            
            elapsed = time.time() - start_time
            content = response.choices[0].message.content
            
            if content == "[CACHED] Hit!" and elapsed < 0.5:
                self.log.info(f"[{self.name}] ✅ Passed: Cache short-circuited payload instantly ({elapsed:.3f}s).")
                self.success_count += 1
            else:
                raise ValueError(f"Cache miss or invalid payload. Content: {content}")
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1

        return GuardrailMsg()

    @step
    async def phase_pii_guardrail(self, msg: GuardrailMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 3] POST_TRANSLATE: PII Security Guardrail")
        try:
            test_guardrail = DummyPIIGuardrail()
            try:
                await acompletion(
                    model=self.target_model,
                    messages=[{"role": "user", "content": "My secret is SECRET-SSN."}],
                    interceptors=[test_guardrail],
                    metadata={"kernel_auth": {"audit_hash": "audit_guard_01"}}
                )
                raise RuntimeError("Request passed the guardrail when it should have been blocked!")
            
            except PermissionError as pe:
                self.log.info(f"[{self.name}] ✅ Passed: Guardrail successfully blocked PII egress. ({pe})")
                self.success_count += 1

        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1

        return UnifiedFacadeMsg()

    @step
    async def phase_unified_facade(self, msg: UnifiedFacadeMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 4] UNIFIED FACADE: Auto-Routing Flat List Injection (Trace ID Propagation)")
        try:
            # 모든 플러그인을 순서 상관없이 한 번에 섞어서 던짐
            tracer = DebugTracer()
            cache = DummySemanticCache()
            guardrail = DummyPIIGuardrail()

            # 정상 요청 검증: 동일한 Trace ID가 캐시 -> 가드레일 -> 트레이서를 관통하며 로깅되는지 확인
            await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Process normal data."}],
                interceptors=[tracer, cache, guardrail], 
                metadata={"kernel_auth": {"audit_hash": "audit_unified_01"}}
            )
            
            await asyncio.sleep(0.1)
            if tracer.started and tracer.ended:
                self.log.info(f"[{self.name}] ✅ Passed: Unified Facade routed all plugins cleanly without collision.")
                self.success_count += 1
            else:
                raise ValueError("Tracer failed to record in Unified mode.")
                
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1

        return StreamTraceMsg()

    @step
    async def phase_stream_trace(self, msg: StreamTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 5] STREAM TRACING: Asynchronous Chunk Tracking")
        try:
            stream_tracer = DebugTracer()
            
            response_stream = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Count from 1 to 3."}],
                interceptors=[stream_tracer],
                stream=True,
                metadata={"kernel_auth": {"audit_hash": "audit_stream_01"}}
            )
            
            # 스트림 소비
            async for chunk in response_stream:
                pass 
                
            await asyncio.sleep(0.1)
            if stream_tracer.started and stream_tracer.ended:
                self.log.info(f"[{self.name}] ✅ Passed: Stream successfully tracked by tracer.")
                self.success_count += 1
            else:
                raise ValueError("Tracer failed on stream execution.")
                
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1
            
        return ErrorTraceMsg()

    @step
    async def phase_error_trace(self, msg: ErrorTraceMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] 🔄 [Phase 6] ERROR TRACING: Exception Boundary & Injection Testing")
        try:
            error_tracer = DebugTracer()
            
            try:
                # 존재하지 않는 모델을 던져 의도적 에러 유발
                await acompletion(
                    model="invalid/fake-model-999",
                    messages=[{"role": "user", "content": "Trigger an error!"}],
                    interceptors=[error_tracer]
                )
            except Exception:
                pass # 메인 파이프라인 에러는 무시. 우리는 트레이서가 예외를 잘 잡았는지만 확인합니다.
                
            await asyncio.sleep(0.1)
            # 시작은 했으나, 성공 종료(ended)되지 않고 에러 훅(error)이 트리거되었는지 확인
            if error_tracer.started and not error_tracer.ended and error_tracer.error:
                self.log.info(f"[{self.name}] ✅ Passed: Error correctly captured by tracer without pipeline crash.")
                self.success_count += 1
            else:
                raise ValueError("Tracer did not capture the error properly.")
                
        except Exception as e:
            self.log.error(f"[{self.name}] ❌ Failed: {e}")
            self.fail_count += 1
            
        return StopMessage(result=True)

    @step
    async def settle_and_terminate(self, msg: StopMessage) -> None:
        self.log.info("\n" + "="*60)
        self.log.info(f"🌌 [TRACE TOPOLOGY FINALIZED] Edge State: {'SUCCESS' if self.fail_count == 0 else 'FRACTURED'}")
        self.log.info(f"  Suite Results        : ✅ {self.success_count} / ❌ {self.fail_count}")
        self.log.info("="*60 + "\n")

    @step
    async def handle_rupture(self, msg: ErrorMessage) -> None:
        self.log.error(f"[{self.name}] 🚨 Fatal topological rupture: {msg.msg}")
        self.post_message(StopMessage(result=False))


# =====================================================================
# [APP & SYSTEM ENTRY] 애플리케이션 바인딩
# =====================================================================

class LlmTraceApplication:
    def __init__(self, scope_kwargs: dict, run_context: dict):
        self.scope_kwargs = scope_kwargs
        self.run_context = run_context
        self.workflow: Optional[LlmTraceWorkflow] = None

    async def _startup_hook(self):
        async with managed_scope(**self.scope_kwargs):
            self.workflow = LlmTraceWorkflow("TraceSuiteApp", self.run_context)
            workflow_task = asyncio.create_task(self.workflow.run())
            await self.workflow.execute()
            await workflow_task

            # [수정 2] raise RuntimeError 방지 -> Graceful Shutdown 처리
            if self.workflow.fail_count > 0:
                log.error(f"🚨 Workflow finished with {self.workflow.fail_count} failures.")

    async def _teardown_hook(self):
        if self.workflow:
            log.info("🧹 Reclaiming trace suite resources...")
            self.workflow.stop()

    def execute(self):
        log.info("🚀 Igniting Launcher Workflow via KernelReactor...")
        PhaseReactor.ignite(
            main_coro_func=self._startup_hook,
            teardown_hook=self._teardown_hook
        )
        
        # 비동기 루프 종료 후 실패가 있었다면 프로세스 종료 코드(exit 1) 반환
        if self.workflow and self.workflow.fail_count > 0:
            sys.exit(1)


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
        log.warning("🚨 [System Offline] Forcing fallback to Local Engine.")
        resolved_model = "ollama/local-gemma-3"
    elif not resolved_model:
        optimal = model_tier_registry.get_optimal_model(requires_tools=False, min_cognitive_score=2)
        resolved_model = f"{optimal[0]}/{optimal[1]}" if isinstance(optimal, tuple) else (f"gemini/{optimal}" if optimal else "gemini/gemini-3.1-flash-lite")

    scope_kwargs = {"use_proxy": use_proxy if is_online else False, "show_logs": True}
    run_context = {
        "use_proxy": scope_kwargs["use_proxy"], 
        "target_model": resolved_model,
    }
    return scope_kwargs, run_context

def main(args: list[str] = None):
    parser = argparse.ArgumentParser(description="LLM Trace & Interceptor Suite Runner")
    parser.add_argument("-m", "--model", type=str, help="Target LLM model to use.")
    parser.add_argument("--proxy", action="store_true", help="Enable remote proxy extension layout.")

    args, _ = parser.parse_known_args(args)
    scope_kwargs, run_context = get_environment_context(args)
    app = LlmTraceApplication(
        scope_kwargs=scope_kwargs,
        run_context=run_context
    )
    app.execute()

if __name__ == "__main__":
    main()