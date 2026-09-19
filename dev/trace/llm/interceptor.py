# fiber.dev.trace.llm.interceptor
import asyncio
import copy
import time
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union, Optional

from fiber.gateway.llm.pipeline import PipelineSlot
from fiber.gateway.llm.context.metadata import ExecutionMetadata
from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("llm.tracer")


# =========================================================================
# [1] LLM Tracer Base Interface & Interceptor Channel
# =========================================================================

class BaseLLMTracer(ABC):
    @abstractmethod
    async def on_llm_start(self, meta: ExecutionMetadata, kwargs: Dict[str, Any]):
        pass

    @abstractmethod
    async def on_llm_end(self, meta: ExecutionMetadata, response: Any, duration_ms: float):
        pass

    @abstractmethod
    async def on_llm_error(self, meta: ExecutionMetadata, exc: Exception, duration_ms: float):
        pass


class TracerInterceptorChannel(DuplexChannel):
    """
    파이프라인에 장착되어 메인 비즈니스 로직(LLM 호출)을 블로킹하지 않고, 
    비동기적(Fire-and-forget)으로 관측 데이터를 외부로 방출하는 채널.
    """
    
    target_slot = PipelineSlot.PRE_OBSERVER

    def __init__(self, tracers: List[BaseLLMTracer]):
        self.tracers = tracers

    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        ctx.set_attr("tracer_start_time", time.time())
        meta = ctx.get_attr("system_meta")
        
        # 원본 msg 변이 방지를 위해 deepcopy 후 비동기 태스크로 던짐
        safe_kwargs = copy.deepcopy(msg)
        for tracer in self.tracers:
            asyncio.create_task(self._safe_execute(tracer.on_llm_start, meta, safe_kwargs))
            
        await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        start_time = ctx.get_attr("tracer_start_time", time.time())
        duration_ms = (time.time() - start_time) * 1000
        meta = ctx.get_attr("system_meta")
        
        for tracer in self.tracers:
            asyncio.create_task(self._safe_execute(tracer.on_llm_end, meta, msg, duration_ms))
            
        await ctx.fire_channel_read(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        start_time = ctx.get_attr("tracer_start_time", time.time())
        duration_ms = (time.time() - start_time) * 1000
        meta = ctx.get_attr("system_meta")
        
        for tracer in self.tracers:
            asyncio.create_task(self._safe_execute(tracer.on_llm_error, meta, exc, duration_ms))
            
        await ctx.fire_exception_caught(exc)

    async def _safe_execute(self, func, *args):
        try:
            await func(*args)
        except Exception as e:
            # 트레이서 내부의 에러가 메인 파이프라인(LLM 스트림 등)을 붕괴시키지 않도록 격리
            log.warning(f"[Tracer] Custom tracer '{func.__self__.__class__.__name__}' fractured: {e}")


# =========================================================================
# [2] LLM Trace Profile & Test Utilities
# =========================================================================

class LlmTraceProfile:
    """
    @desc: TracerInterceptorChannel의 동작 무결성을 검증하고 다양한 Trace 엣지 케이스를 시뮬레이션하기 위한 프로파일.
    """
    def __init__(self, target_model: str):
        self.target_model = target_model

    def _normalize_response(self, response: Union[Dict[str, Any], Any]) -> Dict[str, Any]:
        """Pydantic 객체와 일반 Dict 형식을 하나의 인터페이스로 정규화합니다."""
        if isinstance(response, dict):
            return response
        if hasattr(response, "model_dump"):
            return response.model_dump(exclude_none=True)
        if hasattr(response, "__dict__"):
            return response.__dict__
        return dict(response)

    # =========================================================================
    # [시나리오 1] Mock Bypass (파이프라인 Short-circuit 검증)
    # =========================================================================
    def build_mock_bypass_payload(self) -> Dict[str, Any]:
        return {
            "model": self.target_model,
            "messages": [{"role": "user", "content": "Cost me nothing!"}],
            "mock_response": "This is a bypassed mock response.",
            "mock_delay": 0.1,
            "stream": False,
            "metadata": {"kernel_auth": {"audit_hash": "audit_mock_shared"}}
        }

    def verify_mock_bypass(self, response: Union[Dict[str, Any], Any]) -> bool:
        res_dict = self._normalize_response(response)
        try:
            choices = res_dict.get("choices", [{}])
            if not choices:
                raise ValueError("Trace Miss: Choices array is empty.")
                
            content = choices[0].get("message", {}).get("content", "")
            if "This is a bypassed mock response." not in content:
                raise ValueError(f"Trace Miss: Expected mock string not found. Got: {content}")
            return True
        except (IndexError, AttributeError) as e:
            raise ValueError(f"Trace Miss: Invalid response structure. {e}")

    ## [phase.2] Fuel Breaker
    def build_fuel_breaker_payload(self, budget: int = 5) -> Dict[str, Any]:
        return {
            "model": self.target_model,
            "messages": [{"role": "user", "content": "Write a very long essay about the universe."}],
            "stream": True,
            "metadata": {"kernel_auth": {"fuel_budget": budget}}
        }

    def extract_stream_finish_reason(self, chunk: Union[Dict[str, Any], Any]) -> Optional[str]:
        """스트리밍 청크에서 finish_reason을 추출합니다."""
        chunk_dict = self._normalize_response(chunk)
        choices = chunk_dict.get("choices", [])
        if choices and isinstance(choices, list) and len(choices) > 0:
            # Delta 구조 또는 Message 구조 모두 대응
            return choices[0].get("finish_reason")
        return None

    # =========================================================================
    # [시나리오 3] Fallback Provenance (동적 우회 추적성 검증)
    # =========================================================================
    def build_fallback_payload(self, fallbacks: List[Any]) -> Dict[str, Any]:
        return {
            "model": "invalid-trigger-model", 
            "messages": [{"role": "user", "content": "Trigger fallback mechanism"}],
            "fallbacks": fallbacks,
            "stream": False,
            "metadata": {"kernel_auth": {"audit_hash": "audit_fallback_shared"}}
        }

    def verify_fallback(self, response: Union[Dict[str, Any], Any], expected_models: List[str]) -> bool:
        res_dict = self._normalize_response(response)
        actual_model = res_dict.get("model", "")
        
        # 모델명 검증
        model_matched = any(em in actual_model for em in expected_models)
        if not model_matched:
             raise ValueError(f"Trace Miss: Fallback model mismatch. Actual '{actual_model}' not in expected {expected_models}.")
             
        # E2E Trace 환경일 경우, 메타데이터에 Graph가 기록되었는지 검증
        provider_metadata = res_dict.get("provider_metadata") or {}
        trace_graph = provider_metadata.get("trace_graph", [])
        
        if trace_graph:
            fallback_traced = any(t.get("stage") == "FallbackHandler" for t in trace_graph)
            if not fallback_traced:
                raise ValueError("Trace Miss: Fallback occurred but was not traced in provider_metadata.trace_graph.")
                
        return True

    # =========================================================================
    # [시나리오 4] Prompt Mutation (부수효과 추적성 검증)
    # =========================================================================
    def build_prompt_mutation_payload(self) -> Dict[str, Any]:
        return {
            "model": self.target_model,
            "messages": [{"role": "user", "content": "Testing prompt transformer"}],
            "tools": [],  # 빈 리스트 주입 (의도적 Mutation 유발)
            "stream": False,
            "metadata": {"kernel_auth": {"audit_hash": "audit_mutation_shared"}}
        }

    def verify_prompt_mutation(self, response: Union[Dict[str, Any], Any]) -> bool:
        res_dict = self._normalize_response(response)
        provider_metadata = res_dict.get("provider_metadata") or {}
        trace_graph = provider_metadata.get("trace_graph", [])
        
        if trace_graph:
            mutation_traced = any(
                t.get("stage") == "PromptTransformer" and t.get("action") == "tools_nulled" 
                for t in trace_graph
            )
            if not mutation_traced:
                raise ValueError("Trace Miss: Prompt mutation (empty tools -> None) was not traced in breadcrumbs.")
        
        return True