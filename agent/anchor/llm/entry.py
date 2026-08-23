# agent.anchor.llm.entry
## @lineage: ator.driver.llm.entry
from __future__ import annotations

import asyncio
import httpx
import json
import time
import uuid
from typing import Any, Dict, List, Union

# Eco & Ator Models
from agent.anchor.llm.param import ModelResponse
from agent.anchor.llm.execution import ExecutionMetadata
from agent.anchor.model.types.general import EmbeddingResponse
from agent.loop.runtime.exception.mapping import exception_type
from phase.client.ext.llm.param.processor import CompletionProcessor, EmbeddingProcessor
from agent.llm.router.inter.registry import AdapterRegistry

# Arch & Watcher
from arch.topos.network.bridge import RpcBridge
from arch.topos.network.channel.pipeline import ChannelPipeline, ChannelContext, DuplexChannel
from watcher.plane.emitter import get_emitter

# Phase LLM Components
from agent.llm.stream.wrapper import StreamWrapper

# Loggers
log = get_emitter("runtime.entry")
log_handlers = get_emitter("executor.handlers")
log_pipeline = get_emitter("executor.pipeline")


# =========================================================================
# 1. Pipeline Middlewares (Handlers & Observers)
# =========================================================================

class ContextBinder(DuplexChannel):
    """최초 요청 진입 시 컨텍스트(ExecutionMetadata)를 세팅하고 고유 ID를 부여하는 핸들러"""
    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        if "call_id" not in msg:
            msg["call_id"] = str(uuid.uuid4())

        metadata = msg.get("metadata", {})
        session_id = msg.get("session_id") or metadata.get("session_id")
        trace_id = msg.get("trace_id") or metadata.get("trace_id")
        model = msg.get("model", "unknown")

        system_meta = ExecutionMetadata(
            session_id=session_id,
            trace_id=trace_id,
            call_id=msg["call_id"],
            metadata=metadata,
            base_model=model
        )
        
        # 파이프라인 전역 AttributeMap에 저장하여 하위 핸들러가 접근할 수 있게 함
        ctx.set_attr("system_meta", system_meta)
        ctx.set_attr("request_kwargs", msg)
        
        # 원본 msg에도 심어 하위 호환성 유지
        msg["system_meta"] = system_meta

        await ctx.fire_write(msg)


class ChannelObserver(DuplexChannel):
    """로깅, Trace, 소요 시간 기록을 담당하는 텔레메트리 핸들러"""
    def __init__(self):
        self.emitter = get_emitter("executor.telemetry", phase="LLM_CALL")

    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        ctx.set_attr("start_time", time.time())
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        
        model = msg.get("model", "unknown")
        provider = msg.get("custom_llm_provider", "unknown")

        self.emitter.trace(
            "LLM Request Initiated",
            model=model, provider=provider,
            trace_id=meta.trace_id, session_id=meta.session_id
        )
        await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs")
        
        duration_ms = (time.time() - ctx.get_attr("start_time")) * 1000
        meta.framework_flags["duration_ms"] = duration_ms

        usage = getattr(msg, "usage", None)
        usage_dict = usage.model_dump() if hasattr(usage, "model_dump") else (usage or {})

        self.emitter.info(
            "LLM Request Completed Successfully",
            model=meta.base_model, provider=req.get("custom_llm_provider"),
            duration_ms=duration_ms, usage=usage_dict, trace_id=meta.trace_id
        )
        await ctx.fire_channel_read(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs", {})
        duration_ms = (time.time() - ctx.get_attr("start_time", time.time())) * 1000

        self.emitter.error(
            f"LLM Request Failed: {type(exc).__name__} - {str(exc)}",
            model=meta.base_model if meta else "unknown",
            provider=req.get("custom_llm_provider", "unknown"),
            duration_ms=duration_ms, trace_id=meta.trace_id if meta else None,
            exc_info=True
        )
        await ctx.fire_exception_caught(exc)


class MockBypass(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: dict):
        mock_delay = msg.get("mock_delay")
        if mock_delay and (msg.get("mock_response") or msg.get("mock_tool_calls")):
            await asyncio.sleep(mock_delay)

        if msg.get("mock_timeout") is True:
            timeout = msg.get("timeout", 0)
            if isinstance(timeout, (int, float)):
                await asyncio.sleep(timeout)
            elif isinstance(timeout, httpx.Timeout) and timeout.connect is not None:
                await asyncio.sleep(timeout.connect)
                
            await ctx.fire_exception_caught(TimeoutError("This is a mock timeout error"))
            return

        mock_response = msg.get("mock_response")
        if mock_response:
            mock_res = {"choices": [{"message": {"content": mock_response}}]}
            await ctx.fire_channel_read(mock_res)
            return

        await ctx.fire_write(msg)


class PromptTransformer(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: dict):
        prompt_id = msg.get("prompt_id")
        if prompt_id:
            try:
                log_handlers.debug("Prompt Management requested", prompt_id=prompt_id)
            except Exception as e:
                log_handlers.error("Failed to resolve dynamic prompt", error=str(e))
                await ctx.fire_exception_caught(e)
                return
                
        if msg.get("tools") is not None:
            if len(msg.get("tools", [])) == 0:
                log_handlers.debug("[DEBUG-PROMPT-TRANSFORMER] 빈 tools 리스트가 감지되어 None으로 초기화합니다.")
                msg["tools"] = None
            else:
                log_handlers.debug(f"[DEBUG-PROMPT-TRANSFORMER] {len(msg.get('tools'))}개의 tool이 감지되었습니다.")
        await ctx.fire_write(msg)


class FallbackHandler(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: dict):
        fallbacks = msg.pop("fallbacks", [])
        if fallbacks:
            ctx.set_attr("fallbacks", fallbacks)
            ctx.set_attr("original_msg", msg.copy())
        
        await ctx.fire_write(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        fallbacks = ctx.get_attr("fallbacks", [])
        if not fallbacks:
            await ctx.fire_exception_caught(exc)
            return

        next_fallback = fallbacks.pop(0)
        retry_msg = ctx.get_attr("original_msg").copy()
        if isinstance(next_fallback, dict):
            fallback_config = next_fallback.copy()
            retry_msg["model"] = fallback_config.pop("model", retry_msg.get("model"))
            retry_msg.update(fallback_config)
        else:
            retry_msg["model"] = next_fallback

        log_handlers.warning(f"Fallback attempt triggered. Retrying with model: {retry_msg['model']}", error=str(exc))
        await ctx.fire_write(retry_msg)


class PayloadTranslator(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: dict):
        try:
            model = msg.get("model")
            tools_data = msg.get("tools")
            if tools_data:
                log_handlers.debug(
                    "[DEBUG-PRE-TRANSLATOR] 전달된 원시 tools 스키마:\n"
                    f"{json.dumps(tools_data, ensure_ascii=False, indent=2)}"
                )
            
            if msg.get("aembedding") is True:
                input_data = msg.get("input", [])
                processor = EmbeddingProcessor(model=model, input_data=input_data, kwargs=msg)
                processed_ctx = processor.build()
            else:
                messages = msg.get("messages", [])
                processor = CompletionProcessor(model=model, messages=messages, kwargs=msg)
                processed_ctx = processor.build()
                
            if not msg.get("aembedding") and hasattr(processed_ctx, "original_kwargs"):
                post_tools = processed_ctx.original_kwargs.get("tools")
                if post_tools:
                    log_handlers.debug("[DEBUG-POST-TRANSLATOR] CompletionProcessor 빌드 성공. Tools 속성 유지됨.")

            ctx.set_attr("processed_context", processed_ctx)
            await ctx.fire_write(processed_ctx)
        except Exception as e:
            log_handlers.error("Payload translation failed", error=str(e), exc_info=True)
            await ctx.fire_exception_caught(e)


class StreamAggregator(DuplexChannel):
    """스트리밍 응답 래핑(StreamWrapper) 및 complete_response 옵션 처리를 담당하는 핸들러"""
    def _is_streaming(self, req: Dict[str, Any]) -> bool:
        call_type = req.get("call_type", "")
        if req.get("stream") is True:
            return True
        return "stream" in call_type

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req: dict = ctx.get_attr("request_kwargs", {})
        
        # 스트리밍 요청에 대한 처리
        if self._is_streaming(req):
            meta: ExecutionMetadata = ctx.get_attr("system_meta")
            
            # 1. StreamWrapper 바인딩 (이미 래핑된 객체가 아니면 생성)
            stream_wrapper = msg if isinstance(msg, StreamWrapper) else StreamWrapper(
                completion_stream=msg,
                model=meta.base_model,
                system_meta=meta,
                custom_llm_provider=req.get("custom_llm_provider"),
                stream_options=req.get("stream_options"),
            )

            # 2. Complete Response 요구 시 스트림 조기 소진 (Exhaust)
            if req.get("complete_response") is True:
                async for _ in stream_wrapper:
                    pass
                complete_res = stream_wrapper.pipeline.attributes["accumulator"].get_complete_response()
                # 토큰 보정 등의 처리가 완료된 단일 응답 객체를 상위로 반환
                await ctx.fire_channel_read(complete_res)
                return

            await ctx.fire_channel_read(stream_wrapper)
        else:
            await ctx.fire_channel_read(msg)


# =========================================================================
# 2. Pipeline Transports (Head Endpoints)
# =========================================================================

class CompletionTransport(DuplexChannel):
    """LLM 텍스트 생성(Completion) 파이프라인의 종단점"""
    async def write(self, ctx: ChannelContext, msg: Any):
        try:
            log.debug("Core Completion Transport 진입", model=msg.model, provider=msg.custom_llm_provider)
            adapter = AdapterRegistry.get_adapter(task_type="llm", provider_name=msg.custom_llm_provider)
            
            if asyncio.iscoroutinefunction(adapter.execute):
                response = await adapter.execute(msg)
            else:
                response = adapter.execute(msg)
            await ctx.fire_channel_read(response)
            
        except Exception as e:
            error_kwargs = {"model": msg.model, "messages": msg.messages, **msg.original_kwargs}
            mapped_exc = exception_type(
                model=msg.model,
                custom_llm_provider=msg.custom_llm_provider,
                original_exception=e,
                completion_kwargs=error_kwargs,
                extra_kwargs=msg.original_kwargs
            )
            await ctx.fire_exception_caught(mapped_exc)


class EmbeddingTransport(DuplexChannel):
    """텍스트 임베딩(Embedding) 파이프라인의 종단점"""
    async def write(self, ctx: ChannelContext, msg: Any):
        try:
            log.debug("Core Embedding Transport 진입", model=msg.model, provider=msg.custom_llm_provider)
            adapter = AdapterRegistry.get_adapter(task_type="embedding", provider_name=msg.custom_llm_provider)
            
            if asyncio.iscoroutinefunction(adapter.execute):
                response = await adapter.execute(msg)
            else:
                response = adapter.execute(msg)
                
            # 내부 파라미터 은닉 처리 (LiteLLM 호환성 유지)
            if isinstance(response, EmbeddingResponse) and hasattr(response, "_hidden_params"):
                response._hidden_params["custom_llm_provider"] = msg.custom_llm_provider
                
            await ctx.fire_channel_read(response)
            
        except Exception as e:
            log.error(f"[bound.embedding] 임베딩 코어 엔진 예외 발생: {str(e)}")
            error_kwargs = {"model": msg.model, "input": msg.input, **msg.original_kwargs}
            mapped_exc = exception_type(
                model=msg.model, 
                custom_llm_provider=msg.custom_llm_provider, 
                original_exception=e,
                completion_kwargs=error_kwargs, 
                extra_kwargs=msg.original_kwargs,
            )
            await ctx.fire_exception_caught(mapped_exc)


# =========================================================================
# 3. Bootstrap (Factory) & Sync Helper
# =========================================================================

class PipelineBootstrap:
    """동시성 충돌 방지를 위해 요청당 독립된 파이프라인을 생성 및 실행하는 Factory"""

    @classmethod
    async def execute_completion(cls, model: str, messages: List, **kwargs) -> Union[ModelResponse, StreamWrapper, Any]:
        pipeline = ChannelPipeline()
        bridge = RpcBridge()
        
        # --- [Head] ---
        pipeline.add_last(CompletionTransport())
        # --- [Middle] ---
        pipeline.add_last(StreamAggregator())       # Completion 특화 (스트림 누적)
        pipeline.add_last(PayloadTranslator())
        pipeline.add_last(FallbackHandler())
        pipeline.add_last(PromptTransformer())      # Completion 특화 (프롬프트 변환)
        pipeline.add_last(MockBypass())
        pipeline.add_last(ChannelObserver())
        pipeline.add_last(ContextBinder())
        # --- [Tail] ---
        pipeline.add_last(bridge)

        await pipeline.fire_channel_active()
        payload = {"model": model, "messages": messages, "acompletion": True, **kwargs}
        return await bridge.request(payload, timeout=kwargs.get("timeout", 60.0))

    @classmethod
    async def execute_embedding(cls, model: str, input_data: Union[str, List[str]], **kwargs) -> EmbeddingResponse:
        pipeline = ChannelPipeline()
        bridge = RpcBridge()
        
        # --- [Head] ---
        pipeline.add_last(EmbeddingTransport())
        # --- [Middle] (Embedding은 Stream과 Prompt 변환이 불필요) ---
        pipeline.add_last(PayloadTranslator())
        pipeline.add_last(FallbackHandler())
        pipeline.add_last(MockBypass())
        pipeline.add_last(ChannelObserver())
        pipeline.add_last(ContextBinder())
        # --- [Tail] ---
        pipeline.add_last(bridge)

        await pipeline.fire_channel_active()
        payload = {"model": model, "input": input_data, "aembedding": True, **kwargs}
        return await bridge.request(payload, timeout=kwargs.get("timeout", 60.0))


def _run_sync(coro: Any) -> Any:
    """이벤트 루프 안전 처리를 위한 동기화 래퍼 헬퍼 (DRY)"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        try:
            import nest_asyncio
            nest_asyncio.apply()
        except ImportError:
            log.warning("[System] nest_asyncio is required to safely run a sync wrapper inside an active event loop.")
        return loop.run_until_complete(coro)
    else:
        return asyncio.run(coro)


# =========================================================================
# 4. Global Entrypoints
# =========================================================================

async def acompletion(model: str, messages: List = None, **kwargs) -> Any:
    """비동기 LLM 호출 진입점"""
    messages = messages or []
    return await PipelineBootstrap.execute_completion(model, messages, **kwargs)

def completion(model: str, messages: List = None, **kwargs) -> Any:
    """동기 LLM 호출 진입점"""
    messages = messages or []
    return _run_sync(acompletion(model, messages, **kwargs))

async def aembedding(*args, **kwargs) -> EmbeddingResponse:
    """비동기 임베딩 호출 진입점"""
    model = args[0] if len(args) > 0 else kwargs.get("model")
    input_data = kwargs.get("input", [])
    
    if not model:
        raise ValueError("model param not passed in.")
        
    return await PipelineBootstrap.execute_embedding(model=model, input_data=input_data, **kwargs)

def embedding(*args, **kwargs) -> EmbeddingResponse:
    """동기 임베딩 호출 진입점"""
    return _run_sync(aembedding(*args, **kwargs))