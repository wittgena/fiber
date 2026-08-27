# fiber.llm.entry
from __future__ import annotations

import asyncio
import httpx
import json
import time
import uuid
from typing import Any, Dict, List, Union

# Eco & Ator Models
from fiber.llm.param import ModelResponse
from fiber.llm.execution import ExecutionMetadata
from fiber.llm.model.types.general import EmbeddingResponse
from fiber.llm.exception.mapping import exception_type
from fiber.llm.stream.wrapper import StreamWrapper

from fiber.dphi.model.ext.llm.param.processor import CompletionProcessor, EmbeddingProcessor
from fiber.dphi.model.inter.registry import AdapterRegistry

# Arch & Watcher
from xphi.kernel.dphi.schema import DphiKey, KernelAuthPayload
from xphi.kernel.space.topos.network.bridge import RpcBridge
from xphi.kernel.space.topos.network.channel.pipeline import ChannelPipeline, ChannelContext, DuplexChannel
from xphi.watcher.plane.emitter import get_emitter

# Loggers
log = get_emitter("runtime.entry")
log_handlers = get_emitter("executor.handlers")
log_pipeline = get_emitter("executor.pipeline")

"""Pipeline Middlewares (Handlers & Observers)"""
class DphiFuelInterceptor(DuplexChannel):
    """[DPHI] Kernel 인가 정보 관리, 연료(Fuel) 측정 및 물리적 트랩(Kill-switch)을 전담하는 핸들러"""
    
    async def write(self, ctx: ChannelContext, msg: Any):
        # [🛠️ 핵심 개선] PayloadTranslator에 의해 객체화된 msg 대신, ContextBinder가 저장해둔 원시 딕셔너리 사용
        req_kwargs = ctx.get_attr("request_kwargs", {})
        metadata = req_kwargs.get("metadata", {})
        
        ## 공통 키 상수를 통해 접근 및 Pydantic 객체화로 타입 안정성 확보
        raw_auth = metadata.get(DphiKey.KERNEL_AUTH.value, {})
        kernel_auth = KernelAuthPayload(**raw_auth) if isinstance(raw_auth, dict) else (raw_auth or KernelAuthPayload())
        
        ctx.set_attr(DphiKey.FUEL_BUDGET.value, kernel_auth.fuel_budget)
        ctx.set_attr(DphiKey.FUEL_CONSUMED.value, 0)
        ctx.set_attr(DphiKey.AUDIT_HASH.value, kernel_auth.audit_hash)

        await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req: dict = ctx.get_attr("request_kwargs", {})
        is_stream = req.get("stream", False) or "stream" in req.get("call_type", "")
        
        fuel_budget = ctx.get_attr(DphiKey.FUEL_BUDGET.value, float('inf'))
        audit_hash = ctx.get_attr(DphiKey.AUDIT_HASH.value)

        ## 1. 비스트리밍 단일 응답: 토큰 카운트를 연료로 환산 & Audit Hash 은닉 탈취
        if not is_stream and not hasattr(msg, "__aiter__"):
            usage = getattr(msg, "usage", None)
            if usage and hasattr(usage, "total_tokens"):
                ctx.set_attr(DphiKey.FUEL_CONSUMED.value, getattr(usage, "total_tokens", 0))

            if audit_hash and hasattr(msg, "system_fingerprint"):
                msg.system_fingerprint = audit_hash

        ## 2. 스트리밍 응답: Kinetic Membrane 가동 (물리적 차단)
        elif hasattr(msg, "__aiter__") and fuel_budget < float('inf'):
            msg = self._fuel_trap_generator(msg, fuel_budget, ctx)

        await ctx.fire_channel_read(msg)

    async def _fuel_trap_generator(self, raw_stream, budget, context):
        """스트림 청크를 가로채어 연료를 차감하고, 파산 시 물리적으로 커넥션을 절단합니다."""
        consumed = 0
        try:
            async for raw_chunk in raw_stream:
                # 간소화된 연료 계산: 1 Chunk = 1 Fuel
                consumed += 1 
                context.set_attr(DphiKey.FUEL_CONSUMED.value, consumed)
                
                if consumed > budget:
                    log_pipeline.warning(f"[DPHI_TRAP] Fuel exhausted ({budget}). Killing stream physically.")
                    break
                yield raw_chunk
        except Exception as e:
            log_pipeline.error(f"Stream interrupted during fuel metering: {e}")
            raise

class ContextBinder(DuplexChannel):
    """최초 요청 진입 시 순수 실행 컨텍스트(ExecutionMetadata)만 세팅하는 핸들러 (SRP 준수)"""
    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        if "call_id" not in msg:
            msg["call_id"] = str(uuid.uuid4())

        ctx.set_attr("trace_errors", msg.get("trace_errors", False))
        metadata = msg.get("metadata", {})
        
        system_meta = ExecutionMetadata(
            session_id=msg.get("session_id") or metadata.get("session_id"),
            trace_id=msg.get("trace_id") or metadata.get("trace_id"),
            call_id=msg["call_id"],
            metadata=metadata,
            base_model=msg.get("model", "unknown")
        )
        
        ctx.set_attr("system_meta", system_meta)
        ctx.set_attr("request_kwargs", msg)
        msg["system_meta"] = system_meta
        
        await ctx.fire_write(msg)

class ChannelObserver(DuplexChannel):
    """텔레메트리 및 로깅 전담 핸들러"""
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

        ## DphiFuelInterceptor가 갱신해둔 상태를 키 상수로 조회만 수행
        fuel_consumed = ctx.get_attr(DphiKey.FUEL_CONSUMED.value, 0)
        fuel_budget = ctx.get_attr(DphiKey.FUEL_BUDGET.value, float('inf'))
        audit_hash = ctx.get_attr(DphiKey.AUDIT_HASH.value)

        self.emitter.info(
            "LLM Request Completed Successfully (Sealed)",
            model=meta.base_model, provider=req.get("custom_llm_provider"),
            duration_ms=duration_ms, 
            usage=usage_dict,                                 
            trace_id=meta.trace_id,
            kernel_fuel={                                     
                "consumed": fuel_consumed,
                "budget": fuel_budget if fuel_budget != float('inf') else None
            },
            audit_hash=audit_hash
        )
        await ctx.fire_channel_read(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs", {})
        duration_ms = (time.time() - ctx.get_attr("start_time", time.time())) * 1000
        show_trace = ctx.get_attr("trace_errors", False)

        self.emitter.error(
            f"LLM Request Failed: {type(exc).__name__} - {str(exc)}",
            model=meta.base_model if meta else "unknown",
            provider=req.get("custom_llm_provider", "unknown"),
            duration_ms=duration_ms, trace_id=meta.trace_id if meta else None,
            exc_info=show_trace
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
            mock_res_dict = {
                "id": f"chatcmpl-mock-{uuid.uuid4().hex[:8]}",
                "model": msg.get("model", "mock-model"),
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": mock_response
                        },
                        "finish_reason": "stop"
                    }
                ],
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0
                }
            }
            mock_res = ModelResponse(**mock_res_dict)
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
            show_trace = ctx.get_attr("trace_errors", False)
            log_handlers.error("Payload translation failed", error=str(e), exc_info=show_trace)
            await ctx.fire_exception_caught(e)

class StreamAggregator(DuplexChannel):
    """스트리밍 응답 래핑(StreamWrapper) 전담 핸들러"""
    def _is_streaming(self, req: Dict[str, Any]) -> bool:
        call_type = req.get("call_type", "")
        if req.get("stream") is True:
            return True
        return "stream" in call_type

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req: dict = ctx.get_attr("request_kwargs", {})
        
        if self._is_streaming(req):
            meta: ExecutionMetadata = ctx.get_attr("system_meta")
            
            ## CompletionTransport (혹은 DphiFuelInterceptor)에서 올라온 스트림을 래핑
            stream_wrapper = msg if isinstance(msg, StreamWrapper) else StreamWrapper(
                completion_stream=msg,
                model=meta.base_model,
                system_meta=meta,
                custom_llm_provider=req.get("custom_llm_provider"),
                stream_options=req.get("stream_options"),
            )

            if req.get("complete_response") is True:
                async for _ in stream_wrapper:
                    pass
                complete_res = stream_wrapper.pipeline.attributes["accumulator"].get_complete_response()
                await ctx.fire_channel_read(complete_res)
                return

            await ctx.fire_channel_read(stream_wrapper)
        else:
            await ctx.fire_channel_read(msg)

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

class PipelineBootstrap:
    """동시성 충돌 방지를 위해 요청당 독립된 파이프라인을 생성 및 실행하는 Factory"""
    @classmethod
    async def execute_completion(cls, model: str, messages: List, **kwargs) -> Union[ModelResponse, StreamWrapper, Any]:
        pipeline = ChannelPipeline()
        bridge = RpcBridge()
        
        ## Head
        pipeline.add_last(CompletionTransport())
        pipeline.add_last(DphiFuelInterceptor())

        ## Middle
        pipeline.add_last(StreamAggregator())       
        pipeline.add_last(PayloadTranslator())
        pipeline.add_last(FallbackHandler())
        pipeline.add_last(PromptTransformer())      
        pipeline.add_last(MockBypass())
        pipeline.add_last(ChannelObserver())        
        pipeline.add_last(ContextBinder())          

        ## Tail
        pipeline.add_last(bridge)

        await pipeline.fire_channel_active()
        payload = {"model": model, "messages": messages, "acompletion": True, **kwargs}
        return await bridge.request(payload, timeout=kwargs.get("timeout", 60.0))

    @classmethod
    async def execute_embedding(cls, model: str, input_data: Union[str, List[str]], **kwargs) -> EmbeddingResponse:
        pipeline = ChannelPipeline()
        bridge = RpcBridge()
        
        ## Head
        pipeline.add_last(EmbeddingTransport())
        pipeline.add_last(DphiFuelInterceptor())
        
        ## Middle
        pipeline.add_last(PayloadTranslator())
        pipeline.add_last(FallbackHandler())
        pipeline.add_last(MockBypass())
        pipeline.add_last(ChannelObserver())
        pipeline.add_last(ContextBinder())

        ## Tail
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

"""Global Entrypoints"""
async def acompletion(model: str, messages: List = None, **kwargs) -> Any:
    """비동기 LLM 호출 진입점 (DPHI Kernel 종속)"""
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