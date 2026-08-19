# ator.runtime.entry
## @lineage: ator.agent.engine.entry
import asyncio
from typing import Any, List, Union

from ator.client.model.param import ModelResponse
from eco.model.types.general import EmbeddingResponse
from eco.bound.exception.mapping import exception_type

from phase.llm.stream.wrapper import StreamWrapper
from phase.llm.call.pipeline import ContextBinder, ChannelObserver, StreamAggregator
from phase.llm.call.handler import PromptTransformer, MockBypass, FallbackHandler, PayloadTranslator
from eco.model.router.inter.registry import AdapterRegistry

from arch.topos.network.bridge import RpcBridge
from arch.topos.network.channel.pipeline import ChannelPipeline, ChannelContext, DuplexChannel
from watcher.plane.emitter import get_emitter

log = get_emitter("client.entry")

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