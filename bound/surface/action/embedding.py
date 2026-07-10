# bound.surface.action.embedding
## @lineage: bound.channel.action.embedding
## @lineage: bound.channel.client.action.embedding
## @lineage: anchor.channel.client.action.embedding
## @lineage: anchor.channel.action.embedding
## @lineage: bound.channel.bridge.action.embedding
## @lineage: bound.bridge.action.embedding
## @lineage: anchor.action.embedding
## @lineage: anchor.surface.legacy.action.embedding
"""
@manifold: Core Embedding Execution Boundary
@desc: Translates raw embedding requests into normalized contexts and delegates to the Multi-dimensional Adapter Registry.
"""
from typing import Any, Coroutine, List, Literal, Optional, Union
from typing_extensions import overload
from bound.surface.legacy.types import EmbeddingResponse
from bound.adapter.mapper.exception import exception_type
from bound.surface.action.client.wrapper import client
from bound.adapter.action.preprocessor import EmbeddingPreprocessor
from anchor.registry.adapter import AdapterRegistry
from watcher.plane.emitter import get_emitter

log = get_emitter("action.embedding")

@client
async def aembedding(*args, **kwargs) -> EmbeddingResponse:
    """비동기 임베딩 코어 진입점"""
    model = args[0] if len(args) > 0 else kwargs.get("model")
    input_data = kwargs.get("input", [])
    kwargs["aembedding"] = True
    
    return await async_core_embedding(model=model, input=input_data, **kwargs)

async def async_core_embedding(model: str, input: Union[str, List[str]], **kwargs) -> EmbeddingResponse:
    if not model:
        raise ValueError("model param not passed in.")

    ## @phase: 전처리 및 컨텍스트 빌드
    preprocessor = EmbeddingPreprocessor(model=model, input=input, kwargs=kwargs)
    ctx = preprocessor.build()
    
    log.debug(f"[bound.embedding] 임베딩 코어 진입: model={ctx.model}, provider={ctx.custom_llm_provider}")

    try:
        ## @resolve: 1차원 키(task_type="embedding")를 통한 어댑터 동적 할당
        adapter = AdapterRegistry.get_adapter(task_type="embedding", provider_name=ctx.custom_llm_provider)
        
        ## @execute: 어댑터에 Context 위임 (LlamaIndex 등 외부 위상은 Adapter 내부에서 처리됨)
        response = await adapter.execute(ctx)
        
        # 내부 파라미터 은닉 처리 (LiteLLM 호환성 유지)
        if isinstance(response, EmbeddingResponse) and hasattr(response, "_hidden_params"):
            response._hidden_params["custom_llm_provider"] = ctx.custom_llm_provider
            
        return response

    except Exception as e:
        log.error(f"[bound.embedding] 임베딩 코어 엔진 예외 발생: {str(e)}")
        
        # 레거시 로깅 유지
        if ctx.logging_obj:
            ctx.logging_obj.post_call(
                input=ctx.input, api_key=ctx.api_key, original_response=str(e), additional_args={"headers": ctx.original_kwargs.get("headers")}
            )
            
        error_completion_kwargs = {"model": model, "input": input, **ctx.original_kwargs}
        raise exception_type(
            model=ctx.model, 
            custom_llm_provider=ctx.custom_llm_provider, 
            original_exception=e,
            completion_kwargs=error_completion_kwargs, 
            extra_kwargs=ctx.original_kwargs,
        )

def embedding(model, input=[], **kwargs) -> EmbeddingResponse:
    import asyncio
    ## 이벤트 루프를 가져와 async_core_embedding을 동기적으로 실행
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(async_core_embedding(model, input, **kwargs))