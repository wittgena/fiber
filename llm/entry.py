# fiber.llm.entry
from __future__ import annotations

import asyncio
from typing import Any, List, Dict

from fiber.gateway.llm.pipeline import PipelineBootstrap, PipelineSlot
from fiber.llm.types.provider.general import EmbeddingResponse
from fiber.dev.trace.llm.interceptor import BaseLLMTracer, TracerInterceptorChannel

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("llm.entry")

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

def _route_interceptors(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    [Facade] OpenAI 호환 kwargs에서 커스텀 플러그인(interceptors)을 가로채어,
    파이프라인 슬롯(pipeline_hooks)으로 자동 라우팅합니다.
    """
    # 1. 1차원 리스트로 들어온 커스텀 채널들 추출 (하위 LLM SDK로 넘어가지 않게 안전하게 pop)
    interceptors = kwargs.pop("interceptors", [])
    
    # [하위 호환성] 기존 레거시 파라미터 'llm_tracers'도 추출하여 interceptors에 병합
    legacy_tracers = kwargs.pop("llm_tracers", [])
    if legacy_tracers:
        interceptors.extend(legacy_tracers)
        
    # 2. 시스템 규격에 맞는 조립용 딕셔너리 준비 (유저가 직접 지정한 훅이 있다면 병합)
    hooks = kwargs.pop("pipeline_hooks", {}) 
    
    for interceptor in interceptors:
        # 편의성 보장: BaseLLMTracer를 직접 던졌다면 자동으로 Channel 객체로 래핑
        if isinstance(interceptor, BaseLLMTracer):
            interceptor = TracerInterceptorChannel([interceptor])
            
        # 3. 채널이 스스로 선언한 슬롯 위치를 확인 (명시되지 않았으면 PRE_OBSERVER로 간주)
        slot = getattr(interceptor, "target_slot", PipelineSlot.PRE_OBSERVER)
        hooks.setdefault(slot, []).append(interceptor)
        
    # 4. 정렬된 훅을 코어 팩토리(PipelineBootstrap)로 전달
    kwargs["pipeline_hooks"] = hooks
    return kwargs

"""Global Entrypoints"""
async def acompletion(model: str, messages: List = None, **kwargs) -> Any:
    """비동기 LLM 호출 진입점 (DPHI Kernel 종속)"""
    messages = messages or []
    # ✨ 호출 전, 1차원 리스트를 코어 슬롯으로 스마트 라우팅
    kwargs = _route_interceptors(kwargs)
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
        
    kwargs = _route_interceptors(kwargs)
    return await PipelineBootstrap.execute_embedding(model=model, input_data=input_data, **kwargs)

def embedding(*args, **kwargs) -> EmbeddingResponse:
    """동기 임베딩 호출 진입점"""
    return _run_sync(aembedding(*args, **kwargs))