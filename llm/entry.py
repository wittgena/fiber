# fiber.llm.entry
from __future__ import annotations

import asyncio
from typing import Any, List

# Core Pipeline
from fiber.llm.pipeline import PipelineBootstrap
from fiber.llm.model.types.general import EmbeddingResponse
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("runtime.entry")

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