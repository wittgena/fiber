# fiber.llm.entry
from __future__ import annotations

import asyncio
import json
from typing import Any, List, Dict, AsyncGenerator
import httpx 

from fiber.dev.trace.llm.interceptor import BaseLLMTracer, TracerInterceptorChannel
from fiber.gateway.llm.pipeline import PipelineBootstrap, PipelineSlot
from fiber.gateway.llm.stream.wrapper import StreamWrapper
from fiber.llm.types.provider.general import EmbeddingResponse
from fiber.llm.exception.eco import UnsupportedParamsError

from xphi.arch.model.dphi.auth import DphiKey, KernelAuthPayload
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("llm.entry")

def _run_sync(coro: Any) -> Any:
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
    interceptors = kwargs.pop("interceptors", [])
    legacy_tracers = kwargs.pop("llm_tracers", [])
    if legacy_tracers:
        interceptors.extend(legacy_tracers)
        
    hooks = kwargs.pop("pipeline_hooks", {}) 
    for interceptor in interceptors:
        if isinstance(interceptor, BaseLLMTracer):
            interceptor = TracerInterceptorChannel([interceptor])
            
        slot = getattr(interceptor, "target_slot", PipelineSlot.PRE_OBSERVER)
        hooks.setdefault(slot, []).append(interceptor)
        
    kwargs["pipeline_hooks"] = hooks
    return kwargs

def _ensure_kernel_auth(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    metadata = kwargs.get("metadata")
    if metadata is None:
        metadata = {}
        kwargs["metadata"] = metadata
        
    if DphiKey.KERNEL_AUTH.value not in metadata:
        tenant_id = kwargs.pop("tenant_id", "internal_system")
        metadata[DphiKey.KERNEL_AUTH.value] = KernelAuthPayload(
            tenant_id=tenant_id,
            fuel_budget=float('inf'),
            is_enforced=False
        ).model_dump()
        
    return kwargs

"""원격 Gateway 우회(Bypass) 전용 HTTP 클라이언트 로직"""
async def _forward_completion_to_gateway(base_url: str, model: str, messages: List, **kwargs) -> Any:
    """Gateway로 직접 HTTP 통신을 수행 (Pipeline 우회)"""
    headers = kwargs.pop("headers", {})
    headers.setdefault("Content-Type", "application/json")
    stream = kwargs.get("stream", False)
    payload = {
        "model": model,
        "messages": messages,
        **kwargs
    }
    
    target_url = f"{base_url.rstrip('/')}/chat/completions" if not base_url.endswith("/chat/completions") else base_url
    async def _sse_stream_generator() -> AsyncGenerator[Any, None]:
        async with httpx.AsyncClient() as client:
            async with client.stream("POST", target_url, json=payload, headers=headers) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        data_str = line[6:].strip()
                        if data_str == "[DONE]":
                            break
                        yield json.loads(data_str)

    if stream:
        return StreamWrapper(_sse_stream_generator())
    else:
        async with httpx.AsyncClient() as client:
            response = await client.post(target_url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()

async def _forward_embedding_to_gateway(base_url: str, model: str, input_data: Any, **kwargs) -> Any:
    headers = kwargs.pop("headers", {})
    headers.setdefault("Content-Type", "application/json")
    payload = {
        "model": model,
        "input": input_data,
        **kwargs
    }
    
    target_url = f"{base_url.rstrip('/')}/embeddings" if not base_url.endswith("/embeddings") else base_url
    async with httpx.AsyncClient() as client:
        response = await client.post(target_url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json() # 필요 시 EmbeddingResponse 모델로 래핑

"""Global Entrypoints"""
async def acompletion(model: str, messages: List = None, **kwargs) -> Any:
    """비동기 LLM 호출 진입점"""
    messages = messages or []
    base_url = kwargs.pop("base_url", None) or kwargs.pop("api_base", None)
    if base_url:
        return await _forward_completion_to_gateway(base_url, model, messages, **kwargs)
    
    kwargs = _route_interceptors(kwargs)
    kwargs = _ensure_kernel_auth(kwargs)
    return await PipelineBootstrap.execute_completion(model, messages, **kwargs)

def completion(model: str, messages: List = None, **kwargs) -> Any:
    messages = messages or []
    return _run_sync(acompletion(model, messages, **kwargs))

async def aembedding(*args, **kwargs) -> EmbeddingResponse:
    # TEMPORARY DISABLED
    model = args[0] if len(args) > 0 else kwargs.get("model")
    raise UnsupportedParamsError(
        message="The Embedding API is temporarily unsupported.",
        model=model_name,
        llm_provider=kwargs.get("custom_llm_provider", "unknown"),
        status_code=501
    )

    input_data = kwargs.get("input", [])
    
    if not model:
        raise ValueError("model param not passed in.")
        
    base_url = kwargs.pop("base_url", None) or kwargs.pop("api_base", None)
    if base_url:
        return await _forward_embedding_to_gateway(base_url, model, input_data, **kwargs)
        
    kwargs = _route_interceptors(kwargs)
    kwargs = _ensure_kernel_auth(kwargs)
    return await PipelineBootstrap.execute_embedding(model=model, input_data=input_data, **kwargs)

def embedding(*args, **kwargs) -> EmbeddingResponse:
    return _run_sync(aembedding(*args, **kwargs))