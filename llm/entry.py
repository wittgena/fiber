# fiber.llm.entry
from __future__ import annotations

import asyncio
from typing import Any, List, Dict

from fiber.gateway.llm.pipeline import PipelineBootstrap, PipelineSlot
from fiber.llm.types.provider.general import EmbeddingResponse
from fiber.dev.trace.llm.interceptor import BaseLLMTracer, TracerInterceptorChannel

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

"""Global Entrypoints"""
async def acompletion(model: str, messages: List = None, **kwargs) -> Any:
    """비동기 LLM 호출 진입점"""
    messages = messages or []
    kwargs = _route_interceptors(kwargs)
    kwargs = _ensure_kernel_auth(kwargs)
    return await PipelineBootstrap.execute_completion(model, messages, **kwargs)

def completion(model: str, messages: List = None, **kwargs) -> Any:
    messages = messages or []
    return _run_sync(acompletion(model, messages, **kwargs))

async def aembedding(*args, **kwargs) -> EmbeddingResponse:
    model = args[0] if len(args) > 0 else kwargs.get("model")
    input_data = kwargs.get("input", [])
    
    if not model:
        raise ValueError("model param not passed in.")
        
    kwargs = _route_interceptors(kwargs)
    kwargs = _ensure_kernel_auth(kwargs)
    return await PipelineBootstrap.execute_embedding(model=model, input_data=input_data, **kwargs)

def embedding(*args, **kwargs) -> EmbeddingResponse:
    return _run_sync(aembedding(*args, **kwargs))