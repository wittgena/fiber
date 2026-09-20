# fiber.dev.trace.llm.vcr.proxy
import asyncio
import time
import copy
from typing import Optional, Any, AsyncGenerator

from fiber.llm.param import ModelResponse
from fiber.gateway.llm.mapper.traverser import StateTraverser
from fiber.llm.model.registry.adapter import AdapterRegistry
from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig, VCRManager

from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.bound.event.next import next_trace_id

log = get_emitter("llm.vcr.proxy")

# Async Stream Emulators (Time-Window Coalescing)
async def stream_recorder_proxy(
    original_stream: AsyncGenerator, 
    fixture_data: dict, 
    manager: VCRManager, 
    trace_id: str, 
    start_time: float,
    ctx: Any
) -> AsyncGenerator:
    last_time = start_time
    is_first = True
    
    # TICK_MS = 100.0
    tick_ms = manager.config.record_tick_ms
    buffer_text = ""
    buffer_time = 0.0
    
    def flush_buffer():
        nonlocal buffer_text, buffer_time
        if buffer_text:
            fixture_data["response_timeline"].append({
                "delta_ms": buffer_time,
                "chunk": buffer_text
            })
        buffer_text = ""
        buffer_time = 0.0

    try:
        async for chunk in original_stream:
            current_time = time.perf_counter()
            delta_ms = (current_time - last_time) * 1000
            last_time = current_time
            
            # OpenAI 표준 Dict로 교정된 데이터를 안전하게 추출
            content = StateTraverser.resolve(chunk, "choices.0.delta.content", "")
            
            if is_first:
                fixture_data["network_metrics"]["ttfb_ms"] = (current_time - start_time) * 1000
                is_first = False
                if content:
                    fixture_data["response_timeline"].append({
                        "delta_ms": delta_ms,
                        "chunk": content
                    })
                yield chunk
                continue

            buffer_text += content
            buffer_time += delta_ms
            
            if tick_ms <= 0 or buffer_time >= tick_ms:
                flush_buffer()
                
            yield chunk
            
    except Exception as e:
        fixture_data["exception_boundary"] = {
            "occurred": True,
            "error_type": type(e).__name__,
            "message": str(e)
        }
        raise
    finally:
        flush_buffer()
        fixture_data["network_metrics"]["total_duration_ms"] = (time.perf_counter() - start_time) * 1000
        manager.save_fixture(trace_id, fixture_data, ctx)


async def stream_player_emulator(
    fixture_data: dict, 
    config: VCRPlaybackConfig, 
    model_name: str
) -> AsyncGenerator:
    metrics = fixture_data.get("network_metrics", {})
    timeline = fixture_data.get("response_timeline", [])
    
    if config.chaos_latency_ms > 0:
        await asyncio.sleep(config.chaos_latency_ms / 1000.0)
        
    if config.speed == "real" and metrics.get("ttfb_ms", 0) > 0:
        await asyncio.sleep(metrics["ttfb_ms"] / 1000.0)

    for item in timeline:
        if config.speed == "real" and item.get("delta_ms", 0) > 0:
            await asyncio.sleep(item["delta_ms"] / 1000.0)
            
        yield ModelResponse(
            id=f"vcr-{fixture_data.get('trace_id', 'mock')[:8]}",
            model=model_name,
            choices=[{"index": 0, "delta": {"content": item.get("chunk", "")}}]
        )

    exc = fixture_data.get("exception_boundary", {})
    if exc.get("occurred"):
        error_class = __builtins__.get(exc["error_type"], Exception) 
        raise error_class(exc["message"])

# Adapter Proxy & Injector
class VCRAdapterProxy:
    def __init__(self, original_adapter: Any, manager: VCRManager):
        self.original_adapter = original_adapter
        self.manager = manager
        self.config = manager.config

    async def execute(self, ctx: Any) -> Any:
        safe_ctx = copy.deepcopy(ctx)
        
        system_meta = getattr(safe_ctx, "system_meta", None)
        trace_id = getattr(system_meta, "trace_id", None)
        
        if not trace_id:
            trace_id = next_trace_id()

        is_stream = getattr(safe_ctx, "stream", False)
        model_name = getattr(safe_ctx, "model", "vcr-mock-model")

        if self.config.mode == "replay":
            fixture = self.manager.get_fixture(trace_id, safe_ctx) 
            if not fixture:
                raise ValueError(f"No VCR fixture found for trace_id: '{trace_id}'")

            vcr_latency = fixture["network_metrics"].get("total_duration_ms", 0)
            if system_meta and hasattr(system_meta, "metadata"):
                system_meta.metadata["_vcr_injected_latency_ms"] = vcr_latency + self.config.chaos_latency_ms
            
            if not is_stream and fixture["exception_boundary"]["occurred"]:
                if self.config.chaos_latency_ms > 0:
                    await asyncio.sleep(self.config.chaos_latency_ms / 1000.0)
                exc = fixture["exception_boundary"]
                raise __builtins__.get(exc["error_type"], Exception)(exc["message"])

            if is_stream:
                return stream_player_emulator(fixture, self.config, model_name)
            else:
                if self.config.chaos_latency_ms > 0:
                    await asyncio.sleep(self.config.chaos_latency_ms / 1000.0)
                if self.config.speed == "real":
                    await asyncio.sleep(vcr_latency / 1000.0)
                    
                content = "".join([t["chunk"] for t in fixture["response_timeline"]])
                return ModelResponse(
                    id=f"vcr-{trace_id[:8]}",
                    model=model_name,
                    choices=[{"index": 0, "message": {"role": "assistant", "content": content}}],
                )

        start_time = time.perf_counter()
        fixture_data = self.manager.create_empty_fixture(trace_id, safe_ctx)
        
        try:
            response = await self.original_adapter.execute(safe_ctx)
            
            if self.config.mode == "record":
                if is_stream:
                    return stream_recorder_proxy(response, fixture_data, self.manager, trace_id, start_time, safe_ctx)
                else:
                    duration = (time.perf_counter() - start_time) * 1000
                    content = StateTraverser.resolve(response, "choices.0.message.content", "")
                    
                    fixture_data["network_metrics"]["ttfb_ms"] = duration
                    fixture_data["network_metrics"]["total_duration_ms"] = duration
                    fixture_data["response_timeline"].append({"delta_ms": 0, "chunk": content or ""})
                    self.manager.save_fixture(trace_id, fixture_data, safe_ctx)
                    
                    if system_meta and hasattr(system_meta, "metadata"):
                        system_meta.metadata["_vcr_injected_latency_ms"] = duration
            return response
            
        except Exception as e:
            if self.config.mode == "record":
                duration = (time.perf_counter() - start_time) * 1000
                fixture_data["network_metrics"]["total_duration_ms"] = duration
                fixture_data["exception_boundary"] = {
                    "occurred": True,
                    "error_type": type(e).__name__,
                    "message": str(e)
                }
                self.manager.save_fixture(trace_id, fixture_data, safe_ctx)
            raise


class VCRInjector:
    @staticmethod
    def apply(config: VCRPlaybackConfig, fixture_dir: str) -> Optional[VCRManager]:
        if config.mode == "live":
            return None
            
        manager = VCRManager(config=config, fixture_dir=fixture_dir)
        
        if not AdapterRegistry._is_initialized:
            AdapterRegistry.setup_defaults()
            
        log.info(f"🔌 VCRInjector: Applying VCRAdapterProxy to AdapterRegistry ({config.mode} mode)")
        
        for task_type, providers in AdapterRegistry._adapters.items():
            for provider_name, original_adapter in providers.items():
                if not getattr(original_adapter, "_is_vcr_patched", False):
                    proxy = VCRAdapterProxy(original_adapter, manager)
                    proxy._is_vcr_patched = True
                    AdapterRegistry._adapters[task_type][provider_name] = proxy
                    
        for task_type, original_fallback in AdapterRegistry._fallback_adapters.items():
            if not getattr(original_fallback, "_is_vcr_patched", False):
                proxy = VCRAdapterProxy(original_fallback, manager)
                proxy._is_vcr_patched = True
                AdapterRegistry._fallback_adapters[task_type] = proxy
                
        return manager