# fiber.dev.trace.llm.vcr.proxy
import asyncio
import time
import copy
import re
from typing import Optional, Any, AsyncGenerator

from fiber.llm.response import ModelResponse
from fiber.llm.types.provider.core import Usage
from fiber.gateway.llm.state.traverser import StateTraverser, StateMapper
from fiber.llm.router.stream.parser.chunk import StreamChunkParser
from fiber.llm.model.registry.adapter import AdapterRegistry
from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig, VCRManager

# --- Exception Mapping Imports ---
from fiber.llm.exception.mapping import STATUS_CODE_MAPPING, SEMANTIC_ERROR_REGEX
import fiber.llm.exception.eco as eco_exceptions

from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.bound.event.next import next_trace_id

log = get_emitter("llm.vcr.proxy")

def _reconstruct_vcr_exception(exc_data: dict, provider: str, model: str) -> Exception:
    """
    VCR에 기록된 텍스트 기반 에러 데이터를 프레임워크 표준 에러 객체로 복원합니다.
    """
    error_type = exc_data.get("error_type", "Exception")
    message = exc_data.get("message", "")
    replay_msg = f"[VCR Replay] {message}"
    
    # 1. 텍스트 내에서 상태 코드(4xx, 5xx) 추출
    status_code = None
    match = re.search(r"\b([45]\d{2})\b", message)
    if match:
        status_code = int(match.group(1))

    # 2. Semantic 정규식 맵핑 시도
    for pattern, exception_class in SEMANTIC_ERROR_REGEX:
        if pattern.search(message):
            try:
                return exception_class(message=replay_msg, model=model, llm_provider=provider)
            except Exception:
                pass # 파라미터 시그니처 미스매치 시 다음 단계로 폴백

    # 3. 상태 코드 기반 맵핑 시도
    if status_code and status_code in STATUS_CODE_MAPPING:
        exception_class = STATUS_CODE_MAPPING[status_code]
        try:
            return exception_class(message=replay_msg, model=model, llm_provider=provider)
        except Exception:
            pass

    # 4. 프레임워크 표준 예외 클래스(eco 모듈) 직접 매칭 시도
    if hasattr(eco_exceptions, error_type):
        exception_class = getattr(eco_exceptions, error_type)
        try:
            return exception_class(message=replay_msg, llm_provider=provider, model=model)
        except Exception:
            pass

    # 5. 최후의 보루 (파이썬 내장 에러 또는 기본 Exception)
    error_class = __builtins__.get(error_type, Exception)
    return error_class(replay_msg)


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
    
    tick_ms = manager.config.record_tick_ms
    buffer_text = ""
    buffer_time = 0.0
    
    # Provider 추출 로직 (변수명 오류 교정: safe_ctx -> ctx)
    provider = getattr(ctx, "custom_llm_provider", None)
    if not provider:
        model_name = getattr(ctx, "model", "")
        provider = model_name.split("/")[0] if "/" in model_name else "openai"
    
    def flush_buffer(f_reason: Optional[str] = None):
        nonlocal buffer_text, buffer_time
        if buffer_text or f_reason:
            payload = {
                "delta_ms": buffer_time,
                "chunk": buffer_text
            }
            if f_reason:
                payload["finish_reason"] = f_reason
                
            fixture_data["response_timeline"].append(payload)
            
        buffer_text = ""
        buffer_time = 0.0

    try:
        async for chunk in original_stream:
            current_time = time.perf_counter()
            delta_ms = (current_time - last_time) * 1000
            last_time = current_time
            
            parsed_chunk = StreamChunkParser.parse(provider, chunk)
            content = parsed_chunk.get("text", "") if parsed_chunk else ""
            chunk_usage = parsed_chunk.get("usage") if parsed_chunk else None
            finish_reason = parsed_chunk.get("finish_reason") if parsed_chunk else None
            
            if chunk_usage:
                has_real_value = any(v is not None for v in chunk_usage.values()) if isinstance(chunk_usage, dict) else True
                if has_real_value:
                    fixture_data["usage"] = chunk_usage
            
            if is_first:
                fixture_data["network_metrics"]["ttfb_ms"] = (current_time - start_time) * 1000
                is_first = False
                if content or finish_reason:
                    payload = {"delta_ms": delta_ms, "chunk": content}
                    if finish_reason:
                        payload["finish_reason"] = finish_reason
                    fixture_data["response_timeline"].append(payload)
                yield chunk
                continue

            buffer_text += content
            buffer_time += delta_ms
            
            if finish_reason:
                flush_buffer(f_reason=finish_reason)
            elif tick_ms <= 0 or buffer_time >= tick_ms:
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
            choices=[{
                "index": 0, 
                "delta": {"content": item.get("chunk", "")},
                "finish_reason": item.get("finish_reason")
            }]
        )

    usage_data = fixture_data.get("usage")
    if usage_data:
        safe_usage = {k: (v if v is not None else 0) for k, v in usage_data.items()}
        yield ModelResponse(
            id=f"vcr-{fixture_data.get('trace_id', 'mock')[:8]}",
            model=model_name,
            choices=[], 
            usage=Usage(**safe_usage)
        )

    exc = fixture_data.get("exception_boundary", {})
    if exc.get("occurred"):
        # ✨ 프레임워크 표준 예외 복원 로직 적용
        provider = fixture_data.get("request_context", {}).get("provider", "unknown")
        raise _reconstruct_vcr_exception(exc, provider, model_name)


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
                
                # ✨ 프레임워크 표준 예외 복원 로직 적용
                provider = getattr(safe_ctx, "custom_llm_provider", None)
                if not provider:
                    provider = model_name.split("/")[0] if "/" in model_name else "openai"
                raise _reconstruct_vcr_exception(fixture["exception_boundary"], provider, model_name)

            if is_stream:
                return stream_player_emulator(fixture, self.config, model_name)
            else:
                if self.config.chaos_latency_ms > 0:
                    await asyncio.sleep(self.config.chaos_latency_ms / 1000.0)
                if self.config.speed == "real":
                    await asyncio.sleep(vcr_latency / 1000.0)
                    
                content = "".join([t["chunk"] for t in fixture["response_timeline"]])
                usage_data = fixture.get("usage")
                if usage_data:
                    safe_usage = {k: (v if v is not None else 0) for k, v in usage_data.items()}
                    usage_obj = Usage(**safe_usage)
                else:
                    usage_obj = None
                
                return ModelResponse(
                    id=f"vcr-{trace_id[:8]}",
                    model=model_name,
                    choices=[{"index": 0, "message": {"role": "assistant", "content": content}}],
                    usage=usage_obj
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
                    provider = getattr(safe_ctx, "custom_llm_provider", None)
                    if not provider:
                        model_name = getattr(safe_ctx, "model", "")
                        provider = model_name.split("/")[0] if "/" in model_name else "openai"

                    content, usage_dict = StateMapper.extract_sync_response(response, provider)
                    
                    if usage_dict:
                        fixture_data["usage"] = usage_dict
                        if hasattr(response, "usage"):
                            for k, v in usage_dict.items():
                                if hasattr(response.usage, k):
                                    setattr(response.usage, k, v)

                    fixture_data["network_metrics"]["ttfb_ms"] = duration
                    fixture_data["network_metrics"]["total_duration_ms"] = duration
                    fixture_data["response_timeline"].append({"delta_ms": 0, "chunk": content})
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