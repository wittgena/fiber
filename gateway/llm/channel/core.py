# fiber.gateway.llm.channel.core
from __future__ import annotations

import re
import os
import asyncio
import httpx
import json
import time
import uuid
import copy
from typing import Any, Dict, List, Union

from fiber.gateway.llm.context.metadata import ExecutionMetadata

from fiber.llm.response import ModelResponse
from fiber.llm.types.provider.general import EmbeddingResponse
from fiber.llm.exception.mapping import exception_type
from fiber.gateway.llm.stream.wrapper import StreamWrapper
from fiber.gateway.llm.param.processor import CompletionProcessor, EmbeddingProcessor
from fiber.llm.model.router.adapter import AdapterRegistry

from xphi.arch.bound.event.next import next_trace_id
from xphi.arch.model.dphi.auth import DphiKey, KernelAuthPayload
from xphi.state.phase.channel import ChannelPipeline, ChannelContext, DuplexChannel, RpcBridge
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("runtime.entry")
log_handlers = get_emitter("executor.handlers")
log_pipeline = get_emitter("executor.pipeline")

_TRACE_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")

class ContextBinder(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        # 물리적 호출 ID (Span ID) - OTel 표준: 16자리 소문자 16진수 (8-byte)
        call_id = msg.get("call_id")
        if not call_id:
            call_id = os.urandom(8).hex()
            msg["call_id"] = call_id

        ctx.set_attr("trace_errors", msg.get("trace_errors", False))
        metadata = msg.get("metadata", {})
        
        # 논리적 트랜잭션 ID (Trace ID) 정밀 검증
        raw_trace_id = msg.get("trace_id") or metadata.get("trace_id")
        if raw_trace_id and isinstance(raw_trace_id, str) and _TRACE_ID_PATTERN.match(raw_trace_id):
            resolved_trace_id = raw_trace_id
        elif raw_trace_id:
            # 32자리 Hex가 아니면 이를 시드로 삼아 결정론적 OTel 규격으로 변환
            resolved_trace_id = next_trace_id(deterministic_seed=str(raw_trace_id))
        else:
            resolved_trace_id = next_trace_id(deterministic_seed=None)

        resolved_session_id = msg.get("session_id") or metadata.get("session_id")
        kernel_auth = metadata.get(DphiKey.KERNEL_AUTH.value, {})
        if isinstance(kernel_auth, dict):
            tenant_id = kernel_auth.get("tenant_id", "anonymous")
        else:
            tenant_id = getattr(kernel_auth, "tenant_id", "anonymous")
        
        ctx.set_attr("tenant_id", tenant_id)

        # 시스템 메타데이터 조립
        system_meta = ExecutionMetadata(
            session_id=resolved_session_id,
            trace_id=resolved_trace_id,
            call_id=call_id,
            metadata=metadata,
            base_model=msg.get("model", "unknown")
        )
        
        ctx.set_attr("system_meta", system_meta)
        ctx.set_attr("request_kwargs", msg)
        msg["system_meta"] = system_meta
        await ctx.fire_write(msg)
    
    async def channel_read(self, ctx: ChannelContext, msg: Any):
        meta = ctx.get_attr("system_meta")
        if meta is not None:
            try:
                msg.system_meta = meta
            except (AttributeError, TypeError, ValueError):
                pass
        
        await ctx.fire_channel_read(msg)

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
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": mock_response},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            }
            mock_res = ModelResponse(**mock_res_dict)
            await ctx.fire_channel_read(mock_res)
            return

        await ctx.fire_write(msg)


class FallbackHandler(DuplexChannel):
    """
    Intercepts pipeline exceptions and automatically retries with alternative models.
    Acts as a 'reflector' that catches bubbling errors and pushes new requests downward.
    """
    async def write(self, ctx: ChannelContext, msg: dict):
        fallbacks = msg.pop("fallbacks", [])
        if fallbacks:
            ctx.set_attr("fallbacks", fallbacks)
            # Preserve a pristine snapshot. 
            # (Deepcopy ensures downstream processors cannot mutate our backup)
            ctx.set_attr("original_msg", copy.deepcopy(msg))
        
        # Pass the clean message further down the pipeline
        await ctx.fire_write(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        # [FLOW: REVERSE ERROR] 1. Catch bubbling exception from downstream (e.g., Timeout, 502)
        fallbacks = ctx.get_attr("fallbacks", [])
        
        # If fallback pool is exhausted, let the error propagate to the user
        if not fallbacks:
            await ctx.fire_exception_caught(exc)
            return

        # Pop the next candidate and restore the pristine snapshot
        next_fallback = fallbacks.pop(0)
        retry_msg = copy.deepcopy(ctx.get_attr("original_msg")) # Deepcopy for true idempotency
        
        # Patch the request with the new fallback model/configurations
        if isinstance(next_fallback, dict):
            fallback_config = next_fallback.copy()
            retry_msg["model"] = fallback_config.pop("model", retry_msg.get("model"))
            retry_msg.update(fallback_config)
        else:
            retry_msg["model"] = next_fallback
        
        if sys_meta := ctx.get_attr("system_meta"):
            sys_meta.base_model = retry_msg["model"]
        if req_kwargs := ctx.get_attr("request_kwargs"):
            req_kwargs["model"] = retry_msg["model"]

        log_handlers.warning(f"Fallback attempt triggered. Retrying with model: {retry_msg['model']}", error=str(exc))
        
        ## [FLOW: REFLECT HERE] Reflect the modified request back DOWN the pipeline
        ## (This breaks the error chain and re-enters the PayloadTranslator cleanly)
        await ctx.fire_write(retry_msg)

class PayloadTranslator(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: dict):
        try:
            # API Payload(msg)에서 내부 메타데이터를 완전히 팝(pop)하여 격리
            system_meta = msg.pop("system_meta", None) or ctx.get_attr("system_meta")

            ## Payload Pre-processing
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
                    log_handlers.debug("[DEBUG-PAYLOAD-TRANSLATOR] 빈 tools 리스트가 감지되어 None으로 초기화합니다.")
                    msg["tools"] = None
                else:
                    log_handlers.debug(f"[DEBUG-PAYLOAD-TRANSLATOR] {len(msg.get('tools'))}개의 tool이 감지되었습니다.")
            
            model = msg.get("model")
            tools_data = msg.get("tools")
            if tools_data:
                log_handlers.debug(
                    "[DEBUG-PAYLOAD-TRANSLATOR] 전달된 원시 tools 스키마:\n"
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
                    log_handlers.debug("[DEBUG-PAYLOAD-TRANSLATOR] CompletionProcessor 빌드 성공. Tools 속성 유지됨.")
                processed_ctx.original_kwargs.pop("system_meta", None)

            if system_meta:
                processed_ctx.system_meta = system_meta

            ctx.set_attr("processed_context", processed_ctx)
            await ctx.fire_write(processed_ctx)
        except Exception as e:
            show_trace = ctx.get_attr("trace_errors", False)
            log_handlers.error("Payload translation failed", error=str(e), exc_info=show_trace)
            await ctx.fire_exception_caught(e)

class StreamAggregator(DuplexChannel):
    def _is_streaming(self, req: Dict[str, Any]) -> bool:
        call_type = req.get("call_type", "")
        if req.get("stream") is True:
            return True
        return "stream" in call_type

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req: dict = ctx.get_attr("request_kwargs", {})
        
        if self._is_streaming(req):
            meta: ExecutionMetadata = ctx.get_attr("system_meta")
            
            stream_wrapper = msg if isinstance(msg, StreamWrapper) else StreamWrapper(
                completion_stream=msg,
                model=meta.base_model,
                system_meta=meta,
                custom_llm_provider=req.get("custom_llm_provider"),
                stream_options=req.get("stream_options"),
                messages=req.get("messages", []),
            )

            if req.get("complete_response") is True:
                async for _ in stream_wrapper:
                    pass

                complete_res = stream_wrapper.accumulator.get_complete_response()
                await ctx.fire_channel_read(complete_res)
                return

            await ctx.fire_channel_read(stream_wrapper)
        else:
            await ctx.fire_channel_read(msg)

class CompletionTransport(DuplexChannel):
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