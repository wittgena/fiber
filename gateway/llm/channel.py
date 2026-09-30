# fiber.gateway.llm.channel
from __future__ import annotations

import os
import asyncio
import httpx
import json
import time
import uuid
import copy
from typing import Any, Dict, List, Union

from fiber.llm.response import ModelResponse
from fiber.gateway.llm.context.metadata import ExecutionMetadata
from fiber.llm.types.provider.general import EmbeddingResponse
from fiber.llm.exception.mapping import exception_type
from fiber.llm.router.stream.wrapper import StreamWrapper

from fiber.llm.router.param.processor import CompletionProcessor, EmbeddingProcessor
from fiber.llm.model.registry.adapter import AdapterRegistry

from xphi.arch.bound.event.next import next_trace_id
from xphi.arch.model.dphi.auth import DphiKey, KernelAuthPayload
from xphi.state.phase.channel import ChannelPipeline, ChannelContext, DuplexChannel, RpcBridge
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("runtime.entry")
log_handlers = get_emitter("executor.handlers")
log_pipeline = get_emitter("executor.pipeline")

class FuelInterceptor(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: Any):
        req_kwargs = ctx.get_attr("request_kwargs", {})
        metadata = req_kwargs.get("metadata", {})
        
        raw_auth = metadata.get(DphiKey.KERNEL_AUTH.value, {})
        kernel_auth = KernelAuthPayload(**raw_auth) if isinstance(raw_auth, dict) else (raw_auth or KernelAuthPayload())
        
        enforce_quota = os.getenv("FIBER_ENFORCE_QUOTA", "false").lower() == "true"
        
        is_enforced = getattr(kernel_auth, "is_enforced", False)
        tenant_id = getattr(kernel_auth, "tenant_id", "anonymous")

        if enforce_quota and not is_enforced:
            log_pipeline.warning(f"[Security] Unauthorized API Access Attempt blocked. Tenant: {tenant_id}")
            raise PermissionError(f"Strict Quota Enforcement is active. Missing valid Kernel Auth for tenant: {tenant_id}")
        
        ctx.set_attr("tenant_id", tenant_id)
        ctx.set_attr(DphiKey.FUEL_BUDGET.value, kernel_auth.fuel_budget)
        ctx.set_attr(DphiKey.FUEL_CONSUMED.value, 0)
        ctx.set_attr(DphiKey.AUDIT_HASH.value, kernel_auth.audit_hash)
        await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req: dict = ctx.get_attr("request_kwargs", {})
        is_stream = req.get("stream", False) or "stream" in req.get("call_type", "")
        
        fuel_budget = ctx.get_attr(DphiKey.FUEL_BUDGET.value, float('inf'))
        audit_hash = ctx.get_attr(DphiKey.AUDIT_HASH.value)

        if not is_stream and not hasattr(msg, "__aiter__"):
            usage = getattr(msg, "usage", None)
            if usage and hasattr(usage, "total_tokens"):
                ctx.set_attr(DphiKey.FUEL_CONSUMED.value, getattr(usage, "total_tokens", 0))

            if audit_hash and hasattr(msg, "system_fingerprint"):
                msg.system_fingerprint = audit_hash

        elif hasattr(msg, "__aiter__") and fuel_budget < float('inf'):
            msg = self._fuel_trap_generator(msg, fuel_budget, ctx)

        await ctx.fire_channel_read(msg)

    async def _fuel_trap_generator(self, raw_stream, budget, context):
        consumed = 0
        try:
            async for raw_chunk in raw_stream:
                consumed += 1 
                context.set_attr(DphiKey.FUEL_CONSUMED.value, consumed)
                if consumed > budget:
                    log_pipeline.warning(f"[FuelTrap] Fuel exhausted ({budget}). Killing stream physically.")
                    break
                yield raw_chunk
        except Exception as e:
            log_pipeline.error(f"Stream interrupted during fuel metering: {e}")
            raise


class ContextBinder(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        # 1. 물리적 호출 ID (Span ID) - OTel 표준: 16자리 소문자 16진수 (8-byte)
        if "call_id" not in msg:
            msg["call_id"] = os.urandom(8).hex()

        ctx.set_attr("trace_errors", msg.get("trace_errors", False))
        metadata = msg.get("metadata", {})
        
        # 2. 논리적 트랜잭션 ID (Trace ID) - OTel 표준: 32자리 소문자 16진수 (16-byte)
        raw_trace_id = msg.get("trace_id") or metadata.get("trace_id")
        if raw_trace_id:
            # 전달받은 ID가 이미 32자리 OTLP 규격이라면 그대로 수용
            if len(raw_trace_id) == 32:
                resolved_trace_id = raw_trace_id
            # 32자리가 아니라면 이를 시드로 삼아 32자리 OTLP Hex로 결정론적 변환
            else:
                resolved_trace_id = next_trace_id(deterministic_seed=str(raw_trace_id))
        else:
            resolved_trace_id = next_trace_id(deterministic_seed=None)
        
        resolved_session_id = msg.get("session_id") or metadata.get("session_id")

        system_meta = ExecutionMetadata(
            session_id=resolved_session_id,
            trace_id=resolved_trace_id,
            call_id=msg["call_id"],
            metadata=metadata,
            base_model=msg.get("model", "unknown")
        )
        
        ctx.set_attr("system_meta", system_meta)
        ctx.set_attr("request_kwargs", msg)
        msg["system_meta"] = system_meta
        await ctx.fire_write(msg)


class ChannelObserver(DuplexChannel):
    def __init__(self):
        self.emitter = get_emitter("channel.observer", phase="LLM_CALL")

    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        ctx.set_attr("start_time", time.time())
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        model = msg.get("model", "unknown")
        provider = msg.get("custom_llm_provider", "unknown")
        tenant_id = ctx.get_attr("tenant_id", "unknown")

        self.emitter.trace(
            "LLM Request Initiated",
            model=model, provider=provider,
            trace_id=meta.trace_id, session_id=meta.session_id,
            tenant_id=tenant_id
        )
        await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs")
        is_stream = hasattr(msg, "__aiter__")

        # ✨ [개선] 1. 단일이든 스트림이든 호출될 통합 '완료 및 과금' 함수 정의
        def execute_completion_and_billing(total_tokens: int):
            fuel_consumed = ctx.get_attr(DphiKey.FUEL_CONSUMED.value, 0)
            if fuel_consumed == 0 and total_tokens > 0:
                fuel_consumed = total_tokens
                
            fuel_budget = ctx.get_attr(DphiKey.FUEL_BUDGET.value, float('inf'))
            tenant_id = ctx.get_attr("tenant_id", "unknown")
            audit_hash = ctx.get_attr(DphiKey.AUDIT_HASH.value)
            
            duration_ms = (time.time() - ctx.get_attr("start_time")) * 1000
            meta.framework_flags["duration_ms"] = duration_ms

            # 찐 완료 로그 출력 (스트림은 끝난 시점의 real duration이 계산됨)
            self.emitter.info(
                "LLM Request Completed Successfully (Sealed)",
                model=meta.base_model, provider=req.get("custom_llm_provider"),
                duration_ms=duration_ms, 
                usage={"total_tokens": fuel_consumed},                                 
                trace_id=meta.trace_id,
                tenant_id=tenant_id,
                kernel_fuel={
                    "consumed": fuel_consumed,
                    "budget": fuel_budget if fuel_budget != float('inf') else None
                },
                audit_hash=audit_hash
            )

            # 백그라운드 비동기 차감(Billing Deduction) 트리거
            if fuel_consumed > 0 and tenant_id not in ("anonymous", "internal_system", "unknown"):
                try:
                    from fiber.infra.rpc.fuel import execute_direct_fuel_deduction
                    import asyncio
                    # 메인 LLM 응답을 블로킹하지 않도록 Fire-and-Forget 실행
                    asyncio.create_task(
                        execute_direct_fuel_deduction(
                            tenant_id=tenant_id, 
                            consumed_fuel=fuel_consumed, 
                            trace_id=meta.trace_id
                        )
                    )
                except Exception as e:
                    log_pipeline.error(f"Failed to trigger async fuel deduction for {tenant_id}: {e}")

        # ✨ [개선] 2. 스트림 분기: 완료 로깅과 과금을 'StreamWrapper'에게 위임(Deferred)
        if is_stream:
            # 방금 만든 함수를 메타데이터에 달아서 보냄 (StreamWrapper가 마지막 청크에서 호출함)
            meta.framework_flags["on_stream_complete"] = execute_completion_and_billing
            self.emitter.trace(f"LLM Stream Connection Established. Billing deferred to end of stream.")
            await ctx.fire_channel_read(msg)
            return

        # ✨ [개선] 3. 단일 호출 분기: 즉시 실행
        usage = getattr(msg, "usage", None)
        usage_dict = usage.model_dump() if hasattr(usage, "model_dump") else (usage or {})
        total_tokens = usage_dict.get("total_tokens", 0)

        audit_hash = ctx.get_attr(DphiKey.AUDIT_HASH.value)
        if audit_hash and hasattr(msg, "system_fingerprint"):
            msg.system_fingerprint = audit_hash

        execute_completion_and_billing(total_tokens)
        await ctx.fire_channel_read(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs", {})
        duration_ms = (time.time() - ctx.get_attr("start_time", time.time())) * 1000
        show_trace = ctx.get_attr("trace_errors", False)
        tenant_id = ctx.get_attr("tenant_id", "unknown")

        self.emitter.error(
            f"LLM Request Failed: {type(exc).__name__} - {str(exc)}",
            model=meta.base_model if meta else "unknown",
            provider=req.get("custom_llm_provider", "unknown"),
            duration_ms=duration_ms, trace_id=meta.trace_id if meta else None,
            tenant_id=tenant_id,
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
        # [FLOW: OUTBOUND] 1. Extract fallback queue from the outgoing request
        fallbacks = msg.pop("fallbacks", [])
        if fallbacks:
            ctx.set_attr("fallbacks", fallbacks)
            # 2. Preserve a pristine snapshot. 
            # (Deepcopy ensures downstream processors cannot mutate our backup)
            ctx.set_attr("original_msg", copy.deepcopy(msg))
        
        # 3. Pass the clean message further down the pipeline
        await ctx.fire_write(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        # [FLOW: REVERSE ERROR] 1. Catch bubbling exception from downstream (e.g., Timeout, 502)
        fallbacks = ctx.get_attr("fallbacks", [])
        
        # 2. If fallback pool is exhausted, let the error propagate to the user
        if not fallbacks:
            await ctx.fire_exception_caught(exc)
            return

        # 3. Pop the next candidate and restore the pristine snapshot
        next_fallback = fallbacks.pop(0)
        retry_msg = copy.deepcopy(ctx.get_attr("original_msg")) # Deepcopy for true idempotency
        
        # 4. Patch the request with the new fallback model/configurations
        if isinstance(next_fallback, dict):
            fallback_config = next_fallback.copy()
            retry_msg["model"] = fallback_config.pop("model", retry_msg.get("model"))
            retry_msg.update(fallback_config)
        else:
            retry_msg["model"] = next_fallback

        log_handlers.warning(f"Fallback attempt triggered. Retrying with model: {retry_msg['model']}", error=str(exc))
        
        ## [FLOW: REFLECT HERE] Reflect the modified request back DOWN the pipeline
        ## (This breaks the error chain and re-enters the PayloadTranslator cleanly)
        await ctx.fire_write(retry_msg)

class PayloadTranslator(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: dict):
        try:
            # API Payload(msg)에서 내부 메타데이터를 완전히 팝(pop)하여 격리
            system_meta = msg.pop("system_meta", None) or ctx.get_attr("system_meta")

            ## 1. Payload Pre-processing
            prompt_id = msg.get("prompt_id")
            if prompt_id:
                try:
                    log_handlers.debug("Prompt Management requested", prompt_id=prompt_id)
                except Exception as e:
                    log_handlers.error("Failed to resolve dynamic prompt", error=str(e))
                    await ctx.fire_exception_caught(e)
                    return
            
            # Tools 정규화 로직 통합
            if msg.get("tools") is not None:
                if len(msg.get("tools", [])) == 0:
                    log_handlers.debug("[DEBUG-PAYLOAD-TRANSLATOR] 빈 tools 리스트가 감지되어 None으로 초기화합니다.")
                    msg["tools"] = None
                else:
                    log_handlers.debug(f"[DEBUG-PAYLOAD-TRANSLATOR] {len(msg.get('tools'))}개의 tool이 감지되었습니다.")
            
            ## 2. Core Translation (Processor 빌드)
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

            # 격리해둔 메타데이터를 반환 객체의 '독립된 속성'으로 주입
            if system_meta:
                processed_ctx.system_meta = system_meta

            # 다음 파이프라인으로 Context 전달
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