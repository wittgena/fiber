# engine.client.pipeline
import time
import uuid
from typing import Any, Dict

from engine.stream.wrapper import StreamWrapper
from eco.client.model.execution import ExecutionMetadata

from watcher.plane.emitter import get_emitter
from arch.topos.network.channel.pipeline import ChannelContext, DuplexChannel

log = get_emitter("executor.pipeline")

class ContextBinder(DuplexChannel):
    """최초 요청 진입 시 컨텍스트(ExecutionMetadata)를 세팅하고 고유 ID를 부여하는 핸들러"""
    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        if "call_id" not in msg:
            msg["call_id"] = str(uuid.uuid4())

        metadata = msg.get("metadata", {})
        session_id = msg.get("session_id") or metadata.get("session_id")
        trace_id = msg.get("trace_id") or metadata.get("trace_id")
        model = msg.get("model", "unknown")

        system_meta = ExecutionMetadata(
            session_id=session_id,
            trace_id=trace_id,
            call_id=msg["call_id"],
            metadata=metadata,
            base_model=model
        )
        
        # 파이프라인 전역 AttributeMap에 저장하여 하위 핸들러가 접근할 수 있게 함
        ctx.set_attr("system_meta", system_meta)
        ctx.set_attr("request_kwargs", msg)
        
        # 원본 msg에도 심어 하위 호환성 유지
        msg["system_meta"] = system_meta

        await ctx.fire_write(msg)


class ChannelObserver(DuplexChannel):
    """로깅, Trace, 소요 시간 기록을 담당하는 텔레메트리 핸들러"""
    def __init__(self):
        self.emitter = get_emitter("executor.telemetry", phase="LLM_CALL")

    async def write(self, ctx: ChannelContext, msg: Dict[str, Any]):
        ctx.set_attr("start_time", time.time())
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        
        model = msg.get("model", "unknown")
        provider = msg.get("custom_llm_provider", "unknown")

        self.emitter.trace(
            "LLM Request Initiated",
            model=model, provider=provider,
            trace_id=meta.trace_id, session_id=meta.session_id
        )
        await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs")
        
        duration_ms = (time.time() - ctx.get_attr("start_time")) * 1000
        meta.framework_flags["duration_ms"] = duration_ms

        usage = getattr(msg, "usage", None)
        usage_dict = usage.model_dump() if hasattr(usage, "model_dump") else (usage or {})

        self.emitter.info(
            "LLM Request Completed Successfully",
            model=meta.base_model, provider=req.get("custom_llm_provider"),
            duration_ms=duration_ms, usage=usage_dict, trace_id=meta.trace_id
        )
        await ctx.fire_channel_read(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        meta: ExecutionMetadata = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs", {})
        duration_ms = (time.time() - ctx.get_attr("start_time", time.time())) * 1000

        self.emitter.error(
            f"LLM Request Failed: {type(exc).__name__} - {str(exc)}",
            model=meta.base_model if meta else "unknown",
            provider=req.get("custom_llm_provider", "unknown"),
            duration_ms=duration_ms, trace_id=meta.trace_id if meta else None,
            exc_info=True
        )
        await ctx.fire_exception_caught(exc)


class StreamAggregator(DuplexChannel):
    """스트리밍 응답 래핑(StreamWrapper) 및 complete_response 옵션 처리를 담당하는 핸들러"""
    def _is_streaming(self, req: Dict[str, Any]) -> bool:
        call_type = req.get("call_type", "")
        if req.get("stream") is True:
            return True
        return "stream" in call_type

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req: dict = ctx.get_attr("request_kwargs", {})
        
        # 스트리밍 요청에 대한 처리
        if self._is_streaming(req):
            meta: ExecutionMetadata = ctx.get_attr("system_meta")
            
            # 1. StreamWrapper 바인딩 (이미 래핑된 객체가 아니면 생성)
            stream_wrapper = msg if isinstance(msg, StreamWrapper) else StreamWrapper(
                completion_stream=msg,
                model=meta.base_model,
                system_meta=meta,
                custom_llm_provider=req.get("custom_llm_provider"),
                stream_options=req.get("stream_options"),
            )

            # 2. Complete Response 요구 시 스트림 조기 소진 (Exhaust)
            if req.get("complete_response") is True:
                async for _ in stream_wrapper:
                    pass
                complete_res = stream_wrapper.pipeline.attributes["accumulator"].get_complete_response()
                # 토큰 보정 등의 처리가 완료된 단일 응답 객체를 상위로 반환
                await ctx.fire_channel_read(complete_res)
                return

            await ctx.fire_channel_read(stream_wrapper)
        else:
            await ctx.fire_channel_read(msg)