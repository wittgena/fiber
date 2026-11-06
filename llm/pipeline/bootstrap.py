# fiber.llm.pipeline.bootstrap
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Union

from fiber.llm.response import ModelResponse
from fiber.llm.model.types.provider.general import EmbeddingResponse

from fiber.gateway.llm.stream.wrapper import StreamWrapper
from fiber.llm.pipeline.channel.core import (
    CompletionTransport,
    EmbeddingTransport,
    StreamAggregator,
    PayloadTranslator,
    FallbackHandler,
    MockBypass,
    ContextBinder,
)
from fiber.llm.pipeline.channel.breaker import StreamBreaker
from fiber.llm.pipeline.channel.observer import MeteringObserver

from xphi.state.phase.channel import ChannelPipeline, DuplexChannel, RpcBridge


class PipelineSlot(Enum):
    PRE_TRANSLATE = "pre_translate"        # Type: dict
    POST_TRANSLATE = "post_translate"      # Type: Processor Object
    PRE_OBSERVER = "pre_observer"          # Type: Any


class PipelineBootstrap:
    @classmethod
    def _resolve_hooks(cls, kwargs: Dict[str, Any]) -> Dict[PipelineSlot, List[DuplexChannel]]:
        from fiber.llm.pipeline.tracer import BaseLLMTracer, TracerInterceptorChannel

        raw_hooks = kwargs.pop("pipeline_hooks", None) or {}
        hooks: Dict[PipelineSlot, List[DuplexChannel]] = {
            slot: list(channels) for slot, channels in raw_hooks.items()
        }

        interceptors = list(kwargs.pop("interceptors", None) or [])
        legacy_tracers = list(kwargs.pop("llm_tracers", None) or [])
        if legacy_tracers:
            interceptors.extend(legacy_tracers)

        for interceptor in interceptors:
            if isinstance(interceptor, BaseLLMTracer):
                interceptor = TracerInterceptorChannel([interceptor])

            slot = getattr(interceptor, "target_slot", PipelineSlot.PRE_OBSERVER)
            hooks.setdefault(slot, []).append(interceptor)

        return hooks

    @classmethod
    def _inject_hooks(cls, pipeline: ChannelPipeline, hooks: List[Any], slot_name: str):
        for hook in hooks:
            if not isinstance(hook, DuplexChannel):
                raise TypeError(f"Invalid Hook at {slot_name}: {type(hook)} must inherit from DuplexChannel")
            pipeline.add_last(hook)

    @classmethod
    async def execute_completion(cls, model: str, messages: List, **kwargs) -> Union[ModelResponse, StreamWrapper, Any]:
        pipeline = ChannelPipeline()
        bridge = RpcBridge()

        pipeline_hooks = cls._resolve_hooks(kwargs)
        stream_breaker = kwargs.pop("stream_breaker", StreamBreaker())
        metering_observer = kwargs.pop("metering_observer", MeteringObserver())

        ## Head
        pipeline.add_last(CompletionTransport())
        pipeline.add_last(stream_breaker)

        ## Middle
        pipeline.add_last(StreamAggregator())

        # [Slot: POST_TRANSLATE]
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.POST_TRANSLATE, []), PipelineSlot.POST_TRANSLATE.name)

        # Core: 절대 순서 유지
        pipeline.add_last(PayloadTranslator())

        # [Slot: PRE_TRANSLATE]
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.PRE_TRANSLATE, []), PipelineSlot.PRE_TRANSLATE.name)

        # Core: 절대 순서 유지
        pipeline.add_last(FallbackHandler())
        pipeline.add_last(MockBypass())

        # [Slot: PRE_OBSERVER]
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.PRE_OBSERVER, []), PipelineSlot.PRE_OBSERVER.name)

        ## Tail
        # Legacy Core: 절대 순서 유지
        pipeline.add_last(metering_observer)
        pipeline.add_last(ContextBinder())
        pipeline.add_last(bridge)

        await pipeline.fire_channel_active()
        payload = {"model": model, "messages": messages, "acompletion": True, **kwargs}
        return await bridge.request(payload, timeout=kwargs.get("timeout", 60.0))

    @classmethod
    async def execute_embedding(cls, model: str, input_data: Union[str, List[str]], **kwargs) -> EmbeddingResponse:
        pipeline = ChannelPipeline()
        bridge = RpcBridge()

        pipeline_hooks = cls._resolve_hooks(kwargs)
        stream_breaker = kwargs.pop("stream_breaker", StreamBreaker())
        metering_observer = kwargs.pop("metering_observer", MeteringObserver())

        ## Head
        pipeline.add_last(EmbeddingTransport())
        pipeline.add_last(stream_breaker)

        ## Middle
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.POST_TRANSLATE, []), PipelineSlot.POST_TRANSLATE.name)

        # Core: 절대 순서 유지
        pipeline.add_last(PayloadTranslator())

        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.PRE_TRANSLATE, []), PipelineSlot.PRE_TRANSLATE.name)

        # Core: 절대 순서 유지
        pipeline.add_last(FallbackHandler())
        pipeline.add_last(MockBypass())

        # [Slot: PRE_OBSERVER]
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.PRE_OBSERVER, []), PipelineSlot.PRE_OBSERVER.name)

        ## Tail
        # Core: 절대 순서 유지
        pipeline.add_last(metering_observer)
        pipeline.add_last(ContextBinder())
        pipeline.add_last(bridge)

        await pipeline.fire_channel_active()
        payload = {"model": model, "input": input_data, "aembedding": True, **kwargs}
        return await bridge.request(payload, timeout=kwargs.get("timeout", 60.0))