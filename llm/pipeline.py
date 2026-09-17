# fiber.llm.pipeline
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Union

from fiber.llm.param import ModelResponse
from fiber.llm.types.provider.general import EmbeddingResponse
from fiber.llm.router.stream.wrapper import StreamWrapper
from xphi.state.phase.channel import ChannelPipeline, DuplexChannel, RpcBridge

from fiber.llm.channel import (
    CompletionTransport, EmbeddingTransport, DphiFuelInterceptor, 
    StreamAggregator, PayloadTranslator, FallbackHandler, 
    MockBypass, ChannelObserver, ContextBinder
)

class PipelineSlot(Enum):
    PRE_TRANSLATE = "pre_translate"       # Type: dict
    POST_TRANSLATE = "post_translate"     # Type: Processor Object
    PRE_OBSERVER = "pre_observer"         # Type: Any (Legacy llm_tracers 위치)


class PipelineBootstrap:
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
        
        pipeline_hooks: Dict[PipelineSlot, List[DuplexChannel]] = kwargs.pop("pipeline_hooks", {})
        
        ## Head
        pipeline.add_last(CompletionTransport())
        pipeline.add_last(DphiFuelInterceptor())

        ## Middle
        pipeline.add_last(StreamAggregator())       
        
        # [✨ Slot: POST_TRANSLATE] - (Request 흐름상 Translator 통과 직후)
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.POST_TRANSLATE, []), PipelineSlot.POST_TRANSLATE.name)
        
        # 🔒 Legacy Core: 절대 순서 유지
        pipeline.add_last(PayloadTranslator())
        
        # [✨ Slot: PRE_TRANSLATE] - (Request 흐름상 Translator 진입 직전)
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.PRE_TRANSLATE, []), PipelineSlot.PRE_TRANSLATE.name)
        
        # 🔒 Legacy Core: 절대 순서 유지
        pipeline.add_last(FallbackHandler())
        pipeline.add_last(MockBypass())
        
        # [✨ Slot: PRE_OBSERVER] - (기존 llm_tracers 가 주입되던 정확히 그 위치)
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.PRE_OBSERVER, []), PipelineSlot.PRE_OBSERVER.name)
            
        ## Tail
        # 🔒 Legacy Core: 절대 순서 유지
        pipeline.add_last(ChannelObserver())        
        pipeline.add_last(ContextBinder())          
        pipeline.add_last(bridge)

        await pipeline.fire_channel_active()
        payload = {"model": model, "messages": messages, "acompletion": True, **kwargs}
        return await bridge.request(payload, timeout=kwargs.get("timeout", 60.0))

    @classmethod
    async def execute_embedding(cls, model: str, input_data: Union[str, List[str]], **kwargs) -> EmbeddingResponse:
        pipeline = ChannelPipeline()
        bridge = RpcBridge()
        
        pipeline_hooks: Dict[PipelineSlot, List[DuplexChannel]] = kwargs.pop("pipeline_hooks", {})
        
        ## Head
        pipeline.add_last(EmbeddingTransport())
        pipeline.add_last(DphiFuelInterceptor())
        
        ## Middle
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.POST_TRANSLATE, []), PipelineSlot.POST_TRANSLATE.name)
        
        # 🔒 Legacy Core: 절대 순서 유지
        pipeline.add_last(PayloadTranslator())
        
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.PRE_TRANSLATE, []), PipelineSlot.PRE_TRANSLATE.name)
        
        # 🔒 Legacy Core: 절대 순서 유지
        pipeline.add_last(FallbackHandler())
        pipeline.add_last(MockBypass())
        
        # [Slot: PRE_OBSERVER] - (기존 llm_tracers 주입 위치)
        cls._inject_hooks(pipeline, pipeline_hooks.get(PipelineSlot.PRE_OBSERVER, []), PipelineSlot.PRE_OBSERVER.name)
            
        ## Tail
        # 🔒 Legacy Core: 절대 순서 유지
        pipeline.add_last(ChannelObserver())
        pipeline.add_last(ContextBinder())
        pipeline.add_last(bridge)

        await pipeline.fire_channel_active()
        payload = {"model": model, "input": input_data, "aembedding": True, **kwargs}
        return await bridge.request(payload, timeout=kwargs.get("timeout", 60.0))