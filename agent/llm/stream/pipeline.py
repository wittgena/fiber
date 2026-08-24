# agent.llm.stream.pipeline
## @lineage: ator.driver.llm.stream.pipeline
import time
from typing import Any
from xphi.arch.topos.network.channel.pipeline import DuplexChannel, ChannelContext
from fiber.agent.llm.stream.parser.chunk import StreamChunkParser
from fiber.agent.llm.stream.accumulator import StreamAccumulator
from fiber.agent.llm.stream.rule import Rules
from fiber.agent.anchor.model.types.stream import ModelResponseStream

class ChunkCodecHandler(DuplexChannel):
    """1단계: 원시 데이터를 파싱하고 Accumulator를 통해 ModelResponseStream 조립"""
    async def channel_read(self, ctx: ChannelContext, msg: Any):
        provider = ctx.get_attr("provider")
        accumulator: StreamAccumulator = ctx.get_attr("accumulator")
        
        parsed_dict = StreamChunkParser.parse(provider, msg)
        processed_chunk = accumulator.push(parsed_dict)
        
        # 유의미한 데이터가 완성되었을 때만 파이프라인 다음 단계로 Push
        if processed_chunk:
            await ctx.fire_channel_read(processed_chunk)

class RuleGuardHandler(DuplexChannel):
    """2단계: Guardrail / Rules 엔진 검사"""
    async def channel_read(self, ctx: ChannelContext, msg: ModelResponseStream):
        rules: Rules = ctx.get_attr("rules")
        accumulator: StreamAccumulator = ctx.get_attr("accumulator")
        model = ctx.get_attr("model")
        
        try:
            # 유해성 및 룰셋 검증 수행
            rules.post_call_rules(input=accumulator.response_uptil_now, model=model)
            await ctx.fire_channel_read(msg)
        except Exception as e:
            # 룰셋 위반 시 하위로 에러 이벤트를 발송하여 스트림 전파 중단
            await ctx.fire_exception_caught(e)

class StreamTelemetryHandler(DuplexChannel):
    """3단계: 스트리밍 성능 지표 (TTFT) 기록"""
    async def channel_read(self, ctx: ChannelContext, msg: ModelResponseStream):
        system_meta = ctx.get_attr("system_meta")
        
        # 첫 번째 유의미한 청크가 도착했을 때 TTFT(Time To First Token) 기록
        if "time_to_first_token" not in system_meta.framework_flags:
            system_meta.framework_flags["time_to_first_token"] = time.time()
            
        await ctx.fire_channel_read(msg)

class StreamYieldHandler(DuplexChannel):
    """
    [브릿지 핸들러] 
    파이프라인의 끝단(Tail)에 위치하며, 최종 통과된 청크를 
    StreamWrapper가 꺼내어 클라이언트에게 yield 할 수 있도록 버퍼(outbox)에 담습니다.
    """
    async def channel_read(self, ctx: ChannelContext, msg: ModelResponseStream):
        outbox: list = ctx.get_attr("outbox")
        outbox.append(msg)
        # 더 이상 하위로 전파할 핸들러가 없으므로 fire_channel_read() 생략