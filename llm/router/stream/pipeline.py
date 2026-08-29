# fiber.llm.router.stream.pipeline
## @lineage: fiber.llm.stream.pipeline
## @lineage: llm.stream.pipeline
import time
import asyncio
from typing import Any

from xphi.kernel.phase.network.channel.pipeline import DuplexChannel, ChannelContext

from fiber.llm.router.stream.parser.chunk import StreamChunkParser
from fiber.llm.router.stream.accumulator import StreamAccumulator
from fiber.llm.model.types.stream import ModelResponseStream
from fiber.llm.exception.eco import APIResponseValidationError

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
    """2단계: Guardrail / Rules 엔진 검사 (Self-contained Middleware)"""
    async def channel_read(self, ctx: ChannelContext, msg: ModelResponseStream):
        accumulator: StreamAccumulator = ctx.get_attr("accumulator")
        model = ctx.get_attr("model")
        system_meta = ctx.get_attr("system_meta")
        
        # 전역 config(레거시) 대신 현재 요청의 Context(metadata)에서 주입된 동적 룰 리스트를 가져옵니다.
        # (호출자가 acompletion(..., metadata={"post_call_rules": [func1, func2]}) 형태로 주입)
        rules = system_meta.metadata.get("post_call_rules", []) if system_meta and system_meta.metadata else []

        try:
            if rules:
                current_text = accumulator.response_uptil_now
                
                # 룰 순회 및 검증 (동기/비동기 함수 모두 완벽 지원)
                for rule in rules:
                    if callable(rule):
                        if asyncio.iscoroutinefunction(rule):
                            decision = await rule(current_text)
                        else:
                            decision = rule(current_text)
                            
                        # 기존 규칙 엔진과 동일한 검증 로직 내재화
                        if isinstance(decision, bool) and decision is False:
                            raise APIResponseValidationError(
                                message="LLM Response failed post-call-rule check", 
                                llm_provider="", 
                                model=model or "unknown"
                            )
                        elif isinstance(decision, dict):
                            if decision.get("decision", True) is False:
                                raise APIResponseValidationError(
                                    message=decision.get("message", "LLM Response failed post-call-rule check"), 
                                    llm_provider="", 
                                    model=model or "unknown"
                                )
                                
            # 룰셋을 무사히 통과했거나 룰이 없으면 다음 핸들러로 패스
            await ctx.fire_channel_read(msg)
            
        except Exception as e:
            # 룰셋 위반(ValidationError) 등 발생 시 하위로 에러 이벤트를 발송하여 스트림 전파 중단
            await ctx.fire_exception_caught(e)

class StreamTelemetryHandler(DuplexChannel):
    """3단계: 스트리밍 성능 지표 (TTFT) 기록"""
    async def channel_read(self, ctx: ChannelContext, msg: ModelResponseStream):
        system_meta = ctx.get_attr("system_meta")
        
        # 첫 번째 유의미한 청크가 도착했을 때 TTFT(Time To First Token) 기록
        if system_meta and "time_to_first_token" not in system_meta.framework_flags:
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