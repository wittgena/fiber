# phase.llm.call.handler
## @lineage: ator.agent.engine.handler
from __future__ import annotations
import asyncio
import httpx
import json
from typing import Any, Dict

from eco.bound.agent.parser.param.processor import CompletionProcessor, EmbeddingProcessor

from arch.topos.network.channel.pipeline import DuplexChannel, ChannelContext
from watcher.plane.emitter import get_emitter

log = get_emitter("executor.handlers")

class PayloadTranslator(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: dict):
        try:
            model = msg.get("model")
            tools_data = msg.get("tools")
            if tools_data:
                log.debug(
                    "[DEBUG-PRE-TRANSLATOR] 전달된 원시 tools 스키마:\n"
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
                    log.debug("[DEBUG-POST-TRANSLATOR] CompletionProcessor 빌드 성공. Tools 속성 유지됨.")

            ctx.set_attr("processed_context", processed_ctx)
            await ctx.fire_write(processed_ctx)
        except Exception as e:
            log.error("Payload translation failed", error=str(e), exc_info=True)
            await ctx.fire_exception_caught(e)

class FallbackHandler(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: dict):
        fallbacks = msg.pop("fallbacks", [])
        if fallbacks:
            ctx.set_attr("fallbacks", fallbacks)
            ctx.set_attr("original_msg", msg.copy())
        
        await ctx.fire_write(msg)

    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        fallbacks = ctx.get_attr("fallbacks", [])
        if not fallbacks:
            await ctx.fire_exception_caught(exc)
            return

        next_fallback = fallbacks.pop(0)
        retry_msg = ctx.get_attr("original_msg").copy()
        if isinstance(next_fallback, dict):
            fallback_config = next_fallback.copy()
            retry_msg["model"] = fallback_config.pop("model", retry_msg.get("model"))
            retry_msg.update(fallback_config)
        else:
            retry_msg["model"] = next_fallback

        log.warning(f"Fallback attempt triggered. Retrying with model: {retry_msg['model']}", error=str(exc))
        await ctx.fire_write(retry_msg)

class PromptTransformer(DuplexChannel):
    async def write(self, ctx: ChannelContext, msg: dict):
        prompt_id = msg.get("prompt_id")
        if prompt_id:
            try:
                log.debug("Prompt Management requested", prompt_id=prompt_id)
            except Exception as e:
                log.error("Failed to resolve dynamic prompt", error=str(e))
                await ctx.fire_exception_caught(e)
                return
                
        if msg.get("tools") is not None:
            if len(msg.get("tools", [])) == 0:
                log.debug("[DEBUG-PROMPT-TRANSFORMER] 빈 tools 리스트가 감지되어 None으로 초기화합니다.")
                msg["tools"] = None
            else:
                log.debug(f"[DEBUG-PROMPT-TRANSFORMER] {len(msg.get('tools'))}개의 tool이 감지되었습니다.")
        await ctx.fire_write(msg)

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
            mock_res = {"choices": [{"message": {"content": mock_response}}]}
            await ctx.fire_channel_read(mock_res)
            return

        await ctx.fire_write(msg)