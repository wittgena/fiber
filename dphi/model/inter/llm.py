# fiber.dphi.model.inter.llm
## @lineage: dphi.model.inter.llm
import asyncio
import functools
from pathlib import Path
from typing import AsyncGenerator, Generator, Any

from fiber.dphi.model.inter.adapter import BaseProviderAdapter
from fiber.dphi.model.registry.llm import LLMRouter, ModuleMissingError

from fiber.llm.execution import CompletionContext
from fiber.llm.model.provider.registry import get_llm_provider
from fiber.llm.exception.mapping import exception_type

from fiber.dphi.model.mapper.state import StateMapper

from xphi.arch.event.next import uuid4 
from xphi.kernel.space.bind.resolver import find_current_self, get_invoker
from xphi.watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

class InterLLMAdapter(BaseProviderAdapter):
    def __init__(self):
        self.router = LLMRouter()
        self.mapper = StateMapper()

    async def execute(self, ctx: CompletionContext) -> Any:
        req_id = str(uuid4())[:8]
        log.debug(f"[InterLLM-{req_id}] 🚀 execute START | model={ctx.model}, provider={ctx.custom_llm_provider}, stream={ctx.stream}")

        resolved_api_key = ctx.api_key
        if not resolved_api_key or resolved_api_key == "not-needed":
            try:
                _, _, dynamic_key, _ = get_llm_provider(
                    model=ctx.model, custom_llm_provider=ctx.custom_llm_provider
                )
                if dynamic_key:
                    resolved_api_key = dynamic_key
            except Exception as e:
                log.warning(f"[InterLLM-{req_id}] ⚠️ Locator key resolution bypassed/failed: {e}")

        execution_kwargs = {}
        if "tools" in ctx.optional_params:
            execution_kwargs["tools"] = ctx.optional_params.pop("tools")
        if "tool_choice" in ctx.optional_params:
            execution_kwargs["tool_choice"] = ctx.optional_params.pop("tool_choice")

        llama_kwargs = {
            "api_key": resolved_api_key,
            "api_base": ctx.api_base,
            "temperature": ctx.optional_params.get("temperature", 0.7),
            "max_tokens": ctx.optional_params.get("max_tokens"),
            "timeout": ctx.timeout if isinstance(ctx.timeout, (int, float)) else 60.0,
        }
        
        for k, v in ctx.optional_params.items():
            if k not in llama_kwargs:
                llama_kwargs[k] = v

        llama_kwargs = {k: v for k, v in llama_kwargs.items() if v is not None and v != "not-needed"}

        try:
            llm = self.router.route_and_load(
                model_name=ctx.model, 
                custom_llm_provider=ctx.custom_llm_provider, 
                **llama_kwargs
            )
        except Exception as e:
            log.error(f"[InterLLM-{req_id}] 🚨 모델 인스턴스 생성 실패: {e}")
            raise exception_type(
                model=ctx.model,
                original_exception=e,
                custom_llm_provider=ctx.custom_llm_provider,
                completion_kwargs=llama_kwargs
            )

        llama_messages = self.mapper.to_llama_messages(ctx.messages)

        if ctx.stream:
            log.debug(f"[InterLLM-{req_id}] 🌊 Initiating STREAM Execution")
            if ctx.acompletion:
                response_stream = await llm.astream_chat(llama_messages, **execution_kwargs)
            else:
                response_stream = llm.stream_chat(llama_messages, **execution_kwargs)
            
            # 파이프라인에서 StreamWrapper 처리를 하므로 여기선 원시 청크 제너레이터만 반환
            async def stream_generator():
                if ctx.acompletion:
                    async for chunk in response_stream:
                        yield chunk.raw
                else:
                    for chunk in response_stream:
                        yield chunk.raw

            return stream_generator()
            
        else:
            log.debug(f"[InterLLM-{req_id}] ⚡ Initiating SINGULAR Execution")
            if ctx.acompletion:
                response = await llm.achat(llama_messages, **execution_kwargs)
            else:
                chat_func = functools.partial(llm.chat, llama_messages, **execution_kwargs)
                response = await asyncio.to_thread(chat_func)
            
            choice_data = self.mapper.to_openai_choice(response, req_id, log)
            ctx.model_response.choices = [choice_data]
            
            return ctx.model_response