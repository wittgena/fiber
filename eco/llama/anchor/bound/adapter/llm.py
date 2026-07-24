# eco.llama.anchor.bound.adapter.llm
## @lineage: adapter.llama.anchor.bound.adapter.llm
## @lineage: llama.anchor.bound.adapter.llm
## @lineage: anchor.inter.bound.adapter.llm
## @lineage: bound.adapter.inter.llm
## @lineage: bound.adapter.provider.inter
"""
@manifold: Universal Bridge Adapter
@flow: CompletionContext (Brane) -> LLMRouter (Projection) -> LlamaIndex (Execution) -> ModelResponse (Resolution)
@desc: Dynamically binds Brane contexts to LlamaIndex topologies, ensuring bi-directional state translation.
       Mapping rules are delegated to an isolated class for future file extraction.
"""
import os
import json
import asyncio
import functools
from pathlib import Path
from typing import AsyncGenerator, Generator, Any, List

from eco.llama.router.locator import get_llm_provider
from eco.llama.anchor.bound.base.llms.types import ChatMessage, MessageRole
from eco.llama.anchor.bound.adapter.base import BaseProviderAdapter
from bound.mapper.state import StateMapper

from eco.tenant.action.process.pre import CompletionContext
from eco.llama.router.llm import LLMRouter, TopologyMissingError
from bound.gateway.stream.wrapper import StreamWrapper

from arch.gov.gate import uuid4 
from phase.bind.resolver import find_current_self, get_invoker
from watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

class InterLLMAdapter(BaseProviderAdapter):
    def __init__(self):
        self.router = LLMRouter()
        self.mapper = StateMapper()

    async def execute(self, ctx: CompletionContext):
        req_id = str(uuid4())[:8]
        log.debug(f"[InterLLM-{req_id}] 🚀 execute START | model={ctx.model}, provider={ctx.custom_llm_provider}, stream={ctx.stream}, async={ctx.acompletion}")

        ## @phase: Environment & Credential Shim
        resolved_api_key = ctx.api_key
        if not resolved_api_key or resolved_api_key == "not-needed":
            try:
                _, _, dynamic_key, _ = get_llm_provider(
                    model=ctx.model,
                    custom_llm_provider=ctx.custom_llm_provider
                )
                if dynamic_key:
                    log.debug(f"[InterLLM-{req_id}] 🔑 API Key resolved dynamically via Locator.")
                    resolved_api_key = dynamic_key
            except Exception as e:
                log.warning(f"[InterLLM-{req_id}] ⚠️ Locator key resolution bypassed/failed: {e}")

        ## @phase: Parameter Separation (Init vs Execution)
        execution_kwargs = {}
        if "tools" in ctx.optional_params:
            execution_kwargs["tools"] = ctx.optional_params.pop("tools")
        if "tool_choice" in ctx.optional_params:
            execution_kwargs["tool_choice"] = ctx.optional_params.pop("tool_choice")

        ## @phase: Topological Projection (Dynamic Instantiation)
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
        safe_kwargs = {k: v for k, v in llama_kwargs.items() if not k.startswith("api_")}
        log.debug(f"[InterLLM-{req_id}] ⚙️ Routing Init kwargs: {safe_kwargs}")
        log.debug(f"[InterLLM-{req_id}] ⚙️ Routing Exec kwargs: {list(execution_kwargs.keys())}")

        try:
            llm = self.router.route_and_load(
                model_name=ctx.model, 
                custom_llm_provider=ctx.custom_llm_provider, 
                **llama_kwargs
            )
            log.debug(f"[InterLLM-{req_id}] ✅ LLM Topology Loaded: {type(llm).__name__}")
        except TopologyMissingError as te:
            log.error(f"[InterLLM-{req_id}] 🚨 TopologyMissingError: {te}")
            raise te
        except Exception as e:
            log.error(f"[InterLLM-{req_id}] 🚨 [LlamaBridge] 모델 인스턴스 생성 실패: {e}", exc_info=True)
            raise RuntimeError(f"[LlamaBridge] 모델 인스턴스 생성 실패: {e}")

        ## @phase: State Mapping (Context Dict -> ChatMessage) 
        llama_messages = self.mapper.to_llama_messages(ctx.messages)
        log.debug(f"[InterLLM-{req_id}] 📝 State Mapping Complete: {len(llama_messages)} messages translated.")

        ## @phase: Execution & Boundary Resolution
        if ctx.stream:
            log.debug(f"[InterLLM-{req_id}] 🌊 Initiating STREAM Execution")
            if ctx.acompletion:
                response_stream = await llm.astream_chat(llama_messages, **execution_kwargs)
            else:
                response_stream = llm.stream_chat(llama_messages, **execution_kwargs)
            
            async def stream_generator():
                if ctx.acompletion:
                    async for chunk in response_stream:
                        yield chunk.raw
                else:
                    for chunk in response_stream:
                        yield chunk.raw

            log.debug(f"[InterLLM-{req_id}] 🔄 Returning CustomStreamWrapper")
            return StreamWrapper(
                completion_stream=stream_generator(), 
                model=ctx.model, 
                custom_llm_provider=ctx.custom_llm_provider, 
                logging_obj=ctx.logging_obj
            )
        else:
            log.debug(f"[InterLLM-{req_id}] ⚡ Initiating SINGULAR Execution")
            if ctx.acompletion:
                response = await llm.achat(llama_messages, **execution_kwargs)
            else:
                import asyncio
                import functools
                chat_func = functools.partial(llm.chat, llama_messages, **execution_kwargs)
                response = await asyncio.to_thread(chat_func)
            
            log.debug(f"[InterLLM-{req_id}] ✅ Execution Complete. Resolving response boundary.")
            
            ## @phase: State Resolution (ChatResponse -> OpenAI Choice)
            choice_data = self.mapper.to_openai_choice(response, req_id, log)
                
            ## Brane 컨텍스트에 주입
            ctx.model_response.choices = [choice_data]
            
            ## 메트릭 로깅용 변수 추출
            msg_len = len(choice_data["message"].get("content") or "")
            tool_len = len(choice_data["message"].get("tool_calls") or [])
            log.debug(f"[InterLLM-{req_id}] 🏁 execute END. Returning ctx.model_response (Content Length: {msg_len}, Tools: {tool_len})")
            return ctx.model_response