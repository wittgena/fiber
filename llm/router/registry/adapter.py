# fiber.llm.router.registry.adapter
import asyncio
import functools
from pathlib import Path
from typing import Dict, AsyncGenerator, Generator, Any, List, Union

# Adapter & Execution Imports
from fiber.llm.router.inter.adapter import BaseProviderAdapter, GenericHTTPAdapter
from fiber.llm.context.metadata import CompletionContext, EmbeddingContext

# Router & Registry Imports
from fiber.llm.router.registry.llm import LLMRouter, ModuleMissingError
from fiber.llm.router.registry.embedding import EmbeddingRouter

# Mapper & Provider Imports
from fiber.llm.model.provider.resolver import get_llm_provider
from fiber.llm.exception.mapping import exception_type
from fiber.llm.router.mapper.state import StateMapper

# XPHI Framework Imports
from xphi.arch.bound.event.next import uuid4 
from xphi.kernel.space.bind.resolver import find_current_self, get_invoker
from xphi.watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
llm_log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")
registry_log = get_emitter("registry.adapter")

class InterLLMAdapter(BaseProviderAdapter):
    def __init__(self):
        self.router = LLMRouter()
        self.mapper = StateMapper()

    async def execute(self, ctx: CompletionContext) -> Any:
        req_id = str(uuid4())[:8]
        llm_log.debug(f"[InterLLM-{req_id}] 🚀 execute START | model={ctx.model}, provider={ctx.custom_llm_provider}, stream={ctx.stream}")

        resolved_api_key = ctx.api_key
        if not resolved_api_key or resolved_api_key == "not-needed":
            try:
                _, _, dynamic_key, _ = get_llm_provider(
                    model=ctx.model, custom_llm_provider=ctx.custom_llm_provider
                )
                if dynamic_key:
                    resolved_api_key = dynamic_key
            except Exception as e:
                llm_log.warning(f"[InterLLM-{req_id}] ⚠️ Locator key resolution bypassed/failed: {e}")

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
            llm_log.error(f"[InterLLM-{req_id}] 🚨 모델 인스턴스 생성 실패: {e}")
            raise exception_type(
                model=ctx.model,
                original_exception=e,
                custom_llm_provider=ctx.custom_llm_provider,
                completion_kwargs=llama_kwargs
            )

        llama_messages = self.mapper.to_llama_messages(ctx.messages)

        if ctx.stream:
            llm_log.debug(f"[InterLLM-{req_id}] 🌊 Initiating STREAM Execution")
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
            llm_log.debug(f"[InterLLM-{req_id}] ⚡ Initiating SINGULAR Execution")
            if ctx.acompletion:
                response = await llm.achat(llama_messages, **execution_kwargs)
            else:
                chat_func = functools.partial(llm.chat, llama_messages, **execution_kwargs)
                response = await asyncio.to_thread(chat_func)
            
            choice_data = self.mapper.to_openai_choice(response, req_id, llm_log)
            ctx.model_response.choices = [choice_data]
            
            return ctx.model_response

class InterEmbeddingAdapter(BaseProviderAdapter):
    def __init__(self):
        self.router = EmbeddingRouter()

    async def execute(self, ctx: EmbeddingContext):
        llama_kwargs = {
            "api_key": ctx.api_key,
            "api_base": ctx.api_base,
            "timeout": ctx.timeout if isinstance(ctx.timeout, (int, float)) else 60.0,
        }
        
        for k, v in ctx.optional_params.items():
            if k not in llama_kwargs:
                llama_kwargs[k] = v

        llama_kwargs = {k: v for k, v in llama_kwargs.items() if v is not None}

        try:
            embed_model = self.router.route_and_load(model_name=ctx.model, **llama_kwargs)
        except Exception as e:
            raise RuntimeError(f"[LlamaBridge] Embedding 모델 인스턴스 생성 실패: {e}")

        raw_inputs: Union[str, List[str]] = ctx.input
        texts = raw_inputs if isinstance(raw_inputs, list) else [raw_inputs]

        if getattr(ctx, "aembedding", False):
            embeddings = await embed_model.aget_text_embedding_batch(texts)
        else:
            embeddings = embed_model.get_text_embedding_batch(texts)

        data_objects = []
        for idx, emb in enumerate(embeddings):
            data_objects.append({
                "object": "embedding",
                "index": idx,
                "embedding": emb
            })

        if hasattr(ctx, "model_response"):
            setattr(ctx.model_response, "data", data_objects)
            setattr(ctx.model_response, "model", ctx.model)
            setattr(ctx.model_response, "object", "list")
            setattr(ctx.model_response, "usage", {"prompt_tokens": -1, "total_tokens": -1})
            return ctx.model_response
        else:
            return {
                "object": "list",
                "data": data_objects,
                "model": ctx.model,
                "usage": {"prompt_tokens": -1, "total_tokens": -1}
            }

class AdapterRegistry:
    """@state: Multi-dimensional topological boundaries"""
    _adapters: Dict[str, Dict[str, BaseProviderAdapter]] = {
        "llm": {},
        "embedding": {}
    }
    _fallback_adapters: Dict[str, BaseProviderAdapter] = {}
    _is_initialized: bool = False

    @classmethod
    def setup_defaults(cls):
        ## @phase: Initialize primary kernels (Lazy Load Boundary)
        if cls._is_initialized:
            return

        registry_log.debug("[Registry] 시스템 코어 다중 위상(Multi-topology) 레지스트리 초기화 시작")
        
        llm_generic = GenericHTTPAdapter()
        llm_inter = InterLLMAdapter()
        cls._fallback_adapters["llm"] = llm_generic
        
        for provider in ["ollama", "huggingface"]:
            cls._adapters["llm"][provider] = llm_generic

        for provider in ["inter", "anthropic", "gemini"]:
            cls._adapters["llm"][provider] = llm_inter

        embed_inter = InterEmbeddingAdapter()
        cls._fallback_adapters["embedding"] = embed_inter 
        for provider in ["openai", "azure", "cohere", "inter"]:
            cls._adapters["embedding"][provider] = embed_inter

        cls._is_initialized = True
        registry_log.debug("[Registry] 시스템 코어 레지스트리 초기화 완료")

    @classmethod
    def register(cls, task_type: str, provider_name: str, adapter: BaseProviderAdapter):
        if not cls._is_initialized:
            cls.setup_defaults()

        if task_type not in cls._adapters:
            cls._adapters[task_type] = {}
        
        cls._adapters[task_type][provider_name] = adapter
        registry_log.debug(f"[Registry] '{task_type}' 위상에 '{provider_name}' 어댑터 동적 등록됨.")

    @classmethod
    def get_adapter(cls, task_type: str, provider_name: str) -> BaseProviderAdapter:
        if not cls._is_initialized:
            cls.setup_defaults()
            
        task_manifold = cls._adapters.get(task_type, {})
        fallback = cls._fallback_adapters.get(task_type)
        adapter = task_manifold.get(provider_name, fallback)
        if not adapter:
            raise ValueError(f"[Registry Error] '{task_type}' 작업을 처리할 폴백 어댑터조차 구성되지 않았습니다.")
            
        return adapter