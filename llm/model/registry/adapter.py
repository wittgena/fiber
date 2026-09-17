# fiber.llm.model.registry.adapter
## @lineage: fiber.llm.router.registry.adapter
import json
import asyncio
import functools
import httpx
from pathlib import Path
from typing import Dict, AsyncGenerator, Generator, Any, List, Union

# Context & Parameter Imports
from fiber.llm.param import ModelResponse
from fiber.llm.context.metadata import CompletionContext, EmbeddingContext

# Router & Registry Imports
from fiber.llm.model.registry.llm import LLMRouter, ModuleMissingError
from fiber.llm.model.registry.embedding import EmbeddingRouter

# Mapper, Provider & Client Imports
from fiber.llm.model.provider.resolver import get_llm_provider
from fiber.llm.types.exception.mapping import exception_type
from fiber.llm.mapper.state import StateMapper
from fiber.dphi.eco.client.http import get_client

# XPHI Framework Imports
from xphi.arch.bound.event.next import uuid4 
from xphi.kernel.space.bind.resolver import find_current_self, get_invoker
from xphi.watcher.plane.emitter import get_emitter

# ==========================================
# Loggers & Module Initialization
# ==========================================
_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
llm_log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")
registry_log = get_emitter("registry.adapter")
adapter_log = get_emitter("adapter.base")


# ==========================================
# 1. Base Interfaces & Fallback Adapters
# ==========================================
class BaseProviderAdapter:
    """LLM 호출을 수행하고, 단일 응답 객체 또는 원시 청크 제너레이터를 반환하는 어댑터 인터페이스"""
    async def execute(self, ctx: CompletionContext) -> Union[ModelResponse, AsyncGenerator]:
        raise NotImplementedError()


class GenericHTTPAdapter(BaseProviderAdapter):
    """순수 HTTP 통신(OpenAI 호환 포맷 등)을 통해 LLM과 직접 통신하는 경량 폴백 어댑터"""
    async def execute(self, ctx: CompletionContext) -> Union[ModelResponse, AsyncGenerator]:
        adapter_log.debug(f"[GenericHTTP] 🚀 execute START | model={ctx.model}, provider={ctx.custom_llm_provider}")
        
        headers = ctx.headers or {}
        if ctx.custom_llm_provider == "ollama" and ctx.api_key and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {ctx.api_key}"

        client = ctx.client_instance
        # AsyncHTTPClient 대신 기본 httpx.AsyncClient로 타입 체크
        if not isinstance(client, httpx.AsyncClient):
            client = get_client(
                is_async=True,
                params={"ssl_verify": ctx.system_meta.framework_flags.get("ssl_verify", None)},
            )

        payload = {
            "model": ctx.model,
            "messages": ctx.messages,
            "stream": ctx.stream,
        }
        if ctx.optional_params:
            payload.update(ctx.optional_params)

        if ctx.stream:
            adapter_log.debug("[GenericHTTP] 🌊 Initiating STREAM Execution")
            
            # httpx의 네이티브 스트리밍 방식 (Context Manager) 사용
            async def stream_generator():
                async with client.stream(
                    "POST",
                    url=ctx.api_base, 
                    headers=headers, 
                    json=payload, 
                    timeout=ctx.timeout
                ) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if line:
                            yield line
                            
            return stream_generator()

        else:
            adapter_log.debug("[GenericHTTP] ⚡ Initiating SINGULAR Execution")
            response = await client.post(
                url=ctx.api_base, 
                headers=headers, 
                json=payload, 
                timeout=ctx.timeout
            )
            response.raise_for_status()
            data = response.json()
            
            model_response = ctx.model_response
            if "choices" in data:
                model_response.choices = data["choices"]
            if "usage" in data:
                model_response.usage = data["usage"]
            if "id" in data:
                model_response.id = data["id"]
            return model_response


# ==========================================
# 2. Inter Framework Adapters
# ==========================================
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


# ==========================================
# 3. Adapter Registry
# ==========================================
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