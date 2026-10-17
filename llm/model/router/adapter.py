# fiber.llm.model.router.adapter
import json
import asyncio
import functools
import httpx
from pathlib import Path
from typing import Dict, AsyncGenerator, Any, List, Union

from fiber.llm.response import ModelResponse
from fiber.gateway.llm.context.metadata import CompletionContext, EmbeddingContext
from fiber.llm.model.router.llm import LLMRouter
from fiber.llm.model.router.embedding import EmbeddingRouter
from fiber.llm.provider.resolver import get_llm_provider

from fiber.llm.exception.mapping import exception_type
from fiber.gateway.llm.state.traverser import StateMapper
from fiber.gateway.llm.stream.parser.chunk import StreamChunkParser
from fiber.llm.compat.param import PROVIDER_PARAM_RULES

from xphi.arch.bound.client.http import get_client
from xphi.arch.bound.event.next import uuid4 
from xphi.node.space.bind.resolver import get_invoker
from xphi.watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
llm_log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")
registry_log = get_emitter("registry.adapter")
adapter_log = get_emitter("adapter.base")


class BaseProviderAdapter:
    async def execute(self, ctx: Union[CompletionContext, EmbeddingContext]) -> Any:
        raise NotImplementedError()


class GenericHTTPAdapter(BaseProviderAdapter):
    async def execute(self, ctx: CompletionContext) -> Union[ModelResponse, AsyncGenerator]:
        req_id = str(uuid4())[:8]
        provider = ctx.custom_llm_provider
        adapter_log.debug(f"[GenericHTTP-{req_id}] 🚀 execute START | model={ctx.model}, provider={provider}")
        
        headers = (ctx.headers or {}).copy()
        if ctx.api_key and ctx.api_key != "not-needed" and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {ctx.api_key}"

        client = ctx.client_instance
        if not isinstance(client, httpx.AsyncClient):
            ssl_verify = ctx.system_meta.framework_flags.get("ssl_verify", None) if ctx.system_meta else None
            client = get_client(is_async=True, params={"ssl_verify": ssl_verify})
        
        rule = PROVIDER_PARAM_RULES.get(provider, PROVIDER_PARAM_RULES["defaults"])
        msg_schema = rule.get("message_schema", "openai_array")

        payload: Dict[str, Any] = {
            "model": ctx.model,
            "stream": ctx.stream,
        }

        if msg_schema == "string_last":
            content = ctx.messages[-1].get("content", "") if isinstance(ctx.messages, list) and ctx.messages else ""
            payload["message"] = content
        else:
            payload["messages"] = ctx.messages

        if ctx.optional_params:
            mapping_rule = rule.get("mapping", {})
            for k, v in ctx.optional_params.items():
                if v is not None:
                    target_key = mapping_rule.get(k, k)
                    payload[target_key] = v

        target_url = StateMapper.resolve_chat_endpoint(
            provider=provider,
            base_url=ctx.api_base or ""
        )
        adapter_log.debug(f"[GenericHTTP-{req_id}] 📦 URL: {target_url} | Payload: {json.dumps(payload, ensure_ascii=False)[:300]}...")

        if ctx.stream:
            adapter_log.debug(f"[GenericHTTP-{req_id}] 🌊 Initiating STREAM Execution")
            
            async def stream_generator():
                async with client.stream(
                    "POST",
                    url=target_url,
                    headers=headers, 
                    json=payload, 
                    timeout=ctx.timeout
                ) as response:
                    if response.is_error:
                        await response.aread()
                        adapter_log.error(f"[GenericHTTP-{req_id}] 🚨 스트림 HTTP 에러 ({response.status_code}): {response.text}")
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if line:
                            yield line
                            
            return stream_generator()
        else:
            adapter_log.debug(f"[GenericHTTP-{req_id}] ⚡ Initiating SINGULAR Execution")
            response = await client.post(
                url=target_url, 
                headers=headers, 
                json=payload, 
                timeout=ctx.timeout
            )

            if response.is_error:
                adapter_log.error(f"[GenericHTTP-{req_id}] 🚨 단일 HTTP 에러 ({response.status_code}): {response.text}")

            response.raise_for_status()
            data = response.json()
            
            model_response = ctx.model_response
            model_response.raw = data.copy() if isinstance(data, dict) else data
            
            if isinstance(data, dict):
                if "id" in data:
                    model_response.id = data["id"]
                
                # 1. 표준 OpenAI 포맷 우선 매핑, 없을 경우 StateMapper를 통한 비표준(Cohere/Ollama 등) 추출
                if "choices" in data and data["choices"]:
                    model_response.choices = data["choices"]
                    if "usage" in data:
                        model_response.usage = data["usage"]
                else:
                    content, usage_dict = StateMapper.extract_response(data, provider=provider)
                    model_response.choices = [{
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop"
                    }]
                    if usage_dict:
                        model_response.usage = usage_dict

            return model_response


class InterLLMAdapter(BaseProviderAdapter):
    def __init__(self):
        self.router = LLMRouter()
        self.mapper = StateMapper()

    async def execute(self, ctx: CompletionContext) -> Any:
        req_id = str(uuid4())[:8]
        provider = ctx.custom_llm_provider or "inter"
        llm_log.debug(f"[InterLLM-{req_id}] 🚀 execute START | model={ctx.model}, provider={provider}, stream={ctx.stream}")

        resolved_api_key = ctx.api_key
        if not resolved_api_key or resolved_api_key == "not-needed":
            try:
                _, _, dynamic_key, _ = get_llm_provider(model=ctx.model, custom_llm_provider=provider)
                if dynamic_key:
                    resolved_api_key = dynamic_key
            except Exception as e:
                llm_log.warning(f"[InterLLM-{req_id}] ⚠️ Locator key resolution bypassed/failed: {e}")

        # 원본 ctx.optional_params 오염 방지를 위해 얕은 복사 수행
        opt_params = (ctx.optional_params or {}).copy()
        
        execution_kwargs = {}
        if "tools" in opt_params:
            execution_kwargs["tools"] = opt_params.pop("tools")
        if "tool_choice" in opt_params:
            execution_kwargs["tool_choice"] = opt_params.pop("tool_choice")

        llama_kwargs = {
            "api_key": resolved_api_key,
            "api_base": ctx.api_base,
            "temperature": opt_params.pop("temperature", 0.7),
            "max_tokens": opt_params.pop("max_tokens", None),
            "timeout": ctx.timeout if isinstance(ctx.timeout, (int, float)) else 60.0,
            **opt_params
        }
        llama_kwargs = {k: v for k, v in llama_kwargs.items() if v is not None and v != "not-needed"}

        try:
            llm = self.router.route_and_load(
                model_name=ctx.model, 
                custom_llm_provider=provider, 
                **llama_kwargs
            )
        except Exception as e:
            llm_log.error(f"[InterLLM-{req_id}] 🚨 모델 인스턴스 생성 실패: {e}")
            raise exception_type(
                model=ctx.model,
                original_exception=e,
                custom_llm_provider=provider,
                completion_kwargs=llama_kwargs
            )

        llama_messages = self.mapper.to_chat_messages(ctx.messages)

        if ctx.stream:
            llm_log.debug(f"[InterLLM-{req_id}] 🌊 Initiating STREAM Execution")
            if ctx.acompletion:
                response_stream = await llm.astream_chat(llama_messages, **execution_kwargs)
            else:
                stream_func = functools.partial(llm.stream_chat, llama_messages, **execution_kwargs)
                response_stream = await asyncio.to_thread(stream_func)
            
            async def stream_generator():
                def normalize_chunk(chunk_obj: Any) -> dict:
                    raw_target = getattr(chunk_obj, "raw", None) or chunk_obj
                    
                    # StreamChunkParser로 파싱 일원화 (inter_llm_fallback 룰셋 활용)
                    parsed = StreamChunkParser.parse(provider, raw_target)
                    
                    # chunk.raw에서 텍스트가 안 뽑혔을 경우 상위 래퍼(chunk_obj.delta 등)에서 2차 시도
                    content = parsed["text"]
                    if not content and raw_target is not chunk_obj:
                        fallback_parsed = StreamChunkParser.parse("inter_llm_fallback", chunk_obj)
                        content = fallback_parsed["text"]

                    raw_finish_reason = parsed["finish_reason"]
                    finish_reason = None
                    if raw_finish_reason:
                        finish_reason = (
                            str(raw_finish_reason.value).lower()
                            if hasattr(raw_finish_reason, "value")
                            else str(raw_finish_reason).split(".")[-1].lower()
                        )

                    delta_payload: Dict[str, Any] = {"role": "assistant"}
                    if content:
                        delta_payload["content"] = content
                    if parsed["tool_calls"]:
                        delta_payload["tool_calls"] = parsed["tool_calls"]

                    normalized = {
                        "id": parsed["id"] or f"chatcmpl-{req_id}",
                        "object": "chat.completion.chunk",
                        "model": ctx.model,
                        "choices": [{
                            "index": 0,
                            "delta": delta_payload,
                            "finish_reason": finish_reason
                        }]
                    }
                    
                    if parsed["usage"]:
                        normalized["usage"] = parsed["usage"]
                    if parsed["logprobs"]:
                        normalized["choices"][0]["logprobs"] = parsed["logprobs"]

                    return normalized

                if ctx.acompletion:
                    async for chunk in response_stream:
                        yield normalize_chunk(chunk)
                else:
                    for chunk in response_stream:
                        yield normalize_chunk(chunk)

            return stream_generator()
            
        else:
            llm_log.debug(f"[InterLLM-{req_id}] ⚡ Initiating SINGULAR Execution")
            if ctx.acompletion:
                response = await llm.achat(llama_messages, **execution_kwargs)
            else:
                chat_func = functools.partial(llm.chat, llama_messages, **execution_kwargs)
                response = await asyncio.to_thread(chat_func)
            
            # 하드코딩된 기본값("gemini") 대신 실제 provider 전달
            choice_data = self.mapper.to_openai_choice(response, req_id, llm_log, provider=provider)
            ctx.model_response.choices = [choice_data]
            
            # 누락되었던 Usage 추출 및 할당 보완
            _, usage_dict = self.mapper.extract_response(response, provider=provider)
            if usage_dict:
                ctx.model_response.usage = usage_dict

            if hasattr(response, "raw"):
                ctx.model_response.raw = response.raw
            
            return ctx.model_response


class InterEmbeddingAdapter(BaseProviderAdapter):
    def __init__(self):
        self.router = EmbeddingRouter()

    async def execute(self, ctx: EmbeddingContext) -> Any:
        opt_params = (ctx.optional_params or {}).copy()
        llama_kwargs = {
            "api_key": ctx.api_key,
            "api_base": ctx.api_base,
            "timeout": ctx.timeout if isinstance(ctx.timeout, (int, float)) else 60.0,
            **opt_params
        }
        llama_kwargs = {k: v for k, v in llama_kwargs.items() if v is not None and v != "not-needed"}

        try:
            embed_model = self.router.route_and_load(model_name=ctx.model, **llama_kwargs)
        except Exception as e:
            raise RuntimeError(f"[LlamaBridge] Embedding 모델 인스턴스 생성 실패: {e}")

        raw_inputs: Union[str, List[str]] = ctx.input
        texts = raw_inputs if isinstance(raw_inputs, list) else [raw_inputs]

        if getattr(ctx, "aembedding", False):
            embeddings = await embed_model.aget_text_embedding_batch(texts)
        else:
            embed_func = functools.partial(embed_model.get_text_embedding_batch, texts)
            embeddings = await asyncio.to_thread(embed_func)

        data_objects = [
            {
                "object": "embedding",
                "index": idx,
                "embedding": emb
            }
            for idx, emb in enumerate(embeddings)
        ]

        usage_placeholder = {"prompt_tokens": -1, "total_tokens": -1}

        if hasattr(ctx, "model_response") and ctx.model_response is not None:
            ctx.model_response.data = data_objects
            ctx.model_response.model = ctx.model
            ctx.model_response.object = "list"
            ctx.model_response.usage = usage_placeholder
            return ctx.model_response
        
        return {
            "object": "list",
            "data": data_objects,
            "model": ctx.model,
            "usage": usage_placeholder
        }


class AdapterRegistry:
    _adapters: Dict[str, Dict[str, BaseProviderAdapter]] = {
        "llm": {},
        "embedding": {}
    }
    _fallback_adapters: Dict[str, BaseProviderAdapter] = {}
    _is_initialized: bool = False

    @classmethod
    def setup_defaults(cls):
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
        for provider in ["ollama", "inter"]:
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
        registry_log.debug(f"[Registry] '{task_type}'에 '{provider_name}' 어댑터 동적 등록됨.")

    @classmethod
    def get_adapter(cls, task_type: str, provider_name: str) -> BaseProviderAdapter:
        if not cls._is_initialized:
            cls.setup_defaults()
            
        task_manifold = cls._adapters.get(task_type, {})
        fallback = cls._fallback_adapters.get(task_type)
        adapter = task_manifold.get(provider_name, fallback)
        if not adapter:
            raise ValueError(f"[Registry Error] '{task_type}' 작업을 처리할 폴백 어댑터가 구성되지 않았습니다.")
            
        return adapter