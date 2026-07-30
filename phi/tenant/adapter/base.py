# phi.tenant.adapter.base
import json
from typing import Any, AsyncGenerator, Union, Dict

from tenant.switch.params import ModelResponse
from phi.runtime.executor.pre import CompletionContext
from bound.client import get_client, AsyncHTTPClient
from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.base")

class BaseProviderAdapter:
    """LLM 호출을 수행하고, 단일 응답 객체(ModelResponse) 또는 원시 청크 제너레이터(AsyncGenerator)를 반환하는 순수 어댑터 인터페이스"""
    async def execute(self, ctx: CompletionContext) -> Union[ModelResponse, AsyncGenerator]:
        raise NotImplementedError()

class GenericHTTPAdapter(BaseProviderAdapter):
    """순수 HTTP 통신(OpenAI 호환 포맷 등)을 통해 LLM과 직접 통신하는 경량 폴백(Fallback) 어댑터"""
    async def execute(self, ctx: CompletionContext) -> Union[ModelResponse, AsyncGenerator]:
        log.debug(f"[GenericHTTP] 🚀 execute START | model={ctx.model}, provider={ctx.custom_llm_provider}")
        
        ## 헤더 및 인증 세팅
        headers = ctx.headers or {}
        if ctx.custom_llm_provider == "ollama" and ctx.api_key and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {ctx.api_key}"

        ## HTTP 클라이언트 준비
        client = ctx.client_instance
        if not isinstance(client, AsyncHTTPClient):
            client = get_client(
                is_async=True,
                llm_provider=ctx.custom_llm_provider,
                params={"ssl_verify": ctx.litellm_params.get("ssl_verify", None)},
                shared_session=ctx.shared_session,
            )

        ## Payload 준비
        payload = {
            "model": ctx.model,
            "messages": ctx.messages,
            "stream": ctx.stream,
        }
        
        ## ctx.optional_params 내의 추가 인자(temperature 등) 병합
        if ctx.optional_params:
            payload.update(ctx.optional_params)

        ## 스트리밍(Stream) 처리 -> 제너레이터 반환
        if ctx.stream:
            log.debug("[GenericHTTP] 🌊 Initiating STREAM Execution")
            response = await client.post(
                url=ctx.api_base, 
                headers=headers, 
                json=payload, 
                timeout=ctx.timeout,
                stream=True
            )
            async def stream_generator():
                async for line in response.aiter_lines():
                    if line:
                        yield line
            
            return stream_generator()

        ## 비-스트리밍(Sync) 처리 -> JSON 파싱 후 ModelResponse 반환
        else:
            log.debug("[GenericHTTP] ⚡ Initiating SINGULAR Execution")
            response = await client.post(
                url=ctx.api_base, 
                headers=headers, 
                json=payload, 
                timeout=ctx.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            
            ## 이미 OpenAI 규격인 경우 그대로 매핑
            model_response = ctx.model_response
            if "choices" in data:
                model_response.choices = data["choices"]
            if "usage" in data:
                model_response.usage = data["usage"]
            if "id" in data:
                model_response.id = data["id"]
            return model_response