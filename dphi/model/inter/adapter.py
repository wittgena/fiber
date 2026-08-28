# fiber.dphi.model.inter.adapter
## @lineage: dphi.model.inter.adapter
## @lineage: agent.llm.router.base
import json
from typing import AsyncGenerator, Union
import httpx

from fiber.llm.param import ModelResponse
from fiber.llm.execution import CompletionContext 
from fiber.dphi.client.http import get_client

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("adapter.base")

class BaseProviderAdapter:
    """LLM 호출을 수행하고, 단일 응답 객체 또는 원시 청크 제너레이터를 반환하는 어댑터 인터페이스"""
    async def execute(self, ctx: CompletionContext) -> Union[ModelResponse, AsyncGenerator]:
        raise NotImplementedError()

class GenericHTTPAdapter(BaseProviderAdapter):
    """순수 HTTP 통신(OpenAI 호환 포맷 등)을 통해 LLM과 직접 통신하는 경량 폴백 어댑터"""
    async def execute(self, ctx: CompletionContext) -> Union[ModelResponse, AsyncGenerator]:
        log.debug(f"[GenericHTTP] 🚀 execute START | model={ctx.model}, provider={ctx.custom_llm_provider}")
        
        headers = ctx.headers or {}
        if ctx.custom_llm_provider == "ollama" and ctx.api_key and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {ctx.api_key}"

        client = ctx.client_instance
        # 변경 1: AsyncHTTPClient 대신 기본 httpx.AsyncClient로 타입 체크
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
            log.debug("[GenericHTTP] 🌊 Initiating STREAM Execution")
            
            # 변경 2: httpx의 네이티브 스트리밍 방식 (Context Manager) 사용
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
            log.debug("[GenericHTTP] ⚡ Initiating SINGULAR Execution")
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