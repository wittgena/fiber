# eco.legacy.openai.embedding
## @lineage: adapter.legacy.openai.embedding
## @lineage: bound.surface.legacy.openai.embedding
## @lineage: bound.adapter.surface.legacy.openai.embedding
## @lineage: anchor.provider.legacy.openai.embedding
## @lineage: anchor.surface.model.client.openai.embedding
## @lineage: anchor.surface.model.openai.embedding
## @lineage: anchor.surface.model.types.openai.embedding
## @lineage: anchor.surface.model.legacy.openai.embedding
## @lineage: bound.adapter.legacy.llm.openai.embedding
## @lineage: anchor.surface.legacy.llm.openai.embedding
## @lineage: anchor.surface.legacy.openai.embedding
## @lineage: bound.inter.llms.openai.embedding
## @lineage: bound.adapter.llama.llms.openai.embedding
## @lineage: bound.channel.bridge.llms.openai.embedding
## @lineage: channel.bridge.llms.openai.embedding
## @lineage: anchor.model.llms.openai.embedding
## @lineage: channel.llms.openai.embedding
## @lineage: gate.llms.openai.embedding
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union
import httpx
if TYPE_CHECKING:
    from aiohttp import ClientSession
import openai
from openai import AsyncOpenAI, OpenAI
from pydantic import BaseModel
from resolver.model.config.constants import DEFAULT_MAX_RETRIES
from eco.legacy.types import EmbeddingResponse
from gateway.adapter.response import convert_to_model_response_object
from eco.legacy.openai.base import BaseOpenAILLM, OpenAIError
from watcher.plane.emitter import get_emitter

log = get_emitter("openai.embedding")


@dataclass
class OpenAIEmbeddingContext:
    """OpenAI Embedding 요청에 필요한 상태와 파라미터를 담는 컨텍스트 객체"""
    model: str
    input: list
    optional_params: dict
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    
    ## 이전 에러를 방지하기 위해 default_factory + lambda 사용
    timeout: Union[float, httpx.Timeout] = field(default_factory=lambda: httpx.Timeout(60.0))
    max_retries: int = 2
    
    client: Optional[Union[OpenAI, AsyncOpenAI]] = None
    shared_session: Optional[Any] = None
    
    # 세팅 과정을 통해 채워지는 내부 데이터
    request_data: dict = field(default_factory=dict)


class OpenAIEmbedding(BaseOpenAILLM):
    def create_context(
        self,
        model: str,
        input: list,
        optional_params: dict,
        timeout: Union[float, httpx.Timeout],
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
        max_retries: Optional[int] = None,
        shared_session: Optional[Any] = None,
        **kwargs  # logging_obj 등 불필요한 파라미터 무시 용도
    ) -> OpenAIEmbeddingContext:
        """파라미터를 받아 검증하고 변환하여 실행 가능한 Context 객체를 반환합니다."""
        
        safe_max_retries = max_retries if max_retries is not None else DEFAULT_MAX_RETRIES
        if not isinstance(safe_max_retries, int):
            raise OpenAIError(status_code=422, message=f"max_retries must be an int. Passed: {safe_max_retries}")

        request_data = {"model": model, "input": input, **optional_params}

        return OpenAIEmbeddingContext(
            model=model,
            input=input,
            optional_params=optional_params,
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=safe_max_retries,
            client=client,
            shared_session=shared_session,
            request_data=request_data
        )

    # ------------------------------------------------------------------
    # 2. 메인 실행 (동기 / 비동기)
    # ------------------------------------------------------------------
    def embedding(self, ctx: OpenAIEmbeddingContext, model_response: EmbeddingResponse) -> EmbeddingResponse:
        """동기(Sync) 임베딩 엔트리 포인트"""
        client = self._get_openai_client(
            is_async=False, 
            api_key=ctx.api_key, 
            api_base=ctx.api_base, 
            timeout=ctx.timeout, 
            max_retries=ctx.max_retries, 
            client=ctx.client
        )
        
        try:
            headers, response = self._make_sync_request(client, ctx)
            return self._process_response(response, model_response, headers)
        except Exception as e:
            raise self._format_error(e)

    async def aembedding(self, ctx: OpenAIEmbeddingContext, model_response: EmbeddingResponse) -> EmbeddingResponse:
        """비동기(Async) 임베딩 엔트리 포인트"""
        client = self._get_openai_client(
            is_async=True, 
            api_key=ctx.api_key, 
            api_base=ctx.api_base, 
            timeout=ctx.timeout, 
            max_retries=ctx.max_retries, 
            client=ctx.client,
            shared_session=ctx.shared_session
        )

        try:
            headers, response = await self._make_async_request(client, ctx)
            return self._process_response(response, model_response, headers)
        except Exception as e:
            raise self._format_error(e)

    # ------------------------------------------------------------------
    # 3. HTTP 호출 코어
    # ------------------------------------------------------------------
    def _make_sync_request(self, client: OpenAI, ctx: OpenAIEmbeddingContext) -> Tuple[dict, BaseModel]:
        raw_response = client.embeddings.with_raw_response.create(**ctx.request_data, timeout=ctx.timeout)
        headers = dict(raw_response.headers) if hasattr(raw_response, "headers") else {}
        response = raw_response.parse()
        return headers, response

    async def _make_async_request(self, client: AsyncOpenAI, ctx: OpenAIEmbeddingContext) -> Tuple[dict, BaseModel]:
        raw_response = await client.embeddings.with_raw_response.create(**ctx.request_data, timeout=ctx.timeout)
        headers = dict(raw_response.headers) if hasattr(raw_response, "headers") else {}
        response = raw_response.parse()
        return headers, response

    # ------------------------------------------------------------------
    # 4. 응답 후처리 및 에러 교정 로직
    # ------------------------------------------------------------------
    def _process_response(
        self, response: BaseModel, model_response: EmbeddingResponse, headers: dict
    ) -> EmbeddingResponse:
        """Pydantic 응답 객체를 표준 ModelResponse 포맷으로 변환"""
        stringified_response = response.model_dump()
        return convert_to_model_response_object(
            response_object=stringified_response,
            model_response_object=model_response,
            response_type="embedding",
            _response_headers=headers,
        )

    def _format_error(self, e: Exception) -> Exception:
        """통일된 예외 포맷터"""
        if isinstance(e, OpenAIError):
            return e
            
        status_code = getattr(e, "status_code", 500)
        error_headers = getattr(e, "headers", None)
        error_text = getattr(e, "text", str(e))
        error_response = getattr(e, "response", None)
        
        if error_headers is None and error_response:
            error_headers = getattr(error_response, "headers", None)
            
        return OpenAIError(
            status_code=status_code, 
            message=error_text, 
            headers=error_headers
        )