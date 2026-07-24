# bound.resolver.openai.completion
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union, cast
from urllib.parse import urlparse
import httpx
if TYPE_CHECKING:
    from aiohttp import ClientSession

import openai
from openai import AsyncOpenAI, OpenAI
from pydantic import BaseModel

from eco.exception import BaseLLMException
from eco.tenant.switch.params import ModelResponse, ModelResponseStream
from eco.llama.router.config import ProviderConfigManager

from bound.resolver.model.config.base import BaseConfig
from bound.resolver.model.config.resolver import config
from bound.resolver.model.config.constants import DEFAULT_MAX_RETRIES
from bound.resolver.model.protype import ProviderTypes

from bound.resolver.legacy.types import EmbeddingResponse
from bound.gateway.parser.response import convert_to_model_response_object
from bound.gateway.stream.wrapper import StreamWrapper
from bound.resolver.openai.base import (
    BaseOpenAILLM,
    OpenAIError,
    drop_params_from_unprocessable_entity_error,
)
from bound.resolver.openai.config import OpenAIConfig
from watcher.plane.emitter import get_emitter

log = get_emitter("openai.completion")


@dataclass
class OpenAIContext:
    """OpenAI 요청에 필요한 모든 파라미터와 내부 상태를 담는 컨텍스트 객체"""
    model: str
    messages: list
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    api_version: Optional[str] = None
    organization: Optional[str] = None
    timeout: Union[float, httpx.Timeout] = field(default_factory=lambda: httpx.Timeout(60.0))
    max_retries: int = 2
    stream: bool = False
    
    optional_params: dict = field(default_factory=dict)
    litellm_params: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)
    
    client: Optional[Union[OpenAI, AsyncOpenAI]] = None
    shared_session: Optional[Any] = None
    provider_config: BaseConfig = field(default_factory=OpenAIConfig)
    request_data: dict = field(default_factory=dict)
    stream_options: Optional[dict] = None

class OpenAIChatCompletion(BaseOpenAILLM):
    def __init__(self) -> None:
        super().__init__()

    def create_context(
        self,
        model: str,
        messages: list,
        optional_params: dict,
        litellm_params: dict,
        timeout: Union[float, httpx.Timeout],
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        api_version: Optional[str] = None,
        organization: Optional[str] = None,
        headers: Optional[dict] = None,
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
        custom_llm_provider: Optional[str] = None,
        shared_session: Optional[Any] = None,
        **kwargs  # 기존 인터페이스 호환용 (logging_obj 등 무시)
    ) -> OpenAIContext:
        """파라미터를 받아 검증하고 변환하여 실행 가능한 Context 객체를 반환합니다."""
        
        # 기본 옵션 추출
        inference_params = optional_params.copy()
        stream = inference_params.pop("stream", False)
        stream_options = inference_params.pop("stream_options", None)
        max_retries = inference_params.pop("max_retries", 2)
        
        headers = headers or {}
        if headers:
            inference_params["extra_headers"] = headers

        # Provider Config 세팅
        provider_config = None
        if custom_llm_provider and model:
            try:
                provider_config = ProviderConfigManager.get_provider_chat_config(
                    model=model, provider=ProviderTypes(custom_llm_provider)
                )
            except ValueError:
                pass
        provider_config = provider_config or OpenAIConfig()

        # Request 데이터 트랜스폼 (JSON Body 등 구성)
        request_data = provider_config.transform_request(
            model=model, messages=messages, optional_params=inference_params,
            litellm_params=litellm_params, headers=headers
        )
        
        # 스트림 옵션 추가
        if stream:
            request_data["stream"] = True
            if stream_options:
                request_data["stream_options"] = stream_options
            elif not api_base or urlparse(api_base).hostname == "api.openai.com":
                request_data["stream_options"] = {"include_usage": True}

        return OpenAIContext(
            model=model, messages=messages, api_key=api_key, api_base=api_base,
            api_version=api_version, organization=organization, timeout=timeout,
            max_retries=max_retries, stream=stream, stream_options=stream_options,
            optional_params=inference_params, litellm_params=litellm_params,
            headers=headers, client=client, shared_session=shared_session,
            provider_config=provider_config, request_data=request_data
        )
    
    def _set_dynamic_params_on_client(
        self,
        client: Union[OpenAI, AsyncOpenAI],
        organization: Optional[str] = None,
        max_retries: Optional[int] = None,
    ):
        if organization is not None:
            client.organization = organization
        if max_retries is not None:
            client.max_retries = max_retries

    def _get_openai_client(
        self,
        is_async: bool,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        api_version: Optional[str] = None,
        timeout: Union[float, httpx.Timeout] = httpx.Timeout(None),
        max_retries: Optional[int] = DEFAULT_MAX_RETRIES,
        organization: Optional[str] = None,
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> Optional[Union[OpenAI, AsyncOpenAI]]:
        client_initialization_params: Dict = locals()
        if client is None:
            if not isinstance(max_retries, int):
                raise OpenAIError(
                    status_code=422,
                    message="max retries must be an int. Passed in value: {}".format(
                        max_retries
                    ),
                )
            cached_client = self.get_cached_openai_client(
                client_initialization_params=client_initialization_params,
                client_type="openai",
            )

            if cached_client:
                if isinstance(cached_client, OpenAI) or isinstance(
                    cached_client, AsyncOpenAI
                ):
                    return cached_client
            if is_async:
                _new_client: Union[OpenAI, AsyncOpenAI] = AsyncOpenAI(
                    api_key=api_key,
                    base_url=api_base,
                    http_client=OpenAIChatCompletion._get_async_http_client(
                        shared_session=shared_session
                    ),
                    timeout=timeout,
                    max_retries=max_retries,
                    organization=organization,
                )
            else:
                _new_client = OpenAI(
                    api_key=api_key,
                    base_url=api_base,
                    http_client=OpenAIChatCompletion._get_sync_http_client(),
                    timeout=timeout,
                    max_retries=max_retries,
                    organization=organization,
                )

            ## SAVE CACHE KEY
            self.set_cached_openai_client(
                openai_client=_new_client,
                client_initialization_params=client_initialization_params,
                client_type="openai",
            )
            return _new_client

        else:
            self._set_dynamic_params_on_client(
                client=client,
                organization=organization,
                max_retries=max_retries,
            )
            return client

    # ------------------------------------------------------------------
    # 2. 메인 실행 (동기 / 비동기)
    # ------------------------------------------------------------------
    def completion(self, ctx: OpenAIContext, model_response: ModelResponse) -> Union[ModelResponse, StreamWrapper]:
        """동기(Sync) 처리 엔트리 포인트"""
        client = self._get_openai_client(
            is_async=False, api_key=ctx.api_key, api_base=ctx.api_base, 
            api_version=ctx.api_version, timeout=ctx.timeout, max_retries=ctx.max_retries,
            organization=ctx.organization, client=ctx.client
        )
        
        # 재시도 로직 (OpenAI 역할 순서 교정 등)
        for attempt in range(2):
            try:
                headers, response = self._make_sync_request(client, ctx)
                return self._process_response(ctx, model_response, headers, response)
                
            except openai.UnprocessableEntityError as e:
                if config.drop_params:
                    ctx.request_data = drop_params_from_unprocessable_entity_error(e, ctx.request_data)
                    continue
                raise self._format_error(e)
            except Exception as e:
                if self._attempt_message_reformat_on_error(e, ctx):
                    continue
                raise self._format_error(e)

    async def acompletion(self, ctx: OpenAIContext, model_response: ModelResponse) -> Union[ModelResponse, StreamWrapper]:
        """비동기(Async) 처리 엔트리 포인트"""
        client = self._get_openai_client(
            is_async=True, api_key=ctx.api_key, api_base=ctx.api_base, 
            api_version=ctx.api_version, timeout=ctx.timeout, max_retries=ctx.max_retries,
            organization=ctx.organization, client=ctx.client, shared_session=ctx.shared_session
        )

        for attempt in range(2):
            try:
                headers, response = await self._make_async_request(client, ctx)
                return self._process_response(ctx, model_response, headers, response)
                
            except openai.UnprocessableEntityError as e:
                if config.drop_params:
                    ctx.request_data = drop_params_from_unprocessable_entity_error(e, ctx.request_data)
                    continue
                raise self._format_error(e)
            except Exception as e:
                if self._attempt_message_reformat_on_error(e, ctx):
                    continue
                raise self._format_error(e)

    # ------------------------------------------------------------------
    # 3. HTTP 호출 코어
    # ------------------------------------------------------------------
    def _make_sync_request(self, client: OpenAI, ctx: OpenAIContext) -> Tuple[dict, BaseModel]:
        try:
            raw_response = client.chat.completions.with_raw_response.create(**ctx.request_data, timeout=ctx.timeout)
            headers = dict(raw_response.headers) if hasattr(raw_response, "headers") else {}
            response = raw_response.parse()
            
            if not ctx.stream and not hasattr(response, "model_dump"):
                raise OpenAIError(status_code=500, message="Empty or invalid response from LLM endpoint.")
            return headers, response
        except Exception as e:
            raise e

    async def _make_async_request(self, client: AsyncOpenAI, ctx: OpenAIContext) -> Tuple[dict, BaseModel]:
        try:
            raw_response = await client.chat.completions.with_raw_response.create(**ctx.request_data, timeout=ctx.timeout)
            headers = dict(raw_response.headers) if hasattr(raw_response, "headers") else {}
            response = raw_response.parse()
            
            if not ctx.stream and not hasattr(response, "model_dump"):
                raise OpenAIError(status_code=500, message="Empty or invalid response from LLM endpoint.")
            return headers, response
        except Exception as e:
            raise e

    # ------------------------------------------------------------------
    # 4. 응답 후처리 및 에러 교정 로직
    # ------------------------------------------------------------------
    def _process_response(self, ctx: OpenAIContext, model_response: ModelResponse, headers: dict, response: Any):
        """스트림과 논스트림 응답을 통일성 있게 포맷팅하여 반환"""
        if ctx.stream:
            return StreamWrapper(
                completion_stream=response,
                model=ctx.model,
                custom_llm_provider="openai",
                stream_options=ctx.stream_options,
                _response_headers=headers,
            )
            
        stringified_response = response.model_dump()
        return convert_to_model_response_object(
            response_object=stringified_response,
            model_response_object=model_response,
            _response_headers=headers,
        )

    def _attempt_message_reformat_on_error(self, e: Exception, ctx: OpenAIContext) -> bool:
        """역할(Role) 순서 오류 발생 시 메시지를 교정하고 재시도를 허용할지 판단합니다."""
        error_msg = str(e)
        if "Conversation roles must alternate" in error_msg or "user and assistant roles should be alternating" in error_msg:
            log.debug("[OpenAI] 역할 순서 교정(Reformat) 진행")
            msgs = ctx.request_data.get("messages", [])
            new_msgs = []
            for i in range(len(msgs) - 1):
                new_msgs.append(msgs[i])
                if msgs[i]["role"] == msgs[i + 1]["role"]:
                    insert_role = "assistant" if msgs[i]["role"] == "user" else "user"
                    new_msgs.append({"role": insert_role, "content": ""})
            new_msgs.append(msgs[-1])
            ctx.request_data["messages"] = new_msgs
            return True
            
        if "Last message must have role `user`" in error_msg:
            ctx.request_data["messages"].append({"role": "user", "content": ""})
            return True
            
        if "unknown field: parameter index is not a valid field" in error_msg:
            config.remove_index_from_tool_calls(messages=ctx.request_data.get("messages", []))
            return True
            
        return False

    def _format_error(self, e: Exception) -> Exception:
        """통일된 예외 반환"""
        if isinstance(e, OpenAIError):
            return e
            
        status_code = getattr(e, "status_code", 500)
        error_headers = getattr(e, "headers", None)
        error_text = getattr(e, "text", getattr(e, "message", str(e)))
        error_response = getattr(e, "response", None)
        exception_body = getattr(e, "body", None)
        
        if error_headers is None and error_response:
            error_headers = getattr(error_response, "headers", None)
            
        return OpenAIError(
            status_code=status_code, message=error_text, headers=error_headers, body=exception_body
        )