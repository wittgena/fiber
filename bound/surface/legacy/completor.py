# bound.surface.legacy.completor
## @lineage: bound.adapter.surface.legacy.completor
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, Union
import httpx

from bound.adapter.switch.params import ModelResponse
from bound.router.config import ProviderConfigManager
from anchor.registry.model.config.base import BaseConfig
from anchor.registry.model.config.resolver import config
from bound.surface.legacy.info import ProviderTypes
from bound.transport.bridge.client import AsyncHTTPClient
from bound.transport.bridge.client import HTTPClient
from bound.transport.bridge.client import get_client
from bound.transport.stream.wrapper import StreamWrapper

from watcher.plane.emitter import get_emitter

log = get_emitter("completion.handler")


@dataclass
class RequestContext:
    """내부 메서드 간 파라미터 전달을 단순화하기 위한 컨텍스트 객체"""
    model: str
    messages: list
    api_base: Optional[str]
    custom_llm_provider: str
    optional_params: dict
    litellm_params: dict
    timeout: Union[float, httpx.Timeout]
    stream: bool
    api_key: Optional[str]
    headers: Dict[str, Any]
    client: Optional[Union[HTTPClient, AsyncHTTPClient]]
    shared_session: Optional[Any]
    
    # 셋업 과정에서 채워지는 내부 상태
    provider_config: Optional[BaseConfig] = None
    json_mode: bool = False
    request_data: dict = field(default_factory=dict)
    signed_json_body: Optional[bytes] = None


class CompletionHandler:
    
    def completion(
        self,
        model: str,
        messages: list,
        api_base: Optional[str],
        custom_llm_provider: str,
        model_response: ModelResponse,
        encoding: Any,
        logging_obj: Any,
        optional_params: dict,
        timeout: Union[float, httpx.Timeout],
        litellm_params: dict,
        acompletion: bool,
        stream: Optional[bool] = False,
        fake_stream: bool = False,
        api_key: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        client: Optional[Union[HTTPClient, AsyncHTTPClient]] = None,
        provider_config: Optional[BaseConfig] = None,
        shared_session: Optional[Any] = None,
    ):
        log.debug(f"[Completion] 진입: model={model}, provider={custom_llm_provider}, async={acompletion}, stream={stream}")
        
        ctx = RequestContext(
            model=model, messages=messages, api_base=api_base,
            custom_llm_provider=custom_llm_provider, optional_params=optional_params,
            litellm_params=litellm_params, timeout=timeout, stream=stream or False,
            api_key=api_key, headers=headers or {}, client=client,
            shared_session=shared_session, provider_config=provider_config
        )
        
        # 1. 요청 전처리
        self._prepare_request_context(ctx)

        # 2. 실행 분기
        if acompletion:
            log.debug("[Completion] 비동기(Async) 처리 흐름으로 분기합니다.")
            return self._run_async(ctx, model_response, encoding)
        
        log.debug("[Completion] 동기(Sync) 처리 흐름으로 분기합니다.")
        return self._run_sync(ctx, model_response, encoding)

    # ------------------------------------------------------------------
    # 1. 내부 전처리 로직
    # ------------------------------------------------------------------
    def _prepare_request_context(self, ctx: RequestContext) -> None:
        ctx.json_mode = ctx.optional_params.pop("json_mode", False)
        extra_body = ctx.optional_params.pop("extra_body", None)

        ctx.provider_config = ctx.provider_config or ProviderConfigManager.get_provider_chat_config(
            model=ctx.model, provider=ProviderTypes(ctx.custom_llm_provider)
        )
        if not ctx.provider_config:
            log.error(f"[Prepare] Provider config 획득 실패: {ctx.model} / {ctx.custom_llm_provider}")
            raise ValueError(f"Provider config not found for model: {ctx.model}")

        ctx.headers = ctx.provider_config.validate_environment(
            api_key=ctx.api_key, headers=ctx.headers, model=ctx.model,
            messages=ctx.messages, optional_params=ctx.optional_params,
            api_base=ctx.api_base, litellm_params=ctx.litellm_params,
        )

        ctx.api_base = ctx.provider_config.get_complete_url(
            api_base=ctx.api_base, api_key=ctx.api_key, model=ctx.model,
            optional_params=ctx.optional_params, stream=ctx.stream, litellm_params=ctx.litellm_params,
        )

        ctx.request_data = ctx.provider_config.transform_request(
            model=ctx.model, messages=ctx.messages, optional_params=ctx.optional_params,
            litellm_params=ctx.litellm_params, headers=ctx.headers,
        )
        if extra_body:
            ctx.request_data.update(extra_body)
            
        if ctx.stream and ctx.provider_config.supports_stream_param_in_request_body:
            ctx.request_data["stream"] = True

        ctx.headers, ctx.signed_json_body = ctx.provider_config.sign_request(
            headers=ctx.headers, optional_params=ctx.optional_params, request_data=ctx.request_data,
            api_base=ctx.api_base, api_key=ctx.api_key, stream=ctx.stream,
            fake_stream=False, model=ctx.model,
        )
        log.debug(f"[Prepare] 컨텍스트 세팅 완료. 최종 호출 URL: {ctx.api_base}")

    async def _run_async(self, ctx: RequestContext, model_response: ModelResponse, encoding: Any):
        if not isinstance(ctx.client, AsyncHTTPClient):
            log.debug("[AsyncFlow] 새로운 Async HTTP Client를 생성합니다.")
            client = get_client(
                is_async=True,
                llm_provider=ProviderTypes(ctx.custom_llm_provider),
                params={"ssl_verify": ctx.litellm_params.get("ssl_verify", None)},
                shared_session=ctx.shared_session,
            )
        else:
            log.debug("[AsyncFlow] 기존에 주입된 Async HTTP Client를 재사용합니다.")
            client = ctx.client

        if ctx.stream:
            if ctx.provider_config.has_custom_stream_wrapper:
                log.debug("[AsyncFlow] Custom Stream Wrapper를 반환합니다.")
                return await ctx.provider_config.get_async_custom_stream_wrapper(
                    model=ctx.model, custom_llm_provider=ctx.custom_llm_provider,
                    api_base=ctx.api_base, headers=ctx.headers, data=ctx.request_data,
                    messages=ctx.messages, client=client, json_mode=ctx.json_mode,
                    signed_json_body=ctx.signed_json_body, logging_obj=None,
                )
                
            response = await self._execute_http_call(client.post, ctx)
            log.debug("[AsyncFlow] 기본 Stream Wrapper 생성을 완료했습니다.")
            return StreamWrapper(
                completion_stream=ctx.provider_config.get_model_response_iterator(
                    streaming_response=response.aiter_lines(), sync_stream=False
                ),
                model=ctx.model, custom_llm_provider=ctx.custom_llm_provider, logging_obj=None
            )

        # 논스트림
        response = await self._execute_http_call(client.post, ctx)
        log.debug("[AsyncFlow] 논스트림 응답 변환을 진행합니다.")
        return ctx.provider_config.transform_response(
            model=ctx.model, raw_response=response, model_response=model_response,
            api_key=ctx.api_key, request_data=ctx.request_data, messages=ctx.messages,
            optional_params=ctx.optional_params, litellm_params=ctx.litellm_params,
            encoding=encoding, json_mode=ctx.json_mode, logging_obj=None,
        )

    # ------------------------------------------------------------------
    # 2-B. 동기 (Sync) 실행 흐름
    # ------------------------------------------------------------------
    def _run_sync(self, ctx: RequestContext, model_response: ModelResponse, encoding: Any):
        if not isinstance(ctx.client, HTTPClient):
            log.debug("[SyncFlow] 새로운 Sync HTTP Client를 생성합니다.")
            client = get_client(is_async=False, params={"ssl_verify": ctx.litellm_params.get("ssl_verify", None)})
        else:
            log.debug("[SyncFlow] 기존에 주입된 Sync HTTP Client를 재사용합니다.")
            client = ctx.client

        if ctx.stream:
            if ctx.provider_config.has_custom_stream_wrapper:
                log.debug("[SyncFlow] Custom Stream Wrapper를 반환합니다.")
                return ctx.provider_config.get_sync_custom_stream_wrapper(
                    model=ctx.model, custom_llm_provider=ctx.custom_llm_provider,
                    api_base=ctx.api_base, headers=ctx.headers, data=ctx.request_data,
                    signed_json_body=ctx.signed_json_body, messages=ctx.messages,
                    client=client, json_mode=ctx.json_mode, logging_obj=None,
                )
                
            response = self._execute_http_call(client.post, ctx)
            log.debug("[SyncFlow] 기본 Stream Wrapper 생성을 완료했습니다.")
            return StreamWrapper(
                completion_stream=ctx.provider_config.get_model_response_iterator(
                    streaming_response=response.iter_lines(), sync_stream=True, json_mode=ctx.json_mode
                ),
                model=ctx.model, custom_llm_provider=ctx.custom_llm_provider, logging_obj=None
            )

        # 논스트림
        response = self._execute_http_call(client.post, ctx)
        log.debug("[SyncFlow] 논스트림 응답 변환을 진행합니다.")
        return ctx.provider_config.transform_response(
            model=ctx.model, raw_response=response, model_response=model_response,
            api_key=ctx.api_key, request_data=ctx.request_data, messages=ctx.messages,
            optional_params=ctx.optional_params, litellm_params=ctx.litellm_params,
            encoding=encoding, json_mode=ctx.json_mode, logging_obj=None,
        )

    # ------------------------------------------------------------------
    # 3. 코어 HTTP 통신 및 에러 핸들링 로직
    # ------------------------------------------------------------------
    def _execute_http_call(self, post_method: Any, ctx: RequestContext) -> httpx.Response:
        max_retry = max(ctx.provider_config.max_retry_on_unprocessable_entity_error, 1)
        response = None
        
        for i in range(max_retry):
            try:
                payload = ctx.signed_json_body if ctx.signed_json_body is not None else json.dumps(ctx.request_data)
                
                log.debug(f"[HTTP] 요청 발송: {ctx.api_base} (시도 {i+1}/{max_retry})")
                result = post_method(
                    url=ctx.api_base, headers=ctx.headers, data=payload,
                    timeout=ctx.timeout, stream=ctx.stream
                )
                
                import asyncio
                if asyncio.iscoroutine(result):
                    import nest_asyncio
                    response = asyncio.run(result)
                else:
                    response = result
                
                log.debug(f"[HTTP] 응답 수신 성공: 상태코드 {response.status_code}")
                break
                
            except httpx.HTTPStatusError as e:
                log.debug(f"[HTTP] HTTPStatusError 발생: {e.response.status_code} - {e.response.text}")
                hit_max_retry = i + 1 == max_retry
                if not hit_max_retry and ctx.provider_config.should_retry_llm_api_inside_llm_translation_on_http_error(e, ctx.litellm_params):
                    log.debug("[HTTP] 재시도 조건 충족. 요청 데이터를 변환하여 다시 시도합니다.")
                    ctx.request_data = ctx.provider_config.transform_request_on_unprocessable_entity_error(e, ctx.request_data)
                    continue
                raise self._handle_error(e, ctx.provider_config)
            except Exception as e:
                log.debug(f"[HTTP] 예외 발생: {str(e)}")
                raise self._handle_error(e, ctx.provider_config)

        if response is None:
            log.error("[HTTP] API로부터 유효한 응답을 받지 못했습니다.")
            raise ctx.provider_config.get_error_class("No response from the API", 422, {})
            
        return response

    def _handle_error(self, e: Exception, provider_config: BaseConfig):
        log.debug("[ErrorHandler] 에러 핸들링 처리 진입")
        status_code = getattr(e, "status_code", 500)
        error_headers = getattr(e, "headers", {})
        
        if isinstance(e, httpx.HTTPStatusError):
            error_text = e.response.text
            status_code = e.response.status_code
        else:
            error_text = getattr(e, "text", str(e))
            
        error_response = getattr(e, "response", None)
        if error_response:
            error_headers = dict(getattr(error_response, "headers", error_headers))
            error_text = getattr(error_response, "text", error_text)

        if provider_config is None:
            from anchor.registry.model.config.base import BaseLLMException
            log.error(f"[ErrorHandler] Provider config 없음. BaseLLMException 발생: {error_text}")
            raise BaseLLMException(status_code=status_code, message=error_text, headers=error_headers)

        log.error(f"[ErrorHandler] Provider 에러 클래스 발생: 상태 {status_code}, 메시지: {error_text}")
        raise provider_config.get_error_class(
            error_message=error_text, status_code=status_code, headers=error_headers
        )