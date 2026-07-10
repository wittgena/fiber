# bound.surface.legacy.response.api.handler
## @lineage: bound.transport.response.api.handler
import json
import ssl
from typing import Any, Coroutine, Dict, List, Literal, Optional, Union, Tuple
import httpx
from aiohttp import ClientSession

from anchor.provider.param.response import DeleteResponseResult
from anchor.provider.param.legacy import GenericLiteLLMParams

from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from bound.surface.legacy.provider import ProviderTypes
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.types import CallTypes
from bound.surface.legacy.openai.types import ResponseInputParam, ResponsesAPIResponse
from bound.surface.legacy.transport.client import AsyncHTTPClient
from bound.surface.legacy.transport.sync import HTTPClient
from bound.surface.legacy.transport.factory import get_client
from bound.surface.bridge.channel.stream.iterator import ResponseStreamIterator, MockResponsesAPIStreamingIterator, ResponsesAPIStreamingIterator, SyncResponsesAPIStreamingIterator
from bound.surface.legacy.response.api.context import ResponseAPIContext

from watcher.plane.emitter import get_emitter 

log = get_emitter("handler.api")

class ResponseApiHandler:
    def _resolve_client(self, ctx: ResponseAPIContext) -> Union[HTTPClient, AsyncHTTPClient]:
        """Context를 분석하여 적절한 동기/비동기 HTTP 클라이언트를 반환합니다."""
        ssl_verify_params = {"ssl_verify": ctx.provider.litellm_params.get("ssl_verify", None)}
        
        if ctx.is_async:
            if ctx.exec.client is not None and isinstance(ctx.exec.client, AsyncHTTPClient):
                return ctx.exec.client
            if ctx.exec.shared_session:
                log.debug(f"Creating async HTTP client with shared_session: {id(ctx.exec.shared_session)}")
                
            return get_client(
                is_async=True,
                llm_provider=ProviderTypes(ctx.provider.custom_llm_provider) if ctx.provider.custom_llm_provider else None,
                params=ssl_verify_params,
                shared_session=ctx.exec.shared_session,
            )
        else:
            if ctx.exec.client is not None and isinstance(ctx.exec.client, HTTPClient):
                return ctx.exec.client
            return get_client(is_async=False, params=ssl_verify_params)

    def _prepare_common_env(self, ctx: ResponseAPIContext, model: str = "None") -> Tuple[Dict[str, Any], str]:
        """환경 설정을 검증하고 병합된 헤더와 api_base URL을 추출합니다."""
        headers = ctx.provider.responses_api_provider_config.validate_environment(
            headers=ctx.exec.extra_headers or {},
            model=model,
            litellm_params=ctx.provider.litellm_params,
        )
        if ctx.exec.extra_headers:
            headers.update(ctx.exec.extra_headers)

        api_base = ctx.provider.responses_api_provider_config.get_complete_url(
            api_base=ctx.provider.litellm_params.api_base,
            litellm_params=dict(ctx.provider.litellm_params),
        )
        return headers, api_base

    def _execute_sync_request(
        self, ctx: ResponseAPIContext, client: HTTPClient, method: str, url: str, 
        headers: Dict[str, Any], input_data: Any = "", json_data: Optional[Dict] = None, 
        params: Optional[Dict] = None, stream: bool = False
    ) -> Any:
        """동기(Sync) 네트워크 요청 실행, 로깅, 예외 처리를 전담합니다."""
        if ctx.provider.logging_obj:
            ctx.provider.logging_obj.pre_call(
                input=input_data, api_key="",
                additional_args={"complete_input_dict": json_data or params or {}, "api_base": url, "headers": headers}
            )
        try:
            kwargs = {"url": url, "headers": headers, "timeout": ctx.exec.timeout}
            if json_data is not None: kwargs["json"] = json_data
            if params is not None: kwargs["params"] = params
            if stream: kwargs["stream"] = stream
            
            return getattr(client, method.lower())(**kwargs)
        except Exception as e:
            if hasattr(log, "exception"): log.exception(f"Error executing {method.upper()} request: {e}")
            raise self._handle_error(e=e, provider_config=ctx.provider.responses_api_provider_config)

    async def _execute_async_request(
        self, ctx: ResponseAPIContext, client: AsyncHTTPClient, method: str, url: str, 
        headers: Dict[str, Any], input_data: Any = "", json_data: Optional[Dict] = None, 
        params: Optional[Dict] = None, stream: bool = False
    ) -> Any:
        """비동기(Async) 네트워크 요청 실행, 로깅, 예외 처리를 전담합니다."""
        if ctx.provider.logging_obj:
            ctx.provider.logging_obj.pre_call(
                input=input_data, api_key="",
                additional_args={"complete_input_dict": json_data or params or {}, "api_base": url, "headers": headers}
            )
        try:
            kwargs = {"url": url, "headers": headers, "timeout": ctx.exec.timeout}
            if json_data is not None: kwargs["json"] = json_data
            if params is not None: kwargs["params"] = params
            if stream: kwargs["stream"] = stream
            
            req_func = getattr(client, method.lower())
            return await req_func(**kwargs)
        except Exception as e:
            if hasattr(log, "exception"): log.exception(f"Error executing {method.upper()} request: {e}")
            raise self._handle_error(e=e, provider_config=ctx.provider.responses_api_provider_config)

    def response_api_handler(
        self, ctx: ResponseAPIContext
    ) -> Union[ResponsesAPIResponse, ResponseStreamIterator, Coroutine[Any, Any, Union[ResponsesAPIResponse, ResponseStreamIterator]]]:
        if ctx.is_async:
            return self.async_response_api_handler(ctx)

        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx, model=ctx.payload.model)

        req_params = getattr(ctx, "_responses_api_request_params", {})
        data = ctx.provider.responses_api_provider_config.transform_responses_api_request(
            model=ctx.payload.model, input=ctx.payload.input,
            response_api_optional_request_params=req_params,
            litellm_params=ctx.provider.litellm_params, headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)
        if ctx.exec.extra_body:
            data.update(ctx.exec.extra_body)

        stream = ctx.payload.stream
        fake_stream = ctx.exec.extra_body.get("_fake_stream", False)
        
        request_context = {"input": ctx.payload.input, **req_params, "litellm_params": dict(ctx.provider.litellm_params)}

        if stream:
            if fake_stream:
                stream, data = self._prepare_fake_stream_request(stream=stream, data=data, fake_stream=fake_stream)
                
            response = self._execute_sync_request(ctx, client, "post", api_base, headers, input_data=ctx.payload.input, json_data=data, stream=stream)
            
            iterator_class = MockResponsesAPIStreamingIterator if fake_stream else SyncResponsesAPIStreamingIterator
            return iterator_class(
                response=response, model=ctx.payload.model, logging_obj=ctx.provider.logging_obj,
                responses_api_provider_config=ctx.provider.responses_api_provider_config,
                litellm_metadata=getattr(ctx, "_raw_kwargs", {}).get("litellm_metadata"),
                custom_llm_provider=ctx.provider.custom_llm_provider,
                request_data=request_context, call_type=CallTypes.responses.value,
            )
        
        response = self._execute_sync_request(ctx, client, "post", api_base, headers, input_data=ctx.payload.input, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_response_api_response(
            model=ctx.payload.model, raw_response=response, logging_obj=ctx.provider.logging_obj
        )

    async def async_response_api_handler(
        self, ctx: ResponseAPIContext
    ) -> Union[ResponsesAPIResponse, ResponseStreamIterator]:
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx, model=ctx.payload.model)

        req_params = getattr(ctx, "_responses_api_request_params", {})
        data = ctx.provider.responses_api_provider_config.transform_responses_api_request(
            model=ctx.payload.model, input=ctx.payload.input,
            response_api_optional_request_params=req_params,
            litellm_params=ctx.provider.litellm_params, headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)
        if ctx.exec.extra_body:
            data.update(ctx.exec.extra_body)

        stream = ctx.payload.stream
        fake_stream = ctx.exec.extra_body.get("_fake_stream", False)
        request_context = {"input": ctx.payload.input, **req_params, "litellm_params": dict(ctx.provider.litellm_params)}

        if stream:
            if fake_stream:
                stream, data = self._prepare_fake_stream_request(stream=stream, data=data, fake_stream=fake_stream)
                
            response = await self._execute_async_request(ctx, client, "post", api_base, headers, input_data=ctx.payload.input, json_data=data, stream=stream)
            
            iterator_class = MockResponsesAPIStreamingIterator if fake_stream else ResponsesAPIStreamingIterator
            return iterator_class(
                response=response, model=ctx.payload.model, logging_obj=ctx.provider.logging_obj,
                responses_api_provider_config=ctx.provider.responses_api_provider_config,
                litellm_metadata=getattr(ctx, "_raw_kwargs", {}).get("litellm_metadata"),
                custom_llm_provider=ctx.provider.custom_llm_provider,
                request_data=request_context, call_type=CallTypes.responses.value,
            )
        
        response = await self._execute_async_request(ctx, client, "post", api_base, headers, input_data=ctx.payload.input, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_response_api_response(
            model=ctx.payload.model, raw_response=response, logging_obj=ctx.provider.logging_obj
        )

    def delete_response_api_handler(self, ctx: ResponseAPIContext) -> Union[DeleteResponseResult, Coroutine[Any, Any, DeleteResponseResult]]:
        if ctx.is_async: return self.async_delete_response_api_handler(ctx)
        
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)

        url, data = ctx.provider.responses_api_provider_config.transform_delete_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, 
            litellm_params=ctx.provider.litellm_params, headers=headers
        )
        headers.setdefault("Content-Type", "application/json")
        if data and ctx.exec.extra_body: data.update(ctx.exec.extra_body)

        response = self._execute_sync_request(ctx, client, "delete", url, headers, input_data=ctx.payload.response_id, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_delete_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_delete_response_api_handler(self, ctx: ResponseAPIContext) -> DeleteResponseResult:
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)

        url, data = ctx.provider.responses_api_provider_config.transform_delete_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, 
            litellm_params=ctx.provider.litellm_params, headers=headers
        )
        headers.setdefault("Content-Type", "application/json")
        if data and ctx.exec.extra_body: data.update(ctx.exec.extra_body)

        response = await self._execute_async_request(ctx, client, "delete", url, headers, input_data=ctx.payload.response_id, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_delete_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    def cancel_response_api_handler(self, ctx: ResponseAPIContext) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
        if ctx.is_async: return self.async_cancel_response_api_handler(ctx)
        
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)

        url, data = ctx.provider.responses_api_provider_config.transform_cancel_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, 
            litellm_params=ctx.provider.litellm_params, headers=headers
        )

        response = self._execute_sync_request(ctx, client, "post", url, headers, input_data=ctx.payload.response_id, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_cancel_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_cancel_response_api_handler(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)

        url, data = ctx.provider.responses_api_provider_config.transform_cancel_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, 
            litellm_params=ctx.provider.litellm_params, headers=headers
        )

        response = await self._execute_async_request(ctx, client, "post", url, headers, input_data=ctx.payload.response_id, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_cancel_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    def compact_response_api_handler(self, ctx: ResponseAPIContext) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
        if ctx.is_async: return self.async_compact_response_api_handler(ctx)
        
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx, model=ctx.payload.model)

        url, data = ctx.provider.responses_api_provider_config.transform_compact_response_api_request(
            model=ctx.payload.model, input=ctx.payload.input,
            response_api_optional_request_params=getattr(ctx, "_responses_api_request_params", {}),
            api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)

        response = self._execute_sync_request(ctx, client, "post", url, headers, input_data=ctx.payload.input, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_compact_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_compact_response_api_handler(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx, model=ctx.payload.model)

        url, data = ctx.provider.responses_api_provider_config.transform_compact_response_api_request(
            model=ctx.payload.model, input=ctx.payload.input,
            response_api_optional_request_params=getattr(ctx, "_responses_api_request_params", {}),
            api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)

        response = await self._execute_async_request(ctx, client, "post", url, headers, input_data=ctx.payload.input, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_compact_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    def get_responses(self, ctx: ResponseAPIContext) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
        if ctx.is_async: return self.async_get_responses(ctx)
        
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)

        url, params = ctx.provider.responses_api_provider_config.transform_get_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, 
            litellm_params=ctx.provider.litellm_params, headers=headers
        )

        response = self._execute_sync_request(ctx, client, "get", url, headers, input_data="", params=params)
        return ctx.provider.responses_api_provider_config.transform_get_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_get_responses(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)

        url, params = ctx.provider.responses_api_provider_config.transform_get_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, 
            litellm_params=ctx.provider.litellm_params, headers=headers
        )

        response = await self._execute_async_request(ctx, client, "get", url, headers, input_data="", params=params)
        return ctx.provider.responses_api_provider_config.transform_get_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    def list_responses_input_items(self, ctx: ResponseAPIContext) -> Union[Dict, Coroutine[Any, Any, Dict]]:
        if ctx.is_async: return self.async_list_responses_input_items(ctx)
        
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)
        explicit = getattr(ctx, "_explicit_args", {})

        url, params = ctx.provider.responses_api_provider_config.transform_list_input_items_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers,
            after=explicit.get("after"), before=explicit.get("before"), include=ctx.payload.include, 
            limit=explicit.get("limit", 20), order=explicit.get("order", "desc")
        )

        response = self._execute_sync_request(ctx, client, "get", url, headers, input_data="", params=params)
        return ctx.provider.responses_api_provider_config.transform_list_input_items_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_list_responses_input_items(self, ctx: ResponseAPIContext) -> Dict:
        client = self._resolve_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)
        explicit = getattr(ctx, "_explicit_args", {})

        url, params = ctx.provider.responses_api_provider_config.transform_list_input_items_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers,
            after=explicit.get("after"), before=explicit.get("before"), include=ctx.payload.include, 
            limit=explicit.get("limit", 20), order=explicit.get("order", "desc")
        )

        response = await self._execute_async_request(ctx, client, "get", url, headers, input_data="", params=params)
        return ctx.provider.responses_api_provider_config.transform_list_input_items_response(raw_response=response, logging_obj=ctx.provider.logging_obj)
