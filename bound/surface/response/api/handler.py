# bound.surface.response.api.handler
import json
import inspect
from typing import Any, Coroutine, Dict, Optional, Union, Tuple

from anchor.provider.param.response import DeleteResponseResult
from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from bound.surface.legacy.provider import ProviderTypes
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.types import CallTypes
from bound.surface.legacy.openai.types import ResponsesAPIResponse
from bound.surface.bridge.transport.client import AsyncHTTPClient
from bound.surface.bridge.transport.factory import get_client
from bound.surface.response.stream.iterator import ResponseStreamIterator
from bound.surface.bridge.tosync import AsyncToSyncBridge, SyncStreamAdapter
from bound.surface.response.api.context import ResponseAPIContext

from watcher.plane.emitter import get_emitter 

log = get_emitter("handler.api")

class ResponseApiHandler:
    def _resolve_async_client(self, ctx: ResponseAPIContext) -> AsyncHTTPClient:
        """코어는 100% 비동기로 동작하므로 항상 Async 클라이언트만 준비합니다."""
        if ctx.exec.client is not None and isinstance(ctx.exec.client, AsyncHTTPClient):
            return ctx.exec.client
            
        if ctx.exec.shared_session:
            log.debug(f"Creating async HTTP client with shared_session: {id(ctx.exec.shared_session)}")
            
        ssl_verify_params = {"ssl_verify": ctx.provider.litellm_params.get("ssl_verify", None)}
        return get_client(
            is_async=True,
            llm_provider=ProviderTypes(ctx.provider.custom_llm_provider) if ctx.provider.custom_llm_provider else None,
            params=ssl_verify_params,
            shared_session=ctx.exec.shared_session,
        )

    def _prepare_common_env(self, ctx: ResponseAPIContext, model: str = "None") -> Tuple[Dict[str, Any], str]:
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

    async def _execute_async_request(
        self, ctx: ResponseAPIContext, client: AsyncHTTPClient, method: str, url: str, 
        headers: Dict[str, Any], input_data: Any = "", json_data: Optional[Dict] = None, 
        params: Optional[Dict] = None, stream: bool = False
    ) -> Any:
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

    # =========================================================================
    # 코어 비동기(Async) 핸들러 로직 (Single Source of Truth)
    # =========================================================================

    async def async_response_api_handler(self, ctx: ResponseAPIContext) -> Union[ResponsesAPIResponse, ResponseStreamIterator]:
        client = self._resolve_async_client(ctx)
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
        request_context = {"input": ctx.payload.input, **req_params, "litellm_params": dict(ctx.provider.litellm_params)}

        response = await self._execute_async_request(
            ctx, client, "post", api_base, headers, input_data=ctx.payload.input, json_data=data, stream=stream
        )

        if stream:
            return ResponseStreamIterator(
                response=response, model=ctx.payload.model, logging_obj=ctx.provider.logging_obj,
                responses_api_provider_config=ctx.provider.responses_api_provider_config,
                litellm_metadata=getattr(ctx, "_raw_kwargs", {}).get("litellm_metadata"),
                custom_llm_provider=ctx.provider.custom_llm_provider,
                request_data=request_context, call_type=CallTypes.responses.value,
            )
        
        return ctx.provider.responses_api_provider_config.transform_response_api_response(
            model=ctx.payload.model, raw_response=response, logging_obj=ctx.provider.logging_obj
        )

    async def async_delete_response_api_handler(self, ctx: ResponseAPIContext) -> DeleteResponseResult:
        client = self._resolve_async_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)

        url, data = ctx.provider.responses_api_provider_config.transform_delete_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers
        )
        headers.setdefault("Content-Type", "application/json")
        if data and ctx.exec.extra_body: data.update(ctx.exec.extra_body)

        response = await self._execute_async_request(ctx, client, "delete", url, headers, input_data=ctx.payload.response_id, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_delete_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_cancel_response_api_handler(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        client = self._resolve_async_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)

        url, data = ctx.provider.responses_api_provider_config.transform_cancel_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers
        )
        response = await self._execute_async_request(ctx, client, "post", url, headers, input_data=ctx.payload.response_id, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_cancel_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_compact_response_api_handler(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        client = self._resolve_async_client(ctx)
        headers, api_base = self._prepare_common_env(ctx, model=ctx.payload.model)

        url, data = ctx.provider.responses_api_provider_config.transform_compact_response_api_request(
            model=ctx.payload.model, input=ctx.payload.input,
            response_api_optional_request_params=getattr(ctx, "_responses_api_request_params", {}),
            api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)
        response = await self._execute_async_request(ctx, client, "post", url, headers, input_data=ctx.payload.input, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_compact_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_get_responses(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        client = self._resolve_async_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)

        url, params = ctx.provider.responses_api_provider_config.transform_get_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers
        )
        response = await self._execute_async_request(ctx, client, "get", url, headers, input_data="", params=params)
        return ctx.provider.responses_api_provider_config.transform_get_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_list_responses_input_items(self, ctx: ResponseAPIContext) -> Dict:
        client = self._resolve_async_client(ctx)
        headers, api_base = self._prepare_common_env(ctx)
        explicit = getattr(ctx, "_explicit_args", {})

        url, params = ctx.provider.responses_api_provider_config.transform_list_input_items_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers,
            after=explicit.get("after"), before=explicit.get("before"), include=ctx.payload.include, limit=explicit.get("limit", 20), order=explicit.get("order", "desc")
        )
        response = await self._execute_async_request(ctx, client, "get", url, headers, input_data="", params=params)
        return ctx.provider.responses_api_provider_config.transform_list_input_items_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    # =========================================================================
    # 동기(Sync) 레거시 지원용 어댑터 래퍼 (얇은 껍데기)
    # =========================================================================

    def response_api_handler(self, ctx: ResponseAPIContext) -> Union[ResponsesAPIResponse, Any]:
        if ctx.is_async:
            return self.async_response_api_handler(ctx)
            
        # 1. 비동기 핸들러를 동기로 실행하여 결과 수신
        result = AsyncToSyncBridge.run_coroutine(self.async_response_api_handler(ctx))
        
        # 2. 결과가 스트림(비동기 제너레이터)인 경우 동기 스트림으로 래핑
        if isinstance(result, ResponseStreamIterator):
            return SyncStreamAdapter(result)
        return result

    def delete_response_api_handler(self, ctx: ResponseAPIContext) -> DeleteResponseResult:
        if ctx.is_async: return self.async_delete_response_api_handler(ctx)
        return AsyncToSyncBridge.run_coroutine(self.async_delete_response_api_handler(ctx))

    def cancel_response_api_handler(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        if ctx.is_async: return self.async_cancel_response_api_handler(ctx)
        return AsyncToSyncBridge.run_coroutine(self.async_cancel_response_api_handler(ctx))

    def compact_response_api_handler(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        if ctx.is_async: return self.async_compact_response_api_handler(ctx)
        return AsyncToSyncBridge.run_coroutine(self.async_compact_response_api_handler(ctx))

    def get_responses(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        if ctx.is_async: return self.async_get_responses(ctx)
        return AsyncToSyncBridge.run_coroutine(self.async_get_responses(ctx))

    def list_responses_input_items(self, ctx: ResponseAPIContext) -> Dict:
        if ctx.is_async: return self.async_list_responses_input_items(ctx)
        return AsyncToSyncBridge.run_coroutine(self.async_list_responses_input_items(ctx))