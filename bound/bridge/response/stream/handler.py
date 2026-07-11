# bound.bridge.response.stream.handler
## @lineage: bound.transport.response.stream.handler
## @lineage: bound.transport.stream.api.handler
## @lineage: bound.surface.response.handler
## @lineage: bound.surface.response.api.handler
import inspect
from typing import Any, Coroutine, Dict, List, Literal, Optional, Union, Tuple
import httpx

from bound.surface.legacy.param.response import DeleteResponseResult
from bound.surface.legacy.param.legacy import GenericLiteLLMParams
from anchor.registry.router.config import ProviderConfigManager
from anchor.registry.router.locator import get_llm_provider

from anchor.registry.model.config.response import BaseResponsesAPIConfig
from bound.surface.legacy.info import ProviderTypes
from anchor.registry.model.config.resolver import config
from anchor.registry.model.config.constants import request_timeout
from bound.surface.legacy.types import CallTypes
from bound.surface.legacy.openai.types import ResponseInputParam, ResponsesAPIResponse

from bound.bridge.transport.client import AsyncHTTPClient
from bound.bridge.transport.client import get_client
from bound.surface.stream.iterator.response import ResponseStreamIterator
from bound.bridge.tosync import AsyncToSyncBridge, SyncStreamAdapter

from bound.bridge.response.stream.context import ResponseAPIContext, ContextBuilder, ExecutionContext, ProviderContext, LLMPayloadContext
from bound.bridge.response.stream.identity import IdentityRouter
from anchor.registry.model.api.base import APIBridge
from bound.surface.client.param.litellm import infer_openai_data_residency
from bound.surface.client.action.client.wrapper import client

from watcher.plane.emitter import get_emitter 

log = get_emitter("handler.api")


class ResponseApiHandler:
    """Core 비동기 API 핸들러: 모든 API의 요청 준비 및 실행을 담당합니다."""
    
    def _resolve_async_client(self, ctx: ResponseAPIContext) -> AsyncHTTPClient:
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
        self, ctx: ResponseAPIContext, method: str, url: str, 
        headers: Dict[str, Any], input_data: Any = "", json_data: Optional[Dict] = None, 
        params: Optional[Dict] = None, stream: bool = False
    ) -> Any:
        client = self._resolve_async_client(ctx)
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
            raise config.exception_type(
                model=None, custom_llm_provider=ctx.provider.custom_llm_provider,
                original_exception=e, completion_kwargs={}, extra_kwargs={}
            )

    # -------------------------------------------------------------------------
    # 비동기 Core Action Methods
    # -------------------------------------------------------------------------
    
    async def async_response_api_handler(self, ctx: ResponseAPIContext) -> Union[ResponsesAPIResponse, ResponseStreamIterator]:
        headers, api_base = self._prepare_common_env(ctx, model=ctx.payload.model)
        req_params = getattr(ctx, "_responses_api_request_params", {})
        
        data = ctx.provider.responses_api_provider_config.transform_responses_api_request(
            model=ctx.payload.model, input=ctx.payload.input, response_api_optional_request_params=req_params,
            litellm_params=ctx.provider.litellm_params, headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)
        if ctx.exec.extra_body: data.update(ctx.exec.extra_body)

        stream = ctx.payload.stream
        request_context = {"input": ctx.payload.input, **req_params, "litellm_params": dict(ctx.provider.litellm_params)}
        response = await self._execute_async_request(ctx, "post", api_base, headers, input_data=ctx.payload.input, json_data=data, stream=stream)

        if stream:
            return ResponseStreamIterator(
                response=response, model=ctx.payload.model, logging_obj=ctx.provider.logging_obj,
                responses_api_provider_config=ctx.provider.responses_api_provider_config,
                litellm_metadata=getattr(ctx, "_raw_kwargs", {}).get("litellm_metadata"),
                custom_llm_provider=ctx.provider.custom_llm_provider, request_data=request_context, call_type=CallTypes.responses.value,
            )
        return ctx.provider.responses_api_provider_config.transform_response_api_response(model=ctx.payload.model, raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_delete(self, ctx: ResponseAPIContext) -> DeleteResponseResult:
        headers, api_base = self._prepare_common_env(ctx)
        url, data = ctx.provider.responses_api_provider_config.transform_delete_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers
        )
        headers.setdefault("Content-Type", "application/json")
        if data and ctx.exec.extra_body: data.update(ctx.exec.extra_body)
        response = await self._execute_async_request(ctx, "delete", url, headers, input_data=ctx.payload.response_id, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_delete_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_cancel(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        headers, api_base = self._prepare_common_env(ctx)
        url, data = ctx.provider.responses_api_provider_config.transform_cancel_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers
        )
        response = await self._execute_async_request(ctx, "post", url, headers, input_data=ctx.payload.response_id, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_cancel_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_compact(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        headers, api_base = self._prepare_common_env(ctx, model=ctx.payload.model)
        url, data = ctx.provider.responses_api_provider_config.transform_compact_response_api_request(
            model=ctx.payload.model, input=ctx.payload.input, response_api_optional_request_params=getattr(ctx, "_responses_api_request_params", {}),
            api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)
        response = await self._execute_async_request(ctx, "post", url, headers, input_data=ctx.payload.input, json_data=data)
        return ctx.provider.responses_api_provider_config.transform_compact_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_get(self, ctx: ResponseAPIContext) -> ResponsesAPIResponse:
        headers, api_base = self._prepare_common_env(ctx)
        url, params = ctx.provider.responses_api_provider_config.transform_get_response_api_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers
        )
        response = await self._execute_async_request(ctx, "get", url, headers, input_data="", params=params)
        return ctx.provider.responses_api_provider_config.transform_get_response_api_response(raw_response=response, logging_obj=ctx.provider.logging_obj)

    async def async_list(self, ctx: ResponseAPIContext) -> Dict:
        headers, api_base = self._prepare_common_env(ctx)
        explicit = getattr(ctx, "_explicit_args", {})
        url, params = ctx.provider.responses_api_provider_config.transform_list_input_items_request(
            response_id=ctx.payload.response_id, api_base=api_base, litellm_params=ctx.provider.litellm_params, headers=headers,
            after=explicit.get("after"), before=explicit.get("before"), include=ctx.payload.include, limit=explicit.get("limit", 20), order=explicit.get("order", "desc")
        )
        response = await self._execute_async_request(ctx, "get", url, headers, input_data="", params=params)
        return ctx.provider.responses_api_provider_config.transform_list_input_items_response(raw_response=response, logging_obj=ctx.provider.logging_obj)


_handler = ResponseApiHandler()
def _build_context(action: str, explicit_args: Dict[str, Any], kwargs: Dict[str, Any]) -> ResponseAPIContext:
    """공통 Context Builder (구 ResponseCRUDPreprocessor)"""
    raw_response_id = explicit_args.get("response_id", "")
    custom_llm_provider = explicit_args.get("custom_llm_provider")
    
    decoded = IdentityRouter._decode_responses_api_response_id(response_id=raw_response_id)
    response_id = decoded.get("response_id") or raw_response_id
    custom_llm_provider = decoded.get("custom_llm_provider") or custom_llm_provider

    if not custom_llm_provider: raise ValueError("custom_llm_provider is required but passed as None")

    provider_config = ProviderConfigManager.get_provider_responses_api_config(model=None, provider=custom_llm_provider)
    if not provider_config: raise ValueError(f"{action} responses is not supported for {custom_llm_provider}")

    async_flag_key = f"a{action.lower()}_responses" if action != "LIST" else "alist_input_items"
    is_async = kwargs.pop(async_flag_key, False) is True

    ctx = ResponseAPIContext(
        exec=ExecutionContext(
            is_async=is_async, timeout=explicit_args.get("timeout") or request_timeout,
            client=kwargs.get("client"), shared_session=kwargs.get("shared_session"),
            extra_headers=explicit_args.get("extra_headers") or {}, extra_body=explicit_args.get("extra_body") or {}, extra_query=explicit_args.get("extra_query") or {},
        ),
        provider=ProviderContext(
            custom_llm_provider=custom_llm_provider, litellm_params=GenericLiteLLMParams(**kwargs),
            responses_api_provider_config=provider_config, logging_obj=kwargs.get("log_delegator"),
        ),
        payload=LLMPayloadContext(response_id=response_id, include=explicit_args.get("include"))
    )
    ctx._action = action
    ctx._explicit_args = explicit_args
    ctx._raw_kwargs = kwargs
    return ctx

def _execute_with_bridge(ctx: ResponseAPIContext, async_func) -> Any:
    """Core 비동기 함수를 실행하고, 필요한 경우 Sync 브릿지를 태워 반환합니다."""
    # Pre-call logging
    if ctx.provider.logging_obj:
        merged_kwargs = {**ctx._explicit_args, **ctx._raw_kwargs}
        ctx.provider.logging_obj.update_from_kwargs(
            kwargs=merged_kwargs, model=None, optional_params={"response_id": ctx.payload.response_id},
            litellm_params={"call_id": ctx._raw_kwargs.get("call_id")}, custom_llm_provider=ctx.provider.custom_llm_provider,
        )

    if ctx.exec.is_async:
        return async_func(ctx)
        
    result = AsyncToSyncBridge.run_coroutine(async_func(ctx))
    if isinstance(result, ResponseStreamIterator):
        return SyncStreamAdapter(result)
        
    # ID 후처리 업데이트 (ResponsesAPIResponse인 경우)
    if isinstance(result, ResponsesAPIResponse):
        result = APIBridge._update_responses_api_response_id_with_model_id(
            responses_api_response=result, litellm_metadata=ctx._raw_kwargs.get("litellm_metadata", {}), custom_llm_provider=ctx.provider.custom_llm_provider
        )
    return result


@client
def delete_responses(response_id: str, extra_headers: Optional[Dict] = None, extra_query: Optional[Dict] = None, extra_body: Optional[Dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None, **kwargs) -> Union[DeleteResponseResult, Coroutine[Any, Any, DeleteResponseResult]]:
    explicit_args = {"response_id": response_id, "extra_headers": extra_headers, "extra_query": extra_query, "extra_body": extra_body, "timeout": timeout, "custom_llm_provider": custom_llm_provider}
    return _execute_with_bridge(_build_context("DELETE", explicit_args, kwargs), _handler.async_delete)

@client
def get_responses(response_id: str, extra_headers: Optional[Dict] = None, extra_query: Optional[Dict] = None, extra_body: Optional[Dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None, **kwargs) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
    explicit_args = {"response_id": response_id, "extra_headers": extra_headers, "extra_query": extra_query, "extra_body": extra_body, "timeout": timeout, "custom_llm_provider": custom_llm_provider}
    return _execute_with_bridge(_build_context("GET", explicit_args, kwargs), _handler.async_get)

@client
def cancel_responses(response_id: str, extra_headers: Optional[Dict] = None, extra_query: Optional[Dict] = None, extra_body: Optional[Dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None, **kwargs) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
    explicit_args = {"response_id": response_id, "extra_headers": extra_headers, "extra_query": extra_query, "extra_body": extra_body, "timeout": timeout, "custom_llm_provider": custom_llm_provider}
    return _execute_with_bridge(_build_context("CANCEL", explicit_args, kwargs), _handler.async_cancel)

@client
def list_input_items(response_id: str, after: Optional[str] = None, before: Optional[str] = None, include: Optional[List[str]] = None, limit: int = 20, order: Literal["asc", "desc"] = "desc", extra_headers: Optional[Dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None, **kwargs) -> Union[Dict, Coroutine[Any, Any, Dict]]:
    explicit_args = {"response_id": response_id, "after": after, "before": before, "include": include, "limit": limit, "order": order, "extra_headers": extra_headers, "timeout": timeout, "custom_llm_provider": custom_llm_provider}
    return _execute_with_bridge(_build_context("LIST", explicit_args, kwargs), _handler.async_list)

@client
def compact_responses(
    input: Union[str, ResponseInputParam], model: str, instructions: Optional[str] = None,
    previous_response_id: Optional[str] = None, extra_headers: Optional[Dict] = None,
    extra_query: Optional[Dict] = None, extra_body: Optional[Dict] = None,
    timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None,
    **kwargs,
) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
    # compact_responses는 Context 생성 방식이 약간 달라서 개별 처리
    explicit_args = {
        "input": input, "model": model, "instructions": instructions, "previous_response_id": previous_response_id,
        "extra_headers": extra_headers, "extra_query": extra_query, "extra_body": extra_body,
        "timeout": timeout, "custom_llm_provider": custom_llm_provider
    }
    
    litellm_params = GenericLiteLLMParams(**kwargs)
    resolved_model, resolved_provider, dynamic_api_key, dynamic_api_base = get_llm_provider(
        model=model, custom_llm_provider=custom_llm_provider, api_base=litellm_params.api_base, api_key=litellm_params.api_key,
    )
    if dynamic_api_key: litellm_params.api_key = dynamic_api_key
    if dynamic_api_base: litellm_params.api_base = dynamic_api_base
    if resolved_provider is None: raise ValueError("custom_llm_provider is required but passed as None")

    provider_config = ProviderConfigManager.get_provider_responses_api_config(model=resolved_model, provider=resolved_provider)
    if provider_config is None: raise ValueError(f"COMPACT responses is not supported for {resolved_provider}")

    explicit_args["model"], explicit_args["custom_llm_provider"] = resolved_model, resolved_provider
    ctx = ContextBuilder.from_explicit_args(
        explicit_args=explicit_args, litellm_params=litellm_params, is_async=kwargs.pop("acompact_responses", False) is True,
        responses_api_provider_config=provider_config, log_delegator=kwargs.get("log_delegator"), client=kwargs.get("client"), shared_session=kwargs.get("shared_session"),
    )
    
    merged_vars = {**explicit_args, **kwargs, "custom_llm_provider": resolved_provider}
    response_api_optional_params = APIBridge.get_requested_response_api_optional_param(merged_vars)
    ctx._responses_api_request_params = dict(APIBridge.get_optional_params_responses_api(
        model=resolved_model, responses_api_provider_config=provider_config,
        response_api_optional_params=response_api_optional_params, allowed_openai_params=None,
    ))
    
    ctx.payload.input = APIBridge._restore_encrypted_content_item_ids_in_input(input)
    ctx._explicit_args = explicit_args
    ctx._raw_kwargs = kwargs
    
    return _execute_with_bridge(ctx, _handler.async_compact)