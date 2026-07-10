# bound.surface.response.api.response_crud
## @lineage: bound.surface.legacy.response.api.response_crud
## @lineage: bound.transport.response.api.response_crud
import asyncio
import contextvars
from functools import partial
from typing import Any, Coroutine, Dict, List, Literal, Optional, Union, cast

import httpx
from pydantic import BaseModel

from anchor.provider.param.response import *
from anchor.provider.param.legacy import GenericLiteLLMParams
from anchor.registry.router.config import ProviderConfigManager
from anchor.registry.router.locator import get_llm_provider

from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.config.constants import request_timeout
from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from bound.surface.legacy.openai.types import (
    ResponseInputParam,
    ResponsesAPIOptionalRequestParams,
    ResponsesAPIResponse,
)
from bound.surface.bridge.param.litellm import infer_openai_data_residency
from bound.surface.bridge.api import APIBridge
from bound.surface.response.api.handler import ResponseApiHandler
from bound.surface.action.client.wrapper import client
from bound.surface.response.identity import ResponseIdentityManager
from bound.surface.response.api.context import ResponseAPIContext, ContextBuilder, ExecutionContext, ProviderContext, LLMPayloadContext

from watcher.plane.emitter import get_emitter

log = get_emitter("api.response_crud")
LiteLLMLoggingObj = Any
api_handler = ResponseApiHandler()


class ResponseCRUDPreprocessor:
    """
    response_id를 디코딩하고 Provider를 검증하여 범용 ResponseAPIContext를 빌드합니다.
    """
    def __init__(self, action: Literal["DELETE", "GET", "LIST", "CANCEL"], explicit_args: Dict[str, Any], kwargs: Dict[str, Any]):
        self.action = action
        self.explicit_args = explicit_args
        self.kwargs = kwargs
        
        self.raw_response_id = explicit_args.get("response_id", "")
        self.custom_llm_provider = explicit_args.get("custom_llm_provider")
        self.log_delegator = kwargs.get("log_delegator")
        
        # Async Flag Mapping
        async_flag_key = f"a{action.lower()}_responses" if action != "LIST" else "alist_input_items"
        self.is_async = kwargs.pop(async_flag_key, False) is True
        
        self.litellm_params = GenericLiteLLMParams(**kwargs)

    def build(self) -> ResponseAPIContext:
        # ID 디코딩 및 Provider 추론
        decoded = ResponseIdentityManager._decode_responses_api_response_id(response_id=self.raw_response_id)
        response_id = decoded.get("response_id") or self.raw_response_id
        custom_llm_provider = decoded.get("custom_llm_provider") or self.custom_llm_provider

        if custom_llm_provider is None:
            raise ValueError("custom_llm_provider is required but passed as None")

        provider_config = ProviderConfigManager.get_provider_responses_api_config(
            model=None, provider=custom_llm_provider
        )
        if provider_config is None:
            raise ValueError(f"{self.action} responses is not supported for {custom_llm_provider}")

        # [개선] 15개가 넘는 파라미터를 3개의 명확한 카테고리(Execution, Provider, Payload)로 조립
        exec_ctx = ExecutionContext(
            is_async=self.is_async,
            timeout=self.explicit_args.get("timeout") or request_timeout,
            client=self.kwargs.get("client"),
            shared_session=self.kwargs.get("shared_session"),
            extra_headers=self.explicit_args.get("extra_headers") or {},
            extra_body=self.explicit_args.get("extra_body") or {},
            extra_query=self.explicit_args.get("extra_query") or {},
        )

        provider_ctx = ProviderContext(
            custom_llm_provider=custom_llm_provider,
            litellm_params=self.litellm_params,
            responses_api_provider_config=provider_config,
            logging_obj=self.log_delegator,
        )

        payload_ctx = LLMPayloadContext(
            response_id=response_id,
            include=self.explicit_args.get("include"),
        )

        ctx = ResponseAPIContext(exec=exec_ctx, provider=provider_ctx, payload=payload_ctx)
        
        # Dispatcher나 Handler에서 참조할 수 있도록 원본/명시적 인자 부착
        ctx._action = self.action
        ctx._explicit_args = self.explicit_args
        ctx._raw_kwargs = self.kwargs
        
        return ctx


class ResponseCRUDDispatcher:
    """
    정제된 ResponseAPIContext를 바탕으로 적절한 api_handler 메서드에 단일 객체를 넘깁니다.
    """
    def __init__(self, context: ResponseAPIContext):
        self.ctx = context

    def execute(self) -> Any:
        # Pre Call logging
        if self.ctx.provider.logging_obj:
            merged_kwargs = {**self.ctx._explicit_args, **self.ctx._raw_kwargs}
            self.ctx.provider.logging_obj.update_from_kwargs(
                kwargs=merged_kwargs,
                model=None,
                optional_params={"response_id": self.ctx.payload.response_id},
                litellm_params={"call_id": self.ctx._raw_kwargs.get("call_id")},
                custom_llm_provider=self.ctx.provider.custom_llm_provider,
            )

        # [개선] 복잡한 **common_args 언패킹 제거, 단일 ctx 전달
        action = getattr(self.ctx, "_action", "")
        if action == "DELETE":
            response = api_handler.delete_response_api_handler(ctx=self.ctx)
        elif action == "GET":
            response = api_handler.get_responses(ctx=self.ctx)
        elif action == "CANCEL":
            response = api_handler.cancel_response_api_handler(ctx=self.ctx)
        elif action == "LIST":
            response = api_handler.list_responses_input_items(ctx=self.ctx)
        else:
            raise ValueError(f"Unknown CRUD action: {action}")

        # ID 후처리 업데이트
        if isinstance(response, ResponsesAPIResponse):
            response = APIBridge._update_responses_api_response_id_with_model_id(
                responses_api_response=response,
                litellm_metadata=self.ctx._raw_kwargs.get("litellm_metadata", {}),
                custom_llm_provider=self.ctx.provider.custom_llm_provider,
            )
        return response


def _execute_crud(action: Literal["DELETE", "GET", "LIST", "CANCEL"], explicit_args: Dict, kwargs: Dict) -> Any:
    """CRUD 공통 실행 래퍼"""
    try:
        context = ResponseCRUDPreprocessor(action, explicit_args, kwargs).build()
        return ResponseCRUDDispatcher(context).execute()
    except Exception as e:
        custom_llm_provider = explicit_args.get("custom_llm_provider")
        if not custom_llm_provider and "response_id" in explicit_args:
            decoded = ResponseIdentityManager._decode_responses_api_response_id(explicit_args["response_id"])
            custom_llm_provider = decoded.get("custom_llm_provider")
            
        raise config.exception_type(
            model=None,
            custom_llm_provider=custom_llm_provider,
            original_exception=e,
            completion_kwargs={**explicit_args, **kwargs},
            extra_kwargs=kwargs,
        )


# --- CRUD Client Interfaces ---

@client
def delete_responses(response_id: str, extra_headers: Optional[Dict] = None, extra_query: Optional[Dict] = None, extra_body: Optional[Dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None, **kwargs) -> Union[DeleteResponseResult, Coroutine[Any, Any, DeleteResponseResult]]:
    explicit_args = {"response_id": response_id, "extra_headers": extra_headers, "extra_query": extra_query, "extra_body": extra_body, "timeout": timeout, "custom_llm_provider": custom_llm_provider}
    return _execute_crud("DELETE", explicit_args, kwargs)

@client
def get_responses(response_id: str, extra_headers: Optional[Dict] = None, extra_query: Optional[Dict] = None, extra_body: Optional[Dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None, **kwargs) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
    explicit_args = {"response_id": response_id, "extra_headers": extra_headers, "extra_query": extra_query, "extra_body": extra_body, "timeout": timeout, "custom_llm_provider": custom_llm_provider}
    return _execute_crud("GET", explicit_args, kwargs)

@client
def cancel_responses(response_id: str, extra_headers: Optional[Dict] = None, extra_query: Optional[Dict] = None, extra_body: Optional[Dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None, **kwargs) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
    explicit_args = {"response_id": response_id, "extra_headers": extra_headers, "extra_query": extra_query, "extra_body": extra_body, "timeout": timeout, "custom_llm_provider": custom_llm_provider}
    return _execute_crud("CANCEL", explicit_args, kwargs)

@client
def list_input_items(response_id: str, after: Optional[str] = None, before: Optional[str] = None, include: Optional[List[str]] = None, limit: int = 20, order: Literal["asc", "desc"] = "desc", extra_headers: Optional[Dict] = None, timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None, **kwargs) -> Union[Dict, Coroutine[Any, Any, Dict]]:
    explicit_args = {"response_id": response_id, "after": after, "before": before, "include": include, "limit": limit, "order": order, "extra_headers": extra_headers, "timeout": timeout, "custom_llm_provider": custom_llm_provider}
    return _execute_crud("LIST", explicit_args, kwargs)

@client
def compact_responses(
    input: Union[str, ResponseInputParam], model: str, instructions: Optional[str] = None,
    previous_response_id: Optional[str] = None, extra_headers: Optional[Dict] = None,
    extra_query: Optional[Dict] = None, extra_body: Optional[Dict] = None,
    timeout: Optional[Union[float, httpx.Timeout]] = None, custom_llm_provider: Optional[str] = None,
    **kwargs,
) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
    
    explicit_args = {
        "input": input, "model": model, "instructions": instructions, "previous_response_id": previous_response_id,
        "extra_headers": extra_headers, "extra_query": extra_query, "extra_body": extra_body,
        "timeout": timeout, "custom_llm_provider": custom_llm_provider
    }
    
    try:
        log_delegator = kwargs.get("log_delegator")
        is_async = kwargs.pop("acompact_responses", False) is True
        litellm_params = GenericLiteLLMParams(**kwargs)

        # Provider Resolve
        resolved_model, resolved_provider, dynamic_api_key, dynamic_api_base = get_llm_provider(
            model=model,
            custom_llm_provider=custom_llm_provider,
            api_base=litellm_params.api_base,
            api_key=litellm_params.api_key,
        )
        
        if dynamic_api_key: litellm_params.api_key = dynamic_api_key
        if dynamic_api_base: litellm_params.api_base = dynamic_api_base
        if resolved_provider is None:
            raise ValueError("custom_llm_provider is required but passed as None")

        provider_config = ProviderConfigManager.get_provider_responses_api_config(
            model=resolved_model, provider=resolved_provider
        )
        if provider_config is None:
            raise ValueError(f"COMPACT responses is not supported for {resolved_provider}")

        # [개선] 별도로 흩어져 있던 Compact 파라미터 빌드 과정을 ContextBuilder로 흡수
        explicit_args["model"] = resolved_model
        explicit_args["custom_llm_provider"] = resolved_provider
        
        ctx = ContextBuilder.from_explicit_args(
            explicit_args=explicit_args,
            litellm_params=litellm_params,
            is_async=is_async,
            responses_api_provider_config=provider_config,
            log_delegator=log_delegator,
            client=kwargs.get("client"),
            shared_session=kwargs.get("shared_session"),
        )
        
        merged_vars = {**explicit_args, **kwargs, "custom_llm_provider": resolved_provider}
        response_api_optional_params = APIBridge.get_requested_response_api_optional_param(merged_vars)
        ctx._responses_api_request_params = dict(APIBridge.get_optional_params_responses_api(
            model=resolved_model,
            responses_api_provider_config=provider_config,
            response_api_optional_params=response_api_optional_params,
            allowed_openai_params=None,
        ))

        # Pre Call Logging
        if log_delegator:
            log_delegator.update_from_kwargs(
                kwargs=merged_vars,
                model=resolved_model,
                optional_params=ctx._responses_api_request_params,
                litellm_params={
                    **ctx._responses_api_request_params,
                    "call_id": kwargs.get("call_id"),
                    "data_residency": infer_openai_data_residency(resolved_provider, litellm_params.api_base),
                },
                custom_llm_provider=resolved_provider,
            )

        # Execute
        ctx.payload.input = APIBridge._restore_encrypted_content_item_ids_in_input(input)
        
        # [개선] 10여개의 인자를 ctx 단 하나로 교체
        response = api_handler.compact_response_api_handler(ctx=ctx)

        if isinstance(response, ResponsesAPIResponse):
            response = APIBridge._update_responses_api_response_id_with_model_id(
                responses_api_response=response,
                litellm_metadata=kwargs.get("litellm_metadata", {}),
                custom_llm_provider=resolved_provider,
            )
        return response

    except Exception as e:
        raise config.exception_type(
            model=explicit_args.get("model", model),
            custom_llm_provider=explicit_args.get("custom_llm_provider", custom_llm_provider),
            original_exception=e,
            completion_kwargs={**explicit_args, **kwargs},
            extra_kwargs=kwargs,
        )