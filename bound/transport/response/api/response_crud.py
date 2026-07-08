# bound.transport.response.api.response_crud
## @lineage: bound.transport.channel.response.api.response_crud
## @lineage: bound.channel.action.api.response_crud
import asyncio
import contextvars
from dataclasses import dataclass
from functools import partial
from typing import Any, Coroutine, Dict, List, Literal, Optional, Union

import httpx
from pydantic import BaseModel

from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.config.constants import request_timeout
from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from anchor.registry.router.locator import get_llm_provider
from bound.surface.legacy.openai.types import (
    ResponseInputParam,
    ResponsesAPIOptionalRequestParams,
    ResponsesAPIResponse,
)
from anchor.provider.param.response import *
from anchor.provider.param.legacy import GenericLiteLLMParams
from anchor.registry.router.config import ProviderConfigManager
from bound.transport.response.api.handler import ResponseApiHandler
from bound.channel.param.litellm import infer_openai_data_residency
from bound.transport.client.wrapper import client
from bound.channel.api import APIBridge
from bound.transport.response.identity import ResponseIdentityManager

from watcher.plane.emitter import get_emitter

log = get_emitter("api.response_crud")
LiteLLMLoggingObj = Any
api_handler = ResponseApiHandler()

@dataclass
class ResponseCRUDContext:
    """ID 기반 CRUD 요청을 위한 공통 상태 벡터"""
    action: Literal["DELETE", "GET", "LIST", "CANCEL"]
    response_id: str
    custom_llm_provider: str
    responses_api_provider_config: BaseResponsesAPIConfig
    litellm_params: GenericLiteLLMParams
    log_delegator: Optional[LiteLLMLoggingObj]
    is_async: bool
    explicit_args: Dict[str, Any]
    kwargs: Dict[str, Any]

class ResponseCRUDPreprocessor:
    """response_id를 디코딩하고 Provider를 검증하여 CRUD Context를 빌드"""
    def __init__(self, action: Literal["DELETE", "GET", "LIST", "CANCEL"], explicit_args: Dict[str, Any], kwargs: Dict[str, Any]):
        self.action = action
        self.explicit_args = explicit_args
        self.kwargs = kwargs
        
        self.raw_response_id = explicit_args.get("response_id", "")
        self.custom_llm_provider = explicit_args.get("custom_llm_provider")
        self.log_delegator = kwargs.get("log_delegator")
        
        # Async Flag Mapping (adelete_responses, aget_responses 등)
        async_flag_key = f"a{action.lower()}_responses" if action != "LIST" else "alist_input_items"
        self.is_async = kwargs.pop(async_flag_key, False) is True
        
        self.litellm_params = GenericLiteLLMParams(**kwargs)

    def build(self) -> ResponseCRUDContext:
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

        return ResponseCRUDContext(
            action=self.action,
            response_id=response_id,
            custom_llm_provider=custom_llm_provider,
            responses_api_provider_config=provider_config,
            litellm_params=self.litellm_params,
            log_delegator=self.log_delegator,
            is_async=self.is_async,
            explicit_args=self.explicit_args,
            kwargs=self.kwargs,
        )

class ResponseCRUDDispatcher:
    """정제된 CRUD Context를 바탕으로 적절한 api_handler 메서드 호출"""
    def __init__(self, context: ResponseCRUDContext):
        self.ctx = context

    def execute(self) -> Any:
        # Pre Call logging
        if self.ctx.log_delegator:
            merged_kwargs = {**self.ctx.explicit_args, **self.ctx.kwargs}
            self.ctx.log_delegator.update_from_kwargs(
                kwargs=merged_kwargs,
                model=None,
                optional_params={"response_id": self.ctx.response_id},
                litellm_params={"call_id": self.ctx.kwargs.get("call_id")},
                custom_llm_provider=self.ctx.custom_llm_provider,
            )

        timeout = self.ctx.explicit_args.get("timeout") or request_timeout
        common_args = {
            "response_id": self.ctx.response_id,
            "custom_llm_provider": self.ctx.custom_llm_provider,
            "responses_api_provider_config": self.ctx.responses_api_provider_config,
            "litellm_params": self.ctx.litellm_params,
            "logging_obj": self.ctx.log_delegator,
            "extra_headers": self.ctx.explicit_args.get("extra_headers"),
            "extra_body": self.ctx.explicit_args.get("extra_body"),
            "timeout": timeout,
            "_is_async": self.ctx.is_async,
            "client": self.ctx.kwargs.get("client"),
            "shared_session": self.ctx.kwargs.get("shared_session"),
        }

        # 라우팅
        if self.ctx.action == "DELETE":
            response = api_handler.delete_api_handler(**common_args)
        elif self.ctx.action == "GET":
            response = api_handler.get_responses(**common_args)
        elif self.ctx.action == "CANCEL":
            response = api_handler.cancel_api_handler(**common_args)
        elif self.ctx.action == "LIST":
            response = api_handler.list_responses_input_items(
                **common_args,
                after=self.ctx.explicit_args.get("after"),
                before=self.ctx.explicit_args.get("before"),
                include=self.ctx.explicit_args.get("include"),
                limit=self.ctx.explicit_args.get("limit", 20),
                order=self.ctx.explicit_args.get("order", "desc"),
            )

        # ID 후처리 업데이트 (ResponsesAPIResponse 타입일 경우)
        if isinstance(response, ResponsesAPIResponse):
            response = APIBridge._update_responses_api_response_id_with_model_id(
                responses_api_response=response,
                litellm_metadata=self.ctx.kwargs.get("litellm_metadata", {}),
                custom_llm_provider=self.ctx.custom_llm_provider,
            )
        return response

def _execute_crud(action: Literal["DELETE", "GET", "LIST", "CANCEL"], explicit_args: Dict, kwargs: Dict) -> Any:
    """CRUD 공통 실행 래퍼 (예외 처리 포함)"""
    try:
        context = ResponseCRUDPreprocessor(action, explicit_args, kwargs).build()
        return ResponseCRUDDispatcher(context).execute()
    except Exception as e:
        custom_llm_provider = explicit_args.get("custom_llm_provider")
        # 에러 발생 시 디코딩 시도 (로그용)
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
        call_id = kwargs.get("call_id")
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

        # Build Optional Params
        merged_vars = {**explicit_args, **kwargs, "custom_llm_provider": resolved_provider}
        response_api_optional_params = APIBridge.get_requested_response_api_optional_param(merged_vars)
        responses_api_request_params = dict(APIBridge.get_optional_params_responses_api(
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
                optional_params=responses_api_request_params,
                litellm_params={
                    **responses_api_request_params,
                    "call_id": call_id,
                    "data_residency": infer_openai_data_residency(resolved_provider, litellm_params.api_base),
                },
                custom_llm_provider=resolved_provider,
            )

        # Execute
        restored_input = APIBridge._restore_encrypted_content_item_ids_in_input(input)
        response = api_handler.compact_api_handler(
            model=resolved_model,
            input=restored_input,
            responses_api_provider_config=provider_config,
            response_api_optional_request_params=responses_api_request_params,
            litellm_params=litellm_params,
            logging_obj=log_delegator,
            custom_llm_provider=resolved_provider,
            extra_headers=extra_headers,
            extra_body=extra_body,
            timeout=timeout or request_timeout,
            _is_async=is_async,
            client=kwargs.get("client"),
            shared_session=kwargs.get("shared_session"),
        )

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