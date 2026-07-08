# bound.surface.action.response
## @lineage: bound.channel.action.api.response
import asyncio
import contextvars
from dataclasses import dataclass, field
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Coroutine,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Type,
    Union,
    cast,
)
import httpx
from pydantic import BaseModel
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.config.constants import request_timeout
from bound.surface.legacy.openai.types import (
    AllMessageValues,
    PromptObject,
    Reasoning,
    ResponseIncludable,
    ResponseInputParam,
    ResponsesAPIOptionalRequestParams,
    ResponsesAPIResponse,
    ToolChoice,
    ToolParam,
)
from anchor.provider.param.response import *
from anchor.provider.param.legacy import GenericLiteLLMParams
from bound.surface.legacy.openai.types import ResponseText

from anchor.registry.router.config import ProviderConfigManager
from anchor.registry.router.locator import get_llm_provider

from bound.transport.mcp.handler import MCPHandler
from bound.transport.response.api.handler import ResponseApiHandler
from bound.channel.convert.asyncify import run_async_function
from bound.channel.param.litellm import infer_openai_data_residency
from bound.transport.client.wrapper import client
from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from bound.transport.response.template import update_responses_input_with_model_file_ids, update_responses_tools_with_model_file_ids
from bound.channel.api import APIBridge
from bound.transport.response.identity import ResponseIdentityManager

from phase.gov.proto.gate import uuid
from watcher.plane.emitter import get_emitter

log = get_emitter("api.response")

LiteLLMLoggingObj = Any

api_handler = ResponseApiHandler()

_OPENAI_CHAT_COMPLETIONS_RESPONSES_MODEL_PREFIX = "openai/chat_completions/"

def _has_file_search_tool(tools: Optional[Any]) -> bool:
    if not tools:
        return False
    return any(isinstance(t, dict) and t.get("type") == "file_search" for t in tools)

@dataclass
class ResponsesContext:
    """전처리가 완료된 Responses API 요청 상태 객체"""
    model: str
    input: Union[str, ResponseInputParam]
    custom_llm_provider: str
    tools: Optional[Iterable[ToolParam]]
    text: Optional[Any]
    
    # 설정 및 파라미터
    responses_api_provider_config: Optional[BaseResponsesAPIConfig]
    litellm_params: GenericLiteLLMParams
    response_api_optional_params: ResponsesAPIOptionalRequestParams
    responses_api_request_params: Dict[str, Any]
    
    # 메타 및 유틸리티 플래그
    log_delegator: Optional[LiteLLMLoggingObj]
    use_chat_completions_api: bool
    is_async: bool
    timeout: Union[float, httpx.Timeout]
    
    # Dispatch를 위해 전달받은 명시적 인자 모음
    explicit_args: Dict[str, Any]
    kwargs: Dict[str, Any]
    original_kwargs: Dict[str, Any]

class ResponsesPreprocessor:
    """raw 파라미터를 검증, 조립하여 ResponsesContext를 생성하는 빌더"""
    def __init__(self, explicit_args: Dict[str, Any], kwargs: Dict[str, Any]):
        self.explicit_args = explicit_args
        self.kwargs = kwargs
        self.original_kwargs = kwargs.copy()
        
        self.model = explicit_args.get("model", "")
        self.input = explicit_args.get("input")
        self.custom_llm_provider = explicit_args.get("custom_llm_provider")
        self.tools = explicit_args.get("tools")
        self.text = explicit_args.get("text")
        
        self.log_delegator = kwargs.get("log_delegator")
        self.is_async = kwargs.pop("aresponses", False) is True
        self.use_chat_completions_api = bool(kwargs.pop("use_chat_completions_api", None))
        
        self.litellm_params = GenericLiteLLMParams(**kwargs)
        self.merged_vars = {**explicit_args, **kwargs}

    def build(self) -> ResponsesContext:
        self._format_text()
        self._normalize_model_and_provider()
        self._apply_prompt_management()
        self._apply_file_id_mapping()
        self._map_reasoning_effort()
        
        provider_config = None
        if self.custom_llm_provider:
            provider_config = ProviderConfigManager.get_provider_responses_api_config(
                model=self.model, provider=self.custom_llm_provider
            )

        # Build final parameter sets
        response_api_optional_params = APIBridge.get_requested_response_api_optional_param(self.merged_vars)
        responses_api_request_params = dict(APIBridge.get_optional_params_responses_api(
            model=self.model,
            responses_api_provider_config=provider_config,
            response_api_optional_params=response_api_optional_params,
            allowed_openai_params=self.explicit_args.get("allowed_openai_params"),
        ))

        timeout = self.explicit_args.get("timeout")
        if timeout is None:
            timeout = request_timeout

        return ResponsesContext(
            model=self.model,
            input=self.input,
            custom_llm_provider=self.custom_llm_provider,
            tools=self.tools,
            text=self.text,
            responses_api_provider_config=provider_config,
            litellm_params=self.litellm_params,
            response_api_optional_params=response_api_optional_params,
            responses_api_request_params=responses_api_request_params,
            log_delegator=self.log_delegator,
            use_chat_completions_api=self.use_chat_completions_api,
            is_async=self.is_async,
            timeout=timeout,
            explicit_args=self.explicit_args,
            kwargs=self.kwargs,
            original_kwargs=self.original_kwargs,
        )

    def _format_text(self):
        self.text = APIBridge.convert_text_format_to_text_param(
            text_format=self.explicit_args.get("text_format"), text=self.text
        )
        if self.text is not None:
            self.merged_vars["text"] = self.text

    def _normalize_model_and_provider(self):
        if self.model.startswith(_OPENAI_CHAT_COMPLETIONS_RESPONSES_MODEL_PREFIX):
            remainder = self.model[len(_OPENAI_CHAT_COMPLETIONS_RESPONSES_MODEL_PREFIX) :]
            if remainder:
                self.model = f"openai/{remainder}"
                self.use_chat_completions_api = True

        (
            self.model,
            self.custom_llm_provider,
            dynamic_api_key,
            dynamic_api_base,
        ) = get_llm_provider(
            model=self.model,
            custom_llm_provider=self.custom_llm_provider,
            api_base=self.litellm_params.api_base,
            api_key=self.litellm_params.api_key,
        )
        self.merged_vars["model"] = self.model
        self.merged_vars["custom_llm_provider"] = self.custom_llm_provider

        if dynamic_api_key: self.litellm_params.api_key = dynamic_api_key
        if dynamic_api_base: self.litellm_params.api_base = dynamic_api_base

    def _apply_prompt_management(self):
        async_merged = self.kwargs.pop("_async_prompt_merged_params", None)
        if async_merged is not None:
            self.merged_vars.update(async_merged)
            return

        prompt_id = self.kwargs.get("prompt_id")
        original_model = self.model

        client_input = [{"role": "user", "content": self.input}] if isinstance(self.input, str) else [
            item for item in self.input if isinstance(item, dict) and "role" in item # type: ignore[misc]
        ]

        if hasattr(self.log_delegator, "should_run_prompt_management_hooks") and \
           self.log_delegator.should_run_prompt_management_hooks(prompt_id=prompt_id, non_default_params=self.kwargs):
            
            self.model, merged_input, merged_optional_params = self.log_delegator.get_chat_completion_prompt(
                model=self.model, messages=client_input, non_default_params=self.kwargs,
                prompt_id=prompt_id, prompt_variables=self.kwargs.get("prompt_variables"),
                prompt_label=self.kwargs.get("prompt_label"), prompt_version=self.kwargs.get("prompt_version"),
            )
            self.input = cast(Union[str, ResponseInputParam], merged_input)
            self.merged_vars["input"] = self.input
            self.merged_vars["model"] = self.model
            
            if self.model != original_model:
                _, self.custom_llm_provider, _, _ = get_llm_provider(model=self.model)
                self.merged_vars["custom_llm_provider"] = self.custom_llm_provider
                
            self.merged_vars.update(merged_optional_params)

    def _apply_file_id_mapping(self):
        model_file_id_mapping = self.kwargs.get("model_file_id_mapping")
        model_info_id = self.kwargs.get("model_info", {}).get("id") if isinstance(self.kwargs.get("model_info"), dict) else None

        self.input = cast(Union[str, ResponseInputParam], update_responses_input_with_model_file_ids(
            input=self.input, model_id=model_info_id, model_file_id_mapping=model_file_id_mapping
        ))
        if self.tools:
            self.tools = cast(Optional[Iterable[ToolParam]], update_responses_tools_with_model_file_ids(
                tools=cast(Optional[List[Dict[str, Any]]], self.tools),
                model_id=model_info_id, model_file_id_mapping=model_file_id_mapping
            ))
        self.merged_vars["input"] = self.input
        self.merged_vars["tools"] = self.tools

    def _map_reasoning_effort(self):
        if self.explicit_args.get("reasoning") is not None:
            return

        reasoning_effort = self.merged_vars.pop("reasoning_effort", None)
        if reasoning_effort:
            effort_level = str(reasoning_effort).strip().lower()
            valid_levels = {"low", "medium", "high"}
            if effort_level in valid_levels:
                mapped_reasoning = {
                    "type": "effort",
                    "level": effort_level
                }
                self.explicit_args["reasoning"] = mapped_reasoning
                self.merged_vars["reasoning"] = mapped_reasoning
            else:
                log.warning(f"[Responses] Invalid reasoning_effort value: '{reasoning_effort}'. Ignored.")

class ResponsesDispatcher:
    """생성된 ResponsesContext를 기반으로 적절한 핸들러로 라우팅하는 역할"""
    def __init__(self, context: ResponsesContext):
        self.ctx = context

    def execute(self) -> Any:
        if mcp_res := self._dispatch_mcp():
            return mcp_res
            
        if fs_res := self._dispatch_file_search():
            return fs_res
            
        return self._dispatch_final_api()

    def _dispatch_mcp(self) -> Optional[Any]:
        if not MCPHandler._should_use_litellm_mcp_gateway(tools=self.ctx.tools):
            return None
            
        mcp_kwargs = {
            **self.ctx.explicit_args,
            "input": self.ctx.input,
            "model": self.ctx.model,
            "tools": self.ctx.tools,
            "custom_llm_provider": self.ctx.custom_llm_provider,
            "timeout": self.ctx.timeout,
            **self.ctx.kwargs
        }
        
        if self.ctx.is_async:
            from bound.surface.action.aresponse import aresponses_api_with_mcp
            return aresponses_api_with_mcp(**mcp_kwargs)
        from bound.surface.action.aresponse import aresponses_api_with_mcp
        return run_async_function(aresponses_api_with_mcp, **mcp_kwargs)

    def _dispatch_file_search(self) -> Optional[Any]:
        from bound.surface.action.search import aresponses_with_emulated_file_search
        
        if not _has_file_search_tool(self.ctx.tools) or not (
            self.ctx.responses_api_provider_config is None
            or self.ctx.use_chat_completions_api is True
            or not self.ctx.responses_api_provider_config.supports_native_file_search()
        ):
            return None

        emulated_kwargs = {
            **self.ctx.explicit_args,
            "custom_llm_provider": self.ctx.custom_llm_provider,
            "timeout": self.ctx.timeout,
            **{k: v for k, v in self.ctx.kwargs.items() if k not in {"call_id", "aresponses"}}
        }
        if self.ctx.use_chat_completions_api:
            emulated_kwargs["use_chat_completions_api"] = True

        if self.ctx.is_async:
            return aresponses_with_emulated_file_search(
                input=self.ctx.input, model=self.ctx.model, tools=self.ctx.tools, **emulated_kwargs
            )
        return run_async_function(
            aresponses_with_emulated_file_search,
            input=self.ctx.input, model=self.ctx.model, tools=self.ctx.tools, **emulated_kwargs
        )

    def _dispatch_final_api(self) -> Any:
        if self.ctx.log_delegator:
            self.ctx.log_delegator.update_from_kwargs(
                kwargs=self.ctx.kwargs,
                model=self.ctx.model,
                user=self.ctx.explicit_args.get("user"),
                optional_params=self.ctx.responses_api_request_params,
                litellm_params={
                    **self.ctx.responses_api_request_params,
                    "aresponses": self.ctx.is_async,
                    "call_id": self.ctx.kwargs.get("call_id"),
                    "model_info": self.ctx.kwargs.get("model_info"),
                    "data_residency": infer_openai_data_residency(
                        self.ctx.custom_llm_provider, self.ctx.litellm_params.api_base
                    ),
                    "metadata": self.ctx.kwargs.get("litellm_metadata", self.ctx.kwargs.get("metadata")),
                },
                custom_llm_provider=self.ctx.custom_llm_provider,
            )

        final_input = APIBridge._restore_encrypted_content_item_ids_in_input(self.ctx.input)
        if self.ctx.custom_llm_provider is None:
            raise ValueError("custom_llm_provider is required but passed as None")

        response = api_handler.api_handler(
            model=self.ctx.model,
            input=final_input,
            responses_api_provider_config=self.ctx.responses_api_provider_config,
            response_api_optional_request_params=self.ctx.responses_api_request_params,
            custom_llm_provider=self.ctx.custom_llm_provider,
            litellm_params=self.ctx.litellm_params,
            logging_obj=self.ctx.log_delegator,
            extra_headers=self.ctx.explicit_args.get("extra_headers"),
            extra_body=self.ctx.explicit_args.get("extra_body"),
            timeout=self.ctx.timeout,
            _is_async=self.ctx.is_async,
            client=self.ctx.kwargs.get("client"),
            fake_stream=self.ctx.responses_api_provider_config.should_fake_stream(
                model=self.ctx.model, stream=self.ctx.explicit_args.get("stream"), custom_llm_provider=self.ctx.custom_llm_provider
            ) if self.ctx.responses_api_provider_config else False,
            litellm_metadata=self.ctx.kwargs.get("litellm_metadata", {}),
            shared_session=self.ctx.kwargs.get("shared_session"),
        )

        if isinstance(response, ResponsesAPIResponse):
            response = APIBridge._update_responses_api_response_id_with_model_id(
                responses_api_response=response,
                litellm_metadata=self.ctx.kwargs.get("litellm_metadata", {}),
                custom_llm_provider=self.ctx.custom_llm_provider,
            )
            response._hidden_params["custom_llm_provider"] = self.ctx.custom_llm_provider
            
        return response


## entrypoint
@client
def responses(
    input: Union[str, ResponseInputParam],
    model: str,
    include: Optional[List[ResponseIncludable]] = None,
    instructions: Optional[str] = None,
    max_output_tokens: Optional[int] = None,
    prompt: Optional[PromptObject] = None,
    metadata: Optional[Dict[str, Any]] = None,
    parallel_tool_calls: Optional[bool] = None,
    previous_response_id: Optional[str] = None,
    reasoning: Optional[Reasoning] = None,
    store: Optional[bool] = None,
    background: Optional[bool] = None,
    stream: Optional[bool] = None,
    temperature: Optional[float] = None,
    text: Optional["ResponseText"] = None,
    text_format: Optional[Union[Type["BaseModel"], dict]] = None,
    tool_choice: Optional[ToolChoice] = None,
    tools: Optional[Iterable[ToolParam]] = None,
    top_p: Optional[float] = None,
    truncation: Optional[Literal["auto", "disabled"]] = None,
    user: Optional[str] = None,
    service_tier: Optional[str] = None,
    safety_identifier: Optional[str] = None,
    extra_headers: Optional[Dict[str, Any]] = None,
    extra_query: Optional[Dict[str, Any]] = None,
    extra_body: Optional[Dict[str, Any]] = None,
    timeout: Optional[Union[float, httpx.Timeout]] = None,
    allowed_openai_params: Optional[List[str]] = None,
    custom_llm_provider: Optional[str] = None,
    **kwargs,
):
    """모든 요청 파라미터를 수집하여 Preprocessor로 Context를 빌드하고, Dispatcher를 통해 적절한 내부 핸들러로 라우팅"""
    explicit_args = {
        "input": input, "model": model, "include": include, "instructions": instructions,
        "max_output_tokens": max_output_tokens, "prompt": prompt, "metadata": metadata,
        "parallel_tool_calls": parallel_tool_calls, "previous_response_id": previous_response_id,
        "reasoning": reasoning, "store": store, "background": background, "stream": stream,
        "temperature": temperature, "text": text, "text_format": text_format,
        "tool_choice": tool_choice, "tools": tools, "top_p": top_p, "truncation": truncation,
        "user": user, "service_tier": service_tier, "safety_identifier": safety_identifier,
        "extra_headers": extra_headers, "extra_query": extra_query, "extra_body": extra_body,
        "timeout": timeout, "allowed_openai_params": allowed_openai_params, 
        "custom_llm_provider": custom_llm_provider
    }

    try:
        context = ResponsesPreprocessor(explicit_args=explicit_args, kwargs=kwargs).build()
        dispatcher = ResponsesDispatcher(context=context)
        return dispatcher.execute()
    except Exception as e:
        ## 에러 발생 시, 원본 kwargs와 explicit_args를 병합하여 디버깅 정보 제공
        completion_kwargs = {**explicit_args, **kwargs}
        raise config.exception_type(
            model=explicit_args.get("model", model),
            custom_llm_provider=explicit_args.get("custom_llm_provider", custom_llm_provider),
            original_exception=e,
            completion_kwargs=completion_kwargs,
            extra_kwargs=kwargs,
        )