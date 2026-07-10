# bound.surface.action.response
import asyncio
import contextvars
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Dict, Iterable, List, Literal, Optional, Type, Union, cast
import base64
import json
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
    ResponseText
)
from anchor.provider.param.response import *
from anchor.provider.param.legacy import GenericLiteLLMParams

from anchor.registry.router.config import ProviderConfigManager
from anchor.registry.router.locator import get_llm_provider

from bound.adapter.protocol.mcp.handler import MCPHandler
from bound.surface.response.api.handler import ResponseApiHandler
from bound.surface.bridge.convert.asyncify import run_async_function
from bound.surface.bridge.param.litellm import infer_openai_data_residency
from bound.surface.action.client.wrapper import client
from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from bound.surface.bridge.api import APIBridge
from bound.surface.response.identity import ResponseIdentityManager
from bound.surface.response.api.context import ResponseAPIContext, ContextBuilder

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

class TemplateDecoder:
    @staticmethod
    def is_base64_encoded_unified_id(uid: str) -> bool:
        if not isinstance(uid, str) or not uid:
            return False
        try:
            decoded_bytes = base64.b64decode(uid, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            return "llm_output_file_id," in decoded_str or "provider_resource_id" in decoded_str
        except Exception:
            return False

    @staticmethod
    def decode_file_id(uid: str) -> str:
        try:
            return base64.b64decode(uid).decode("utf-8")
        except Exception:
            return uid

    @staticmethod
    def parse_vector_store_id(uid: str) -> dict:
        try:
            decoded = base64.b64decode(uid).decode("utf-8")
            return json.loads(decoded)
        except Exception:
            return {}

class ResponseTemplateProcessor:
    """Response Template의 Input 및 Tools 객체를 변환하는 프로세서"""
    def __init__(
        self, 
        model_id: Optional[str] = None, 
        model_file_id_mapping: Optional[Dict[str, Dict[str, str]]] = None
    ):
        self.model_id = model_id
        self.mapping = model_file_id_mapping or {}

    def process_input(self, input_data: Any) -> Union[str, List[Dict[str, Any]]]:
        if isinstance(input_data, str) or not isinstance(input_data, list):
            return input_data

        updated_input = []
        for item in input_data:
            if not isinstance(item, dict):
                updated_input.append(item)
                continue

            updated_item = item.copy()
            content = item.get("content")

            if isinstance(content, list):
                updated_item["content"] = self._process_input_content(content)

            updated_input.append(updated_item)
        
        return updated_input

    def _process_input_content(self, content: List[Any]) -> List[Any]:
        updated_content = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "input_file":
                file_id = item.get("file_id")
                if file_id:
                    updated_item = item.copy()
                    updated_item["file_id"] = self._resolve_input_file_id(file_id)
                    updated_content.append(updated_item)
                    continue
            
            updated_content.append(item)
        return updated_content

    def _resolve_input_file_id(self, file_id: str) -> str:
        # 1. 맵핑 테이블에 존재하는지 확인
        if self.model_id and file_id in self.mapping:
            return self.mapping[file_id].get(self.model_id) or file_id

        # 2. Base64로 인코딩된 Unified ID인지 확인 및 디코딩
        if TemplateDecoder.is_base64_encoded_unified_id(file_id):
            decoded_str = TemplateDecoder.decode_file_id(file_id)
            if "llm_output_file_id," in decoded_str:
                return decoded_str.split("llm_output_file_id,")[1].split(";")[0]

        # 3. 매칭되지 않으면 원본 반환
        return file_id

    def process_tools(self, tools: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
        if not tools or not isinstance(tools, list):
            return tools

        # Pass 1: decode unified vector_store_ids (조건 없이 항상 실행)
        updated_tools = self._decode_vector_store_ids(tools)

        # Pass 2: mapping이 필요한 code_interpreter 처리
        if not self.mapping or not self.model_id:
            return updated_tools

        final_tools = []
        for tool in updated_tools:
            if not isinstance(tool, dict) or tool.get("type") != "code_interpreter":
                final_tools.append(tool)
                continue

            updated_tool = tool.copy()
            container = tool.get("container")

            if isinstance(container, dict) and isinstance(container.get("file_ids"), list):
                updated_container = container.copy()
                updated_container["file_ids"] = [
                    self._resolve_tool_file_id(fid) for fid in container.get("file_ids")
                ]
                updated_tool["container"] = updated_container
            
            final_tools.append(updated_tool)
            
        return final_tools

    def _resolve_tool_file_id(self, file_id: Any) -> Any:
        if isinstance(file_id, str) and file_id in self.mapping:
            return self.mapping[file_id].get(self.model_id) or file_id
        return file_id

    def _decode_vector_store_ids(self, tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        updated_tools = []
        for tool in tools:
            if not isinstance(tool, dict) or tool.get("type") != "file_search":
                updated_tools.append(tool)
                continue

            vector_store_ids = tool.get("vector_store_ids")
            if not isinstance(vector_store_ids, list):
                updated_tools.append(tool)
                continue

            decoded_ids = []
            for vs_id in vector_store_ids:
                if isinstance(vs_id, str) and TemplateDecoder.is_base64_encoded_unified_id(vs_id):
                    parsed_dict = TemplateDecoder.parse_vector_store_id(vs_id)
                    provider_resource_id = parsed_dict.get("provider_resource_id")

                    if provider_resource_id:
                        decoded_ids.append(provider_resource_id)
                    else:
                        log.warning("file_search tool contains unified vector_store_id '%s' that could not be parsed.", vs_id)
                        decoded_ids.append(vs_id)
                else:
                    decoded_ids.append(vs_id)

            updated_tool = tool.copy()
            updated_tool["vector_store_ids"] = decoded_ids
            updated_tools.append(updated_tool)

        return updated_tools

def update_responses_input_with_model_file_ids(
    input: Any,
    model_id: Optional[str] = None,
    model_file_id_mapping: Optional[Dict[str, Dict[str, str]]] = None,
) -> Union[str, List[Dict[str, Any]]]:
    processor = ResponseTemplateProcessor(model_id, model_file_id_mapping)
    return processor.process_input(input)

def update_responses_tools_with_model_file_ids(
    tools: Optional[List[Dict[str, Any]]],
    model_id: Optional[str] = None,
    model_file_id_mapping: Optional[Dict[str, Dict[str, str]]] = None,
) -> Optional[List[Dict[str, Any]]]:
    processor = ResponseTemplateProcessor(model_id, model_file_id_mapping)
    return processor.process_tools(tools)


class ResponsesPreprocessor:
    """
    Validates, normalizes, and applies business logic to raw parameters,
    ultimately delegating to ContextBuilder to yield a unified ResponseAPIContext.
    """
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

    def build(self) -> ResponseAPIContext:
        ## Mutate and normalize core parameters
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

        ## Sync mutated attributes back to explicit_args for the ContextBuilder
        self.explicit_args.update({
            "model": self.model,
            "input": self.input,
            "tools": self.tools,
            "text": self.text,
            "custom_llm_provider": self.custom_llm_provider
        })

        ## Delegate to ContextBuilder to create the structured payload
        ctx = ContextBuilder.from_explicit_args(
            explicit_args=self.explicit_args,
            litellm_params=self.litellm_params,
            is_async=self.is_async,
            responses_api_provider_config=provider_config,
            log_delegator=self.log_delegator,
            client=self.kwargs.get("client"),
            shared_session=self.kwargs.get("shared_session"),
        )

        ## Evaluate optional bridge parameters and attach transient states to the context
        response_api_optional_params = APIBridge.get_requested_response_api_optional_param(self.merged_vars)
        ctx._responses_api_request_params = dict(APIBridge.get_optional_params_responses_api(
            model=self.model,
            responses_api_provider_config=provider_config,
            response_api_optional_params=response_api_optional_params,
            allowed_openai_params=self.explicit_args.get("allowed_openai_params"),
        ))
        
        ctx._raw_kwargs = self.kwargs
        ctx._use_chat_completions_api = self.use_chat_completions_api

        return ctx

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
            item for item in self.input if isinstance(item, dict) and "role" in item
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
    """
    Routes the structured ResponseAPIContext to the appropriate endpoint.
    Eliminates long parameter lists by passing the unified context directly.
    """
    def __init__(self, context: ResponseAPIContext):
        self.ctx = context

    def execute(self) -> Any:
        if mcp_res := self._dispatch_mcp():
            return mcp_res
            
        if fs_res := self._dispatch_file_search():
            return fs_res
            
        return self._dispatch_final_api()

    def _dispatch_mcp(self) -> Optional[Any]:
        if not MCPHandler._should_use_litellm_mcp_gateway(tools=self.ctx.payload.tools):
            return None
            
        mcp_kwargs = {
            **self.ctx._raw_kwargs,
            "input": self.ctx.payload.input,
            "model": self.ctx.payload.model,
            "tools": self.ctx.payload.tools,
            "custom_llm_provider": self.ctx.provider.custom_llm_provider,
            "timeout": self.ctx.exec.timeout,
        }
        
        if self.ctx.is_async:
            from bound.surface.action.aresponse import aresponses_api_with_mcp
            return aresponses_api_with_mcp(**mcp_kwargs)
            
        from bound.surface.action.aresponse import aresponses_api_with_mcp
        return run_async_function(aresponses_api_with_mcp, **mcp_kwargs)

    def _dispatch_file_search(self) -> Optional[Any]:
        from bound.surface.action.search import aresponses_with_emulated_file_search
        
        provider_config = self.ctx.provider.responses_api_provider_config
        if not _has_file_search_tool(self.ctx.payload.tools) or not (
            provider_config is None
            or self.ctx._use_chat_completions_api is True
            or not provider_config.supports_native_file_search()
        ):
            return None

        emulated_kwargs = {
            "custom_llm_provider": self.ctx.provider.custom_llm_provider,
            "timeout": self.ctx.exec.timeout,
            **{k: v for k, v in self.ctx._raw_kwargs.items() if k not in {"call_id", "aresponses"}}
        }
        if self.ctx._use_chat_completions_api:
            emulated_kwargs["use_chat_completions_api"] = True

        if self.ctx.is_async:
            return aresponses_with_emulated_file_search(
                input=self.ctx.payload.input, model=self.ctx.payload.model, tools=self.ctx.payload.tools, **emulated_kwargs
            )
        return run_async_function(
            aresponses_with_emulated_file_search,
            input=self.ctx.payload.input, model=self.ctx.payload.model, tools=self.ctx.payload.tools, **emulated_kwargs
        )

    def _dispatch_final_api(self) -> Any:
        ctx = self.ctx
        
        ## Apply Pre-call Logging
        if ctx.provider.logging_obj:
            ctx.provider.logging_obj.update_from_kwargs(
                kwargs=ctx._raw_kwargs,
                model=ctx.payload.model,
                user=ctx.payload.user,
                optional_params=ctx._responses_api_request_params,
                litellm_params={
                    **ctx._responses_api_request_params,
                    "aresponses": ctx.is_async,
                    "call_id": ctx._raw_kwargs.get("call_id"),
                    "model_info": ctx._raw_kwargs.get("model_info"),
                    "data_residency": infer_openai_data_residency(
                        ctx.provider.custom_llm_provider, ctx.provider.litellm_params.api_base
                    ),
                    "metadata": ctx._raw_kwargs.get("litellm_metadata", ctx._raw_kwargs.get("metadata")),
                },
                custom_llm_provider=ctx.provider.custom_llm_provider,
            )

        ## Final Input Restoration
        final_input = APIBridge._restore_encrypted_content_item_ids_in_input(ctx.payload.input)
        if ctx.provider.custom_llm_provider is None:
            raise ValueError("custom_llm_provider is required but passed as None")

        # Mutate the input within the payload for the final handler
        ctx.payload.input = final_input
        
        # Determine fake stream requirements and stash it in execution context
        provider_config = ctx.provider.responses_api_provider_config
        fake_stream = False
        if provider_config:
            fake_stream = provider_config.should_fake_stream(
                model=ctx.payload.model, 
                stream=ctx.payload.stream, 
                custom_llm_provider=ctx.provider.custom_llm_provider
            )
        ctx.exec.extra_body["_fake_stream"] = fake_stream

        ## [개선] 15개의 흩어진 인자 대신 단일 Context 객체 패싱
        response = api_handler.response_api_handler(ctx=ctx)

        ## Post-processing identity mappings
        if isinstance(response, ResponsesAPIResponse):
            response = APIBridge._update_responses_api_response_id_with_model_id(
                responses_api_response=response,
                litellm_metadata=ctx._raw_kwargs.get("litellm_metadata", {}),
                custom_llm_provider=ctx.provider.custom_llm_provider,
            )
            response._hidden_params["custom_llm_provider"] = ctx.provider.custom_llm_provider
            
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
    """
    Entrypoint for responses API. 
    Aggregates parameters, builds the structured Context via Preprocessor, 
    and dispatches to the underlying handlers.
    """
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
        completion_kwargs = {**explicit_args, **kwargs}
        raise config.exception_type(
            model=explicit_args.get("model", model),
            custom_llm_provider=explicit_args.get("custom_llm_provider", custom_llm_provider),
            original_exception=e,
            completion_kwargs=completion_kwargs,
            extra_kwargs=kwargs,
        )