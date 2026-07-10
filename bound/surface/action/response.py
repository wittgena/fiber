# bound.surface.action.response
import asyncio
import base64
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Literal, Optional, Type, Union, cast

import httpx
from pydantic import BaseModel

from bound.surface.legacy.param.response import *
from bound.surface.legacy.param.legacy import GenericLiteLLMParams
from anchor.registry.router.config import ProviderConfigManager
from anchor.registry.router.locator import get_llm_provider

from anchor.registry.model.config.resolver import config
from bound.surface.legacy.openai.types import (
    AllMessageValues, PromptObject, Reasoning, ResponseIncludable, ResponseInputParam,
    ResponsesAPIResponse, ToolChoice, ToolParam, ResponseText
)

# --- MCP & Websocket ---
from bound.transport.protocol.mcp.handler import MCPHandler
from bound.transport.protocol.mcp.parser.payload import MCPPayloadParser
from bound.transport.stream.mcp import MCPStreamIterator
from bound.transport.protocol.mcp.event.tool import create_mcp_list_tools_events
from bound.transport.ws import ResponseWebsocketHandler

# --- Bridge & Context ---
from anchor.registry.model.api.base import APIBridge
from bound.surface.bridge.param.litellm import get_litellm_params, infer_openai_data_residency
from bound.transport.stream.api.context import ResponseAPIContext, ContextBuilder
from bound.transport.stream.api.identity import IdentityRouter
from bound.surface.action.client.wrapper import client
from bound.transport.stream.api.handler import ResponseApiHandler, _build_context, _execute_with_bridge
from bound.surface.bridge.tosync import AsyncToSyncBridge, SyncStreamAdapter
from bound.transport.stream.iterator import ResponseStreamIterator

from phase.gov.proto.gate import uuid
from watcher.plane.emitter import get_emitter

log = get_emitter("action.response")
api_handler = ResponseApiHandler()
ws_handler = ResponseWebsocketHandler()

_OPENAI_CHAT_COMPLETIONS_RESPONSES_MODEL_PREFIX = "openai/chat_completions/"


# =============================================================================
# Helper Utilities (템플릿 프로세서 등)
# =============================================================================
def _has_file_search_tool(tools: Optional[Any]) -> bool:
    if not tools: return False
    return any(isinstance(t, dict) and t.get("type") == "file_search" for t in tools)

class TemplateDecoder:
    @staticmethod
    def is_base64_encoded_unified_id(uid: str) -> bool:
        if not isinstance(uid, str) or not uid: return False
        try:
            decoded_str = base64.b64decode(uid, validate=True).decode("utf-8")
            return "llm_output_file_id," in decoded_str or "provider_resource_id" in decoded_str
        except Exception: return False

    @staticmethod
    def decode_file_id(uid: str) -> str:
        try: return base64.b64decode(uid).decode("utf-8")
        except Exception: return uid

    @staticmethod
    def parse_vector_store_id(uid: str) -> dict:
        try: return json.loads(base64.b64decode(uid).decode("utf-8"))
        except Exception: return {}

class ResponseTemplateProcessor:
    def __init__(self, model_id: Optional[str] = None, mapping: Optional[Dict[str, Dict[str, str]]] = None):
        self.model_id = model_id
        self.mapping = mapping or {}

    def process_input(self, input_data: Any) -> Union[str, List[Dict[str, Any]]]:
        if isinstance(input_data, str) or not isinstance(input_data, list): return input_data
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
        updated = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "input_file":
                file_id = item.get("file_id")
                if file_id:
                    upd = item.copy()
                    upd["file_id"] = self._resolve_id(file_id)
                    updated.append(upd)
                    continue
            updated.append(item)
        return updated

    def _resolve_id(self, file_id: str) -> str:
        if self.model_id and file_id in self.mapping:
            return self.mapping[file_id].get(self.model_id) or file_id
        if TemplateDecoder.is_base64_encoded_unified_id(file_id):
            decoded_str = TemplateDecoder.decode_file_id(file_id)
            if "llm_output_file_id," in decoded_str:
                return decoded_str.split("llm_output_file_id,")[1].split(";")[0]
        return file_id

    def process_tools(self, tools: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
        if not tools or not isinstance(tools, list): return tools
        updated = self._decode_vector_store_ids(tools)
        if not self.mapping or not self.model_id: return updated
        
        final = []
        for tool in updated:
            if not isinstance(tool, dict) or tool.get("type") != "code_interpreter":
                final.append(tool)
                continue
            upd_tool = tool.copy()
            container = tool.get("container")
            if isinstance(container, dict) and isinstance(container.get("file_ids"), list):
                upd_container = container.copy()
                upd_container["file_ids"] = [self._resolve_id(fid) for fid in container.get("file_ids")]
                upd_tool["container"] = upd_container
            final.append(upd_tool)
        return final

    def _decode_vector_store_ids(self, tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        updated = []
        for tool in tools:
            if not isinstance(tool, dict) or tool.get("type") != "file_search":
                updated.append(tool)
                continue
            vs_ids = tool.get("vector_store_ids")
            if not isinstance(vs_ids, list):
                updated.append(tool)
                continue
            decoded_ids = []
            for vs_id in vs_ids:
                if isinstance(vs_id, str) and TemplateDecoder.is_base64_encoded_unified_id(vs_id):
                    pid = TemplateDecoder.parse_vector_store_id(vs_id).get("provider_resource_id")
                    if pid: decoded_ids.append(pid)
                    else: decoded_ids.append(vs_id)
                else: decoded_ids.append(vs_id)
            upd_tool = tool.copy()
            upd_tool["vector_store_ids"] = decoded_ids
            updated.append(upd_tool)
        return updated


# =============================================================================
# Context Builder & Dispatcher (Async Core)
# =============================================================================
class ResponsesPreprocessor:
    def __init__(self, explicit_args: Dict[str, Any], kwargs: Dict[str, Any]):
        self.explicit_args = explicit_args
        self.kwargs = kwargs
        self.model = explicit_args.get("model", "")
        self.input = explicit_args.get("input")
        self.custom_llm_provider = explicit_args.get("custom_llm_provider")
        self.tools = explicit_args.get("tools")
        self.text = explicit_args.get("text")
        self.log_delegator = kwargs.get("log_delegator")
        self.is_async = True # Core is ALWAYS async
        self.use_chat_completions_api = bool(kwargs.pop("use_chat_completions_api", None))
        self.litellm_params = GenericLiteLLMParams(**kwargs)
        self.merged_vars = {**explicit_args, **kwargs}

    async def build(self) -> ResponseAPIContext:
        self._format_text()
        self._normalize_model_and_provider()
        await self._apply_prompt_management()
        self._apply_file_id_mapping()
        self._map_reasoning_effort()
        
        provider_config = None
        if self.custom_llm_provider:
            provider_config = ProviderConfigManager.get_provider_responses_api_config(model=self.model, provider=self.custom_llm_provider)

        self.explicit_args.update({"model": self.model, "input": self.input, "tools": self.tools, "text": self.text, "custom_llm_provider": self.custom_llm_provider})
        ctx = ContextBuilder.from_explicit_args(
            explicit_args=self.explicit_args, litellm_params=self.litellm_params, is_async=self.is_async,
            responses_api_provider_config=provider_config, log_delegator=self.log_delegator,
            client=self.kwargs.get("client"), shared_session=self.kwargs.get("shared_session"),
        )
        
        opt_params = APIBridge.get_requested_response_api_optional_param(self.merged_vars)
        ctx._responses_api_request_params = dict(APIBridge.get_optional_params_responses_api(
            model=self.model, responses_api_provider_config=provider_config, response_api_optional_params=opt_params, allowed_openai_params=self.explicit_args.get("allowed_openai_params"),
        ))
        ctx.exec.raw_kwargs = self.kwargs
        ctx._use_chat_completions_api = self.use_chat_completions_api
        return ctx

    def _format_text(self):
        self.text = APIBridge.convert_text_format_to_text_param(text_format=self.explicit_args.get("text_format"), text=self.text)
        if self.text is not None: self.merged_vars["text"] = self.text

    def _normalize_model_and_provider(self):
        if self.model.startswith(_OPENAI_CHAT_COMPLETIONS_RESPONSES_MODEL_PREFIX):
            self.model = f"openai/{self.model[len(_OPENAI_CHAT_COMPLETIONS_RESPONSES_MODEL_PREFIX):]}"
            self.use_chat_completions_api = True
        self.model, self.custom_llm_provider, dyn_key, dyn_base = get_llm_provider(
            model=self.model, custom_llm_provider=self.custom_llm_provider, api_base=self.litellm_params.api_base, api_key=self.litellm_params.api_key
        )
        self.merged_vars.update({"model": self.model, "custom_llm_provider": self.custom_llm_provider})
        if dyn_key: self.litellm_params.api_key = dyn_key
        if dyn_base: self.litellm_params.api_base = dyn_base

    async def _apply_prompt_management(self):
        async_merged = self.kwargs.pop("_async_prompt_merged_params", None)
        if async_merged is not None:
            self.merged_vars.update(async_merged)
            return

        prompt_id = self.kwargs.get("prompt_id")
        client_input = [{"role": "user", "content": self.input}] if isinstance(self.input, str) else [i for i in self.input if isinstance(i, dict) and "role" in i]
        
        if self.log_delegator and hasattr(self.log_delegator, "async_get_chat_completion_prompt") and self.log_delegator.should_run_prompt_management_hooks(prompt_id=prompt_id, non_default_params=self.kwargs):
            self.model, self.input, merged_opt = await self.log_delegator.async_get_chat_completion_prompt(
                model=self.model, messages=client_input, non_default_params=self.kwargs, prompt_id=prompt_id,
                prompt_variables=self.kwargs.get("prompt_variables"), prompt_label=self.kwargs.get("prompt_label"), prompt_version=self.kwargs.get("prompt_version"),
            )
            self.merged_vars.update({"input": self.input, "model": self.model, **merged_opt})
            _, self.custom_llm_provider, _, _ = get_llm_provider(model=self.model)
            self.merged_vars["custom_llm_provider"] = self.custom_llm_provider

    def _apply_file_id_mapping(self):
        mapping = self.kwargs.get("model_file_id_mapping")
        mid = self.kwargs.get("model_info", {}).get("id") if isinstance(self.kwargs.get("model_info"), dict) else None
        processor = ResponseTemplateProcessor(mid, mapping)
        self.input = processor.process_input(self.input)
        if self.tools: self.tools = processor.process_tools(cast(List[Dict[str, Any]], self.tools))
        self.merged_vars.update({"input": self.input, "tools": self.tools})

    def _map_reasoning_effort(self):
        if self.explicit_args.get("reasoning") is not None: return
        if effort := self.merged_vars.pop("reasoning_effort", None):
            effort = str(effort).strip().lower()
            if effort in {"low", "medium", "high"}:
                self.explicit_args["reasoning"] = self.merged_vars["reasoning"] = {"type": "effort", "level": effort}


class AsyncResponsesDispatcher:
    def __init__(self, context: ResponseAPIContext):
        self.ctx = context

    async def execute(self) -> Any:
        if mcp_res := await self._dispatch_mcp(): return mcp_res
        if fs_res := await self._dispatch_file_search(): return fs_res
        return await self._dispatch_final_api()

    async def _dispatch_mcp(self) -> Optional[Any]:
        if not MCPHandler._should_use_litellm_mcp_gateway(tools=self.ctx.payload.tools): return None
        
        mcp_tools, other_tools = MCPPayloadParser._parse_mcp_tools(self.ctx.payload.tools)
        user_auth = self.ctx.exec.raw_kwargs.get("user_api_key_auth") or self.ctx.exec.raw_kwargs.get("litellm_metadata", {}).get("user_api_key_auth")
        
        mcp_auth, server_auth = None, None
        if secret_fields := self.ctx.exec.raw_kwargs.get("secret_fields"):
            mcp_auth, server_auth, _, _ = IdentityRouter.extract_mcp_headers_from_request(secret_fields=secret_fields, tools=self.ctx.payload.tools)

        orig_tools, tool_map = await MCPHandler._process_mcp_tools_without_openai_transform(
            user_api_key_auth=user_auth, mcp_tools_with_litellm_proxy=mcp_tools, litellm_trace_id=self.ctx.exec.raw_kwargs.get("litellm_trace_id"),
            mcp_auth_header=mcp_auth, mcp_server_auth_headers=server_auth,
        )
        all_tools = MCPHandler._transform_mcp_tools_to_openai(orig_tools) + other_tools if (orig_tools or other_tools) else None
        
        call_params = {"stream": self.ctx.payload.stream, "previous_response_id": self.ctx.payload.previous_response_id, **self.ctx.exec.raw_kwargs}
        if self.ctx.payload.stream and mcp_tools:
            mcp_events = await create_mcp_list_tools_events(mcp_tools, user_auth, f"mcp_{uuid.uuid4().hex[:8]}", orig_tools)
            req_params = MCPPayloadParser._build_request_params(input=self.ctx.payload.input, model=self.ctx.payload.model, all_tools=all_tools, call_params=call_params, **self.ctx.exec.raw_kwargs)
            return MCPStreamIterator(base_iterator=None, mcp_events=mcp_events, tool_server_map=tool_map, mcp_tools_with_litellm_proxy=mcp_tools, user_api_key_auth=user_auth, original_request_params=req_params)

        auto_exec = bool(mcp_tools) and MCPHandler._should_auto_execute_tools(tools=mcp_tools)
        init_params = MCPPayloadParser._prepare_initial_call_params(call_params=call_params, should_auto_execute=auto_exec)

        # 재귀 호출 (Phase 1)
        response = await aresponses(input=self.ctx.payload.input, model=self.ctx.payload.model, tools=all_tools, previous_response_id=self.ctx.payload.previous_response_id, **init_params)

        # 오케스트레이션 루프 (Phase 2)
        if auto_exec and isinstance(response, ResponsesAPIResponse) and (tool_calls := MCPPayloadParser._extract_tool_calls_from_response(response)):
            tool_results = await MCPHandler._execute_tool_calls(
                tool_server_map=tool_map, tool_calls=tool_calls, user_api_key_auth=user_auth,
                mcp_auth_header=mcp_auth, mcp_server_auth_headers=server_auth, oauth2_headers=None, raw_headers=None,
                call_id=self.ctx.exec.raw_kwargs.get("call_id"), litellm_trace_id=self.ctx.exec.raw_kwargs.get("litellm_trace_id"),
            )
            if tool_results:
                follow_up_input = MCPPayloadParser._create_follow_up_input(response=response, tool_results=tool_results, original_input=self.ctx.payload.input)
                follow_up_params = MCPPayloadParser._prepare_follow_up_call_params(call_params=call_params, original_stream_setting=self.ctx.payload.stream or False)
                
                final_resp = await aresponses(input=follow_up_input, model=self.ctx.payload.model, tools=all_tools, previous_response_id=response.id, **follow_up_params)
                
                if not self.ctx.payload.stream and isinstance(final_resp, ResponsesAPIResponse):
                    mcp_output_tools, _ = await MCPHandler._process_mcp_tools_without_openai_transform(user_api_key_auth=user_auth, mcp_tools_with_litellm_proxy=mcp_tools, mcp_auth_header=mcp_auth, mcp_server_auth_headers=server_auth)
                    final_resp = MCPPayloadParser._add_mcp_output_elements_to_response(response=final_resp, mcp_tools_fetched=mcp_output_tools, tool_results=tool_results)
                return final_resp
        return response

    async def _dispatch_file_search(self) -> Optional[Any]:
        from bound.surface.action.search import aresponses_with_emulated_file_search
        p_config = self.ctx.provider.responses_api_provider_config
        if not _has_file_search_tool(self.ctx.payload.tools) or not (p_config is None or self.ctx._use_chat_completions_api or not p_config.supports_native_file_search()):
            return None

        emulated = {"custom_llm_provider": self.ctx.provider.custom_llm_provider, "timeout": self.ctx.exec.timeout, **{k: v for k, v in self.ctx.exec.raw_kwargs.items() if k not in {"call_id", "aresponses"}}}
        if self.ctx._use_chat_completions_api: emulated["use_chat_completions_api"] = True
        return await aresponses_with_emulated_file_search(input=self.ctx.payload.input, model=self.ctx.payload.model, tools=self.ctx.payload.tools, **emulated)

    async def _dispatch_final_api(self) -> Any:
        ctx = self.ctx
        if ctx.provider.logging_obj:
            ctx.provider.logging_obj.update_from_kwargs(
                kwargs=ctx.exec.raw_kwargs, model=ctx.payload.model, user=ctx.payload.user, optional_params=ctx._responses_api_request_params,
                litellm_params={**ctx._responses_api_request_params, "aresponses": True, "call_id": ctx.exec.raw_kwargs.get("call_id"), "model_info": ctx.exec.raw_kwargs.get("model_info"), "data_residency": infer_openai_data_residency(ctx.provider.custom_llm_provider, ctx.provider.litellm_params.api_base), "metadata": ctx.exec.raw_kwargs.get("litellm_metadata", ctx.exec.raw_kwargs.get("metadata"))},
                custom_llm_provider=ctx.provider.custom_llm_provider,
            )

        ctx.payload.input = APIBridge._restore_encrypted_content_item_ids_in_input(ctx.payload.input)
        if ctx.provider.custom_llm_provider is None: raise ValueError("custom_llm_provider is required but passed as None")

        response = await api_handler.async_response_api_handler(ctx=ctx)

        if isinstance(response, ResponsesAPIResponse):
            response = APIBridge._update_responses_api_response_id_with_model_id(responses_api_response=response, litellm_metadata=ctx.exec.raw_kwargs.get("litellm_metadata", {}), custom_llm_provider=ctx.provider.custom_llm_provider)
            response._hidden_params["custom_llm_provider"] = ctx.provider.custom_llm_provider
        return response


# =============================================================================
# 클라이언트 퍼블릭 인터페이스 (Facade)
# =============================================================================
@client
async def aresponses(
    input: Union[str, ResponseInputParam], model: str, include: Optional[List[ResponseIncludable]] = None,
    instructions: Optional[str] = None, max_output_tokens: Optional[int] = None, prompt: Optional[PromptObject] = None,
    metadata: Optional[Dict[str, Any]] = None, parallel_tool_calls: Optional[bool] = None, previous_response_id: Optional[str] = None,
    reasoning: Optional[Reasoning] = None, store: Optional[bool] = None, background: Optional[bool] = None, stream: Optional[bool] = None,
    temperature: Optional[float] = None, text: Optional["ResponseText"] = None, text_format: Optional[Union[Type["BaseModel"], dict]] = None,
    tool_choice: Optional[ToolChoice] = None, tools: Optional[Iterable[ToolParam]] = None, top_p: Optional[float] = None,
    truncation: Optional[Literal["auto", "disabled"]] = None, user: Optional[str] = None, service_tier: Optional[str] = None,
    safety_identifier: Optional[str] = None, extra_headers: Optional[Dict[str, Any]] = None, extra_query: Optional[Dict[str, Any]] = None,
    extra_body: Optional[Dict[str, Any]] = None, timeout: Optional[Union[float, httpx.Timeout]] = None,
    allowed_openai_params: Optional[List[str]] = None, custom_llm_provider: Optional[str] = None, **kwargs,
) -> Union[ResponsesAPIResponse, ResponseStreamIterator]:
    """
    [Core Async Entrypoint]
    LLM 추론, MCP 연결, 템플릿 맵핑을 관장하는 최상위 비동기 호출 인터페이스입니다.
    """
    kwargs["aresponses"] = True
    explicit_args = {
        "input": input, "model": model, "include": include, "instructions": instructions, "max_output_tokens": max_output_tokens,
        "prompt": prompt, "metadata": metadata, "parallel_tool_calls": parallel_tool_calls, "previous_response_id": previous_response_id,
        "reasoning": reasoning, "store": store, "background": background, "stream": stream, "temperature": temperature,
        "text": text, "text_format": text_format, "tool_choice": tool_choice, "tools": tools, "top_p": top_p, "truncation": truncation,
        "user": user, "service_tier": service_tier, "safety_identifier": safety_identifier, "extra_headers": extra_headers,
        "extra_query": extra_query, "extra_body": extra_body, "timeout": timeout, "allowed_openai_params": allowed_openai_params,
        "custom_llm_provider": custom_llm_provider
    }

    try:
        context = await ResponsesPreprocessor(explicit_args=explicit_args, kwargs=kwargs).build()
        dispatcher = AsyncResponsesDispatcher(context=context)
        return await dispatcher.execute()
    except Exception as e:
        raise config.exception_type(
            model=explicit_args.get("model", model), custom_llm_provider=explicit_args.get("custom_llm_provider", custom_llm_provider),
            original_exception=e, completion_kwargs={**explicit_args, **kwargs}, extra_kwargs=kwargs,
        )

@client
def responses(*args, **kwargs) -> Union[ResponsesAPIResponse, Any]:
    """
    [Sync Bridge]
    동기 환경(Legacy)에서 호출될 때 `aresponses`를 브릿징하여 동기로 반환합니다.
    """
    is_stream = kwargs.get("stream", False)
    result = AsyncToSyncBridge.run_coroutine(aresponses(*args, **kwargs))
    
    if is_stream and isinstance(result, ResponseStreamIterator):
        return SyncStreamAdapter(result)
    return result

@client
async def _aresponses_websocket(model: str, websocket: Any, api_base: Optional[str] = None, api_key: Optional[str] = None, timeout: Optional[float] = None, **kwargs):
    """웹소켓 핸들러 라우팅"""
    log_delegator = kwargs.get("log_delegator")
    user = kwargs.get("user", None)
    litellm_params = GenericLiteLLMParams(**kwargs)
    litellm_params_dict = get_litellm_params(**kwargs)

    model, _custom_llm_provider, dyn_key, dyn_base = get_llm_provider(model=model, api_base=api_base, api_key=api_key)
    litellm_params_dict["data_residency"] = infer_openai_data_residency(_custom_llm_provider, dyn_base or litellm_params.api_base or getattr(config, "api_base", None))

    if log_delegator:
        log_delegator.update_from_kwargs(kwargs=kwargs, model=model, user=user, optional_params={}, litellm_params=litellm_params_dict, custom_llm_provider=_custom_llm_provider)

    provider_config = ProviderConfigManager.get_provider_responses_api_config(model=model, provider=ProviderTypes(_custom_llm_provider)) if _custom_llm_provider else None

    resolved_api_base = dyn_base or litellm_params.api_base or getattr(config, "api_base", None)
    from xphi.xor.secret.manager import get_secret_str
    resolved_api_key = dyn_key or litellm_params.api_key or getattr(config, "api_key", None) or getattr(config, "openai_key", None) or get_secret_str("OPENAI_API_KEY")

    _explicit_keys = {"user_api_key_dict", "litellm_metadata", "custom_llm_provider", "model", "websocket", "log_delegator", "api_base", "api_key", "timeout"}
    remaining_kwargs = {k: v for k, v in kwargs.items() if k not in _explicit_keys}

    await ws_handler.async_responses_websocket(
        model=model, websocket=websocket, logging_obj=log_delegator, responses_api_provider_config=provider_config,
        api_base=resolved_api_base, api_key=resolved_api_key, timeout=timeout, user_api_key_dict=kwargs.get("user_api_key_dict"),
        litellm_metadata=kwargs.get("litellm_metadata", {}), custom_llm_provider=_custom_llm_provider, **remaining_kwargs,
    )