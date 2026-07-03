# bound.channel.client.action.api.aresponse
## @lineage: anchor.channel.client.action.api.aresponse
## @lineage: anchor.channel.action.api.aresponse
## @lineage: bound.channel.action.api.aresponse
import asyncio
import contextvars
from functools import partial
from typing import Any, Coroutine, Dict, Iterable, List, Literal, Optional, Type, Union, cast
import httpx
from pydantic import BaseModel

from bound.channel.config.resolver import config
from bound.adapter.mcp.handler import MCPHandler
from bound.adapter.mcp.parser.payload import MCPPayloadParser
from bound.adapter.mcp.stream import MCPStreamIterator
from bound.adapter.mcp.event.tool import create_mcp_list_tools_events

from anchor.provider.model.param.response import *
from anchor.provider.model.param.legacy import GenericLiteLLMParams
from anchor.provider.legacy.openai.types import ResponseText
from anchor.provider.legacy.openai.types import (
    AllMessageValues, PromptObject, Reasoning, ResponseIncludable, ResponseInputParam,
    ResponsesAPIResponse, ToolChoice, ToolParam,
)

from bound.router.provider.config import ProviderConfigManager
from anchor.provider.types import ProviderTypes
from bound.router.provider.locator import get_llm_provider

from bound.channel.client.action.param.litellm import get_litellm_params, infer_openai_data_residency
from bound.channel.config.response import BaseResponsesAPIConfig
from bound.channel.client.ws import ResponseWebsocketHandler
from bound.channel.client.wrapper import client
from bound.channel.client.response.identity import ResponseIdentityManager

from bound.channel.client.action.api.handler import ResponseApiHandler
from bound.channel.client.action.api.response import responses
from bound.channel.client.action.api.response_crud import delete_responses, get_responses, list_input_items, cancel_responses, compact_responses

from bound.transport.stream.iterator import ResponseStreamIterator
from xphi.xor.auth.secret.manager import get_secret_str

from phase.gov.proto.gate import uuid
from watcher.plane.emitter import get_emitter

log = get_emitter("api.aresponse")
LiteLLMLoggingObj = Any
ws_handler = ResponseWebsocketHandler()

async def _execute_sync_via_threadpool(func: Any, *args, **kwargs) -> Any:
    loop = asyncio.get_event_loop()
    ctx = contextvars.copy_context()
    func_with_context = partial(ctx.run, *args, **kwargs)
    
    init_response = await loop.run_in_executor(None, func_with_context)
    if asyncio.iscoroutine(init_response):
        return await init_response
    return init_response

@client
async def aresponses(
    input: Union[str, ResponseInputParam], model: str, **kwargs
) -> Union[ResponsesAPIResponse, ResponseStreamIterator]:
    kwargs["aresponses"] = True
    
    litellm_logging_obj = kwargs.get("litellm_logging_obj")
    prompt_id = kwargs.get("prompt_id")
    
    if isinstance(litellm_logging_obj, LiteLLMLoggingObj) and litellm_logging_obj.should_run_prompt_management_hooks(
        prompt_id=prompt_id, non_default_params=kwargs
    ):
        client_input: List[AllMessageValues] = [{"role": "user", "content": input}] if isinstance(input, str) else [
            item for item in input if isinstance(item, dict) and "role" in item # type: ignore[misc]
        ]
        model, merged_input, merged_optional_params = await litellm_logging_obj.async_get_chat_completion_prompt(
            model=model, messages=client_input, non_default_params=kwargs,
            prompt_id=prompt_id, prompt_variables=kwargs.get("prompt_variables"),
            prompt_label=kwargs.get("prompt_label"), prompt_version=kwargs.get("prompt_version"),
        )
        input = cast(Union[str, ResponseInputParam], merged_input)
        kwargs.pop("prompt_id", None)
        kwargs["_async_prompt_merged_params"] = merged_optional_params

    return await _execute_sync_via_threadpool(responses, input=input, model=model, **kwargs)

async def aresponses_api_with_mcp(
    input: Union[str, ResponseInputParam], 
    model: str, 
    tools: Optional[Iterable[ToolParam]] = None, 
    stream: Optional[bool] = None, 
    previous_response_id: Optional[str] = None, 
    **kwargs
) -> Union[ResponsesAPIResponse, ResponseStreamIterator]:
    mcp_tools_with_litellm_proxy, other_tools = MCPPayloadParser._parse_mcp_tools(tools)
    user_api_key_auth = kwargs.get("user_api_key_auth") or kwargs.get("litellm_metadata", {}).get("user_api_key_auth")

    mcp_auth_header, mcp_server_auth_headers = None, None
    if secret_fields := kwargs.get("secret_fields"):
        mcp_auth_header, mcp_server_auth_headers, _, _ = ResponseIdentityManager.extract_mcp_headers_from_request(secret_fields=secret_fields, tools=tools)

    # @adapter.delegate
    original_mcp_tools, tool_server_map = await MCPHandler._process_mcp_tools_without_openai_transform(
        user_api_key_auth=user_api_key_auth, mcp_tools_with_litellm_proxy=mcp_tools_with_litellm_proxy,
        litellm_trace_id=kwargs.get("litellm_trace_id"), mcp_auth_header=mcp_auth_header, mcp_server_auth_headers=mcp_server_auth_headers,
    )
    openai_tools = MCPHandler._transform_mcp_tools_to_openai(original_mcp_tools)
    all_tools = openai_tools + other_tools if (openai_tools or other_tools) else None
    
    call_params = {"stream": stream, "previous_response_id": previous_response_id, **kwargs}
    if stream and mcp_tools_with_litellm_proxy:
        mcp_discovery_events = await create_mcp_list_tools_events(
            mcp_tools_with_litellm_proxy=mcp_tools_with_litellm_proxy, user_api_key_auth=user_api_key_auth,
            base_item_id=f"mcp_{uuid.uuid4().hex[:8]}", pre_processed_mcp_tools=original_mcp_tools,
        )
        request_params = MCPPayloadParser._build_request_params(
            input=input, model=model, all_tools=all_tools, call_params=call_params, previous_response_id=previous_response_id, **kwargs,
        )
        return MCPStreamIterator(
            base_iterator=None,  
            mcp_events=mcp_discovery_events,  
            tool_server_map=tool_server_map,
            mcp_tools_with_litellm_proxy=mcp_tools_with_litellm_proxy,
            user_api_key_auth=user_api_key_auth,
            original_request_params=request_params,
        )

    should_auto_execute = bool(mcp_tools_with_litellm_proxy) and MCPHandler._should_auto_execute_tools(tools=mcp_tools_with_litellm_proxy)
    initial_call_params = MCPPayloadParser._prepare_initial_call_params(call_params=call_params, should_auto_execute=should_auto_execute)

    ## @execute.phase_1: 최초 LLM 추론 호출
    response = await aresponses(input=input, model=model, tools=all_tools, previous_response_id=previous_response_id, **initial_call_params)
    
    ## @execute.phase_2: 자동 실행 및 후속 추론 (오케스트레이션 루프)
    if should_auto_execute and isinstance(response, ResponsesAPIResponse) and (tool_calls := MCPPayloadParser._extract_tool_calls_from_response(response)):
        ## 도구 실행의 책임은 MCPHandler(어댑터)로 위임
        tool_results = await MCPHandler._execute_tool_calls(
            tool_server_map=tool_server_map, tool_calls=tool_calls, user_api_key_auth=user_api_key_auth,
            mcp_auth_header=mcp_auth_header, mcp_server_auth_headers=mcp_server_auth_headers,
            oauth2_headers=None, raw_headers=None, litellm_call_id=kwargs.get("litellm_call_id"), litellm_trace_id=kwargs.get("litellm_trace_id"),
        )
        
        if tool_results:
            follow_up_input = MCPPayloadParser._create_follow_up_input(response=response, tool_results=tool_results, original_input=input)
            follow_up_call_params = MCPPayloadParser._prepare_follow_up_call_params(call_params=call_params, original_stream_setting=stream or False)
            
            # 🚀 [TOPOLOGICAL ALIGNMENT] 
            # 어댑터(Handler)에게 후속 호출을 부탁하지 않고, aresponse가 직접 자기 자신을 재귀 호출합니다.
            final_response = await aresponses(
                input=follow_up_input, 
                model=model, 
                tools=all_tools, 
                previous_response_id=response.id, 
                **follow_up_call_params
            )
            
            if not stream and isinstance(final_response, ResponsesAPIResponse):
                mcp_tools_for_output, _ = await MCPHandler._process_mcp_tools_without_openai_transform(
                    user_api_key_auth=user_api_key_auth, mcp_tools_with_litellm_proxy=mcp_tools_with_litellm_proxy,
                    mcp_auth_header=mcp_auth_header, mcp_server_auth_headers=mcp_server_auth_headers,
                )
                final_response = MCPPayloadParser._add_mcp_output_elements_to_response(response=final_response, mcp_tools_fetched=mcp_tools_for_output, tool_results=tool_results)
            return final_response
    return response

@client
async def _aresponses_websocket(
    model: str,
    websocket: Any,
    api_base: Optional[str] = None,
    api_key: Optional[str] = None,
    timeout: Optional[float] = None,
    **kwargs,
):
    litellm_logging_obj: LiteLLMLoggingObj = kwargs.get("litellm_logging_obj")  # type: ignore
    user = kwargs.get("user", None)
    litellm_params = GenericLiteLLMParams(**kwargs)
    litellm_params_dict = get_litellm_params(**kwargs)

    (
        model,
        _custom_llm_provider,
        dynamic_api_key,
        dynamic_api_base,
    ) = get_llm_provider(
        model=model,
        api_base=api_base,
        api_key=api_key,
    )

    litellm_params_dict["data_residency"] = infer_openai_data_residency(
        _custom_llm_provider,
        dynamic_api_base or litellm_params.api_base or config.api_base,
    )

    litellm_logging_obj.update_from_kwargs(
        kwargs=kwargs,
        model=model,
        user=user,
        optional_params={},
        litellm_params=litellm_params_dict,
        custom_llm_provider=_custom_llm_provider,
    )

    responses_api_provider_config: Optional[BaseResponsesAPIConfig] = None
    if _custom_llm_provider is not None:
        responses_api_provider_config = (
            ProviderConfigManager.get_provider_responses_api_config(
                model=model,
                provider=ProviderTypes(_custom_llm_provider),
            )
        )

    resolved_api_base = (
        dynamic_api_base or litellm_params.api_base or getattr(config, "api_base", None)
    )
    resolved_api_key = (
        dynamic_api_key
        or litellm_params.api_key
        or getattr(config, "api_key", None)
        or getattr(config, "openai_key", None)
        or get_secret_str("OPENAI_API_KEY")
    )

    # Extract params that we're passing explicitly to avoid duplicates in **kwargs
    _explicit_keys = {
        "user_api_key_dict",
        "litellm_metadata",
        "custom_llm_provider",
        "model",
        "websocket",
        "litellm_logging_obj",
        "api_base",
        "api_key",
        "timeout",
    }
    remaining_kwargs = {k: v for k, v in kwargs.items() if k not in _explicit_keys}
    
    # helper for metadata extraction (assumed to exist in context or external utility)
    def _build_litellm_metadata_for_ws(kws):
        return kws.get("litellm_metadata", {})

    await ws_handler.async_responses_websocket(
        model=model,
        websocket=websocket,
        logging_obj=litellm_logging_obj,
        responses_api_provider_config=responses_api_provider_config,
        api_base=resolved_api_base,
        api_key=resolved_api_key,
        timeout=timeout,
        user_api_key_dict=kwargs.get("user_api_key_dict"),
        litellm_metadata=_build_litellm_metadata_for_ws(kwargs),
        custom_llm_provider=_custom_llm_provider,
        **remaining_kwargs,
    )