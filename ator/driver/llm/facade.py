# ator.driver.llm.facade
## @lineage: driver.llm.facade
from __future__ import annotations

import asyncio
import copy
import warnings
from collections.abc import Sequence
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from eco.model.info import get_features
from eco.bound.exception.mapping import map_provider_exception
from eco.bound.exception.types import LLMNoResponseError
from eco.tracker.model.metric import MetricsSnapshot

from engine.client.entry import acompletion as brane_acompletion
from engine.stream.wrapper import StreamWrapper
from eco.client.model.param import (
    ModelResponseStream, 
    ModelResponse, 
    ChatCompletionToolParam
)

from ator.conv.schema.message import Message
from ator.conv.schema.types import TokenCallbackType, ConversationTokenCallbackType
from ator.conv.schema.event import Event, LLMConvertibleEvent

from ator.conv.protocol.llm.response import LLMResponse
from ator.agent.action.builder import ActionDefinition
from engine.xor.visual.action import View
from ator.driver.llm.model import LLMModel
from ator.driver.llm.strategy.retry import create_retry_decorator, LLM_RETRY_EXCEPTIONS

from watcher.plane.emitter import get_emitter

log = get_emitter("llm.facade")

class MessageBuilder:
    @staticmethod
    def _apply_prompt_caching(messages: list[Message]) -> None:
        if len(messages) > 0 and messages[0].role == "system":
            sys_content = messages[0].content
            if len(sys_content) >= 2:
                sys_content[0].cache_prompt = True
                sys_content[1].cache_prompt = False
            elif len(sys_content) == 1:
                sys_content[0].cache_prompt = True

        for message in reversed(messages):
            if message.role in ("user", "tool"):
                message.content[-1].cache_prompt = True
                break

    @classmethod
    def format_messages_for_llm(cls, driver: LLMModel, messages: list[Message]) -> list[dict]:
        messages = copy.deepcopy(messages)
        if driver.is_caching_prompt_active():
            cls._apply_prompt_caching(messages)

        model_features = get_features(driver._model_name_for_capabilities())
        cache_enabled = driver.is_caching_prompt_active()
        vision_enabled = driver.vision_is_active()
        function_calling_enabled = driver.native_tool_calling
        
        force_string_serializer = (
            driver.force_string_serializer
            if driver.force_string_serializer is not None
            else model_features.force_string_serializer
        )
        send_reasoning_content = model_features.send_reasoning_content
        
        return [
            message.to_chat_dict(
                cache_enabled=cache_enabled,
                vision_enabled=vision_enabled,
                function_calling_enabled=function_calling_enabled,
                force_string_serializer=force_string_serializer,
                send_reasoning_content=send_reasoning_content,
            )
            for message in messages
        ]

    @staticmethod
    def prepare_llm_messages(
        events: Sequence[Event],
        additional_messages: list[Message] | None = None,
        llm: LLMModel | None = None,
    ) -> list[Message]:
        log.debug("[message.builder] prepare_llm_messages")
        view = View.from_events(events)
        messages = LLMConvertibleEvent.events_to_messages(view.events)
        
        if additional_messages:
            messages.extend(additional_messages)

        log.debug('[message.builder] Flatting nested text blocks for LLM constraints')
        for msg in messages:
            is_dict = isinstance(msg, dict)
            role = msg.get("role") if is_dict else getattr(msg, "role", None)
            content = msg.get("content") if is_dict else getattr(msg, "content", None)
            
            if role == "assistant" and isinstance(content, list):
                if not content:
                    new_content = ""
                else:
                    text_parts = []
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            text_parts.append(block.get("text", ""))
                        elif isinstance(block, str):
                            text_parts.append(block)
                    new_content = "\n".join(text_parts) if text_parts else content

                if is_dict:
                    msg["content"] = new_content
                else:
                    msg.content = new_content

        return messages


class DriverIO:
    """@desc: 순수 LLM 네트워크 통신, 리트라이 로직, 스트리밍 청크 처리를 전담하는 모듈"""
    
    @classmethod
    async def request(
        cls,
        driver: LLMModel,
        formatted_messages: list[dict],
        cc_tools: list[ChatCompletionToolParam] | None = None,
        on_token: TokenCallbackType | None = None,
        **kwargs,
    ) -> ModelResponse:
        
        enable_streaming = kwargs.get("stream", driver.stream)
        if enable_streaming and on_token is None:
            raise ValueError("Streaming requires an on_token callback")

        has_tools_flag = bool(cc_tools) and driver.native_tool_calling
        kwargs["tools"] = cc_tools if has_tools_flag else None
        
        raw_intent = {
            "top_k": driver.top_k,
            "top_p": driver.top_p,
            "temperature": driver.temperature,
            "max_completion_tokens": driver.max_output_tokens,
            "max_output_tokens": driver.max_output_tokens,
            "seed": driver.seed,
            "reasoning_effort": driver.reasoning_effort,
            "extended_thinking_budget": driver.extended_thinking_budget,
            "prompt_cache_retention": driver.prompt_cache_retention,
            "extra_headers": driver.extra_headers,
            "extra_body": driver.brane_extra_body,
        }
        
        for k, v in kwargs.items():
            if v is not None:
                raw_intent[k] = v

        call_kwargs = {k: v for k, v in raw_intent.items() if v is not None}
        assert driver._observer is not None, "DriverObserver is not initialized."
        telemetry_ctx: dict[str, Any] = {"context_window": driver.max_input_tokens or 0}
        
        if driver._observer.log_enabled:
            telemetry_ctx.update({
                "messages": formatted_messages[:], 
                "tools": cc_tools,
                "kwargs": {k: v for k, v in call_kwargs.items()},
            })

        retry_wrapper = create_retry_decorator(
            num_retries=driver.num_retries,
            retry_exceptions=LLM_RETRY_EXCEPTIONS,
            retry_min_wait=driver.retry_min_wait,
            retry_max_wait=driver.retry_max_wait,
            retry_multiplier=driver.retry_multiplier,
            retry_listener=driver.retry_listener,
        )

        @retry_wrapper
        async def _one_attempt(**retry_kwargs) -> ModelResponse:
            req_start = driver._observer.on_request(telemetry_ctx=telemetry_ctx)
            
            final_kwargs = {**call_kwargs, **retry_kwargs}
            vendor_kwargs = driver.vendor_config.get_vendor_transport_kwargs()
            merged_kwargs = {**vendor_kwargs, **final_kwargs}
            merged_kwargs.pop("base_model", None)

            if enable_streaming:
                merged_kwargs["stream"] = True

            api_key_value = driver._get_api_key_value()
            
            completion_payload = {
                "model": driver.model,
                "api_key": api_key_value,
                "api_base": driver.base_url,
                "api_version": driver.api_version,
                "timeout": driver.timeout,
                "drop_params": driver.drop_params,
                "seed": driver.seed,
                "messages": formatted_messages,
                **merged_kwargs,
            }
            
            completion_payload = {
                k: v for k, v in completion_payload.items() 
                if v is not None or k not in ["api_key", "api_base", "api_version"]
            }
            
            with driver._brane_modify_params_ctx(driver.modify_params):
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=DeprecationWarning, module="httpx.*")
                    warnings.filterwarnings("ignore", message=r".*content=.*upload.*", category=DeprecationWarning)
                    warnings.filterwarnings("ignore", message=r"There is no current event loop", category=DeprecationWarning)
                    warnings.filterwarnings("ignore", category=UserWarning)
                    warnings.filterwarnings("ignore", category=DeprecationWarning, message="Accessing the 'model_fields' attribute.*")

                    resp = await brane_acompletion(**completion_payload)

                    if enable_streaming and on_token is not None:
                        assert isinstance(resp, StreamWrapper), "Streaming response must be handled by StreamWrapper Bridge."
                        
                        async for chunk in resp:
                            if asyncio.iscoroutinefunction(on_token):
                                await on_token(chunk)
                            else:
                                on_token(chunk)
                        
                        accumulator = resp.pipeline.attributes["accumulator"]
                        final_response = accumulator.get_complete_response()

                        if getattr(final_response.usage, "prompt_tokens", 0) == 0 and getattr(final_response.usage, "completion_tokens", 0) == 0:
                            from engine.parser.token.evaluator import calculate_fallback_usage
                            
                            content = final_response.choices[0].message.content or ""
                            fallback_usage = calculate_fallback_usage(
                                model=driver.model,
                                messages=formatted_messages,
                                completion_text=content,
                                custom_tokenizer=getattr(driver, "_tokenizer", None)
                            )
                            
                            final_response.usage.prompt_tokens = fallback_usage["prompt_tokens"]
                            final_response.usage.completion_tokens = fallback_usage["completion_tokens"]
                            final_response.usage.total_tokens = fallback_usage["total_tokens"]

                        resp = final_response

            assert isinstance(resp, ModelResponse), f"Expected ModelResponse, got {type(resp)}"
            driver._observer.track_success(resp, start_time=req_start, telemetry_ctx=telemetry_ctx)

            if not resp.get("choices") or len(resp["choices"]) < 1:
                raise LLMNoResponseError("Response choices is less than 1. Response: " + str(resp))
            return resp

        # DriverIO는 통신 중 발생한 예외를 매핑만 하고 그대로 상위(Facade)로 던짐
        try:
            resp = await _one_attempt()
            if isinstance(resp, dict):
                resp = ModelResponse(**resp)
            return resp
        except Exception as e:
            mapped = map_provider_exception(e)
            if mapped is not e:
                raise mapped from e
            raise


class LLMFacade:
    """@desc: LLM 파이프라인(메시지 조립 -> 통신 요청 -> 예외 복구 및 응답 래핑)을 조율하는 단일 진입점"""
    
    @classmethod
    async def _handle_fallback(
        cls, 
        driver: LLMModel, 
        error: Exception, 
        original_kwargs: dict[str, Any]
    ) -> LLMResponse:
        """@desc: 네트워크/비즈니스 에러 발생 시 설정된 전략에 따라 대안 모델로 재시도를 주도함"""
        if driver._observer is not None:
            driver._observer.track_rupture(error)
        
        if getattr(driver, "fallback_strategy", None) and driver.fallback_strategy.should_fallback(error):
            log.warning(f"Fallback triggered for {driver.model} due to error: {error}")
            result = driver.fallback_strategy.try_fallback(
                primary_model=driver.model,
                primary_error=error,
                primary_metrics=driver.metrics,
                call_fn=lambda fb_model: cls.make_completion(llm=fb_model, **original_kwargs),
            )
            
            # try_fallback이 awaitable 객체를 반환할 수 있으므로 보장
            if asyncio.iscoroutine(result):
                result = await result
                
            if result is not None:
                return result
                
        raise error

    @classmethod
    async def make_completion(
        cls,
        llm: LLMModel,
        messages: list[Message],
        tools: list[ActionDefinition] | None = None,
        on_token: ConversationTokenCallbackType | None = None,
        **kwargs,
    ) -> LLMResponse:
        """
        @desc: 클라이언트(Agent/Action)에게 노출되는 명시적 비동기 파이프라인.
        """
        add_security_risk_prediction = kwargs.pop("add_security_risk_prediction", True)
        
        # 1. Message 조립 위임 (Facade -> MessageBuilder)
        formatted_messages = MessageBuilder.format_messages_for_llm(llm, messages)
        
        cc_tools: list[ChatCompletionToolParam] = []
        if tools:
            cc_tools = [
                t.to_openai_tool(add_security_risk_prediction=add_security_risk_prediction)
                for t in tools
            ]

        try:
            # 2. 순수 네트워크 통신 위임 (Facade -> DriverIO)
            raw_response = await DriverIO.request(
                driver=llm,
                formatted_messages=formatted_messages,
                cc_tools=cc_tools,
                on_token=on_token,
                **kwargs
            )
            
            # 3. 애플리케이션 레벨의 LLMResponse 및 Metrics 래핑
            choices = getattr(raw_response, "choices", None) or raw_response.get("choices", [])
            first_choice = choices[0]
            raw_msg = getattr(first_choice, "message", None) or first_choice.get("message")

            if isinstance(raw_msg, dict):
                raw_msg = SimpleNamespace(**raw_msg)

            message = Message.from_llm_chat_message(raw_msg)
            
            metrics_snapshot = MetricsSnapshot(
                model_name=llm.metrics.model_name,
                accumulated_cost=llm.metrics.accumulated_cost,
                max_budget_per_task=llm.metrics.max_budget_per_task,
                accumulated_token_usage=llm.metrics.accumulated_token_usage,
            )

            return LLMResponse(message=message, metrics=metrics_snapshot, raw_response=raw_response)
            
        except Exception as e:
            # 4. 예외 발생 시 Fallback 복구 흐름 조율
            return await cls._handle_fallback(
                driver=llm,
                error=e,
                original_kwargs={
                    "messages": messages,
                    "tools": tools,
                    "on_token": on_token,
                    "add_security_risk_prediction": add_security_risk_prediction,
                    **kwargs
                }
            )

    @classmethod
    async def responses(
        cls,
        driver: LLMModel,
        messages: list[Message],
        tools: Sequence[ActionDefinition] | None = None,
        include: list[str] | None = None,
        store: bool | None = None,
        _return_metrics: bool = False,
        add_security_risk_prediction: bool = True,
        on_token: TokenCallbackType | None = None,
        **kwargs,
    ) -> LLMResponse:
        """@desc: Legacy Responses API 지원을 위한 브릿지 메서드"""
        if include or store:
            log.debug(
                "LLMFacade.responses adapter: 'include' and 'store' parameters are legacy Responses API "
                "specific and will be ignored by the completion backend."
            )
        
        return await cls.make_completion(
            llm=driver,
            messages=messages,
            tools=list(tools) if tools else None,
            on_token=on_token,
            add_security_risk_prediction=add_security_risk_prediction,
            **kwargs,
        )