# fiber.dev.ex.facade.driver
from __future__ import annotations

import asyncio
import copy
import time
import warnings
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Protocol
from collections.abc import Sequence

from fiber.llm.model.usage import MetricsSnapshot, TokenUsage
from fiber.llm.exception.mapping import map_provider_exception
from fiber.llm.exception.types import LLMNoResponseError
from fiber.llm.entry import acompletion
from fiber.llm.router.stream.wrapper import StreamWrapper
from fiber.llm.param import (
    ModelResponseStream, 
    ModelResponse, 
    ChatCompletionToolParam
)
from fiber.llm.model.message import Message
from fiber.llm.exception.eco import (
    APIConnectionError,
    InternalServerError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout,
)

from fiber.llm.model.profile import BaseLLMProfile
from fiber.dev.ex.facade.observer import extract_usage 
from fiber.dev.ex.facade.response import LLMResponse
from fiber.dev.ex.facade.strategy.retry import create_retry_decorator, LLM_RETRY_EXCEPTIONS

from xphi.watcher.plane.emitter import get_emitter, _flow_context

log = get_emitter("facade.driver")

TokenCallbackType = Callable[[Any], Coroutine[Any, Any, None] | None]
ConversationTokenCallbackType = TokenCallbackType

_LLM_FALLBACK_EXCEPTIONS = (
    APIConnectionError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout,
    InternalServerError,
    LLMNoResponseError,
)

class OpenAIToolConvertible(Protocol):
    def to_openai_tool(self, add_security_risk_prediction: bool = True) -> ChatCompletionToolParam:
        ...

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
    def format_messages_for_llm(cls, driver: BaseLLMProfile, messages: list[Message]) -> list[dict]:
        messages = copy.deepcopy(messages)
        if driver.is_caching_prompt_active():
            cls._apply_prompt_caching(messages)

        cache_enabled = driver.is_caching_prompt_active()
        vision_enabled = driver.vision_is_active()
        function_calling_enabled = driver.native_tool_calling
        
        return [
            message.to_chat_dict(
                cache_enabled=cache_enabled,
                vision_enabled=vision_enabled,
                function_calling_enabled=function_calling_enabled,
                force_string_serializer=getattr(driver, "force_string_serializer", False),
                send_reasoning_content=True,
            )
            for message in messages
        ]


class DriverIO:
    @classmethod
    async def request(
        cls,
        driver: BaseLLMProfile,
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
        
        trace_id = kwargs.pop("trace_id", None)
        metadata = kwargs.pop("metadata", None)
        
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
            "extra_body": driver.extra_body,
        }
        
        # 순수 LLM 파라미터만 raw_intent에 병합
        for k, v in kwargs.items():
            if v is not None:
                raw_intent[k] = v

        call_kwargs = {k: v for k, v in raw_intent.items() if v is not None}

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
            final_kwargs = {**call_kwargs, **retry_kwargs}
            vendor_config = getattr(driver, "vendor_config", None)
            vendor_kwargs = vendor_config.get_vendor_transport_kwargs() if vendor_config else {}
            
            merged_kwargs = {**vendor_kwargs, **final_kwargs}
            merged_kwargs.pop("base_model", None)

            if enable_streaming:
                merged_kwargs["stream"] = True

            api_key_value = driver.api_key.get_secret_value() if hasattr(driver.api_key, "get_secret_value") else driver.api_key
            
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
            
            if trace_id:
                completion_payload["trace_id"] = trace_id
            if metadata:
                completion_payload["metadata"] = metadata
            
            ctx_manager = getattr(driver, "_brane_modify_params_ctx", None)
            
            async def perform_request():
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=DeprecationWarning, module="httpx.*")
                    warnings.filterwarnings("ignore", message=r".*content=.*upload.*", category=DeprecationWarning)
                    warnings.filterwarnings("ignore", message=r"There is no current event loop", category=DeprecationWarning)
                    warnings.filterwarnings("ignore", category=UserWarning)
                    warnings.filterwarnings("ignore", category=DeprecationWarning, message="Accessing the 'model_fields' attribute.*")

                    resp = await acompletion(**completion_payload)

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
                            from fiber.llm.model.token.counter import calculate_fallback_usage
                            
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
                return resp

            if ctx_manager:
                with ctx_manager(driver.modify_params):
                    resp = await perform_request()
            else:
                resp = await perform_request()

            assert isinstance(resp, ModelResponse), f"Expected ModelResponse, got {type(resp)}"
            if not resp.get("choices") or len(resp["choices"]) < 1:
                raise LLMNoResponseError("Response choices is less than 1. Response: " + str(resp))
            return resp

        # Stateless Metric 파이프라인 연동
        req_start = time.time()
        is_internal = _flow_context.get().get("is_internal_call", False) if _flow_context else False
        
        try:
            resp = await _one_attempt()
            latency = time.time() - req_start
            
            # 비용 매니페스트 구성 (프로필 정보 기반)
            manifest = {
                "default": {
                    "prompt_token_cost": getattr(driver, "input_cost_per_token", 0.0) or 0.0,
                    "completion_token_cost": getattr(driver, "output_cost_per_token", 0.0) or 0.0,
                }
            }
            
            # 순수 함수로 사용량 및 비용 파싱
            usage_data = extract_usage(resp, manifest)
            
            # xphi 인터셉터가 잡을 수 있도록 Signal 방출
            log.signal(
                "LLM Request Completed",
                type="llm_metric",
                model_name=driver.model,
                latency_sec=latency,
                cost=usage_data.cost,
                usage={
                    "prompt": usage_data.prompt,
                    "completion": usage_data.completion,
                    "reasoning": usage_data.reasoning
                },
                is_internal_call=is_internal
            )
            
            # 상위 호출자가 확인할 수 있도록 영수증(Receipt) 데이터 박제
            resp._parsed_cost = usage_data.cost
            resp._parsed_usage = usage_data
            return resp
            
        except Exception as e:
            latency = time.time() - req_start
            mapped_err = map_provider_exception(e)
            
            log.signal(
                "LLM Request Failed",
                type="llm_rupture",
                model_name=driver.model,
                error=str(mapped_err),
                latency_sec=latency,
                is_internal_call=is_internal
            )
            raise mapped_err


class LLMFacade:
    @classmethod
    async def _handle_fallback(
        cls, 
        primary_driver: BaseLLMProfile, 
        fallback_models: Sequence[BaseLLMProfile],
        error: Exception, 
        original_kwargs: dict[str, Any]
    ) -> LLMResponse:
        """@desc: 네트워크/비즈니스 에러 발생 시 주입된 대안 모델(fallback_models) 리스트로 재시도를 주도함"""

        if not isinstance(error, _LLM_FALLBACK_EXCEPTIONS):
            raise error

        total = len(fallback_models)
        for i, fb_model in enumerate(fallback_models):
            log.warning(
                f"[LLMFacade] Primary LLM ({primary_driver.model}) failed with {type(error).__name__}. "
                f"Trying fallback {i + 1}/{total} ({fb_model.model})..."
            )
            
            fallback_kwargs = dict(original_kwargs)
            fallback_kwargs.pop("fallback_models", None)
            
            try:
                # Fallback 모델로 통신 수행 (성공 시 알아서 Metrics를 담은 LLMResponse가 반환됨)
                result = await cls.make_completion(llm=fb_model, **fallback_kwargs)
                log.info(f"[LLMFacade] Fallback LLM ({fb_model.model}) succeeded.")
                return result
                
            except Exception as fb_error:
                log.warning(f"[LLMFacade] Fallback {i + 1} ({fb_model.model}) failed: {fb_error}")
                continue
                
        log.error(f"[LLMFacade] All {total} fallback LLMs failed. Re-raising primary error.")
        raise error

    @classmethod
    async def make_completion(
        cls,
        llm: BaseLLMProfile,
        messages: list[Message],
        tools: Sequence[OpenAIToolConvertible] | None = None,
        on_token: ConversationTokenCallbackType | None = None,
        fallback_models: Sequence[BaseLLMProfile] | None = None, 
        **kwargs,
    ) -> LLMResponse:
        add_security_risk_prediction = kwargs.pop("add_security_risk_prediction", True)
        formatted_messages = MessageBuilder.format_messages_for_llm(llm, messages)
        cc_tools: list[ChatCompletionToolParam] = []
        
        if tools:
            cc_tools = [
                t.to_openai_tool(add_security_risk_prediction=add_security_risk_prediction)
                for t in tools
            ]

        try:
            raw_response = await DriverIO.request(
                driver=llm,
                formatted_messages=formatted_messages,
                cc_tools=cc_tools,
                on_token=on_token,
                **kwargs
            )
            
            choices = getattr(raw_response, "choices", None) or raw_response.get("choices", [])
            first_choice = choices[0]
            raw_msg = getattr(first_choice, "message", None) or first_choice.get("message")

            if isinstance(raw_msg, dict):
                raw_msg = SimpleNamespace(**raw_msg)

            message = Message.from_llm_chat_message(raw_msg)
            
            # 1회 호출에 대한 비용 영수증(Receipt) 래핑
            parsed_usage = getattr(raw_response, "_parsed_usage", None)
            if parsed_usage:
                token_usage = TokenUsage(
                    prompt_tokens=parsed_usage.prompt,
                    completion_tokens=parsed_usage.completion,
                    cache_read_tokens=parsed_usage.cache_read,
                    cache_write_tokens=parsed_usage.cache_write,
                    reasoning_tokens=parsed_usage.reasoning,
                    total_tokens=(parsed_usage.prompt + parsed_usage.completion)
                )
                metrics_snapshot = MetricsSnapshot(
                    model_name=llm.model,
                    accumulated_cost=parsed_usage.cost,
                    accumulated_token_usage=token_usage
                )
            else:
                metrics_snapshot = MetricsSnapshot(model_name=llm.model)

            return LLMResponse(message=message, metrics=metrics_snapshot, raw_response=raw_response)
            
        except Exception as e:
            if fallback_models:
                return await cls._handle_fallback(
                    primary_driver=llm,
                    fallback_models=fallback_models,
                    error=e,
                    original_kwargs={
                        "messages": messages,
                        "tools": tools,
                        "on_token": on_token,
                        "fallback_models": fallback_models,
                        "add_security_risk_prediction": add_security_risk_prediction,
                        **kwargs
                    }
                )
            raise e

    @classmethod
    async def responses(
        cls,
        driver: BaseLLMProfile,
        messages: list[Message],
        tools: Sequence[OpenAIToolConvertible] | None = None,
        include: list[str] | None = None,
        store: bool | None = None,
        _return_metrics: bool = False,
        add_security_risk_prediction: bool = True,
        on_token: TokenCallbackType | None = None,
        fallback_models: Sequence[BaseLLMProfile] | None = None,
        **kwargs,
    ) -> LLMResponse:
        if include or store:
            log.debug(
                "LLMFacade.responses adapter: 'include' and 'store' parameters are legacy Responses API "
                "specific and will be ignored by the completion backend."
            )
        
        return await cls.make_completion(
            llm=driver,
            messages=messages,
            tools=tools,
            on_token=on_token,
            fallback_models=fallback_models,
            add_security_risk_prediction=add_security_risk_prediction,
            **kwargs,
        )