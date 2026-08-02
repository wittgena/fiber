# phi.driver.io
from __future__ import annotations

import asyncio
import copy
import warnings
from collections.abc import Sequence
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from mesh.model.info import get_features
from mesh.bound.param.optional import DriverParamNormalizer
from mesh.bound.exception.mapping import map_provider_exception
from mesh.bound.exception.types import LLMNoResponseError
from mesh.cost.tracker.metric import MetricsSnapshot

from runtime.client.completion import acompletion as brane_acompletion
from runtime.stream.wrapper import StreamWrapper
from runtime.client.param import (
    ModelResponseStream, 
    ModelResponse, 
    ChatCompletionToolParam
)

from agent.atoa.schema.llm.response import LLMResponse
from agent.atoa.conv.message import Message
from agent.atoa.conv.types import TokenCallbackType
from agent.action.builder import ActionDefinition

from phi.driver.llm.model import LLMModel
from phi.driver.strategy.retry import create_retry_decorator, LLM_RETRY_EXCEPTIONS

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class DriverIO:
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

    @classmethod
    async def _handle_fallback(
        cls, 
        driver: LLMModel, 
        error: Exception, 
        original_kwargs: dict[str, Any]
    ) -> LLMResponse:
        """@desc: 에러 발생 시 fallback_strategy가 있다면 실행하는 책임 (재귀 호출 방식을 통해 IO 내에서 처리)"""
        if driver._observer is not None:
            driver._observer.track_rupture(error)
        
        if getattr(driver, "fallback_strategy", None) and driver.fallback_strategy.should_fallback(error):
            result = driver.fallback_strategy.try_fallback(
                primary_model=driver.model,
                primary_error=error,
                primary_metrics=driver.metrics,
                # Fallback 모델(fb_model)을 주입하여 현재 메서드(completion)를 다시 호출
                call_fn=lambda fb_model: cls.completion(driver=fb_model, **original_kwargs),
            )
            if result is not None:
                return result
                
        mapped = map_provider_exception(error)
        if mapped is not error:
            raise mapped from error
        raise

    @classmethod
    async def completion(
        cls,
        driver: LLMModel,
        messages: list[Message],
        tools: Sequence[ActionDefinition] | None = None,
        _return_metrics: bool = False,
        add_security_risk_prediction: bool = False,
        on_token: TokenCallbackType | None = None,
        **kwargs,
    ) -> LLMResponse:
        
        enable_streaming = kwargs.get("stream", driver.stream)
        if enable_streaming and on_token is None:
            raise ValueError("Streaming requires an on_token callback")

        ## 1. Serialize messages
        formatted_messages = cls.format_messages_for_llm(driver, messages)

        ## 2. Convert Tool objects to ChatCompletionToolParam
        cc_tools: list[ChatCompletionToolParam] = []
        if tools:
            cc_tools = [
                t.to_openai_tool(add_security_risk_prediction=add_security_risk_prediction)
                for t in tools
            ]

        ## 3. Normalize provider params
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

        call_kwargs = DriverParamNormalizer.normalize(
            model=driver.model,
            raw_params=raw_intent,
            has_tools=has_tools_flag
        )

        ## 4. Request context for telemetry
        assert driver._observer is not None, "DriverObserver is not initialized."
        telemetry_ctx: dict[str, Any] = {"context_window": driver.max_input_tokens or 0}
        
        if driver._observer.log_enabled:
            telemetry_ctx.update(
                {
                    "messages": formatted_messages[:], 
                    "tools": tools,
                    "kwargs": {k: v for k, v in call_kwargs.items()},
                }
            )

        ## 5. 동적 Retry 데코레이터 생성 (상태 객체의 속성 활용)
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
            
            # [핵심] 합성된 VendorConfig 객체에서 네트워크 인증 정보를 추출
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
                            from mesh.token.counter import calculate_fallback_usage
                            
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

        try:
            resp = await _one_attempt()
            if isinstance(resp, dict):
                resp = ModelResponse(**resp)

            choices = getattr(resp, "choices", None) or resp.get("choices", [])
            first_choice = choices[0]
            raw_msg = getattr(first_choice, "message", None) or first_choice.get("message")

            if isinstance(raw_msg, dict):
                raw_msg = SimpleNamespace(**raw_msg)

            message = Message.from_llm_chat_message(raw_msg)
            
            metrics_snapshot = MetricsSnapshot(
                model_name=driver.metrics.model_name,
                accumulated_cost=driver.metrics.accumulated_cost,
                max_budget_per_task=driver.metrics.max_budget_per_task,
                accumulated_token_usage=driver.metrics.accumulated_token_usage,
            )

            return LLMResponse(message=message, metrics=metrics_snapshot, raw_response=resp)
        except Exception as e:
            # 6. 폴백 핸들링 (에러 발생 시 Fallback 처리 책임을 내부로 통합)
            return await cls._handle_fallback(
                driver=driver,
                error=e,
                original_kwargs={
                    "messages": messages,
                    "tools": tools,
                    "_return_metrics": _return_metrics,
                    "add_security_risk_prediction": add_security_risk_prediction,
                    "on_token": on_token,
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
        add_security_risk_prediction: bool = False,
        on_token: TokenCallbackType | None = None,
        **kwargs,
    ) -> LLMResponse:
        if include or store:
            log.debug(
                "DriverIO.responses adapter: 'include' and 'store' parameters are legacy Responses API "
                "specific and will be ignored by the completion backend."
            )
        return await cls.completion(
            driver=driver,
            messages=messages,
            tools=tools,
            _return_metrics=_return_metrics,
            add_security_risk_prediction=add_security_risk_prediction,
            on_token=on_token,
            **kwargs,
        )