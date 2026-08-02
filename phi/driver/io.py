# phi.driver.io
## @lineage: agent.llm.driver.io
import warnings
import asyncio
import copy
from typing import TYPE_CHECKING, Any, Sequence, cast, Final
from types import SimpleNamespace

from mesh.bound.exception.eco import (
    APIConnectionError,
    InternalServerError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout as Timeout,
)
from runtime.client.completion import acompletion as brane_acompletion
from runtime.stream.wrapper import StreamWrapper
from runtime.client.param import Delta, ModelResponseStream, StreamingChoices, ModelResponse, ChatCompletionToolParam
from mesh.cost.tracker.metric import MetricsSnapshot
from mesh.model.info import get_features

from mesh.bound.exception.types import LLMNoResponseError
from agent.atoa.schema.llm.response import LLMResponse
from agent.atoa.conv.message import Message
from watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from phi.driver.llm.tensor import Driver
    from agent.atoa.conv.types import TokenCallbackType
    from agent.action.builder import ActionDefinition

log = get_emitter(__name__)

LLM_RETRY_EXCEPTIONS: Final[tuple[type[Exception], ...]] = (
    APIConnectionError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout,
    InternalServerError,
    LLMNoResponseError,
)

def apply_defaults_if_absent(user_kwargs: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    out = dict(user_kwargs)
    for key, value in defaults.items():
        if key not in out and value is not None:
            out[key] = value
    return out


def select_chat_options(llm: Any, user_kwargs: dict[str, Any], has_tools: bool) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "top_k": llm.top_k,
        "top_p": llm.top_p,
        "temperature": llm.temperature,
        "max_completion_tokens": llm.max_output_tokens,
    }
    out = apply_defaults_if_absent(user_kwargs, defaults)

    if llm.model.startswith("azure"):
        if "max_completion_tokens" in out:
            out["max_tokens"] = out.pop("max_completion_tokens")

    if llm.extra_headers is not None and "extra_headers" not in out:
        out["extra_headers"] = dict(llm.extra_headers)

    supports_reasoning_effort = get_features(llm.model).supports_reasoning_effort
    if supports_reasoning_effort:
        if llm.reasoning_effort is not None:
            out["reasoning_effort"] = llm.reasoning_effort

        if "gemini" not in llm.model.lower():
            out.pop("temperature", None)
            out.pop("top_p", None)

    if get_features(llm.model).supports_extended_thinking:
        if llm.extended_thinking_budget:
            budget_tokens = min(llm.extended_thinking_budget, llm.max_output_tokens - 1)
            out["thinking"] = {
                "type": "enabled",
                "budget_tokens": budget_tokens,
            }
            existing = out.get("extra_headers") or {}
            out["extra_headers"] = {
                "anthropic-beta": "interleaved-thinking-2025-05-14",
                **existing,
            }
            out["max_tokens"] = llm.max_output_tokens
        out.pop("temperature", None)
        out.pop("top_p", None)

    if not has_tools:
        out.pop("tools", None)
        out.pop("tool_choice", None)

    if get_features(llm.model).supports_prompt_cache_retention and llm.prompt_cache_retention:
        out["prompt_cache_retention"] = llm.prompt_cache_retention

    if llm.brane_extra_body:
        out["extra_body"] = llm.brane_extra_body

    # 어댑터가 None 값을 기반으로 분기할 수 있도록 원형 반환 보장
    return out


def select_responses_options(llm: Any, user_kwargs: dict[str, Any], include: list[str] | None = None, store: bool | None = None) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "top_k": llm.top_k,
        "top_p": llm.top_p,
        "temperature": llm.temperature,
        "max_completion_tokens": llm.max_output_tokens,
        "seed": llm.seed,
    }
    
    if include is not None:
        defaults["include"] = include
    if store is not None:
        defaults["store"] = store
        
    out = apply_defaults_if_absent(user_kwargs, defaults)

    if llm.model.startswith("azure"):
        if "max_completion_tokens" in out:
            out["max_tokens"] = out.pop("max_completion_tokens")

    if llm.extra_headers is not None and "extra_headers" not in out:
        out["extra_headers"] = dict(llm.extra_headers)

    if get_features(llm.model).supports_reasoning_effort:
        if llm.reasoning_effort is not None:
            out.setdefault("reasoning_effort", llm.reasoning_effort)
            
        if "gemini" not in llm.model.lower():
            out.pop("temperature", None)
            out.pop("top_p", None)

    if get_features(llm.model).supports_extended_thinking:
        if llm.extended_thinking_budget:
            budget_tokens = min(llm.extended_thinking_budget, llm.max_output_tokens - 1)
            out["thinking"] = {
                "type": "enabled",
                "budget_tokens": budget_tokens,
            }
            existing = out.get("extra_headers") or {}
            out["extra_headers"] = {
                "anthropic-beta": "interleaved-thinking-2025-05-14",
                **existing,
            }
            out["max_tokens"] = llm.max_output_tokens
            
        out.pop("temperature", None)
        out.pop("top_p", None)

    if llm.brane_extra_body:
        existing_extra_body = out.get("extra_body", {})
        out["extra_body"] = {**existing_extra_body, **llm.brane_extra_body}

    # 어댑터가 None 값을 기반으로 분기할 수 있도록 원형 반환 보장
    return out


# ============================================================================
# DriverIO Class Implementation
# ============================================================================

class DriverIO:
    def __init__(self, driver: "Driver"):
        self.driver = driver

    def _apply_prompt_caching(self, messages: list[Message]) -> None:
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

    def format_messages_for_llm(self, messages: list[Message]) -> list[dict]:
        messages = copy.deepcopy(messages)
        if self.driver.is_caching_prompt_active():
            self._apply_prompt_caching(messages)

        model_features = get_features(self.driver._model_name_for_capabilities())
        cache_enabled = self.driver.is_caching_prompt_active()
        vision_enabled = self.driver.vision_is_active()
        function_calling_enabled = self.driver.native_tool_calling
        
        force_string_serializer = (
            self.driver.force_string_serializer
            if self.driver.force_string_serializer is not None
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

    async def completion(
        self,
        messages: list[Message],
        tools: Sequence["ActionDefinition"] | None = None,
        _return_metrics: bool = False,
        add_security_risk_prediction: bool = False,
        on_token: "TokenCallbackType | None" = None,
        **kwargs,
    ) -> LLMResponse:
        enable_streaming = bool(kwargs.get("stream", False)) or self.driver.stream
        if enable_streaming and on_token is None:
            raise ValueError("Streaming requires an on_token callback")
        if enable_streaming:
            kwargs["stream"] = True

        ## 1. Serialize messages
        formatted_messages = self.format_messages_for_llm(messages)

        ## 2. Convert Tool objects to ChatCompletionToolParam
        cc_tools: list[ChatCompletionToolParam] = []
        if tools:
            cc_tools = [
                t.to_openai_tool(
                    add_security_risk_prediction=add_security_risk_prediction,
                )
                for t in tools
            ]

        ## 3. Normalize provider params (Native Tool Calling Only)
        use_native_fc = getattr(self.driver, "native_tool_calling", True)
        has_tools_flag = bool(cc_tools) and use_native_fc
        
        kwargs["tools"] = cc_tools if has_tools_flag else None
        call_kwargs = select_chat_options(self.driver, kwargs, has_tools=has_tools_flag)

        ## 4. Request context for telemetry
        assert self.driver._observer is not None, "DriverObserver is not initialized."
        telemetry_ctx: dict[str, Any] = {"context_window": self.driver.max_input_tokens or 0}
        
        if self.driver._observer.log_enabled:
            telemetry_ctx.update(
                {
                    "messages": formatted_messages[:],  # already simple dicts
                    "tools": tools,
                    "kwargs": {k: v for k, v in call_kwargs.items()},
                }
            )

        ## 5. Execute call with retries
        @self.driver.retry_decorator(
            num_retries=self.driver.num_retries,
            retry_exceptions=LLM_RETRY_EXCEPTIONS,
            retry_min_wait=self.driver.retry_min_wait,
            retry_max_wait=self.driver.retry_max_wait,
            retry_multiplier=self.driver.retry_multiplier,
            retry_listener=self.driver._retry_listener_fn,
        )
        async def _one_attempt(**retry_kwargs) -> ModelResponse:
            assert self.driver._observer is not None
            
            # [Stateless Tracking] Capture exact start time dynamically
            req_start = self.driver._observer.on_request(telemetry_ctx=telemetry_ctx)
            
            # 5-1. 파라미터 병합 및 페이로드 구성
            final_kwargs = {**call_kwargs, **retry_kwargs}
            vendor_kwargs = self.driver.get_vendor_transport_kwargs()
            merged_kwargs = {**vendor_kwargs, **final_kwargs}
            merged_kwargs.pop("base_model", None)

            if enable_streaming:
                merged_kwargs["stream"] = True

            api_key_value = self.driver._get_api_key_value()
            
            completion_payload = {
                "model": self.driver.model,
                "api_key": api_key_value,
                "api_base": self.driver.base_url,
                "api_version": self.driver.api_version,
                "timeout": self.driver.timeout,
                "drop_params": self.driver.drop_params,
                "seed": self.driver.seed,
                "messages": formatted_messages,
                **merged_kwargs,
            }
            
            # 통신에 방해가 되는 명백한 식별용 None 파라미터만 안전하게 제거
            completion_payload = {
                k: v for k, v in completion_payload.items() 
                if v is not None or k not in ["api_key", "api_base", "api_version"]
            }
            
            # 5-2. 실제 네트워크 I/O 실행
            with self.driver._brane_modify_params_ctx(self.driver.modify_params):
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=DeprecationWarning, module="httpx.*")
                    warnings.filterwarnings("ignore", message=r".*content=.*upload.*", category=DeprecationWarning)
                    warnings.filterwarnings("ignore", message=r"There is no current event loop", category=DeprecationWarning)
                    warnings.filterwarnings("ignore", category=UserWarning)
                    warnings.filterwarnings("ignore", category=DeprecationWarning, message="Accessing the 'model_fields' attribute.*")

                    resp = await brane_acompletion(**completion_payload)

                    # 5-3. 스트리밍 청크 제어 (순수 비동기)
                    if enable_streaming and on_token is not None:
                        assert isinstance(resp, StreamWrapper), "Streaming response must be handled by StreamWrapper Bridge."
                        
                        async for chunk in resp:
                            if asyncio.iscoroutinefunction(on_token):
                                await on_token(chunk)
                            else:
                                on_token(chunk)
                        
                        accumulator = resp.pipeline.attributes["accumulator"]
                        final_response = accumulator.get_complete_response()

                        # [위임] Usage 누락 방어 폴백 로직
                        if getattr(final_response.usage, "prompt_tokens", 0) == 0 and getattr(final_response.usage, "completion_tokens", 0) == 0:
                            from mesh.token.counter import calculate_fallback_usage
                            
                            content = final_response.choices[0].message.content or ""
                            fallback_usage = calculate_fallback_usage(
                                model=self.driver.model,
                                messages=formatted_messages,
                                completion_text=content,
                                custom_tokenizer=getattr(self.driver, "_tokenizer", None)
                            )
                            
                            final_response.usage.prompt_tokens = fallback_usage["prompt_tokens"]
                            final_response.usage.completion_tokens = fallback_usage["completion_tokens"]
                            final_response.usage.total_tokens = fallback_usage["total_tokens"]

                        resp = final_response

            assert isinstance(resp, ModelResponse), f"Expected ModelResponse, got {type(resp)}"
            
            # [Stateless Tracking] Delegate event tracking securely
            self.driver._observer.track_success(resp, start_time=req_start, telemetry_ctx=telemetry_ctx)

            ## Ensure at least one choice.
            if not resp.get("choices") or len(resp["choices"]) < 1:
                raise LLMNoResponseError("Response choices is less than 1. Response: " + str(resp))
            return resp

        try:
            resp = await _one_attempt()
            if isinstance(resp, dict):
                resp = ModelResponse(**resp)

            ## 안전하게 복원된 객체 프로퍼티 및 딕셔너리 가드 탐색
            choices = getattr(resp, "choices", None) or resp.get("choices", [])
            first_choice = choices[0]
            
            ## choices 역시 내부 원소가 dict 형태일 경우를 고려하여 속성 조회 순서 정렬
            raw_msg = getattr(first_choice, "message", None) or first_choice.get("message")

            ## 딕셔너리 타입 유입 시 SimpleNamespace를 활용해 속성 조회가 가능한 래퍼 객체로 안전하게 전환
            if isinstance(raw_msg, dict):
                raw_msg = SimpleNamespace(**raw_msg)

            message = Message.from_llm_chat_message(raw_msg)
            
            ## Get current metrics snapshot securely from observer
            metrics_snapshot = MetricsSnapshot(
                model_name=self.driver.metrics.model_name,
                accumulated_cost=self.driver.metrics.accumulated_cost,
                max_budget_per_task=self.driver.metrics.max_budget_per_task,
                accumulated_token_usage=self.driver.metrics.accumulated_token_usage,
            )

            ## Create and return LLMResponse
            return LLMResponse(message=message, metrics=metrics_snapshot, raw_response=resp)
        except Exception as e:
            return await self.driver._handle_error(
                e,
                lambda fb: fb.completion(
                    messages,
                    tools,
                    _return_metrics,
                    add_security_risk_prediction,
                    on_token,
                ),
            )

    async def responses(
        self,
        messages: list[Message],
        tools: Sequence["ActionDefinition"] | None = None,
        include: list[str] | None = None,
        store: bool | None = None,
        _return_metrics: bool = False,
        add_security_risk_prediction: bool = False,
        on_token: "TokenCallbackType | None" = None,
        **kwargs,
    ) -> LLMResponse:
        if include or store:
            log.debug(
                "DriverIO.responses adapter: 'include' and 'store' parameters are legacy Responses API "
                "specific and will be ignored by the completion backend."
            )
        return await self.completion(
            messages=messages,
            tools=tools,
            _return_metrics=_return_metrics,
            add_security_risk_prediction=add_security_risk_prediction,
            on_token=on_token,
            **kwargs
        )