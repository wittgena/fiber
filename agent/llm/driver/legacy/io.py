# agent.llm.driver.io
import warnings
from typing import TYPE_CHECKING, Any, Sequence, cast, Final
from types import SimpleNamespace

from mesh.bound.exception.eco import (
    APIConnectionError,
    InternalServerError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout as Timeout,
)
from runtime.client.completion import completion as litellm_completion
from runtime.client.param import Delta, ModelResponseStream, StreamingChoices, ModelResponse, ChatCompletionToolParam
from mesh.cost.tracker.metric import MetricsSnapshot
from mesh.model.info import get_features

from mesh.bound.exception.types import LLMNoResponseError
from agent.atoa.schema.llm.response import LLMResponse
from agent.atoa.conv.message import Message
from watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from agent.llm.driver.tensor import Driver
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

# ============================================================================
# Chat Options Management (Absorbed from agent.llm.option.chat)
# ============================================================================

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

    # Azure -> uses max_tokens instead
    if llm.model.startswith("azure"):
        if "max_completion_tokens" in out:
            out["max_tokens"] = out.pop("max_completion_tokens")

    # If user didn't set extra_headers, propagate from llm config
    if llm.extra_headers is not None and "extra_headers" not in out:
        out["extra_headers"] = dict(llm.extra_headers)

    # Reasoning-model quirks
    supports_reasoning_effort = get_features(llm.model).supports_reasoning_effort
    if supports_reasoning_effort:
        if llm.reasoning_effort is not None:
            out["reasoning_effort"] = llm.reasoning_effort

        # All reasoning models ignore temp/top_p, except Gemini
        if "gemini" not in llm.model.lower():
            out.pop("temperature", None)
            out.pop("top_p", None)

    # Extended thinking models
    if get_features(llm.model).supports_extended_thinking:
        if llm.extended_thinking_budget:
            budget_tokens = min(llm.extended_thinking_budget, llm.max_output_tokens - 1)
            out["thinking"] = {
                "type": "enabled",
                "budget_tokens": budget_tokens,
            }
            # Enable interleaved thinking
            # Merge default header with any user-provided headers; user wins on conflict
            existing = out.get("extra_headers") or {}
            out["extra_headers"] = {
                "anthropic-beta": "interleaved-thinking-2025-05-14",
                **existing,
            }
            out["max_tokens"] = llm.max_output_tokens
        # Anthropic models ignore temp/top_p
        out.pop("temperature", None)
        out.pop("top_p", None)

    # Tools: if not using native, strip tool_choice so we don't confuse providers
    if not has_tools:
        out.pop("tools", None)
        out.pop("tool_choice", None)

    # Send prompt_cache_retention only if model supports it
    if get_features(llm.model).supports_prompt_cache_retention and llm.prompt_cache_retention:
        out["prompt_cache_retention"] = llm.prompt_cache_retention

    # Pass through user-provided extra_body unchanged
    if llm.brane_extra_body:
        out["extra_body"] = llm.brane_extra_body

    return out

def select_responses_options(llm: Any, user_kwargs: dict[str, Any], include: list[str] | None = None, store: bool | None = None) -> dict[str, Any]:
    """
    @desc: Responses API 호출을 위해 Driver 설정과 런타임 kwargs를 병합/정규화합니다.
    """
    defaults: dict[str, Any] = {
        "top_k": llm.top_k,
        "top_p": llm.top_p,
        "temperature": llm.temperature,
        "max_completion_tokens": llm.max_output_tokens,
        "seed": llm.seed,
    }
    
    # Responses API 전용 파라미터 우선 적용
    if include is not None:
        defaults["include"] = include
    if store is not None:
        defaults["store"] = store
        
    out = apply_defaults_if_absent(user_kwargs, defaults)

    # Azure -> uses max_tokens instead
    if llm.model.startswith("azure"):
        if "max_completion_tokens" in out:
            out["max_tokens"] = out.pop("max_completion_tokens")

    # If user didn't set extra_headers, propagate from llm config
    if llm.extra_headers is not None and "extra_headers" not in out:
        out["extra_headers"] = dict(llm.extra_headers)

    # Reasoning-model quirks
    if get_features(llm.model).supports_reasoning_effort:
        if llm.reasoning_effort is not None:
            out.setdefault("reasoning_effort", llm.reasoning_effort)
            
        if "gemini" not in llm.model.lower():
            out.pop("temperature", None)
            out.pop("top_p", None)

    # Extended thinking models
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

    # 안전한 extra_body 병합 (런타임 extra_body와 Driver extra_body 충돌 방지)
    if llm.brane_extra_body:
        existing_extra_body = out.get("extra_body", {})
        out["extra_body"] = {**existing_extra_body, **llm.brane_extra_body}

    # 최종적으로 값이 None인 파라미터는 API Validation 에러를 유발하므로 제거
    return {k: v for k, v in out.items() if v is not None}


# ============================================================================
# DriverIO Class Implementation
# ============================================================================

class DriverIO:
    def __init__(self, driver: "Driver"):
        self.driver = driver

    def completion(
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
        formatted_messages = self.driver.format_messages_for_llm(messages)

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
        def _one_attempt(**retry_kwargs) -> ModelResponse:
            assert self.driver._observer is not None
            
            # [Stateless Tracking] Capture exact start time dynamically
            req_start = self.driver._observer.on_request(telemetry_ctx=telemetry_ctx)
            
            final_kwargs = {**call_kwargs, **retry_kwargs}
            resp = self.driver._transport_call(
                messages=formatted_messages,
                **final_kwargs,
                enable_streaming=enable_streaming,
                on_token=on_token,
            )
            
            # [Stateless Tracking] Delegate event tracking securely
            self.driver._observer.track_success(resp, start_time=req_start, telemetry_ctx=telemetry_ctx)

            ## Ensure at least one choice.
            if not resp.get("choices") or len(resp["choices"]) < 1:
                raise LLMNoResponseError("Response choices is less than 1. Response: " + str(resp))
            return resp

        try:
            resp = _one_attempt()
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
            return self.driver._handle_error(
                e,
                lambda fb: fb.completion(
                    messages,
                    tools,
                    _return_metrics,
                    add_security_risk_prediction,
                    on_token,
                ),
            )

    def responses(
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
        return self.completion(
            messages=messages,
            tools=tools,
            _return_metrics=_return_metrics,
            add_security_risk_prediction=add_security_risk_prediction,
            on_token=on_token,
            **kwargs
        )