# swarm.engine.driver.io
## @lineage: agent.driver.io
## @lineage: atoa.driver.io
## @lineage: agent.atoa.driver.io
## @lineage: atoa.agent.driver.io
import copy
import warnings
from typing import TYPE_CHECKING, Any, Sequence, cast, Final
from types import SimpleNamespace

from atoa.exception.eco import (
    APIConnectionError,
    InternalServerError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout as LiteLLMTimeout,
)
from tenant.action.completion import completion as litellm_completion
from tenant.switch.params import (
    Delta, 
    ModelResponseStream, 
    StreamingChoices,
    ModelResponse,
    ChatCompletionToolParam,
)
from swarm.conver.chat.chat import select_chat_options
from eco.watcher.snapshot.metrics import MetricsSnapshot

from atoa.exception.types import LLMNoResponseError
from swarm.engine.llm.response import LLMResponse
from atoa.conv.message import Message
from watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from swarm.engine.driver.tensor import Driver
    from atoa.conv.types import TokenCallbackType
    from phi.agent.action.definition import ActionDefinition

log = get_emitter(__name__)

LLM_RETRY_EXCEPTIONS: Final[tuple[type[Exception], ...]] = (
    APIConnectionError,
    RateLimitError,
    ServiceUnavailableError,
    LiteLLMTimeout,
    InternalServerError,
    LLMNoResponseError,
)

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

        ## serialize messages
        formatted_messages = self.driver.format_messages_for_llm(messages)

        ## choose function-calling strategy
        use_native_fc = self.driver.native_tool_calling
        original_fncall_msgs = copy.deepcopy(formatted_messages)

        ## Convert Tool objects to ChatCompletionToolParam once here
        cc_tools: list[ChatCompletionToolParam] = []
        if tools:
            cc_tools = [
                t.to_openai_tool(
                    add_security_risk_prediction=add_security_risk_prediction,
                )
                for t in tools
            ]

        use_mock_tools = self.driver.should_mock_tool_calls(cc_tools)
        if use_mock_tools:
            log.debug(f"LLM.completion: mocking function-calling via prompt for model {self.driver.model}")
            formatted_messages, kwargs = self.driver.pre_request_prompt_mock(formatted_messages, cc_tools or [], kwargs)

        ## normalize provider params - Only pass tools when native FC is active
        kwargs["tools"] = cc_tools if (bool(cc_tools) and use_native_fc) else None
        has_tools_flag = bool(cc_tools) and use_native_fc
        call_kwargs = select_chat_options(self.driver, kwargs, has_tools=has_tools_flag)

        ## request context for telemetry (always include context_window for metrics)
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
            if tools and not use_native_fc:
                telemetry_ctx["raw_messages"] = original_fncall_msgs

        ## do the call with retries
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
            
            if use_mock_tools:
                resp = self.driver.post_response_prompt_mock(
                    resp, nonfncall_msgs=formatted_messages, tools=cc_tools
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