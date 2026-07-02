# xphi.scope.plane.telemetry.llm
import time
import traceback
import warnings
from dataclasses import dataclass
from typing import Any, ClassVar
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from anchor.provider.cost.calculator import completion_cost
from anchor.provider.legacy.types import CostPerToken, Usage
from bound.channel.compat.switch.params import ResponseAPIUsage, ResponsesAPIResponse, ModelResponse

from xphi.scope.plane.metrics import Metrics

from phase.gov.proto.gate import uuid4
from arch.proto.event.next import LogEvent
from watcher.plane.emitter import get_emitter, _flow_context

emitter = get_emitter(__name__, phase="llm_generation", boundary="llm_scope")

@dataclass
class ParsedUsage:
    prompt: int = 0
    completion: int = 0
    cache_read: int = 0
    cache_write: int = 0
    reasoning: int = 0
    is_meaningful: bool = False

class Telemetry(BaseModel):
    """
    Handles latency, token/cost accounting, and event emission.
    All legacy file I/O has been delegated to the central Emitter/Interceptor plane.
    """
    model_name: str = Field(default="unknown", description="Name of the LLM model")
    input_cost_per_token: float | None = Field(default=None, ge=0, description="Custom Input cost per token (USD)")
    output_cost_per_token: float | None = Field(default=None, ge=0, description="Custom Output cost per token (USD)")
    metrics: Metrics = Field(..., description="Metrics collector instance")

    log_enabled: bool = Field(default=False, exclude=True, description="Legacy compatibility field")

    ## Runtime fields (not serialized)
    _req_start: float = PrivateAttr(default=0.0)
    _req_ctx: dict[str, Any] = PrivateAttr(default_factory=dict)
    _last_latency: float = PrivateAttr(default=0.0)

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    def on_request(self, telemetry_ctx: dict | None = None) -> None:
        self._req_start = time.time()
        self._req_ctx = telemetry_ctx or {}

    def on_response(
        self,
        resp: ModelResponse | ResponsesAPIResponse,
        raw_resp: ModelResponse | None = None,
    ) -> Metrics:
        """기록, 비용 계산 후 Emitter에 정규화된 측정 이벤트를 발행합니다."""
        ctx = _flow_context.get()
        if ctx.get("is_internal_call"):
            emitter.debug("Skipping telemetry for internal sub-call (is_internal_call=True).")
            return self.metrics.deep_copy()

        self._last_latency = time.time() - (self._req_start or time.time())
        response_id = getattr(resp, "id", uuid4().hex)
        
        self.metrics.add_response_latency(self._last_latency, response_id)

        cost = self._compute_cost(resp)
        if cost:
            self.metrics.add_cost(cost)

        usage = getattr(resp, "usage", None)
        parsed_usage = self._parse_usage(usage)

        if parsed_usage.is_meaningful:
            self.metrics.add_token_usage(
                prompt_tokens=parsed_usage.prompt,
                completion_tokens=parsed_usage.completion,
                cache_read_tokens=parsed_usage.cache_read,
                cache_write_tokens=parsed_usage.cache_write,
                reasoning_tokens=parsed_usage.reasoning,
                context_window=self._req_ctx.get("context_window", 0),
                response_id=response_id,
                )

        ## 중앙 관측망(Observability)으로 정규화된 Payload 발송
        payload = {
            "response_id": response_id,
            "cost": cost or 0.0,
            "latency_ms": self._last_latency * 1000,
            "model_name": self.model_name,
            "usage_metrics": {
                "prompt_tokens": parsed_usage.prompt,
                "completion_tokens": parsed_usage.completion,
                "cache_read_tokens": parsed_usage.cache_read,
                "reasoning_tokens": parsed_usage.reasoning
            },
            "context": self._req_ctx
        }
        emitter.signal("LLM_COMPLETION_TRACKED", payload=payload)
        return self.metrics.deep_copy()

    def on_error(self, _err: BaseException) -> None:
        """에러 발생 시 파일 저장 대신 중앙 Emitter로 실패 이벤트를 전송"""
        ctx = _flow_context.get()
        if ctx.get("is_internal_call"):
            emitter.debug("Internal sub-call failed. Skipping telemetry error log.")
            return

        self._last_latency = time.time() - (self._req_start or time.time())
        
        error_payload = {
            "model_name": self.model_name,
            "latency_sec": self._last_latency,
            "error_type": type(_err).__name__,
            "message": str(_err),
            "traceback": "".join(traceback.format_exception(type(_err), _err, _err.__traceback__)),
            "context": self._req_ctx
        }
        
        try:
            ## 시그널 파이프라인 전송
            emitter.signal("LLM_COMPLETION_FAILED", payload=error_payload)
            
            ## SurfaceEmitter 사양 조율
            if hasattr(emitter, "emit"):
                emitter.emit(LogEvent(
                    level="ERROR",
                    message=f"LLM Generation Failed: {str(_err)}",
                    source_id=f"telemetry::{self.model_name}",
                    context=error_payload
                ))
            elif hasattr(emitter, "error"):
                ## SurfaceEmitter가 일반적인 logger 래퍼 계층이라면 표준 error 메서드로 가로채기
                emitter.error(f"LLM Generation Failed: {str(_err)} | Context: {error_payload}")
            else:
                ## 최후의 보루 파싱 서브 레벨 처리
                print(f"[Telemetry Error Catch] {error_payload['traceback']}")
        except Exception as tel_err:
            warnings.warn(f"Critical: Telemetry observability tracking crashed itself: {tel_err}", RuntimeWarning)

    def _parse_usage(self, usage: Usage | ResponseAPIUsage | Any) -> ParsedUsage:
        if usage is None:
            return ParsedUsage()

        try:
            prompt = int(getattr(usage, "prompt_tokens", None) or getattr(usage, "input_tokens", 0) or 0)
            completion = int(getattr(usage, "completion_tokens", None) or getattr(usage, "output_tokens", 0) or 0)
            
            p_details = getattr(usage, "prompt_tokens_details", None) or getattr(usage, "input_tokens_details", None)
            cache_read = int(getattr(p_details, "cached_tokens", 0) if p_details else getattr(usage, "cached_tokens", 0) or 0)
            
            c_details = getattr(usage, "completion_tokens_details", None) or getattr(usage, "output_tokens_details", None)
            reasoning = int(getattr(c_details, "reasoning_tokens", 0) if c_details else 0)
            
            cache_write = int(getattr(usage, "_cache_creation_input_tokens", 0) or 0)

            is_meaningful = prompt > 0 or completion > 0
            return ParsedUsage(
                prompt=prompt,
                completion=completion,
                cache_read=cache_read,
                cache_write=cache_write,
                reasoning=reasoning,
                is_meaningful=is_meaningful
            )
        except Exception as e:
            emitter.debug(f"Failed to parse usage stats: {e}")
            return ParsedUsage()

    def _compute_cost(self, resp: ModelResponse | ResponsesAPIResponse) -> float | None:
        extra_kwargs = {}
        if self.input_cost_per_token is not None and self.output_cost_per_token is not None:
            extra_kwargs["custom_cost_per_token"] = CostPerToken(
                input_cost_per_token=self.input_cost_per_token,
                output_cost_per_token=self.output_cost_per_token,
            )

        try:
            hidden = getattr(resp, "_hidden_params", {}) or {}
            cost = hidden.get("additional_headers", {}).get("llm_provider-x-litellm-response-cost")
            if cost is not None:
                return float(cost)
        except Exception:
            pass

        if "/" in self.model_name:
            provider, bare = self.model_name.split("/", 1)
            extra_kwargs["model"] = bare
            extra_kwargs["custom_llm_provider"] = provider
        else:
            extra_kwargs["model"] = self.model_name
            
        try:
            return float(completion_cost(completion_response=resp, **extra_kwargs))
        except Exception as e:
            emitter.debug(f"Cost calculation failed: {e}")
            return None