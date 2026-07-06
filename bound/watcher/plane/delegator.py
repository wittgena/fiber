# bound.watcher.plane.delegator
import time
import traceback
import warnings
import inspect
from typing import Any, Dict, List, Optional, Tuple, Coroutine, Any, ClassVar
from dataclasses import dataclass
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from opentelemetry import trace

from anchor.provider.cost.calculator import completion_cost
from anchor.provider.legacy.types import CostPerToken, Usage
from anchor.surface.switch.params import ResponseAPIUsage, ResponsesAPIResponse, ModelResponse
from bound.watcher.plane.metrics import Metrics

from arch.proto.event.next import LogEvent
from phase.gov.proto.gate import uuid4
from watcher.plane.emitter import get_emitter, _flow_context

## PEP 562: 모듈 레벨 속성 접근자
_LEGACY_GLOBALS = {
    "sentry_sdk_instance", "capture_exception", "add_breadcrumb", "slack_app",
    "alerts_channel", "heliconeLogger", "athinaLogger", "promptLayerLogger",
    "logfireLogger", "weightsBiasesLogger", "customLogger", "langFuseLogger",
    "openMeterLogger", "lagoLogger", "dataDogLogger", "prometheusLogger",
    "dynamoLogger", "s3Logger", "greenscaleLogger", "lunaryLogger",
    "supabaseClient", "deepevalLogger", "user_logger_fn", "last_fetched_at",
    "last_fetched_at_keys"
}

def __getattr__(name: str) -> Any:
    if name in _LEGACY_GLOBALS:
        return None
    if name == "callback_list":
        return []
    if name in ("additional_details", "local_cache"):
        return {}
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

class LoggingBase:
    def pre_call(self, input, api_key, model=None, additional_args={}):
        pass

    def post_call(self, original_response, input=None, api_key=None, additional_args={}):
        pass

class Logging(LoggingBase):
    """기존 호출 구조를 유지하면서 내부적으로는 Telemetry 시스템으로 이벤트를 위임"""
    stream: bool = False
    litellm_trace_id: str
    model_call_details: dict = {}
    standard_built_in_tools_params: Any = None
    cost_breakdown: dict = {}
    callback_duration_ms: float = 0.0

    def __init__(self, *args, **kwargs):
        try:
            super().__init__(*args, **kwargs)
        except Exception:
            pass
            
        self.model_call_details = kwargs.get("kwargs", {})
        
        ## OTel 현재 Span에서 trace_id 추출하여 매핑
        span = trace.get_current_span()
        if span.is_recording():
            self.litellm_trace_id = format(span.get_span_context().trace_id, "032x")
        else:
            self.litellm_trace_id = "unknown-trace-id"

        ## 상위에서 주입한 telemetry나 metrics가 있다면 우선 사용
        injected_telemetry = kwargs.get("telemetry")
        injected_metrics = kwargs.get("metrics") or Metrics()
        
        self._telemetry = injected_telemetry or Telemetry(
            model_name=kwargs.get("model", "unknown"), 
            metrics=injected_metrics
        )
        self._telemetry.on_request(telemetry_ctx=kwargs)

    def _handle_response(self, result: Any = None) -> None:
        if result:
            self._telemetry.on_response(result)

    def _handle_error(self, exception: Exception) -> None:
        self._telemetry.on_error(exception)

    def _safe_super_call(self, method_name: str, *args, **kwargs) -> Any:
        if hasattr(super(), method_name):
            method = getattr(super(), method_name)
            return method(*args, **kwargs)
        return None

    ## 생명주기 훅 (Telemetry 위임)
    def success_handler(self, result=None, *args, **kwargs):
        self._handle_response(result)
        return self._safe_super_call('success_handler', result, *args, **kwargs)

    async def async_success_handler(self, result=None, *args, **kwargs):
        self._handle_response(result)
        super_result = self._safe_super_call('async_success_handler', result, *args, **kwargs)
        if inspect.iscoroutine(super_result):
            return await super_result
        return super_result

    def failure_handler(self, exception, traceback_exception=None, *args, **kwargs):
        self._handle_error(exception)
        self._safe_super_call('failure_handler', exception, traceback_exception, *args, **kwargs)
        return exception, traceback_exception

    async def async_failure_handler(self, exception, traceback_exception=None, *args, **kwargs):
        """async_success_handler와 동일한 방어 로직 적용"""
        self._handle_error(exception)
        super_result = self._safe_super_call('async_failure_handler', exception, traceback_exception, *args, **kwargs)
        
        if inspect.iscoroutine(super_result):
            await super_result
        return exception, traceback_exception

    ## Dummy 방어선 (에러 방지용 인터페이스 컨트랙트 충족)
    def pre_call(self, *args, **kwargs): pass
    def _pre_call(self, *args, **kwargs): pass
    def post_call(self, *args, **kwargs): pass
    def update_environment_variables(self, *args, **kwargs): pass
    def update_from_kwargs(self, *args, **kwargs): pass
    def update_messages(self, messages: List[Any]): pass
    def set_cost_breakdown(self, *args, **kwargs): pass
    def _response_cost_calculator(self, *args, **kwargs) -> float: return 0.0
    def should_run_prompt_management_hooks(self, *args, **kwargs) -> bool:
        return False
        
    def handle_sync_success_callbacks_for_async_calls(self, *args, **kwargs) -> None: pass

    def get_chat_completion_prompt(self, model: str, messages: List[Any], non_default_params: Dict, *args, **kwargs) -> Tuple[str, List[Any], Dict]:
        return model, messages, non_default_params

    async def async_get_chat_completion_prompt(self, model: str, messages: List[Any], non_default_params: Dict, *args, **kwargs) -> Tuple[str, List[Any], Dict]:
        return model, messages, non_default_params

    def get_custom_logger_for_prompt_management(self, *args, **kwargs): return None
    def get_router_model_id(self, *args, **kwargs): return None


def get_standard_logging_object_payload(*args, **kwargs):
    return None

def emit_standard_logging_payload(payload):
    if payload:
        emitter = get_emitter("plane.adapter")
        emitter.signal("LEGACY_LOG_EMITTED", payload=payload)

def get_standard_logging_metadata(*args, **kwargs):
    return {}

def scrub_sensitive_keys_in_metadata(litellm_params: Optional[dict] = None):
    return litellm_params or {}

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