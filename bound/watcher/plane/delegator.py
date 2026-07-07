# bound.watcher.plane.delegator
import time
import traceback
import warnings
import inspect
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

from anchor.provider.cost.calculator import completion_cost
from bound.surface.legacy.types import CostPerToken, Usage
from bound.surface.switch.params import ResponseAPIUsage, ResponsesAPIResponse, ModelResponse
from bound.watcher.plane.metrics import Metrics

from arch.proto.event.next import LogEvent
from phase.gov.proto.gate import uuid4
from watcher.plane.emitter import get_emitter, _flow_context

emitter = get_emitter("plane.delegator")

"""PEP 562: Module-level attribute access (LiteLLM legacy global scope compatibility)"""
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

def emit_standard_logging_payload(payload):
    if payload:
        emitter.signal("LEGACY_LOG_EMITTED", payload=payload)

@dataclass
class ParsedUsage:
    prompt: int = 0
    completion: int = 0
    cache_read: int = 0
    cache_write: int = 0
    reasoning: int = 0
    is_meaningful: bool = False

def _get_legacy_trace_id() -> str:
    """
    @desc: Quarantined OpenTelemetry trace extraction. 
    Execution is intentionally bypassed to avoid strict dependencies on OTel.
    Retained purely as a structural placeholder for future deletion.
    """
    # try:
    #     from opentelemetry import trace
    #     span = trace.get_current_span()
    #     if span.is_recording():
    #         return format(span.get_span_context().trace_id, "032x")
    # except ImportError:
    #     pass

    # Fallback to the native topological context flow
    ctx = _flow_context.get()
    return ctx.get("flow_id") or ctx.get("session_id") or uuid4().hex

class DriverObserver:
    """
    @desc: Unified Observability Container. (Replaces stateful Pydantic Telemetry)
    @role: Manages metrics, cost parsing, and stateless event emission to the central plane.
    """
    def __init__(self, model_name: str, config: dict[str, Any], initial_metrics: Metrics | None = None):
        self.model_name = model_name
        self.log_enabled = config.get("log_completions", False)
        self.log_dir = config.get("log_completions_folder")
        self.input_cost_per_token = config.get("input_cost_per_token")
        self.output_cost_per_token = config.get("output_cost_per_token")
        
        self._metrics = initial_metrics or Metrics(model_name=model_name)

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    def restore_metrics(self, metrics: Metrics) -> None:
        self._metrics = metrics

    def reset_metrics(self) -> None:
        self._metrics = None

    def on_request(self, telemetry_ctx: dict | None = None) -> float:
        """
        @desc: Captures the exact start time for stateless tracking.
        Returns the timestamp to be held in the local execution scope, preventing race conditions.
        """
        return time.time()

    def track_success(
        self,
        resp: ModelResponse | ResponsesAPIResponse,
        start_time: float,
        telemetry_ctx: dict | None = None,
    ) -> Metrics:
        """Calculates cost, records metrics, and emits a normalized tracking event to the central plane (Stateless)."""
        ctx = _flow_context.get()
        if ctx.get("is_internal_call"):
            emitter.debug("Skipping telemetry for internal sub-call (is_internal_call=True).")
            return getattr(self, "_metrics", Metrics(model_name=self.model_name)).deep_copy()

        req_ctx = telemetry_ctx or {}
        latency = time.time() - start_time
        response_id = getattr(resp, "id", uuid4().hex)
        
        if self._metrics:
            self._metrics.add_response_latency(latency, response_id)

        cost = self._compute_cost(resp)
        if cost and self._metrics:
            self._metrics.add_cost(cost)

        usage = getattr(resp, "usage", None)
        parsed_usage = self._parse_usage(usage)

        if parsed_usage.is_meaningful and self._metrics:
            self._metrics.add_token_usage(
                prompt_tokens=parsed_usage.prompt,
                completion_tokens=parsed_usage.completion,
                cache_read_tokens=parsed_usage.cache_read,
                cache_write_tokens=parsed_usage.cache_write,
                reasoning_tokens=parsed_usage.reasoning,
                context_window=req_ctx.get("context_window", 0),
                response_id=response_id,
            )

        ## @dispatch: normalized payload to the central Observability plane
        payload = {
            "response_id": response_id,
            "cost": cost or 0.0,
            "latency_ms": latency * 1000,
            "model_name": self.model_name,
            "usage_metrics": {
                "prompt_tokens": parsed_usage.prompt,
                "completion_tokens": parsed_usage.completion,
                "cache_read_tokens": parsed_usage.cache_read,
                "reasoning_tokens": parsed_usage.reasoning
            },
            "context": req_ctx
        }
        
        emitter.signal("LLM_COMPLETION_TRACKED", payload=payload)
        return self._metrics.deep_copy() if self._metrics else Metrics(model_name=self.model_name)

    def track_rupture(
        self, 
        error: BaseException, 
        start_time: float | None = None, 
        telemetry_ctx: dict | None = None
    ) -> None:
        """Emits a failure event to the central Emitter upon error, replacing legacy file logging (Stateless)."""
        ctx = _flow_context.get()
        if ctx.get("is_internal_call"):
            emitter.debug("Internal sub-call failed. Skipping telemetry error log.")
            return

        latency = (time.time() - start_time) if start_time else 0.0
        req_ctx = telemetry_ctx or {}
        
        error_payload = {
            "model_name": self.model_name,
            "latency_sec": latency,
            "error_type": type(error).__name__,
            "message": str(error),
            "traceback": "".join(traceback.format_exception(type(error), error, error.__traceback__)),
            "context": req_ctx
        }
        
        try:
            # Invoke the fully integrated Emitter interface
            emitter.signal("LLM_COMPLETION_FAILED", payload=error_payload)
            emitter.error("LLM Generation Failed", exc_info=True, extra_context=error_payload)
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

class LoggingBase:
    def pre_call(self, input, api_key, model=None, additional_args={}): pass
    def post_call(self, original_response, input=None, api_key=None, additional_args={}): pass

class LogDelegator(LoggingBase):
    """@compat.anchor: Entry point for LiteLLM & Openhands ecosystem integration"""
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
        
        # Safely extract trace_id bypassing actual OTel invocation
        self.litellm_trace_id = _get_legacy_trace_id()

        # Use injected DriverObserver if available, otherwise fallback to creating one
        injected_observer = kwargs.get("observer")
        if injected_observer:
            self.observer = injected_observer
        else:
            model_name = kwargs.get("model", "unknown")
            metrics = kwargs.get("metrics") or Metrics()
            config = {
                "input_cost_per_token": kwargs.get("input_cost_per_token"),
                "output_cost_per_token": kwargs.get("output_cost_per_token"),
                "log_completions": kwargs.get("log_enabled", False)
            }
            self.observer = DriverObserver(model_name=model_name, config=config, initial_metrics=metrics)

    def _safe_super_call(self, method_name: str, *args, **kwargs) -> Any:
        if hasattr(super(), method_name):
            method = getattr(super(), method_name)
            return method(*args, **kwargs)
        return None

    ## @lifecycle.hooks: Delegated to DriverObserver
    def success_handler(self, kwargs, result=None, start_time=None, end_time=None):
        req_start = start_time or kwargs.get("start_time", time.time())
        req_end = end_time or time.time()
        telemetry_ctx = kwargs.get("telemetry_ctx", {})
        
        if result:
            # 기존에는 track_success 내부에서 time.time()을 썼다면, 
            # DriverObserver.track_success도 end_time을 받도록 수정하거나 아래처럼 start_time을 보정할 수 있습니다.
            # (가장 좋은 것은 DriverObserver.track_success가 latency를 직접 받거나 end_time을 받는 것입니다)
            self.observer.track_success(resp=result, start_time=req_start, telemetry_ctx=telemetry_ctx)
        return self._safe_super_call('success_handler', kwargs, result, start_time)

    async def async_success_handler(self, kwargs, result=None, start_time=None, end_time=None):
        req_start = start_time or kwargs.get("start_time", time.time())
        telemetry_ctx = kwargs.get("telemetry_ctx", {})
        
        if result:
            self.observer.track_success(resp=result, start_time=req_start, telemetry_ctx=telemetry_ctx)
            
        super_result = self._safe_super_call('async_success_handler', kwargs, result, start_time)
        if inspect.iscoroutine(super_result):
            return await super_result
        return super_result

    def failure_handler(self, kwargs, exception, traceback_exception=None, start_time=None, end_time=None):
        req_start = start_time or kwargs.get("start_time", time.time())
        telemetry_ctx = kwargs.get("telemetry_ctx", {})
        
        self.observer.track_rupture(error=exception, start_time=req_start, telemetry_ctx=telemetry_ctx)
        self._safe_super_call('failure_handler', kwargs, exception, traceback_exception, start_time)
        return exception, traceback_exception

    async def async_failure_handler(self, kwargs, exception, traceback_exception=None, start_time=None, end_time=None):
        req_start = start_time or kwargs.get("start_time", time.time())
        telemetry_ctx = kwargs.get("telemetry_ctx", {})
        
        self.observer.track_rupture(error=exception, start_time=req_start, telemetry_ctx=telemetry_ctx)
        
        super_result = self._safe_super_call('async_failure_handler', kwargs, exception, traceback_exception, start_time)
        if inspect.iscoroutine(super_result):
            await super_result
        return exception, traceback_exception

    ## @defense: Dummy satisfies LiteLLM interface contracts
    def pre_call(self, *args, **kwargs): pass
    def _pre_call(self, *args, **kwargs): pass
    def post_call(self, *args, **kwargs): pass
    def update_environment_variables(self, *args, **kwargs): pass
    def update_from_kwargs(self, *args, **kwargs): pass
    def update_messages(self, messages: List[Any]): pass
    def set_cost_breakdown(self, *args, **kwargs): pass
    def _response_cost_calculator(self, *args, **kwargs) -> float: return 0.0
    def should_run_prompt_management_hooks(self, *args, **kwargs) -> bool: return False
    def handle_sync_success_callbacks_for_async_calls(self, *args, **kwargs) -> None: pass
    
    def get_chat_completion_prompt(self, model: str, messages: List[Any], non_default_params: Dict, *args, **kwargs) -> Tuple[str, List[Any], Dict]:
        return model, messages, non_default_params

    async def async_get_chat_completion_prompt(self, model: str, messages: List[Any], non_default_params: Dict, *args, **kwargs) -> Tuple[str, List[Any], Dict]:
        return model, messages, non_default_params

    def get_custom_logger_for_prompt_management(self, *args, **kwargs): return None
    def get_router_model_id(self, *args, **kwargs): return None

"""@expose: DUMMY METADATA (LITELLM COMPAT)"""
def get_standard_logging_object_payload(*args, **kwargs): return None
def get_standard_logging_metadata(*args, **kwargs): return {}
def scrub_sensitive_keys_in_metadata(litellm_params: Optional[dict] = None): return litellm_params or {}