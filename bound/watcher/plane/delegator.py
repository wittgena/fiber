# bound.watcher.plane.delegator
## @lineage: xphi.scope.plane.delegator
import inspect
from typing import Any, Dict, List, Optional, Tuple, Coroutine
from bound.watcher.plane.telemetry.llm import Telemetry
from bound.watcher.plane.metrics import Metrics
from watcher.plane.emitter import get_emitter
from opentelemetry import trace

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