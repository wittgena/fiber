# bound.watcher.plane.tracker.shingle
## @lineage: xphi.scope.plane.tracker.shingle
from __future__ import annotations
import time
import functools
from typing import Any, Callable, Dict
from arch.contract.exp.promise import future, Promise
from arch.proto.event.next import LogEvent
from watcher.plane.emitter import get_emitter, register_interceptor
from bound.watcher.plane.tracker.history import append_trace_event

emitter = get_emitter(__name__, phase="observer", boundary="shingle_tracker")

decoupling_promise = Promise(
    contract="The Shingle tracking core must not depend on any specific external infrastructure.",
    invariant="Measured data is emitted purely as a Dict via the internal event bus only.",
    consequence="Replacing external monitoring tools contaminates the core domain logic.",
)

shingle_mapping_promise = Promise(
    contract="All external token data must be converted into internal Shingle (overlapping N-grams) units.",
    invariant="Follows the S = max(0, N - w + 1) formula, where the window size (w) is restricted to 1-4 to prevent semantic loss.",
    consequence="The nature of words and the calculation method mismatch, collapsing the philosophical foundation of the internal domain.",
)

class ShingleUsageTracker:
    """
    @role: Contextual Shingle Meter
    @desc: Core domain that verifies cache status and converts token segments into contextual Shingle units
    """

    @future("Raw token estimation logic based on text length for streaming environments lacking usage metadata")
    def _estimate_raw_tokens(self, text: str) -> int:
        return len(text) // 4 if text else 0

    def _clamp_window(self, w: int) -> int:
        """Enforce window size clipping to prevent computational load and semantic loss"""
        clamped = max(1, min(w, 4))
        if w > 4:
            emitter.warning(f"Requested Shingle window {w} exceeds meaningful limit. Clamped to 4.")
        return clamped

    def _convert_to_shingle(self, token_count: int, w: int) -> int:
        """Token -> Shingle conversion formula: S = max(0, N - w + 1)"""
        if token_count == 0:
            return 0
        return max(0, token_count - w + 1)

    def calculate_metrics(self, window_size: int, cache_hit: bool, raw_usage: Dict[str, int]) -> Dict[str, Any]:
        """Pure domain calculation: Injects external token data and converts it into the internal Shingle metrics structure"""
        w = self._clamp_window(window_size)

        raw_prompt = raw_usage.get("prompt_tokens", 0) or self._estimate_raw_tokens("dummy")
        raw_comp = raw_usage.get("completion_tokens", 0) or self._estimate_raw_tokens("dummy")

        input_shingles = self._convert_to_shingle(raw_prompt, w)
        output_shingles = self._convert_to_shingle(raw_comp, w)

        actual_input = 0 if cache_hit else input_shingles
        actual_output = 0 if cache_hit else output_shingles
        saved_input = input_shingles if cache_hit else 0
        saved_output = output_shingles if cache_hit else 0

        return {
            "shingle_window_size": w,
            "cache_hit": cache_hit,
            "metrics": {
                "actual_shingles": actual_input + actual_output,
                "saved_shingles": saved_input + saved_output,
            }
        }


shingle_tracker = ShingleUsageTracker()

def shingle_tracker_interceptor(event: LogEvent) -> None:
    """
    @desc: 
    - Anti-Corruption Layer (ACL) attached to the watcher.plane.emitter bus
    - Intercepts Telemetry signals for Usage tracking AND captures topology shifts (Errors/Fallbacks) for Dynamic Simulation assertions
    """
    ## Extract trace_id to link events to the simulation runner's query context
    trace_id = event.context.get("trace_id", "default_trace")

    ## @tracking: Usage Metrics & Shingle Calculation
    if event.level == "SIGNAL" and "LLM_COMPLETION_TRACKED" in event.message:
        payload = event.context.get("payload", {})
        
        ## usage_metrics for clean, standardized internal data, while ignoring the raw legacy 'usage' payload
        metrics = payload.get("usage_metrics", {})
        prompt_tokens = metrics.get("prompt_tokens", 0)
        completion_tokens = metrics.get("completion_tokens", 0)
        cache_read = metrics.get("cache_read_tokens", 0)
        
        cache_hit = cache_read > 0
        raw_usage = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens
        }
        
        ## Extract the window size configured in the context layer (Default: Bigram=2)
        window_size = event.context.get("shingle_window_size", 2)
        
        ## Delegate to the pure domain logic calculation
        shingle_data = shingle_tracker.calculate_metrics(
            window_size=window_size,
            cache_hit=cache_hit,
            raw_usage=raw_usage
        )
        
        ## Safely inject Shingle metrics into the existing telemetry event context without side-effects
        event.context["shingle_telemetry"] = {
            "module": event.context.get("bound") or payload.get("model_name", "unknown"),
            "latency_ms": payload.get("latency_ms", 0.0), # Fixed key to match latency_ms in payload
            **shingle_data
        }
        
        ## Record trace logs to integrate with distributed tracing systems (e.g., Laminar)
        emitter.trace(
            f"Shingle alignment optimized for {event.context['shingle_telemetry']['module']}", 
            metrics=event.context["shingle_telemetry"]["metrics"]
        )

        ## Record the successful completion in the history store
        append_trace_event(trace_id, "completion_success", event.context["shingle_telemetry"])
    elif event.level == "WARNING" and "LLM_EXCEPTION_TRACKED" in event.message:
        ## Captures API errors (e.g., from mock_api_error injection)
        append_trace_event(trace_id, "api_error_encountered", {
            "error_type": event.context.get("error_type", "UnknownError"),
            "status_code": event.context.get("status_code"),
            "model": event.context.get("model")
        })
    elif event.level == "SIGNAL" and "FALLBACK_TRIGGERED" in event.message:
        ## Captures the event where traffic is successfully routed to a secondary model
        append_trace_event(trace_id, "fallback_to_secondary", {
            "primary_model": event.context.get("primary_model"),
            "fallback_model_used": event.context.get("fallback_model")
        })


## Automatically register the interceptor upon system initialization or package load
register_interceptor(shingle_tracker_interceptor)

def track_non_llm_shingle_usage(module_name: str, window_size: int = 2):
    """
    @desc: 
    - AOP decorator for non-LLM modules processing pure text operations
    - Integrates the execution flow with the pipeline scope context
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = await func(*args, **kwargs)
            latency_ms = (time.perf_counter() - start_time) * 1000
            
            cache_hit = getattr(result, "cache_hit", False)
            raw_usage = getattr(result, "usage", {})
            
            shingle_data = shingle_tracker.calculate_metrics(
                window_size=window_size,
                cache_hit=cache_hit,
                raw_usage=raw_usage
            )
            
            ## Unify output through the shared public bus emitter interface instead of independent printing
            emitter.signal(f"NON_LLM_SHINGLE_TRACKED:{module_name}", payload={
                "latency_ms": latency_ms,
                **shingle_data
            })
            return result
        return async_wrapper
    return decorator