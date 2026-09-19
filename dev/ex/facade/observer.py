# fiber.dev.ex.facade.observer
import time
from typing import Any
from dataclasses import dataclass

from fiber.llm.param import ModelResponse
from xphi.arch.bound.event.next import LogEvent
from xphi.watcher.plane.emitter import get_emitter, register_interceptor

log = get_emitter("driver.observer")

@dataclass
class ParsedUsage:
    """LLM 응답에서 추출된 토큰 및 비용 정보를 담는 순수 데이터 클래스"""
    prompt: int = 0
    completion: int = 0
    reasoning: int = 0
    cache_read: int = 0
    cache_write: int = 0
    cost: float = 0.0
    is_meaningful: bool = False

def extract_usage(response: ModelResponse, manifest: dict | None = None) -> ParsedUsage:
    """
    순수 함수(Stateless): ModelResponse 객체에서 토큰 사용량을 추출하고 
    주어진 manifest(단가표)를 바탕으로 비용을 계산합니다.
    """
    usage = getattr(response, "usage", None)
    if not usage:
        return ParsedUsage()

    def _get_val(obj: Any, key: str, default: int = 0) -> int:
        if isinstance(obj, dict): 
            return obj.get(key, default)
        return getattr(obj, key, default)

    prompt = _get_val(usage, "prompt_tokens", 0)
    completion = _get_val(usage, "completion_tokens", 0)
    
    prompt_details = _get_val(usage, "prompt_tokens_details", {})
    cache_read = _get_val(prompt_details, "cached_tokens", 0)
    cache_write = _get_val(usage, "cache_write_tokens", 0) # 지원되는 경우
    
    completion_details = _get_val(usage, "completion_tokens_details", {})
    reasoning = _get_val(completion_details, "reasoning_tokens", 0)

    cost = 0.0
    if manifest:
        # 응답에 박힌 모델명 또는 전송한 모델명으로 단가 조회
        model_name = _get_val(response, "model", "unknown")
        rates = manifest.get(model_name, {})
        # 매니페스트에 특정 모델이 없으면 'default' 요금표를 찾도록 폴백 구성 가능
        if not rates:
            rates = manifest.get("default", {})
            
        cost = (
            prompt * rates.get("prompt_token_cost", 0.0) + 
            completion * rates.get("completion_token_cost", 0.0)
        )

    return ParsedUsage(
        prompt=prompt, 
        completion=completion, 
        reasoning=reasoning,
        cache_read=cache_read, 
        cache_write=cache_write,
        cost=cost, 
        is_meaningful=(prompt + completion > 0)
    )

def llm_metric_interceptor(event: LogEvent) -> None:
    """
    [Interceptor] xphi.watcher의 이벤트 파이프라인에서 SIGNAL 레벨 중 
    'llm_metric' 타입의 이벤트를 가로채어 로깅 및 전역 메트릭 수집을 수행합니다.
    """
    ctx = event.context
    
    # 관심 없는 이벤트는 빠르게 패스
    if event.level != "SIGNAL":
        return
        
    event_type = ctx.get("type")
    
    if event_type == "llm_metric":
        model_name = ctx.get("model_name", "unknown")
        latency = ctx.get("latency_sec", 0.0)
        cost = ctx.get("cost", 0.0)
        usage: dict = ctx.get("usage", {})
        
        # 내부 콜(Internal sub-call) 필터링 - 메인 텔레메트리 로깅
        if not ctx.get("is_internal_call", False):
            log.info(
                f"🟢 [FLOW: SUCCESS] Model: {model_name} | "
                f"Latency: {latency:.3f}s | Cost: ${cost:.6f} | "
                f"Tokens: [P:{usage.get('prompt', 0)} / C:{usage.get('completion', 0)} / R:{usage.get('reasoning', 0)}]"
            )
            
        # 추후 이곳에 Redis/Prometheus 등 전역 메트릭 수집기로 데이터를 비동기 발송하는 로직 추가 가능
            
    elif event_type == "llm_rupture":
        model_name = ctx.get("model_name", "unknown")
        latency = ctx.get("latency_sec", 0.0)
        error_msg = ctx.get("error", "Unknown Error")
        
        if not ctx.get("is_internal_call", False):
            log.error(
                f"🔴 [FLOW: RUPTURE] Model: {model_name} | "
                f"Failed after {latency:.3f}s | "
                f"Error: {error_msg}"
            )


# =====================================================================
# [Module Auto-Registration]
# 파이썬 모듈 시스템(Import)의 특성을 활용하여, 이 모듈이 메모리에 로드(Scan)될 때 
# 단 한 번만 register_interceptor를 전역 파이프라인에 주입합니다.
# 이로 인해 런타임(agent.runtime) 초기화 코드에서 명시적으로 호출할 필요가 사라집니다.
# =====================================================================
_IS_REGISTERED = False

def _auto_register():
    global _IS_REGISTERED
    if not _IS_REGISTERED:
        register_interceptor(llm_metric_interceptor)
        _IS_REGISTERED = True
        log.debug("[Observer] Auto-registered 'llm_metric_interceptor' to global event plane.")

_auto_register()