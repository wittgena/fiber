# llm.driver.strategy.retry
## @lineage: agent.llm.driver.strategy.retry
## @lineage: ator.driver.llm.strategy.retry
## @lineage: driver.strategy.retry
## @lineage: ator.driver.strategy.retry
## @lineage: engine.driver.strategy.retry
## @lineage: phi.driver.strategy.retry
from collections.abc import Callable, Iterable
from typing import Any, cast
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from fiber.llm.exception.types import LLMNoResponseError
from fiber.llm.exception.eco import (
    APIConnectionError,
    InternalServerError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout,
)
from xphi.watcher.plane.emitter import get_logger

logger = get_logger(__name__)

RetryListener = Callable[[int, int, BaseException | None], None]

# [핵심 변경] 재시도 대상 예외 목록을 Retry 모듈 안으로 캡슐화
LLM_RETRY_EXCEPTIONS: tuple[type[Exception], ...] = (
    APIConnectionError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout,
    InternalServerError,
    LLMNoResponseError,
)

def _log_retry_attempt(retry_state: RetryCallState) -> None:
    """@desc: Mixin의 메서드였던 로깅 로직을 순수 함수로 분리 (self 참조 제거)"""
    if retry_state.outcome is None:
        logger.error("retry_state.outcome is None. This should not happen, please check the retry logic.")
        return

    exc = retry_state.outcome.exception()
    if exc is None:
        logger.error("retry_state.outcome.exception() returned None.")
        return

    max_attempts: int | None = None
    retry_obj = getattr(retry_state, "retry_object", None)
    stop_condition = getattr(retry_obj, "stop", None)
    
    if stop_condition is not None:
        stops: Iterable[Any]
        if hasattr(stop_condition, "stops"):
            stops = stop_condition.stops  # type: ignore[attr-defined]
        else:
            stops = [stop_condition]
        for stop_func in stops:
            if hasattr(stop_func, "max_attempts"):
                max_attempts = getattr(stop_func, "max_attempts")
                break

    setattr(cast(Any, exc), "retry_attempt", retry_state.attempt_number)
    if max_attempts is not None:
        setattr(cast(Any, exc), "max_retries", max_attempts)

    logger.error(
        "%s. Attempt #%d | You can customize retry values in the configuration.",
        exc,
        retry_state.attempt_number,
    )

def create_retry_decorator(
    num_retries: int = 5,
    retry_exceptions: tuple[type[BaseException], ...] = LLM_RETRY_EXCEPTIONS,
    retry_min_wait: int = 8,
    retry_max_wait: int = 64,
    retry_multiplier: float = 2.0,
    retry_listener: RetryListener | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    @desc: 런타임에 인자로 받은 설정값을 기반으로 tenacity retry 데코레이터를 동적 생성하여 반환합니다.
    """
    def before_sleep(retry_state: RetryCallState) -> None:
        _log_retry_attempt(retry_state)

        if retry_listener is not None:
            exc = (
                retry_state.outcome.exception()
                if retry_state.outcome is not None
                else None
            )
            retry_listener(retry_state.attempt_number, num_retries, exc)

        if retry_state.outcome is None:
            return
            
        exc = retry_state.outcome.exception()
        if exc is None:
            return

        # [특화 로직] LLM 응답이 완전히 누락된 경우(0.0) Temperature를 강제로 조정하여 다양성 유도
        if isinstance(exc, LLMNoResponseError):
            kwargs = getattr(retry_state, "kwargs", None)
            if isinstance(kwargs, dict):
                current_temp = kwargs.get("temperature", 0)
                if current_temp == 0:
                    kwargs["temperature"] = 1.0
                    logger.warning(
                        "LLMNoResponseError with temperature=0, "
                        "setting temperature to 1.0 for next attempt."
                    )
                else:
                    logger.warning(
                        f"LLMNoResponseError with temperature={current_temp}, "
                        "keeping original temperature"
                    )

    # 설정된 파라미터로 클로저(Closure)를 묶어 최종 데코레이터 함수 반환
    return retry(
        before_sleep=before_sleep,
        stop=stop_after_attempt(num_retries),
        reraise=True,
        retry=retry_if_exception_type(retry_exceptions),
        wait=wait_exponential(
            multiplier=retry_multiplier,
            min=retry_min_wait,
            max=retry_max_wait,
        ),
    )