# arch.xor.driver.retry
from collections.abc import Callable, Iterable
from typing import Any, cast
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from bound.exception.types import LLMNoResponseError
from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

RetryListener = Callable[[int, int, BaseException | None], None]

class RetryMixin:
    def retry_decorator(
        self,
        num_retries: int = 5,
        retry_exceptions: tuple[type[BaseException], ...] = (LLMNoResponseError,),
        retry_min_wait: int = 8,
        retry_max_wait: int = 64,
        retry_multiplier: float = 2.0,
        retry_listener: RetryListener | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def before_sleep(retry_state: RetryCallState) -> None:
            self.log_retry_attempt(retry_state)

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

            # Only adjust temperature for LLMNoResponseError
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

        retry_decorator: Callable[[Callable[..., Any]], Callable[..., Any]] = retry(
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
        return retry_decorator

    def log_retry_attempt(self, retry_state: RetryCallState) -> None:
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
