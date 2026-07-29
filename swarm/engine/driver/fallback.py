# swarm.engine.driver.fallback
## @lineage: agent.driver.fallback
## @lineage: atoa.fallback
## @lineage: agent.atoa.fallback
## @lineage: atoa.agent.fallback
## @lineage: atoa.call.fallback
## @lineage: agent.call.fallback
from __future__ import annotations
from collections.abc import Callable, Generator
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from atoa.exception.eco import (
    APIConnectionError,
    InternalServerError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout as LiteLLMTimeout,
)
from pydantic import BaseModel, Field, PrivateAttr
from atoa.exception.types import LLMNoResponseError
from swarm.engine.driver.registry import LLMProfileStore
from watcher.plane.emitter import get_logger

if TYPE_CHECKING:
    from swarm.engine.llm.response import LLMResponse
    from bound.watcher.snapshot.metrics import Metrics

logger = get_logger(__name__)

_LLM_FALLBACK_EXCEPTIONS: Final[tuple[type[Exception], ...]] = (
    APIConnectionError,
    RateLimitError,
    ServiceUnavailableError,
    LiteLLMTimeout,
    InternalServerError,
    LLMNoResponseError,
)

class FallbackStrategy(BaseModel):
    fallback_llms: list[str] = Field(
        description="Ordered list of LLM profile names to try on transient failure."
    )
    profile_store_dir: str | Path | None = Field(
        default=None,
        description="Path to directory containing profiles. "
    )

    # Private: lazily resolved LLM instances
    _resolved: list[Any] | None = PrivateAttr(default=None)

    def should_fallback(self, error: Exception) -> bool:
        """Whether this error type is eligible for fallback."""
        return isinstance(error, _LLM_FALLBACK_EXCEPTIONS)

    def try_fallback(
        self,
        primary_model: str,
        primary_error: Exception,
        primary_metrics: Metrics,
        call_fn: Callable[[Any], LLMResponse],
    ) -> LLMResponse | None:
        """Try fallback LLMs in order. Merges metrics into primary on success.

        Args:
            primary_model: The primary model name (for logging).
            primary_error: The error from the primary model.
            primary_metrics: The primary LLM's Metrics to merge fallback costs into.
            call_fn: A callable that takes an LLM instance and returns an LLMResponse.

        Returns:
            LLMResponse from the first successful fallback, or None if all fail.
        """
        total = len(self.fallback_llms)
        tried = 0
        for i, fb in enumerate(self._iter_fallbacks()):
            tried += 1
            remaining = total - i - 1
            logger.warning(
                f"[Fallback Strategy]Primary LLM ({primary_model}) failed with "
                f"{type(primary_error).__name__}, "
                f"trying fallback {i + 1}/{total} ({fb.model}); "
                f"{remaining} fallback(s) remaining"
            )
            try:
                # Disable nested fallbacks to prevent recursive chains
                saved_strategy = fb.fallback_strategy
                fb.fallback_strategy = None
                metrics_before = fb.metrics.deep_copy()
                try:
                    result = call_fn(fb)
                finally:
                    fb.fallback_strategy = saved_strategy
                # Merge fallback metrics (cost + tokens) into primary
                metrics_diff = fb.metrics.diff(metrics_before)
                primary_metrics.merge(metrics_diff)
                logger.info(f"[Fallback Strategy] Fallback LLM ({fb.model}) succeeded")
                return result
            except Exception as fb_error:
                logger.warning(
                    "[Fallback Strategy]"
                    f"Fallback {i + 1} ({fb.model}) failed: "
                    f"{type(fb_error).__name__}: {fb_error}"
                )
                continue

        if tried > 0:
            logger.error(
                "[Fallback Strategy] All fallback LLMs failed; re-raising primary error"
            )
        return None

    @cached_property
    def _profile_store(self) -> LLMProfileStore:
        return LLMProfileStore(self.profile_store_dir)

    def _iter_fallbacks(self) -> Generator[Any]:
        if self._resolved is None:
            self._resolved = []

        yield from self._resolved
        remaining_names = self.fallback_llms[len(self._resolved) :]
        for name in remaining_names:
            try:
                fb = self._profile_store.load(name)
                self._resolved.append(fb)
                yield fb
            except (FileNotFoundError, ValueError) as exc:
                logger.error(
                    "[Fallback Strategy] Failed to load "
                    f"fallback profile '{name}': {exc}"
                )
