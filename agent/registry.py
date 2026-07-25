# agent.registry
from collections.abc import Callable
from types import MappingProxyType
from typing import ClassVar, TYPE_CHECKING
from uuid import uuid4
from pydantic import BaseModel, ConfigDict
from agent.driver.tensor import Driver
from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

class RegistryEvent(BaseModel):
    llm: Driver
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True,
    )

class LLMRegistry:
    """A minimal LLM registry for managing LLM instances by usage ID"""
    registry_id: str
    retry_listener: Callable[[int, int], None] | None

    def __init__(
        self,
        retry_listener: Callable[[int, int], None] | None = None,
    ):
        self.registry_id = str(uuid4())
        self.retry_listener = retry_listener
        self._usage_to_llm: dict[str, Driver] = {}
        self._metrics_ids: set[int] = set()
        self.subscriber: Callable[[RegistryEvent], None] | None = None

    def subscribe(self, callback: Callable[[RegistryEvent], None]) -> None:
        self.subscriber = callback

    def notify(self, event: RegistryEvent) -> None:
        if self.subscriber:
            try:
                self.subscriber(event)
            except Exception as e:
                logger.warning(f"Failed to emit event: {e}")

    @property
    def usage_to_llm(self) -> MappingProxyType[str, Driver]:
        """Access the internal usage-ID-to-LLM mapping (read-only view)."""

        return MappingProxyType(self._usage_to_llm)

    def _ensure_independent_metrics(self, llm: Driver) -> None:
        metrics = llm.metrics
        metrics_id = id(metrics)
        if metrics_id in self._metrics_ids:
            logger.debug(
                f"[LLM registry {self.registry_id}]: Detected shared metrics for "
                f"usage '{llm.usage_id}', resetting to independent metrics"
            )
            llm.reset_metrics()
            metrics_id = id(llm.metrics)

        self._metrics_ids.add(metrics_id)

    def add(self, llm: Driver) -> None:
        usage_id = llm.usage_id
        if usage_id in self._usage_to_llm:
            message = (
                f"Usage ID '{usage_id}' already exists in registry. "
                "Use a different usage_id on the LLM or "
                "call get() to retrieve the existing LLM."
            )
            raise ValueError(message)

        self._ensure_independent_metrics(llm)
        self._usage_to_llm[usage_id] = llm
        self.notify(RegistryEvent(llm=llm))
        logger.debug(
            f"[LLM registry {self.registry_id}]: Added LLM for usage {usage_id}"
        )

    def get(self, usage_id: str) -> Driver:
        if usage_id not in self._usage_to_llm:
            raise KeyError(
                f"Usage ID '{usage_id}' not found in registry. "
                "Use add() to register an LLM first."
            )

        logger.info(
            f"[LLM registry {self.registry_id}]: Retrieved LLM for usage {usage_id}"
        )
        return self._usage_to_llm[usage_id]

    def list_usage_ids(self) -> list[str]:
        """List all registered usage IDs."""

        return list(self._usage_to_llm.keys())
