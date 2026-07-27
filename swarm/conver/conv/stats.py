# swarm.conver.conv.stats
## @lineage: topos.conv.stats
## @lineage: mesh.watcher.stats
## @lineage: eco.watcher.stats
from typing import Any
from pydantic import BaseModel, Field, PrivateAttr, model_serializer

from swarm.engine.llm.registry import RegistryEvent
from eco.watcher.snapshot.metrics import Metrics
from watcher.plane.emitter import get_logger

log = get_logger(__name__)

class ConversationStats(BaseModel):
    usage_to_metrics: dict[str, Metrics] = Field(
        default_factory=dict,
        description="Active usage metrics tracked by the registry.",
    )
    _restored_usage_ids: set[str] = PrivateAttr(default_factory=set)

    @model_serializer(mode="wrap")
    def _serialize_with_context(self, serializer: Any, info: Any) -> dict[str, Any]:
        data = serializer(self)
        context = info.context if info else None
        use_snapshot = context.get("use_snapshot", False) if context else False
        if use_snapshot and "usage_to_metrics" in data:
            usage_to_snapshots = {}
            for usage_id, metrics in self.usage_to_metrics.items():
                snapshot = metrics.get_snapshot()
                usage_to_snapshots[usage_id] = snapshot.model_dump()

            data["usage_to_metrics"] = usage_to_snapshots

        return data

    def get_combined_metrics(self) -> Metrics:
        total_metrics = Metrics()
        for metrics in self.usage_to_metrics.values():
            total_metrics.merge(metrics)
        return total_metrics

    def get_metrics_for_usage(self, usage_id: str) -> Metrics:
        if usage_id not in self.usage_to_metrics:
            raise Exception(f"LLM usage does not exist {usage_id}")

        return self.usage_to_metrics[usage_id]

    def register_llm(self, event: RegistryEvent):
        llm = event.llm
        usage_id = llm.usage_id
        if usage_id in self.usage_to_metrics and usage_id not in self._restored_usage_ids:
            llm.restore_metrics(self.usage_to_metrics[usage_id])
            self._restored_usage_ids.add(usage_id)

        if usage_id not in self.usage_to_metrics and llm.metrics:
            self.usage_to_metrics[usage_id] = llm.metrics
