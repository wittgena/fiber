# eco.llama.inst.base.event
## @lineage: adapter.llama.inst.base.event
## @lineage: llama.inst.base.event
## @lineage: xor.loop.inst.base.event
## @lineage: xphi.loop.inst.base.event
## @lineage: bound.adapter.llama.instrumentation.base.event
## @lineage: bound.adapter.instrumentation.base.event
## @lineage: anchor.adapter.instrumentation.base.event
## @lineage: bridge.llama.core.instrumentation.base.event
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, Field
from eco.llama.inst.span import active_span_id


class BaseEvent(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        # copy_on_model_validation = "deep"  # not supported in Pydantic V2...
    )
    timestamp: datetime = Field(default_factory=lambda: datetime.now())
    id_: str = Field(default_factory=lambda: str(uuid4()))
    span_id: Optional[str] = Field(default_factory=active_span_id.get)  # type: ignore
    tags: Dict[str, Any] = Field(default={})

    @classmethod
    def class_name(cls) -> str:
        """Return class name."""
        return "BaseEvent"

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        """Keep for backwards compatibility."""
        return self.model_dump(**kwargs)

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        data = super().model_dump(**kwargs)
        data["class_name"] = self.class_name()
        return data
