# eco.llama.inst.span.base
## @lineage: adapter.llama.inst.span.base
## @lineage: llama.inst.span.base
## @lineage: xor.loop.inst.span.base
## @lineage: xphi.loop.inst.span.base
## @lineage: bound.adapter.llama.instrumentation.span.base
## @lineage: bound.adapter.instrumentation.span.base
## @lineage: anchor.adapter.instrumentation.span.base
## @lineage: bridge.llama.core.instrumentation.span.base
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class BaseSpan(BaseModel):
    """Base data class representing a span."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    id_: str = Field(default_factory=lambda: str(uuid4()), description="Id of span.")
    parent_id: Optional[str] = Field(default=None, description="Id of parent span.")
    tags: Dict[str, Any] = Field(default={})
