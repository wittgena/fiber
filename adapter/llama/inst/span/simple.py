# adapter.llama.inst.span.simple
## @lineage: llama.inst.span.simple
## @lineage: xor.loop.inst.span.simple
## @lineage: xphi.loop.inst.span.simple
## @lineage: bound.adapter.llama.instrumentation.span.simple
## @lineage: bound.adapter.instrumentation.span.simple
## @lineage: anchor.adapter.instrumentation.span.simple
## @lineage: bridge.llama.core.instrumentation.span.simple
from datetime import datetime
from typing import Dict, Optional
from pydantic import Field
from adapter.llama.inst.span.base import BaseSpan

class SimpleSpan(BaseSpan):
    """Simple span class."""

    start_time: datetime = Field(default_factory=lambda: datetime.now())
    end_time: Optional[datetime] = Field(default=None)
    duration: float = Field(default=0.0, description="Duration of span in seconds.")
    metadata: Optional[Dict] = Field(default=None)
