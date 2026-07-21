# eco.llama.inst.span.__init__
## @lineage: adapter.llama.inst.span.__init__
## @lineage: llama.inst.span.__init__
## @lineage: xor.loop.inst.span.__init__
## @lineage: xphi.loop.inst.span.__init__
## @lineage: bound.adapter.llama.instrumentation.span.__init__
## @lineage: bound.adapter.instrumentation.span.__init__
## @lineage: anchor.adapter.instrumentation.span.__init__
## @lineage: bridge.llama.core.instrumentation.span.__init__
from contextvars import ContextVar
from typing import Optional
from eco.llama.inst.span.base import BaseSpan
from eco.llama.inst.span.simple import SimpleSpan

# ContextVar for managing active spans
active_span_id: ContextVar[Optional[str]] = ContextVar("active_span_id", default=None)
active_span_id.set(None)

__all__ = ["BaseSpan", "SimpleSpan", "active_span_id"]
