# llama.inst.base.handler
## @lineage: xor.loop.inst.base.handler
## @lineage: xphi.loop.inst.base.handler
## @lineage: bound.adapter.llama.instrumentation.base.handler
## @lineage: bound.adapter.instrumentation.base.handler
## @lineage: anchor.adapter.instrumentation.base.handler
## @lineage: bridge.llama.core.instrumentation.base.handler
from abc import ABC, abstractmethod


class BaseInstrumentationHandler(ABC):
    @classmethod
    @abstractmethod
    def init(cls) -> None:
        """Initialize the instrumentation handler."""
