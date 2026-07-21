# eco.llama.inst.event_handlers.null
## @lineage: adapter.llama.inst.event_handlers.null
## @lineage: llama.inst.event_handlers.null
## @lineage: xor.loop.inst.event_handlers.null
## @lineage: xphi.loop.inst.event_handlers.null
## @lineage: bound.adapter.llama.instrumentation.event_handlers.null
## @lineage: bound.adapter.instrumentation.event_handlers.null
## @lineage: anchor.adapter.instrumentation.event_handlers.null
## @lineage: bridge.llama.core.instrumentation.event_handlers.null
from typing import Any
from eco.llama.inst.base.event import BaseEvent
from eco.llama.inst.event_handlers.base import BaseEventHandler

class NullEventHandler(BaseEventHandler):
    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "NullEventHandler"

    def handle(self, event: BaseEvent, **kwargs: Any) -> Any:
        """Handle logic - null handler does nothing."""
        return
