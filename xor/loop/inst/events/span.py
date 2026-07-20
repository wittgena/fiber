# xor.loop.inst.events.span
## @lineage: xphi.loop.inst.events.span
## @lineage: bound.adapter.llama.instrumentation.events.span
## @lineage: bound.adapter.instrumentation.events.span
## @lineage: anchor.adapter.instrumentation.events.span
## @lineage: bridge.llama.core.instrumentation.events.span
from xor.loop.inst.base.event import BaseEvent


class SpanDropEvent(BaseEvent):
    """
    SpanDropEvent.

    Args:
        err_str (str): Error string.

    """

    err_str: str

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "SpanDropEvent"
