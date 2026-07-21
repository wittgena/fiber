# llama.inst.events.exception
## @lineage: xor.loop.inst.events.exception
## @lineage: xphi.loop.inst.events.exception
## @lineage: bound.adapter.llama.instrumentation.events.exception
## @lineage: bound.adapter.instrumentation.events.exception
## @lineage: anchor.adapter.instrumentation.events.exception
from llama.inst.events import BaseEvent


class ExceptionEvent(BaseEvent):
    """
    ExceptionEvent.

    Args:
        exception (BaseException): exception.

    """

    exception: BaseException

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "ExceptionEvent"
