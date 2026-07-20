# xor.loop.inst.events.query
## @lineage: xphi.loop.inst.events.query
## @lineage: bound.adapter.llama.instrumentation.events.query
## @lineage: bound.adapter.instrumentation.events.query
## @lineage: anchor.adapter.instrumentation.events.query
from xor.loop.inst.events.base import BaseEvent
from anchor.inter.bound.base.response.schema import RESPONSE_TYPE
from anchor.inter.bound.schema import QueryType


class QueryStartEvent(BaseEvent):
    """
    QueryStartEvent.

    Args:
        query (QueryType): Query as a string or query bundle.

    """

    query: QueryType

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "QueryStartEvent"


class QueryEndEvent(BaseEvent):
    """
    QueryEndEvent.

    Args:
        query (QueryType): Query as a string or query bundle.
        response (RESPONSE_TYPE): Response.

    """

    query: QueryType
    response: RESPONSE_TYPE

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "QueryEndEvent"
