# eco.llama.inst.events.retrieval
## @lineage: adapter.llama.inst.events.retrieval
## @lineage: llama.inst.events.retrieval
## @lineage: xor.loop.inst.events.retrieval
## @lineage: xphi.loop.inst.events.retrieval
## @lineage: bound.adapter.llama.instrumentation.events.retrieval
## @lineage: bound.adapter.instrumentation.events.retrieval
## @lineage: anchor.adapter.instrumentation.events.retrieval
from typing import List
from eco.llama.inst.events.base import BaseEvent
from eco.llama.anchor.bound.schema import QueryType, NodeWithScore


class RetrievalStartEvent(BaseEvent):
    """
    RetrievalStartEvent.

    Args:
        str_or_query_bundle (QueryType): Query bundle.

    """

    str_or_query_bundle: QueryType

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "RetrievalStartEvent"


class RetrievalEndEvent(BaseEvent):
    """
    RetrievalEndEvent.

    Args:
        str_or_query_bundle (QueryType): Query bundle.
        nodes (List[NodeWithScore]): List of nodes with scores.

    """

    str_or_query_bundle: QueryType
    nodes: List[NodeWithScore]

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "RetrievalEndEvent"
