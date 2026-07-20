# xor.loop.inst.events.rerank
## @lineage: xphi.loop.inst.events.rerank
## @lineage: bound.adapter.llama.instrumentation.events.rerank
## @lineage: bound.adapter.instrumentation.events.rerank
## @lineage: anchor.adapter.instrumentation.events.rerank
from typing import List, Optional

from xor.loop.inst.events.base import BaseEvent
from anchor.inter.bound.schema import NodeWithScore, QueryType
from anchor.inter.bound.bridge.pydantic import ConfigDict


class ReRankStartEvent(BaseEvent):
    """
    ReRankStartEvent.

    Args:
        query (QueryType): Query as a string or query bundle.
        nodes (List[NodeWithScore]): List of nodes with scores.
        top_n (int): Number of nodes to return after rerank.
        model_name (str): Name of the model used for reranking.

    """

    model_config = ConfigDict(protected_namespaces=("pydantic_model_",))
    query: Optional[QueryType]
    nodes: List[NodeWithScore]
    top_n: int
    model_name: str

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "ReRankStartEvent"


class ReRankEndEvent(BaseEvent):
    """
    ReRankEndEvent.

    Args:
        nodes (List[NodeWithScore]): List of returned nodes after rerank.

    """

    nodes: List[NodeWithScore]

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "ReRankEndEvent"
