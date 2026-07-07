# xphi.loop.inst.events.synthesis
## @lineage: bound.adapter.llama.instrumentation.events.synthesis
## @lineage: bound.adapter.instrumentation.events.synthesis
## @lineage: anchor.adapter.instrumentation.events.synthesis
from typing import List

from anchor.inter.bound.base.llms.types import ChatMessage
from xphi.loop.inst.events.base import BaseEvent
from anchor.inter.bound.base.response.schema import RESPONSE_TYPE
from anchor.inter.bound.schema import QueryType


class SynthesizeStartEvent(BaseEvent):
    """
    SynthesizeStartEvent.

    Args:
        query (QueryType): Query as a string or query bundle.

    """

    query: QueryType

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "SynthesizeStartEvent"


class SynthesizeEndEvent(BaseEvent):
    """
    SynthesizeEndEvent.

    Args:
        query (QueryType): Query as a string or query bundle.
        response (RESPONSE_TYPE): Response.

    """

    query: QueryType
    response: RESPONSE_TYPE

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "SynthesizeEndEvent"


class GetResponseStartEvent(BaseEvent):
    """
    GetResponseStartEvent.

    Args:
        query_str (str): Query string.
        text_chunks (List[str]): List of text chunks.

    """

    query_str: str
    text_chunks: List[str]

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "GetResponseStartEvent"


class GetResponseEndEvent(BaseEvent):
    """GetResponseEndEvent."""

    # TODO: consumes the first chunk of generators??
    # response: RESPONSE_TEXT_TYPE

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "GetResponseEndEvent"


class GetMessageResponseStartEvent(BaseEvent):
    """
    GetMessageResponseStartEvent.

    Args:
        query_str (str): Query string.
        message_chunks (List[ChatMessage]): List of chat message chunks.

    """

    query_str: str
    message_chunks: List[ChatMessage]

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "GetMessageResponseStartEvent"


class GetMessageResponseEndEvent(BaseEvent):
    """GetMessageResponseEndEvent."""

    # TODO: consumes the first chunk of generators??
    # response: RESPONSE_TEXT_TYPE

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "GetMessageResponseEndEvent"
