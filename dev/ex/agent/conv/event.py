# fiber.dev.ex.agent.conv.event
import __future__
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Literal
from pydantic import ConfigDict, Field
from rich.text import Text

from xphi.arch.model.message import ImageContent, BaseMessage, TextContent
from xphi.arch.model.surge.disc import DiscMixin
from xphi.arch.bound.event.next import ToposId, next_id

N_CHAR_PREVIEW = 500

EventType = Literal["action", "observation", "message", "system_prompt", "agent_error"]
SourceType = Literal["agent", "user", "environment", "hook", "watcher"]
EventID = str
ToolCallID = str


class Event(DiscMixin, ABC):
    """Base class for all events."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid", frozen=True)
    id: EventID = Field(
        default_factory=next_id,
        description="Unique event id",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now().isoformat(),
        description="Event timestamp",
    )
    source: SourceType = Field(..., description="The source of this event")

    @property
    def visualize(self) -> Text:
        """Return Rich Text representation of this event."""
        content = Text()
        content.append(f"Unknown event type: {self.__class__.__name__}")
        content.append(f"\n{self.model_dump()}")
        return content

    def __str__(self) -> str:
        """Plain text string representation for display."""
        return f"{self.__class__.__name__} ({self.source})"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"{self.__class__.__name__}(id='{self.id[:8]}...', "
            f"source='{self.source}', timestamp='{self.timestamp}')"
        )

class LLMConvertibleEvent(Event, ABC):
    @abstractmethod
    def to_llm_message(self) -> BaseMessage:
        raise NotImplementedError()

    def __str__(self) -> str:
        """Plain text string representation showing LLM message content."""
        base_str = super().__str__()
        try:
            llm_message = self.to_llm_message()
            text_parts = []
            for content in llm_message.content:
                if isinstance(content, TextContent):
                    text_parts.append(content.text)
                elif isinstance(content, ImageContent):
                    text_parts.append(f"[Image: {len(content.image_urls)} URLs]")

            if text_parts:
                content_preview = " ".join(text_parts)
                # Truncate long content for display
                if len(content_preview) > N_CHAR_PREVIEW:
                    content_preview = content_preview[: N_CHAR_PREVIEW - 3] + "..."
                return f"{base_str}\n  {llm_message.role}: {content_preview}"
            else:
                return f"{base_str}\n  {llm_message.role}: [no text content]"
        except Exception:
            return base_str