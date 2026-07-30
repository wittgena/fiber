# swarm.atoa.event.conv
## @lineage: atoa.event.conv
import uuid
from typing import Any
from pydantic import Field
from rich.text import Text
from swarm.atoa.conv.event import Event, SourceType
from pydantic import Field, field_validator

class ConversationErrorEvent(Event):
    code: str = Field(description="Code for the error - typically a type")
    detail: str = Field(description="Details about the error")

    @property
    def visualize(self) -> Text:
        content = Text()
        content.append("Conversation Error\n", style="bold")
        content.append("Code: ", style="bold")
        content.append(self.code)
        content.append("\n\nDetail:\n", style="bold")
        content.append(self.detail)
        return content

class LLMCompletionLogEvent(Event):
    source: SourceType = "environment"
    filename: str = Field(
        ...,
        description="The intended filename for this log (relative to log directory)",
    )
    log_data: str = Field(
        ...,
        description="The JSON-encoded log data to be written to the file",
    )
    model_name: str = Field(
        default="unknown",
        description="The model name for context",
    )
    usage_id: str = Field(
        default="default",
        description="The LLM usage_id that produced this log",
    )

    def __str__(self) -> str:
        return (
            f"LLMCompletionLog(usage_id={self.usage_id}, model={self.model_name}, "
            f"file={self.filename})"
        )

class PauseEvent(Event):
    source: SourceType = "user"

    @property
    def visualize(self) -> Text:
        content = Text()
        content.append("Conversation Paused", style="bold")
        return content

    def __str__(self) -> str:
        return f"{self.__class__.__name__} ({self.source}): Agent execution paused"

import uuid
from typing import Any
from pydantic import Field, field_validator
from swarm.atoa.conv.event import Event
from swarm.atoa.conv.event import SourceType

FULL_STATE_KEY = "full_state"

class ConversationStateUpdateEvent(Event):
    source: SourceType = "environment"
    key: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique key for this state update event",
    )
    value: Any = Field(
        default_factory=dict,
        description="Serialized conversation state updates",
    )

    @field_validator("key")
    def validate_key(cls, key: Any) -> str:
        if not isinstance(key, str):
            raise ValueError("Key must be a string")
        return key

    @field_validator("value")
    def validate_value(cls, value: Any, info: Any) -> Any:
        if hasattr(value, "model_dump"):
            try:
                return value.model_dump(mode="json", context={"use_snapshot": True})
            except TypeError:
                return value.model_dump(mode="json")
                
        return value

    @classmethod
    def from_conversation_state(cls, state: Any) -> "ConversationStateUpdateEvent":
        if hasattr(state, "model_dump"):
            state_snapshot = state.model_dump(mode="json", exclude_none=True)
        else:
            state_snapshot = str(state)
            
        return cls(key=FULL_STATE_KEY, value=state_snapshot)

    def __str__(self) -> str:
        return f"ConversationStateUpdate(key={self.key}, value={self.value})"