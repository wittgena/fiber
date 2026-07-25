# atoa.gov.disc.event.conv.state
## @lineage: agent.atoa.disc.event.conv.state
## @lineage: atoa.agent.disc.event.conv.state
## @lineage: atoa.disc.event.conv.state
## @lineage: agent.disc.event.conv.state
import uuid
from typing import Any
from pydantic import Field, field_validator
from eco.fiber.event.base import Event
from eco.fiber.event.types import SourceType

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