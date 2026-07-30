# swarm.atoa.conv.types
## @lineage: atoa.conv.types
## @lineage: eco.tenant.conv.types
## @lineage: atoa.types
import re
import uuid
from collections.abc import Callable
from typing import Annotated
from pydantic import BaseModel, BeforeValidator, Field
from collections.abc import Callable

from swarm.atoa.conv.event import Event
from tenant.client.switch.params import ModelResponseStream
from arch.contract.event.next import ToposId

LLMStreamChunk = ModelResponseStream
TokenCallbackType = Callable[[LLMStreamChunk], None]
ConversationCallbackType = Callable[[Event], None]
ConversationTokenCallbackType = TokenCallbackType
ConversationID = ToposId
TAG_KEY_PATTERN = re.compile(r"^[a-z0-9]+$")
TAG_VALUE_MAX_LENGTH = 256

def _validate_tags(v: dict[str, str] | None) -> dict[str, str]:
    if v is None:
        return {}
    for key, value in v.items():
        if not TAG_KEY_PATTERN.match(key):
            raise ValueError(
                f"Tag key '{key}' is invalid: keys must be lowercase alphanumeric only"
            )
        if len(value) > TAG_VALUE_MAX_LENGTH:
            raise ValueError(
                f"Tag value for '{key}' exceeds maximum length of "
                f"{TAG_VALUE_MAX_LENGTH} characters"
            )
    return v


ConversationTags = Annotated[dict[str, str], BeforeValidator(_validate_tags)]

class StuckDetectionThresholds(BaseModel):
    action_observation: int = Field(default=3, ge=1, description="Threshold for action-observation loop detection")
    action_error: int = Field(default=2, ge=1, description="Threshold for action-error loop detection")
    monologue: int = Field(default=2, ge=1, description="Threshold for agent monologue detection")
    alternating_pattern: int = Field(default=3, ge=1, description="Threshold for alternating pattern detection")
