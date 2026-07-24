# atoa.agent.types
## @lineage: atoa.call.types
## @lineage: agent.call.types
## @lineage: agent.llm.call.types
## @lineage: gov.llm.call.types
## @lineage: gov.gateway.call.types
## @lineage: gov.gate.call.types
## @lineage: gate.call.types
## @lineage: channel.call.types
## @lineage: agent.loop.conv.types
import re
import uuid
from collections.abc import Callable
from typing import Annotated
from pydantic import BaseModel, BeforeValidator, Field
from eco.agent.event.base import Event
from collections.abc import Callable
from eco.legacy.switch.params import ModelResponseStream
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
"""Validated dict of conversation tags.

Keys must be lowercase alphanumeric. Values are arbitrary strings up to 256 chars.
"""


class StuckDetectionThresholds(BaseModel):
    """Configuration for stuck detection thresholds.
    attr:
    - action_observation: Number of repetitions before triggering action-observation loop detection
    - action_error: Number of repetitions before triggering action-error loop detection
    - monologue: Number of consecutive agent messages before triggering monologue detection
    - alternating_pattern: Number of repetitions before triggering alternating pattern detection
    """
    action_observation: int = Field(default=3, ge=1, description="Threshold for action-observation loop detection")
    action_error: int = Field(default=2, ge=1, description="Threshold for action-error loop detection")
    monologue: int = Field(default=2, ge=1, description="Threshold for agent monologue detection")
    alternating_pattern: int = Field(default=3, ge=1, description="Threshold for alternating pattern detection")
