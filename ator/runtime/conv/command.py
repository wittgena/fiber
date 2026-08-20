# ator.runtime.conv.command
## @lineage: agent.context.conv.command
from dataclasses import dataclass
from typing import Any

from ator.driver.security.confirm import ConfirmationPolicyBase
from arch.topos.context.status import ConverStatus

@dataclass(kw_only=True)
class StateCommand:
    reason: str = "No reason provided"

@dataclass(kw_only=True)
class UpdateSecurityPolicy(StateCommand):
    confirmation_policy: ConfirmationPolicyBase | None = None
    security_analyzer: Any = None

@dataclass(kw_only=True)
class TransitionStatus(StateCommand):
    new_status: ConverStatus

@dataclass(kw_only=True)
class BlockAction(StateCommand):
    action_id: str

@dataclass(kw_only=True)
class BlockMessage(StateCommand):
    message_id: str

@dataclass(kw_only=True)
class ActivateSkill(StateCommand):
    skill_name: str

@dataclass(kw_only=True)
class UpdateAgentState(StateCommand):
    key: str
    value: Any

@dataclass(kw_only=True)
class UpdateTags(StateCommand):
    tags_to_update: dict[str, str]