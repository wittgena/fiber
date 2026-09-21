# fiber.dev.ex.agent.protocol.state
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, TYPE_CHECKING

from fiber.dev.ex.agent.conv.confirm import ConfirmPolicy

class AgentStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    WAITING_FOR_USER = "waiting_for_user"
    FINISHED = "finished"
    ERROR = "error"
    STUCK = "stuck"
    DELETING = "deleting"
    NEEDS_REPLAN = "replan"

    def is_terminal(self) -> bool:
        return self in (
            AgentStatus.FINISHED,
            AgentStatus.ERROR,
            AgentStatus.STUCK,
            AgentStatus.NEEDS_REPLAN
        )

@dataclass(kw_only=True)
class StateCommand:
    reason: str = "No reason provided"

@dataclass(kw_only=True)
class UpdateSecurityPolicy(StateCommand):
    confirmation_policy: ConfirmPolicy | None = None
    security_analyzer: Any = None

@dataclass(kw_only=True)
class TransitionStatus(StateCommand):
    new_status: AgentStatus

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