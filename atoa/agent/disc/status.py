# atoa.agent.disc.status
## @lineage: atoa.disc.status
## @lineage: agent.disc.status
from enum import Enum
from typing import Any, Protocol, TYPE_CHECKING

class ConverStatus(str, Enum):
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
            ConverStatus.FINISHED,
            ConverStatus.ERROR,
            ConverStatus.STUCK,
            ConverStatus.NEEDS_REPLAN
        )