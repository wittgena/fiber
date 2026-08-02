# agent.conver.status
## @lineage: actor.conver.status
## @lineage: topos.agent.conver.status
## @lineage: phi.conver.status
## @lineage: swarm.conver.status
## @lineage: agent.status
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