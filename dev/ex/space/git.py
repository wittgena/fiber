# fiber.dev.ex.space.git
## @lineage: theoria.phase.agent.space.git
## @lineage: xphi.bound.space.git.schema
from enum import Enum
from pathlib import Path
from pydantic import BaseModel

class GitChangeStatus(Enum):
    MOVED = "MOVED"
    ADDED = "ADDED"
    DELETED = "DELETED"
    UPDATED = "UPDATED"

class GitChange(BaseModel):
    status: GitChangeStatus
    path: Path

class GitDiff(BaseModel):
    modified: str | None
    original: str | None
