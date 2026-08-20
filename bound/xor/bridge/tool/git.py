# bound.xor.bridge.tool.git
## @lineage: bound.eco.xor.bridge.tool.git
## @lineage: eco.bound.xor.bridge.tool.git
## @lineage: engine.xor.bridge.tool.git
## @lineage: xor.bridge.tool.git
## @lineage: xor.tool.git
## @lineage: arch.xor.bridge.tool.git
## @lineage: arch.gov.tool.git
## @lineage: sandbox.tool.git.models
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
