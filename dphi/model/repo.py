# xphi.arch.contract.model.repo
## @lineage: arch.contract.model.repo
## @lineage: arch.model.contract.repo
## @lineage: arch.contract.schema.repo
from dataclasses import dataclass, asdict, field
from typing import Dict
import json

@dataclass
class RepoCommit:
    """
    @role: local node lineage inscription
    @commit: (parent_anchor_id, parent_commit_id)
    """
    anchor_id: str
    parent_anchor_id: str
    parent_commit_id: str
    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(',', ':'))


@dataclass
class AnchorCommit:
    """
    @role: global boundary + alignment surface
    @anchor_commit: (parent_anchor_id, parent_commit_id) + alignment partition (repos / cached_states)
    """
    anchor_id: str
    parent_anchor_id: str
    parent_commit_id: str
    repos: Dict[str, str] = field(default_factory=dict)
    cached_states: Dict[str, str] = field(default_factory=dict)
    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(',', ':'))