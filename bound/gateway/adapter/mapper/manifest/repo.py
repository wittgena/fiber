# bound.gateway.adapter.mapper.manifest.repo
## @lineage: gateway.adapter.mapper.manifest.repo
## @lineage: eco.mapper.manifest.repo
## @lineage: adapter.mapper.manifest.repo
## @lineage: bound.adapter.mapper.manifest.repo
## @lineage: bound.adapter.mapper.repo.manifest
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

@dataclass
class CliParam:
    """@desc: Represents a single CLI parameter specification including its metadata"""
    name: str
    type: str
    required: bool
    help: str

@dataclass
class CliCommand:
    """@desc: Represents a CLI command containing its description and associated parameters"""
    description: str
    params: List[CliParam] = field(default_factory=list)

@dataclass
class CliSpec:
    """@desc: Container mapping command names to their respective CliCommand specifications."""
    commands: Dict[str, CliCommand] = field(default_factory=dict)

@dataclass
class ModuleMeta:
    """@desc: Metadata placeholder for a module, encapsulating both docstrings and CLI specifications"""
    docstring: Optional[str] = None
    cli_spec: Optional[CliSpec] = None

@dataclass
class RepositoryManifest:
    """@desc: root container unifying macro-arch and micro-module metadata"""
    repo_name: str
    arch: Dict[str, str] = field(default_factory=dict)
    modules: Dict[str, ModuleMeta] = field(default_factory=dict)
    tree: Dict[str, Any] = field(default_factory=dict)