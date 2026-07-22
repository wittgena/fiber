# atoa.agent.disc.memory.codon.registry
## @lineage: atoa.disc.memory.codon.registry
## @lineage: agent.disc.memory.codon.registry
## @lineage: agent.llm.memory.codon.registry
from __future__ import annotations
import os
import re
from collections.abc import Callable
from functools import lru_cache
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, NamedTuple, Final

import frontmatter
from pydantic import BaseModel, Field

from atoa.agent.disc.memory.profile import LLMProfileStore
from eco.agent.residue.depre import warn_deprecated
from watcher.plane.emitter import get_logger

if TYPE_CHECKING:
    from atoa.topos.activator import Activator
    from atoa.agent.driver.tensor import Driver
    from atoa.gov.security.confirm import ConfirmationPolicyBase

logger = get_logger(__name__)

class AtorDefinition(BaseModel):
    """@desc: Defines the configuration and metadata for an Activator."""
    
    name: str = Field(description="Activator name (from frontmatter or filename)")
    description: str = Field(default="", description="Activator description")
    model: str = Field(default="inherit", description="Model to use")
    color: str | None = Field(default=None, description="Display color")
    tools: list[str] = Field(default_factory=list, description="Allowed tools")
    skills: list[str] = Field(default_factory=list, description="Allowed skills")
    system_prompt: str = Field(default="", description="System prompt content")
    source: str | None = Field(default=None, description="Source file path")
    when_to_use_examples: list[str] = Field(default_factory=list)
    permission_mode: str | None = Field(default=None)
    max_iteration_per_run: int | None = Field(default=None, gt=0)
    mcp_servers: dict[str, Any] | None = Field(default=None)
    profile_store_dir: str | None = Field(default=None)
    metadata: dict[str, Any] = Field(default_factory=dict)

    KNOWN_FIELDS: Final[set[str]] = {
        "name", "description", "model", "color", "tools", "skills",
        "max_iteration_per_run", "hooks", "profile_store_dir", 
        "mcp_servers", "permission_mode",
    }
    VALID_MODES: Final[set[str]] = {"always_confirm", "never_confirm", "confirm_risky"}

    def get_confirmation_policy(self) -> ConfirmationPolicyBase | None:
        if not self.permission_mode:
            return None
        if self.permission_mode == "always_confirm":
            from atoa.gov.security.confirm import AlwaysConfirm
            return AlwaysConfirm()
        if self.permission_mode == "never_confirm":
            from atoa.gov.security.confirm import NeverConfirm
            return NeverConfirm()
        if self.permission_mode == "confirm_risky":
            from atoa.gov.security.confirm import ConfirmRisky
            return ConfirmRisky()
        raise AssertionError(f"Unexpected permission_mode: {self.permission_mode}")

    @classmethod
    def load(cls, path: Path) -> AtorDefinition:
        """@desc: 파편화되었던 _extract_* 함수들을 클래스 내부 로직으로 압축 및 캡슐화"""
        with open(path) as f:
            post = frontmatter.load(f)

        fm = post.metadata
        
        # Helper for environment variables
        def resolve_env(val: Any) -> Any:
            if isinstance(val, str): return os.path.expandvars(val)
            if isinstance(val, dict): return {k: resolve_env(v) for k, v in val.items()}
            if isinstance(val, list): return [resolve_env(v) for v in val]
            return val

        raw_tools = fm.get("tools", [])
        tools = [raw_tools] if isinstance(raw_tools, str) else [str(t) for t in raw_tools]
        
        raw_skills = fm.get("skills", [])
        skills = [s.strip() for s in raw_skills.split(",")] if isinstance(raw_skills, str) else [str(s) for s in raw_skills]

        mode = fm.get("permission_mode")
        if mode and str(mode).strip().lower() not in cls.VALID_MODES:
            raise ValueError(f"Invalid permission_mode '{mode}'.")

        mcp = fm.get("mcp_servers")
        if mcp and isinstance(mcp, dict):
            mcp = {k: resolve_env(v) for k, v in mcp.items()}

        desc = str(fm.get("description", ""))
        examples = [m.strip() for m in re.findall(r"<example>(.*?)</example>", desc, re.I | re.S)]

        return cls(
            name=str(fm.get("name", path.stem)),
            description=desc,
            model=str(fm.get("model", "inherit")),
            color=str(fm.get("color")) if fm.get("color") else None,
            tools=tools,
            skills=skills,
            permission_mode=str(mode).strip().lower() if mode else None,
            max_iteration_per_run=int(fm["max_iteration_per_run"]) if "max_iteration_per_run" in fm else None,
            mcp_servers=mcp,
            profile_store_dir=str(fm["profile_store_dir"]) if fm.get("profile_store_dir") else None,
            system_prompt=post.content.strip(),
            source=str(path),
            when_to_use_examples=examples,
            metadata={k: v for k, v in fm.items() if k not in cls.KNOWN_FIELDS},
        )

class AtorFactory(NamedTuple):
    factory_func: Callable[["Driver"], "Ator"]
    definition: AtorDefinition

class AtorRegistry:
    def __init__(self):
        self._factories: dict[str, AtorFactory] = {}
        self._lock = RLock()

    def register(self, name: str, factory_func: Callable, description: str | AtorDefinition, overwrite: bool = False) -> bool:
        definition = description if isinstance(description, AtorDefinition) else AtorDefinition(name=name, description=description)
        with self._lock:
            if name in self._factories and not overwrite:
                return False
            self._factories[name] = AtorFactory(factory_func=factory_func, definition=definition)
            return True

    def get_factory(self, name: str | None) -> AtorFactory:
        factory_name = "general-purpose" if not name else name
        with self._lock:
            factory = self._factories.get(factory_name)
            if not factory:
                raise ValueError(f"Unknown agent '{name}'. Available: {list(self._factories.keys())}")
            return factory

    def get_all_definitions(self) -> list[AtorDefinition]:
        with self._lock:
            return [f.definition for f in self._factories.values()]

    def create_factory_from_def(self, agent_def: AtorDefinition) -> Callable[["Driver"], "Activator"]:
        """@desc: Creates an Activator instantiation closure."""
        def _factory(llm: "Driver") -> "Activator":
            from atoa.topos.activator import Activator
            from atoa.agent.context import AtorContext
            from eco.agent.disc.tool import Tool
            
            if agent_def.model and agent_def.model != "inherit":
                store = self._get_profile_store(agent_def.profile_store_dir)
                llm = store.load(agent_def.model.removesuffix(".json"))
                
            tools = [Tool(name=t) for t in agent_def.tools]
            ctx = AtorContext(system_message_suffix=agent_def.system_prompt) if agent_def.system_prompt else None
            mcp = {"mcpServers": agent_def.mcp_servers} if agent_def.mcp_servers else {}
            
            return Activator(llm=llm, tools=tools, agent_context=ctx, mcp_config=mcp)
        return _factory

    @staticmethod
    @lru_cache(maxsize=32)
    def _get_profile_store(dir_path: str | None) -> LLMProfileStore:
        return LLMProfileStore(dir_path)


class AtorLoader:
    """@desc: File system crawler for Activator definitions."""
    
    DIRS: Final[list[str]] = [".agents/agents"]
    SKIP: Final[set[str]] = {"README.md", "readme.md"}

    @classmethod
    def load_from_dir(cls, base_dir: Path) -> list[AtorDefinition]:
        results = []
        for d in [base_dir / p for p in cls.DIRS]:
            if not d.is_dir(): continue
            for md in sorted(d.iterdir()):
                if md.is_dir() or md.suffix.lower() != ".md" or md.name in cls.SKIP: continue
                try:
                    results.append(AtorDefinition.load(md))
                except Exception:
                    logger.warning(f"Failed to load agent {md}", exc_info=True)
        return results

    @classmethod
    def register_files(cls, work_dir: str | Path, registry: AtorRegistry) -> list[str]:
        project_agents = cls.load_from_dir(Path(work_dir))
        user_agents = cls.load_from_dir(Path.home())
        
        # Deduplicate (Project wins)
        seen = set()
        registered = []
        for agent_def in (project_agents + user_agents):
            if agent_def.name in seen: continue
            seen.add(agent_def.name)
            
            factory = registry.create_factory_from_def(agent_def)
            if registry.register(agent_def.name, factory, agent_def):
                registered.append(agent_def.name)
                logger.info(f"Registered file-based agent '{agent_def.name}'")
        return registered

_GLOBAL_REGISTRY = AtorRegistry()