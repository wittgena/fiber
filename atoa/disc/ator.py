# atoa.disc.ator
## @lineage: agent.disc.ator
## @lineage: meta.agent.disc
## @lineage: meta.ops.agent.disc
from __future__ import annotations
import re
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any
from pydantic import BaseModel, ConfigDict, Field

from atoa.context.ator import AtorContext
from eco.call.disc.tool import Tool
from atoa.disc.schema.reflect import ReflectorBase

from atoa.call.types import ConversationCallbackType, ConversationTokenCallbackType
from atoa.driver.tensor import Driver

from eco.call.mcp.client import MCPClient
from eco.call.mcp.factory import create_mcp_tools
from atoa.call.action.tool.mcp import MCPExecutor
from atoa.call.action.definition import ActionDefinition

from atoa.call.action.factory import CoreAction
from atoa.call.action.resolver import ActionResolver

if TYPE_CHECKING:
    from atoa.context.gov.protocol import ConvStateProtocol
    from atoa.context.gov.context import ConvContext

from arch.topos.surge.disc import DiscMixin
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class Ator(DiscMixin, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    llm: Driver = Field(..., description="LLM configuration for the agent.")
    actions: list[Tool] = Field(
        default_factory=lambda: [Tool(name=action.value, params={}) for action in CoreAction],
        description="List of core cognitive and system control actions (e.g., finish, bridge, think).",
    )
    tools: list[Tool] = Field(
        default_factory=list,
        description="List of external capabilities to initialize (e.g., terminal, browser).",
        examples=[{"name": "terminal", "params": {}},],
    )
    mcp_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional MCP configuration dictionary to create MCP tools.",
    )
    filter_tools_regex: str | None = Field(
        default=None,
        description="Optional regex to filter the external tools available to the agent by name. Core actions are immune to this filter.",
    )
    agent_context: AtorContext = Field(
        default_factory=AtorContext,
        description="AgentContext to manage prompts, secrets, and environment.",
    )
    reflector: ReflectorBase | None = Field(
        default=None,
        description="Optional reflector to evaluate agent actions and messages in real-time.",
    )
    tool_concurrency_limit: int = Field(
        default=1,
        ge=1,
        description="Maximum number of tool calls to execute concurrently within a single agent step.",
    )
    
    runtime_tools: dict[str, ActionDefinition] = Field(default_factory=dict, exclude=True)
    is_initialized: bool = Field(default=False, exclude=True)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def init_state(self, state: "ConvStateProtocol", on_event: ConversationCallbackType) -> None:
        if self.is_initialized:
            log.warning("Agent already initialized; skipping re-initialization.")
            return

        resolved_defs: list[ActionDefinition] = []
        unique_specs: dict[str, Tool] = {}
        for spec in (self.actions + self.tools):
            unique_specs[spec.name] = spec

        combined_specs = self.actions + self.tools
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for spec in combined_specs:
                futures.append(executor.submit(ActionResolver.resolve, spec, state))

            if self.mcp_config:
                futures.append(executor.submit(create_mcp_tools, self.mcp_config, 30))

            for future in futures:
                result = future.result()
                resolved_defs.extend(result)

        if self.filter_tools_regex:
            pattern = re.compile(self.filter_tools_regex)
            resolved_defs = [
                tool for tool in resolved_defs 
                if CoreAction.is_safe_cognitive(tool.name) or pattern.match(tool.name)
            ]
            log.info(f"Filtered tools after applying regex (CoreActions preserved): {[t.name for t in resolved_defs]}")

        for tool in resolved_defs:
            if not isinstance(tool, ActionDefinition):
                raise ValueError(f"Tool {tool} is not an instance of 'ActionDefinition'. Got type: {type(tool)}")

        tool_names = [tool.name for tool in resolved_defs]
        if len(tool_names) != len(set(tool_names)):
            duplicates = set(name for name in tool_names if tool_names.count(name) > 1)
            raise ValueError(f"Duplicate capability names found: {duplicates}")

        self.runtime_tools = {tool.name: tool for tool in resolved_defs}
        self.is_initialized = True
        log.info(f"Successfully initialized Agent '{self.name}' with capabilities: {list(self.runtime_tools.keys())}")
    
    def get_active_mcp_clients(self) -> set[MCPClient]:
        clients: set[MCPClient] = set()
        for tool in self.runtime_tools.values():
            if hasattr(tool, 'executor') and isinstance(tool.executor, MCPExecutor):
                client = tool.executor.client
                clients.add(client)
        return clients

    def close(self) -> None:
        mcp_clients = self.get_active_mcp_clients()
        for client in mcp_clients:
            try:
                # _closed 와 같은 내부 변수 의존도 최소화
                if not getattr(client, "is_closed", False):
                    log.debug("Closing MCP Client explicitly...")
                    client.sync_close()
            except Exception as e:
                log.warning(f"Error while closing MCP client: {e}", exc_info=True)

    @abstractmethod
    def step(
        self,
        conversation: ConvContext,
        on_event: ConversationCallbackType,
        on_token: ConversationTokenCallbackType | None = None,
    ) -> None:
        """Taking a step in the conversation"""

    def verify(self, persisted: Ator, events: Sequence[Any] | None = None) -> Ator:
        if persisted.__class__ is not self.__class__:
            raise ValueError(
                "Cannot load from persisted: persisted agent is of type "
                f"{persisted.__class__.__name__}, but self is of type "
                f"{self.__class__.__name__}."
            )

        runtime_names = {t.name for t in self.actions + self.tools}
        persisted_names = {t.name for t in persisted.actions + persisted.tools}

        missing_in_runtime = persisted_names - runtime_names
        if missing_in_runtime:
            raise ValueError(
                f"Cannot resume conversation: capabilities were removed mid-conversation "
                f"(removed: {sorted(missing_in_runtime)}). "
            )
        return self

    def model_dump_succint(self, **kwargs):
        if "exclude_none" not in kwargs:
            kwargs["exclude_none"] = True
        dumped = super().model_dump(**kwargs)
        if "tools" in dumped and isinstance(dumped["tools"], dict):
            dumped["tools"] = list(dumped["tools"].keys())
        if "actions" in dumped and isinstance(dumped["actions"], dict):
            dumped["actions"] = list(dumped["actions"].keys())
        return dumped

    def get_all_llms(self) -> Generator[Driver]:
        yielded_ids: set[int] = set()
        visited: set[int] = set()

        def _walk(obj: object) -> Iterable[Driver]:
            oid = id(obj)
            if oid in visited:
                return ()
            visited.add(oid)

            if isinstance(obj, Driver):
                llm_out: list[Driver] = []
                if type(obj) is Driver and oid not in yielded_ids:
                    yielded_ids.add(oid)
                    llm_out.append(obj)

                for name in type(obj).model_fields:
                    try:
                        val = getattr(obj, name)
                    except Exception:
                        continue
                    llm_out.extend(_walk(val))
                return llm_out

            if isinstance(obj, BaseModel):
                model_out: list[Driver] = []
                for name in type(obj).model_fields:
                    try:
                        val = getattr(obj, name)
                    except Exception:
                        continue
                    model_out.extend(_walk(val))
                return model_out

            if isinstance(obj, dict):
                dict_out: list[Driver] = []
                for k, v in obj.items():
                    dict_out.extend(_walk(k))
                    dict_out.extend(_walk(v))
                return dict_out

            if isinstance(obj, (list, tuple, set, frozenset)):
                container_out: list[Driver] = []
                for item in obj:
                    container_out.extend(_walk(item))
                return container_out

            return ()
        yield from _walk(self)

    @property
    def tools_map(self) -> dict[str, ActionDefinition]:
        if not self.is_initialized:
            raise RuntimeError("Agent not initialized; call init_state() before use")
        return self.runtime_tools

    def ask(self, question: str) -> str | None:
        return None