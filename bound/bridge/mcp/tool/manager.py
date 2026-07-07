# bound.bridge.mcp.tool.manager
## @lineage: bound.adapter.mcp.tool.manager
from typing import Any, Dict, List, Optional, Protocol, Tuple, Set
from pydantic import BaseModel, ConfigDict
from watcher.plane.emitter import get_emitter

log = get_emitter("tool.manager")

class BraneAuthContext(BaseModel):
    model_config = ConfigDict(frozen=True)
    user_id: str
    is_admin: bool = False
    allowed_mcp_servers: List[str] = []
    allowed_toolsets: List[str] = []

class ToolsetDefinition(BaseModel):
    id: str
    name: str

class ToolRegistryProtocol(Protocol):
    async def get_server_by_name(self, name: str) -> Optional[Any]: ...
    async def get_toolset_by_name(self, name: str) -> Optional[ToolsetDefinition]: ...
    async def get_servers_for_toolset(self, toolset_id: str) -> List[str]: ...
    async def fetch_tools_from_servers(self, server_names: List[str]) -> List[Any]: ...

class ResolutionStrategy(Protocol):
    async def resolve(self, name: str, auth: BraneAuthContext, registry: ToolRegistryProtocol) -> List[str]:
        ...

class ToolsetResolutionStrategy:
    async def resolve(self, name: str, auth: BraneAuthContext, registry: ToolRegistryProtocol) -> List[str]:
        toolset = await registry.get_toolset_by_name(name)
        if toolset and (auth.is_admin or toolset.id in auth.allowed_toolsets):
            return await registry.get_servers_for_toolset(toolset.id)
        return []

class SingleServerResolutionStrategy:
    async def resolve(self, name: str, auth: BraneAuthContext, registry: ToolRegistryProtocol) -> List[str]:
        server = await registry.get_server_by_name(name)
        if server and (auth.is_admin or name in auth.allowed_mcp_servers):
            return [name]
        return []

class ToolCatalogManager:
    def __init__(self, registry: ToolRegistryProtocol):
        self.registry = registry
        self.strategies: List[ResolutionStrategy] = [
            ToolsetResolutionStrategy(),
            SingleServerResolutionStrategy()
        ]

    async def get_authorized_tools(
        self, auth_context: BraneAuthContext, requested_names: List[str]
    ) -> Tuple[List[Any], List[str]]:
        if not auth_context:
            log.warning("No auth_context provided. Returning empty tools.")
            return [], []

        resolved_server_names: Set[str] = set()

        for name in requested_names:
            resolved = False
            for strategy in self.strategies:
                servers = await strategy.resolve(name, auth_context, self.registry)
                if servers:
                    resolved_server_names.update(servers)
                    resolved = True
                    break # 성공적으로 해석했으면 다음 name으로 이동
            
            if not resolved:
                log.debug(f"User '{auth_context.user_id}' lacks access or '{name}' is invalid. Skipping.")

        effective_server_names = list(resolved_server_names)
        if not effective_server_names:
            return [], []

        try:
            tools = await self.registry.fetch_tools_from_servers(effective_server_names)
        except Exception as e:
            log.error(f"Failed to fetch tools from registry: {e}")
            tools = []
            
        return tools, effective_server_names