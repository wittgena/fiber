# swarm.mesh.mcp.config
## @lineage: mesh.mcp.config
## @lineage: eco.fiber.mcp.config
## @lineage: agent.eco.mcp.config
## @lineage: eco.agent.mcp.config
## @lineage: eco.call.mcp.config
## @lineage: adapter.call.mcp.config
## @lineage: bound.adapter.call.mcp.config
## @lineage: gov.policy.mcp.config
from typing import Dict, List, Optional
from arch.topos.bound.surge.disc import SurgeBaseModel

class MCPServerConfig(SurgeBaseModel):
    command: str
    args: List[str] = []
    env: Optional[Dict[str, str]] = None

class MCPConfig(SurgeBaseModel):
    mcpServers: Dict[str, MCPServerConfig]