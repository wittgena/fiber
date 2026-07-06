# bound.adapter.mcp.config
## @lineage: bound.channel.mcp.config
## @lineage: bound.agent.mcp.config
## @lineage: xphi.agent.mcp.config
from typing import Dict, List, Optional
from arch.topos.state.disc import SurgeBaseModel

class MCPServerConfig(SurgeBaseModel):
    command: str
    args: List[str] = []
    env: Optional[Dict[str, str]] = None

class MCPConfig(SurgeBaseModel):
    mcpServers: Dict[str, MCPServerConfig]