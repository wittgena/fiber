# ator.conv.protocol.mcp.config
## @lineage: engine.protocol.mcp.config
## @lineage: engine.atoa.mcp.config
## @lineage: engine.protocol.atoa.mcp.config
## @lineage: phi.agent.atoa.mcp.config
## @lineage: agent.atoa.mcp.config
## @lineage: phi.mcp.config
from typing import Dict, List, Optional
from arch.xor.surge.disc import SurgeBaseModel

class MCPServerConfig(SurgeBaseModel):
    command: str
    args: List[str] = []
    env: Optional[Dict[str, str]] = None

class MCPConfig(SurgeBaseModel):
    mcpServers: Dict[str, MCPServerConfig]