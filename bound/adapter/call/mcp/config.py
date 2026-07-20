# bound.adapter.call.mcp.config
## @lineage: gov.policy.mcp.config
from typing import Dict, List, Optional
from arch.topos.surge.disc import SurgeBaseModel

class MCPServerConfig(SurgeBaseModel):
    command: str
    args: List[str] = []
    env: Optional[Dict[str, str]] = None

class MCPConfig(SurgeBaseModel):
    mcpServers: Dict[str, MCPServerConfig]