# fiber.dev.ex.agent.config.mcp
from typing import Dict, List, Optional
from xphi.arch.model.surge.disc import SurgeBaseModel

class MCPServerConfig(SurgeBaseModel):
    command: str
    args: List[str] = []
    env: Optional[Dict[str, str]] = None

class MCPConfig(SurgeBaseModel):
    mcpServers: Dict[str, MCPServerConfig]