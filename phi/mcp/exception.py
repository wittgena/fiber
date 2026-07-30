# phi.mcp.exception
## @lineage: phi.runtime.mcp.exception
## @lineage: swarm.mesh.mcp.exception
## @lineage: mesh.mcp.exception
## @lineage: eco.fiber.mcp.exception
## @lineage: agent.eco.mcp.exception
## @lineage: eco.agent.mcp.exception
## @lineage: eco.call.mcp.exception
## @lineage: adapter.call.mcp.exception
## @lineage: bound.adapter.call.mcp.exception
## @lineage: gov.policy.mcp.exception

class MCPError(Exception):
    """Base exception for MCP-related errors."""
    pass


class MCPTimeoutError(MCPError):
    """Exception raised when MCP operations timeout."""
    timeout: float
    config: dict | None

    def __init__(self, message: str, timeout: float, config: dict | None = None):
        self.timeout = timeout
        self.config = config
        super().__init__(message)
