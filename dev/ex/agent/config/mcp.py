# fiber.dev.ex.agent.config.mcp
from typing import Dict, List, Optional, Callable, Literal
from pydantic import Field, model_validator
from xphi.arch.model.surge.disc import SurgeBaseModel

class MCPServerConfig(SurgeBaseModel):
    """Configuration for local stdio MCP processes."""
    command: str
    args: List[str] = []
    env: Optional[Dict[str, str]] = None

class MCPConfig(SurgeBaseModel):
    """
    Universal MCP Configuration supporting the 2026-07-28 Spec.
    Handles local 'stdio', modern streamable 'http', and legacy 'sse' transports.
    """
    transport: Literal["stdio", "http", "sse"] = "stdio"
    
    # --- stdio Transport Fields ---
    mcpServers: Optional[Dict[str, MCPServerConfig]] = None
    
    # --- http / sse (Remote) Transport Fields ---
    server_url: Optional[str] = None
    
    # Dynamic header generation (excluded from JSON serialization)
    headers_factory: Optional[Callable[[], Dict[str, str]]] = Field(None, exclude=True)
    
    # Static token authentication support
    auth_token: Optional[str] = None

    @model_validator(mode='after')
    def validate_transport_requirements(self) -> 'MCPConfig':
        """Ensure required fields are present based on the selected transport."""
        if self.transport == "stdio":
            if not self.mcpServers:
                raise ValueError("'mcpServers' is required when transport is 'stdio'.")
        elif self.transport in ["http", "sse"]:
            if not self.server_url:
                raise ValueError(f"'server_url' is required when transport is '{self.transport}'.")
        return self