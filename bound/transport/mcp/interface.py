# bound.transport.mcp.interface
## @lineage: bound.protocol.mcp.interface
## @lineage: bound.bridge.transport.protocol.mcp.interface
## @lineage: bound.bridge.protocol.mcp.interface
## @lineage: bound.transport.protocol.mcp.interface
## @lineage: bound.adapter.protocol.mcp.interface
## @lineage: bound.bridge.mcp.interface
## @lineage: bound.adapter.mcp.interface
## @lineage: xphi.adapter.mcp.interface
## @lineage: bound.adapter.legacy.interface
from typing import Protocol, List, Dict, Any, Optional, Tuple

class MCPExecutionProtocol(Protocol):
    """MCP 서버 도구 실행을 추상화한 인터페이스 (향후 MCP 2.0 Client로 연결)"""
    async def call_tool(self, server_name: str, name: str, arguments: dict, **kwargs) -> Any: ...
    def get_mcp_server_by_name(self, name: str) -> Any: ...

class MCPLoggerProtocol(Protocol):
    """LiteLLM LegacyLogManager를 대체하는 순수 로깅 인터페이스"""
    async def log_pre_call(self, **kwargs) -> Tuple[Any, dict]: ...
    async def log_post_call_success(self, **kwargs) -> None: ...
    async def log_failure(self, user_api_key_auth: Any, request_data: dict, error: Exception) -> None: ...

class MCPAuthManager(Protocol):
    """권한 검증을 담당하는 인터페이스"""
    async def get_allowed_server_names(self, api_key: str, requested_servers: Optional[List[str]]) -> List[str]:
        ...
    
    async def verify_tool_access(self, api_key: str, tool_name: str) -> bool:
        ...

class MCPRouteManager(Protocol):
    """실제 MCP 서버와의 통신 및 툴 실행을 담당하는 인터페이스"""
    async def fetch_tools(self, allowed_servers: List[str]) -> List[Dict[str, Any]]:
        ...
        
    async def execute_tool(self, server_name: str, tool_name: str, arguments: Dict[str, Any]) -> Any:
        ...

class MCPLogManager(Protocol):
    """과금, 추적, 에러 로깅을 담당하는 인터페이스"""
    async def log_pre_call(self, request_data: Dict[str, Any]) -> str:
        """호출 전 기록을 남기고 trace_id를 반환"""
        ...
        
    async def log_success(self, trace_id: str, result: Any) -> None:
        ...
        
    async def log_failure(self, trace_id: str, error: Exception) -> None:
        ...