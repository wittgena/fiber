# bound.transport.mcp.parser.header
## @lineage: bound.protocol.mcp.parser.header
## @lineage: bound.bridge.transport.protocol.mcp.parser.header
## @lineage: bound.bridge.protocol.mcp.parser.header
## @lineage: bound.transport.protocol.mcp.parser.header
## @lineage: bound.adapter.protocol.mcp.parser.header
## @lineage: bound.bridge.mcp.parser.header
## @lineage: bound.adapter.mcp.parser.header
import os
from typing import Dict, Optional
from starlette.datastructures import Headers
from watcher.plane.emitter import get_emitter

log = get_emitter("mcp.header")

class MCPHeaderParser:
    LITELLM_MCP_ACCESS_GROUPS_HEADER_NAME = "x-mcp-access-groups"
    LITELLM_MCP_SERVERS_HEADER_NAME = "x-mcp-servers"
    DEFAULT_CLIENT_SIDE_AUTH_HEADER_NAME = "x-mcp-auth"

    @staticmethod
    def get_mcp_auth_header(headers: Headers) -> Optional[str]:
        # config.yaml의 설정값을 직접 읽지 못하므로, 환경 변수를 우선으로 하되 기본값(Fallback) 사용
        mcp_client_side_auth_header_name = os.getenv(
            "LITELLM_MCP_CLIENT_SIDE_AUTH_HEADER_NAME", 
            MCPHeaderParser.DEFAULT_CLIENT_SIDE_AUTH_HEADER_NAME
        )
        
        auth_header = headers.get(mcp_client_side_auth_header_name)
        if auth_header:
            log.warning(
                f"The '{mcp_client_side_auth_header_name}' header is deprecated. "
                f"Please use server-specific auth headers in the format 'x-mcp-{{server_alias}}-{{header_name}}' instead."
            )
        return auth_header

    @staticmethod
    def get_mcp_server_auth_headers(headers: Headers) -> Dict[str, Dict[str, str]]:
        server_auth_headers: Dict[str, Dict[str, str]] = {}
        prefix = "x-mcp-"
        
        # 소문자로 변환하여 비교 최적화
        skip_headers = {
            MCPHeaderParser.LITELLM_MCP_ACCESS_GROUPS_HEADER_NAME.lower(),
            MCPHeaderParser.LITELLM_MCP_SERVERS_HEADER_NAME.lower(),
        }

        for header_name, header_value in headers.items():
            lower_name = header_name.lower()
            if lower_name.startswith(prefix):
                if lower_name in skip_headers:
                    continue

                remaining = lower_name[len(prefix):]
                if "-" in remaining:
                    parts = remaining.split("-", 1)
                    if len(parts) == 2:
                        server_alias, auth_header_name = parts

                        if auth_header_name == "authorization":
                            auth_header_name = "Authorization"

                        if server_alias not in server_auth_headers:
                            server_auth_headers[server_alias] = {}

                        server_auth_headers[server_alias][auth_header_name] = header_value
                        log.debug(
                            f"Found server auth header: {server_alias} -> {auth_header_name}: {header_value[:10]}..."
                        )

        return server_auth_headers

    @staticmethod
    def get_oauth2_headers(headers: Headers) -> Dict[str, str]:
        oauth2_headers = {}
        for header_name, header_value in headers.items():
            # 기존 로직 유지 (authorization으로 시작하는 헤더 추출)
            if header_name.lower().startswith("authorization"):
                oauth2_headers["Authorization"] = header_value
        return oauth2_headers