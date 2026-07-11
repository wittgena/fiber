# bound.bridge.protocol.mcp.handler
## @lineage: bound.transport.protocol.mcp.handler
## @lineage: bound.adapter.protocol.mcp.handler
## @lineage: bound.transport.mcp.handler
## @lineage: bound.bridge.mcp.handler
## @lineage: bound.adapter.mcp.handler
## @lineage: xphi.adapter.mcp.handler
"""
@phase: Adapter MCP
@desc: Translates MCP protocols into Brane-native topologies and safely executes tools.
@invariant: Strictly zero circular dependencies. No imports from bound.channel.action.*
"""
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Iterable
from fastapi import HTTPException

from bound.surface.exception import BlockedPiiEntityError, GuardrailRaisedException
from bound.surface.legacy.types import CallTypes, StandardLoggingMCPToolCall
from bound.bridge.protocol.mcp.tool.transform import transform_mcp_tool_to_openai_responses_api_tool, transform_mcp_tool_to_openai_tool
from bound.bridge.protocol.mcp.parser.payload import MCPPayloadParser
from bound.bridge.protocol.mcp.tool.manager import ToolCatalogManager
from bound.bridge.protocol.mcp.interface import MCPExecutionProtocol, MCPLoggerProtocol

from phase.gov.proto.gate import uuid
from watcher.plane.emitter import get_emitter

log = get_emitter("mcp.handler")

PROXY_MCP_SERVER_URL = "litellm_proxy"
PROXY_MCP_SERVER_URL_PREFIX = f"{PROXY_MCP_SERVER_URL}/mcp/"

global_brane_catalog: Optional[ToolCatalogManager] = None
global_brane_executor: Optional[MCPExecutionProtocol] = None
global_brane_logger: Optional[MCPLoggerProtocol] = None

def split_server_prefix_from_name(prefixed_name: str) -> Tuple[str, str]:
    if MCP_TOOL_PREFIX_SEPARATOR in prefixed_name:
        parts = prefixed_name.split(MCP_TOOL_PREFIX_SEPARATOR, 1)
        if len(parts) == 2:
            return parts[1], parts[0]
    return prefixed_name, ""

class MCPToolCompiler:
    @staticmethod
    def filter_by_allowed(tools: List[Any], allowed_configs: List[Any]) -> List[Any]:
        """@desc: Pure function to filter tools based on explicit proxy configurations."""
        allowed_tool_names = set()
        for config in allowed_configs:
            if isinstance(config, dict) and "allowed_tools" in config:
                allowed = config.get("allowed_tools", [])
                if isinstance(allowed, list):
                    allowed_tool_names.update(allowed)

        if not allowed_tool_names:
            return tools

        filtered_tools = []
        for mcp_tool in tools:
            tool_name = mcp_tool.get("name") if isinstance(mcp_tool, dict) else getattr(mcp_tool, "name", None)
            if not tool_name:
                continue

            if tool_name in allowed_tool_names:
                filtered_tools.append(mcp_tool)
                continue

            unprefixed_name, _ = split_server_prefix_from_name(tool_name)
            if unprefixed_name in allowed_tool_names:
                filtered_tools.append(mcp_tool)

        return filtered_tools

    @staticmethod
    def deduplicate(tools: List[Any], allowed_servers: List[str]) -> Tuple[List[Any], Dict[str, str]]:
        """@desc: Deduplicates tools by name and maps them to their origin server."""
        seen_names = set()
        deduplicated_tools = []
        tool_server_map: Dict[str, str] = {}

        for tool in tools:
            tool_name = tool.get("name") if isinstance(tool, dict) else getattr(tool, "name", None)

            if tool_name and tool_name not in seen_names:
                seen_names.add(tool_name)
                deduplicated_tools.append(tool)
                if len(allowed_servers) == 1:
                    tool_server_map[tool_name] = allowed_servers[0]
                else:
                    _, tool_server_map[tool_name] = split_server_prefix_from_name(tool_name)

        return deduplicated_tools, tool_server_map

    @staticmethod
    def transform_to_openai(mcp_tools: List[Any], target_format: str = "responses") -> List[Any]:
        """@desc: Compiles MCP structure to native OpenAI interface structure."""
        openai_tools = []
        for mcp_tool in mcp_tools:
            if target_format == "chat":
                openai_tool = transform_mcp_tool_to_openai_tool(mcp_tool)
            else:
                openai_tool = transform_mcp_tool_to_openai_responses_api_tool(mcp_tool)
            openai_tools.append(openai_tool)
        return openai_tools

class MCPToolExecutor:
    @staticmethod
    async def execute_all(
        tool_calls: List[Any], 
        server_map: Dict[str, str], 
        auth_context: Any, 
        **kwargs
    ) -> List[Dict[str, Any]]:
        results = []
        for call in tool_calls:
            result = await MCPToolExecutor._execute_single(call, server_map, auth_context, **kwargs)
            results.append(result)
        return results

    @staticmethod
    async def _execute_single(
        call: Any, 
        server_map: Dict[str, str], 
        auth_context: Any, 
        **kwargs
    ) -> Dict[str, Any]:
        """
        @desc: Executes a single tool. Captures all dimensional ruptures (Exceptions) 
               and converts them into stable return states. Never calls higher topologies.
        """
        async def _safe_log_failure(auth, req_data, err):
            if global_brane_logger: 
                await global_brane_logger.log_failure(auth, req_data, err)

        try:
            name, args, call_id = MCPPayloadParser._extract_tool_call_details(call)[:3]
            if not name:
                log.warning(f"Tool call missing name: {call}")
                return {"tool_call_id": "unknown", "result": "Missing tool name", "name": "unknown"}

            parsed_args = MCPPayloadParser._parse_tool_arguments(args)
            server_name = server_map.get(name, "unknown")

            sanitized_name = name
            unprefixed_name, prefixed_server = split_server_prefix_from_name(name)
            if prefixed_server == server_name and unprefixed_name:
                sanitized_name = unprefixed_name

            start_time = datetime.now()
            tool_logging_call_id = kwargs.get("call_id") or str(uuid.uuid4())
            
            # Request schema for telemetry
            logging_request_data = {
                "model": f"MCP: {name}",
                "metadata": {
                    "tool_call_id": call_id,
                    "tool_name": sanitized_name,
                    "server_name": server_name,
                },
                "input": [{"role": "tool", "content": {"tool_name": sanitized_name, "arguments": parsed_args}}],
                "call_type": CallTypes.call_mcp_tool.value,
                "call_id": tool_logging_call_id,
            }
            if kwargs.get("litellm_trace_id"):
                logging_request_data["litellm_trace_id"] = kwargs.get("litellm_trace_id")

            # 1. Telemetry Pre-call
            log_delegator = None
            if global_brane_logger:
                log_delegator, logging_request_data = await global_brane_logger.log_pre_call(
                    tool_name=name,
                    logging_request_data=logging_request_data,
                    logging_input=logging_request_data["input"],
                    start_time=start_time
                )

            # 2. Membrane Breach: Actual Execution
            if not global_brane_executor:
                raise RuntimeError("global_brane_executor is not initialized.")

            result = await global_brane_executor.call_tool(
                server_name=server_name,
                name=sanitized_name,
                arguments=parsed_args,
                user_api_key_auth=auth_context,
                mcp_auth_header=kwargs.get("mcp_auth_header"),
                mcp_server_auth_headers=kwargs.get("mcp_server_auth_headers"),
                oauth2_headers=kwargs.get("oauth2_headers"),
                raw_headers=kwargs.get("raw_headers"),
            )

            # 3. Telemetry Post-call Success
            if global_brane_logger and log_delegator:
                await global_brane_logger.log_post_call_success(
                    log_delegator=log_delegator,
                    result=result,
                    start_time=start_time,
                    tool_name=name
                )

            result_text = MCPPayloadParser._parse_mcp_result(result)
            return {"tool_call_id": call_id, "result": result_text, "name": name}

        # 4. State Defenses (Exception Handling)
        except BlockedPiiEntityError as e:
            await _safe_log_failure(auth_context, logging_request_data, e)
            return {"tool_call_id": call_id, "result": f"Blocked: PII {getattr(e, 'entity_type', 'unknown')}", "name": name}
        except GuardrailRaisedException as e:
            await _safe_log_failure(auth_context, logging_request_data, e)
            return {"tool_call_id": call_id, "result": f"Blocked: Guardrail violation. {str(e)}", "name": name}
        except HTTPException as e:
            await _safe_log_failure(auth_context, logging_request_data, e)
            return {"tool_call_id": call_id, "result": f"Failed: {str(e.detail) if hasattr(e, 'detail') else str(e)}", "name": name}
        except Exception as e:
            await _safe_log_failure(auth_context, logging_request_data, e)
            log.exception(f"Error executing MCP tool call: {e}")
            return {"tool_call_id": call_id, "result": f"Error executing tool: {str(e)}", "name": name}

class MCPHandler:
    @staticmethod
    def _convert_to_auth_context(user_api_key_auth: Any):
        from bound.adapter.proxy.reverse import BraneAuthContext
        user_id = getattr(user_api_key_auth, "user_id", None) or getattr(user_api_key_auth, "end_user_id", "anonymous")
        user_role = getattr(user_api_key_auth, "user_role", None)
        is_admin = user_role in ["admin", "proxy_admin"]
        
        op = getattr(user_api_key_auth, "object_permission", None)
        mcp_servers = getattr(op, "mcp_servers", []) if op else []
        mcp_toolsets = getattr(op, "mcp_toolsets", []) if op else []
        
        return BraneAuthContext(
            user_id=user_id, 
            is_admin=is_admin, 
            allowed_mcp_servers=mcp_servers, 
            allowed_toolsets=mcp_toolsets
        )

    @staticmethod
    async def _process_mcp_tools_without_openai_transform(
        user_api_key_auth: Any,
        mcp_tools_with_litellm_proxy: List[Any],
        **kwargs
    ) -> Tuple[List[Any], Dict[str, str]]:
        if not global_brane_catalog or not mcp_tools_with_litellm_proxy:
            return [], {}
            
        mcp_servers = []
        for _tool in mcp_tools_with_litellm_proxy:
            server_url = _tool.get("server_url", "") if isinstance(_tool, dict) else ""
            if isinstance(server_url, str) and server_url.startswith(PROXY_MCP_SERVER_URL_PREFIX):
                mcp_servers.append(server_url.split("/")[-1])

        brane_auth = MCPHandler._convert_to_auth_context(user_api_key_auth)
        
        # 1. Fetch from catalog
        fetched_tools, allowed_servers = await global_brane_catalog.get_authorized_tools(
            auth_context=brane_auth,
            requested_names=mcp_servers
        )
        
        # 2. Compile: Filter
        filtered_tools = MCPToolCompiler.filter_by_allowed(fetched_tools, mcp_tools_with_litellm_proxy)
        
        # 3. Compile: Deduplicate
        return MCPToolCompiler.deduplicate(filtered_tools, allowed_servers)

    @staticmethod
    def _transform_mcp_tools_to_openai(mcp_tools: List[Any], target_format: str = "responses") -> List[Any]:
        return MCPToolCompiler.transform_to_openai(mcp_tools, target_format)

    @staticmethod
    def _should_auto_execute_tools(tools: List[Any]) -> bool:
        for tool in tools:
            if isinstance(tool, dict) and tool.get("require_approval") == "never":
                return True
            elif getattr(tool, "require_approval", None) == "never":
                return True
        return False
    
    @staticmethod
    def _should_use_litellm_mcp_gateway(tools: Optional[Iterable[Any]]) -> bool:
        """@desc: 요청된 도구 중 MCP 프록시 서버 라우팅이 필요한 도구가 있는지 검사"""
        if not tools:
            return False
            
        _PROXY_MCP_PATH_RE = re.compile(r"^https?://.+/mcp/([^/]+)$")
        for tool in tools:
            if isinstance(tool, dict) and tool.get("type") == "mcp":
                server_url = tool.get("server_url", "")
                if isinstance(server_url, str):
                    if server_url.startswith(PROXY_MCP_SERVER_URL) or _PROXY_MCP_PATH_RE.match(server_url):
                        return True
        return False

    @staticmethod
    async def _execute_tool_calls(
        tool_server_map: dict, 
        tool_calls: List[Any], 
        user_api_key_auth: Any, 
        **kwargs
    ) -> List[Dict[str, Any]]:
        # Delegation to safe internal executor
        return await MCPToolExecutor.execute_all(tool_calls, tool_server_map, user_api_key_auth, **kwargs)