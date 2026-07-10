# bound.adapter.protocol.mcp.tool.transform
## @lineage: bound.transport.mcp.tool.transform
## @lineage: bound.bridge.mcp.tool.transform
## @lineage: bound.adapter.mcp.tool.transform
import json
from typing import Dict, List, Literal, Union

from openai.types.chat import ChatCompletionToolParam
from openai.types.responses.function_tool_param import FunctionToolParam
from openai.types.shared_params.function_definition import FunctionDefinition

from mcp_types import CallToolRequestParams as MCPCallToolRequestParams
from mcp_types import CallToolResult as MCPCallToolResult
from mcp_types import Tool as MCPTool
from mcp.client.session import ClientSession

from bound.surface.legacy.types import ChatCompletionMessageToolCall

def transform_mcp_tool_to_openai_tool(mcp_tool: MCPTool) -> ChatCompletionToolParam:
    """Convert an MCP tool to an OpenAI tool."""
    normalized_parameters = _normalize_mcp_input_schema(mcp_tool.inputSchema)

    return ChatCompletionToolParam(
        type="function",
        function=FunctionDefinition(
            name=mcp_tool.name,
            description=mcp_tool.description or "",
            parameters=normalized_parameters,
            strict=False,
        ),
    )


def _normalize_mcp_input_schema(input_schema: dict) -> dict:
    if not input_schema:
        return {"type": "object", "properties": {}, "additionalProperties": False}

    normalized_schema = dict(input_schema)
    if "type" not in normalized_schema:
        normalized_schema["type"] = "object"

    if "properties" not in normalized_schema:
        normalized_schema["properties"] = {}

    if "additionalProperties" not in normalized_schema:
        normalized_schema["additionalProperties"] = False

    return normalized_schema


def transform_mcp_tool_to_openai_responses_api_tool(
    mcp_tool: MCPTool,
) -> FunctionToolParam:
    """Convert an MCP tool to an OpenAI Responses API tool."""
    normalized_parameters = _normalize_mcp_input_schema(mcp_tool.inputSchema)

    return FunctionToolParam(
        name=mcp_tool.name,
        parameters=normalized_parameters,
        strict=False,
        type="function",
        description=mcp_tool.description or "",
    )


async def load_mcp_tools(
    session: ClientSession, format: Literal["mcp", "openai"] = "mcp"
) -> Union[List[MCPTool], List[ChatCompletionToolParam]]:
    tools = await session.list_tools()
    if format == "openai":
        return [
            transform_mcp_tool_to_openai_tool(mcp_tool=tool) for tool in tools.tools
        ]
    return tools.tools

async def call_mcp_tool(
    session: ClientSession,
    call_tool_request_params: MCPCallToolRequestParams,
) -> MCPCallToolResult:
    """Call an MCP tool."""
    tool_result = await session.call_tool(
        name=call_tool_request_params.name,
        arguments=call_tool_request_params.arguments,
    )
    return tool_result

def _get_function_arguments(function: FunctionDefinition) -> dict:
    """Helper to safely get and parse function arguments."""
    arguments = function.get("arguments", {})
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except json.JSONDecodeError:
            arguments = {}
    return arguments if isinstance(arguments, dict) else {}


def transform_openai_tool_call_request_to_mcp_tool_call_request(
    openai_tool: Union[ChatCompletionMessageToolCall, Dict],
) -> MCPCallToolRequestParams:
    function = openai_tool["function"]
    return MCPCallToolRequestParams(
        name=function["name"],
        arguments=_get_function_arguments(function),
    )


async def call_openai_tool(
    session: ClientSession,
    openai_tool: ChatCompletionMessageToolCall,
) -> MCPCallToolResult:
    mcp_tool_call_request_params = transform_openai_tool_call_request_to_mcp_tool_call_request(openai_tool=openai_tool)
    return await call_mcp_tool(session=session, call_tool_request_params=mcp_tool_call_request_params,)
