# bound.bridge.mcp.event.tool
## @lineage: bound.adapter.mcp.event.tool
## @lineage: xphi.adapter.mcp.event.tool
## @lineage: bound.adapter.mcp.legacy.stream
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, cast
from bound.surface.legacy.openai.types import OutputItemDoneEvent
from bound.surface.legacy.openai.types import ResponsesAPIStreamEvents
from bound.surface.legacy.openai.types import (
    BaseOpenAIResponse,
    MCPListToolsCompletedEvent,
    MCPListToolsFailedEvent,
    MCPListToolsInProgressEvent,
)
from anchor.bind.switch.params import ResponsesAPIStreamingResponse, ToolParam
from watcher.plane.emitter import get_emitter

log = get_emitter("tool.event")

async def create_mcp_list_tools_events(
    mcp_tools_with_litellm_proxy: List[ToolParam],
    user_api_key_auth: Any,
    base_item_id: str,
    pre_processed_mcp_tools: List[Any],
) -> List[ResponsesAPIStreamingResponse]:
    events: List[ResponsesAPIStreamingResponse] = []
    try:
        mcp_servers = []
        for tool in mcp_tools_with_litellm_proxy:
            if isinstance(tool, dict) and "server_url" in tool:
                server_url = tool.get("server_url")
                if isinstance(server_url, str) and server_url.startswith(
                    "litellm_proxy/mcp/"
                ):
                    server_name = server_url.split("/")[-1]
                    mcp_servers.append(server_name)

        # Emit list tools in progress event
        in_progress_event = MCPListToolsInProgressEvent(
            type=ResponsesAPIStreamEvents.MCP_LIST_TOOLS_IN_PROGRESS,
            sequence_number=1,
            output_index=0,
            item_id=base_item_id,
        )
        events.append(in_progress_event)

        # Use the pre-processed MCP tools that were already fetched, filtered, and deduplicated by the parent
        filtered_mcp_tools = pre_processed_mcp_tools

        # Convert tools to dict format for the event
        mcp_tools_dict = []
        for tool in filtered_mcp_tools:
            if hasattr(tool, "model_dump") and callable(getattr(tool, "model_dump")):
                # Type cast to help mypy understand this is safe after hasattr check
                mcp_tools_dict.append(cast(Any, tool).model_dump())
            elif hasattr(tool, "__dict__"):
                mcp_tools_dict.append(tool.__dict__)
            else:
                mcp_tools_dict.append({"name": getattr(tool, "name", str(tool))})

        # Emit list tools completed event
        completed_event = MCPListToolsCompletedEvent(
            type=ResponsesAPIStreamEvents.MCP_LIST_TOOLS_COMPLETED,
            sequence_number=2,
            output_index=0,
            item_id=base_item_id,
        )
        events.append(completed_event)

        # Extract server label from the first MCP tool config
        server_label = ""
        if mcp_tools_with_litellm_proxy:
            first_tool = mcp_tools_with_litellm_proxy[0]
            if isinstance(first_tool, dict):
                server_label_value = first_tool.get("server_label", "")
                server_label = (
                    str(server_label_value) if server_label_value is not None else ""
                )

        # Format tools for OpenAI output_item.done format
        formatted_tools = []
        for tool in filtered_mcp_tools:
            tool_dict = {
                "name": getattr(tool, "name", "unknown"),
                "description": getattr(tool, "description", ""),
                "annotations": {"read_only": False},
            }

            # Add input_schema if available
            if hasattr(tool, "inputSchema"):
                tool_dict["input_schema"] = getattr(tool, "inputSchema")
            elif hasattr(tool, "input_schema"):
                tool_dict["input_schema"] = getattr(tool, "input_schema")

            formatted_tools.append(tool_dict)

        # Create the output_item.done event with MCP tools list
        output_item_done_event = OutputItemDoneEvent(
            type=ResponsesAPIStreamEvents.OUTPUT_ITEM_DONE,
            output_index=0,
            item=BaseOpenAIResponse(
                **{
                    "id": base_item_id,
                    "type": "mcp_list_tools",
                    "server_label": server_label,
                    "tools": formatted_tools,
                }
            ),
        )
        events.append(output_item_done_event)

        log.debug(f"Created {len(events)} MCP discovery events")

    except Exception as e:
        log.error(f"Error creating MCP list tools events: {e}")
        import traceback

        traceback.print_exc()

        # Emit failed event on error
        failed_event = MCPListToolsFailedEvent(
            type=ResponsesAPIStreamEvents.MCP_LIST_TOOLS_FAILED,
            sequence_number=2,
            output_index=0,
            item_id=base_item_id,
        )
        events.append(failed_event)

        output_item_done_event = OutputItemDoneEvent(
            type=ResponsesAPIStreamEvents.OUTPUT_ITEM_DONE,
            output_index=0,
            item=BaseOpenAIResponse(
                **{
                    "id": base_item_id,
                    "type": "mcp_list_tools",
                    "server_label": "",
                    "tools": [],
                }
            ),
        )
        events.append(output_item_done_event)
    return events