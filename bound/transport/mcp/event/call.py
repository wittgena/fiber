# bound.transport.mcp.event.call
## @lineage: bound.protocol.mcp.event.call
## @lineage: bound.bridge.transport.protocol.mcp.event.call
## @lineage: bound.bridge.protocol.mcp.event.call
## @lineage: bound.transport.protocol.mcp.event.call
## @lineage: bound.adapter.protocol.mcp.event.call
## @lineage: bound.bridge.mcp.event.call
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, cast
from bound.surface.legacy.openai.types import OutputItemDoneEvent
from bound.surface.legacy.openai.types import ResponsesAPIStreamEvents
from bound.surface.legacy.openai.types import (
    BaseOpenAIResponse,
    MCPCallArgumentsDeltaEvent,
    MCPCallArgumentsDoneEvent,
    MCPCallCompletedEvent,
    MCPCallFailedEvent,
    MCPCallInProgressEvent,
)
from bound.adapter.switch.params import ResponsesAPIStreamingResponse
from arch.gov.gate import uuid
from watcher.plane.emitter import get_emitter

log = get_emitter("event.call")

def create_mcp_call_events(
    tool_name: str,
    tool_call_id: str,
    arguments: str,
    result: Optional[str] = None,
    base_item_id: Optional[str] = None,
    sequence_start: int = 1,
) -> List[ResponsesAPIStreamingResponse]:
    """Create MCP call events following OpenAI's specification"""
    events: List[ResponsesAPIStreamingResponse] = []
    item_id = base_item_id or f"mcp_{uuid.uuid4().hex[:8]}"

    # MCP call in progress event
    in_progress_event = MCPCallInProgressEvent(
        type=ResponsesAPIStreamEvents.MCP_CALL_IN_PROGRESS,
        sequence_number=sequence_start,
        output_index=0,
        item_id=item_id,
    )
    events.append(in_progress_event)

    # MCP call arguments delta event (streaming the arguments)
    arguments_delta_event = MCPCallArgumentsDeltaEvent(
        type=ResponsesAPIStreamEvents.MCP_CALL_ARGUMENTS_DELTA,
        output_index=0,
        item_id=item_id,
        delta=arguments,  # JSON string with arguments
        sequence_number=sequence_start + 1,
    )
    events.append(arguments_delta_event)

    # MCP call arguments done event
    arguments_done_event = MCPCallArgumentsDoneEvent(
        type=ResponsesAPIStreamEvents.MCP_CALL_ARGUMENTS_DONE,
        output_index=0,
        item_id=item_id,
        arguments=arguments,  # Complete JSON string with finalized arguments
        sequence_number=sequence_start + 2,
    )
    events.append(arguments_done_event)

    # MCP call completed event (or failed if result indicates failure)
    if result is not None:
        completed_event = MCPCallCompletedEvent(
            type=ResponsesAPIStreamEvents.MCP_CALL_COMPLETED,
            sequence_number=sequence_start + 3,
            item_id=item_id,
            output_index=0,
        )
        events.append(completed_event)

        output_item_done_event = OutputItemDoneEvent(
            type=ResponsesAPIStreamEvents.OUTPUT_ITEM_DONE,
            output_index=0,
            item=BaseOpenAIResponse(
                **{
                    "id": item_id,
                    "type": "mcp_call",
                    "approval_request_id": f"mcpr_{uuid.uuid4().hex[:8]}",
                    "arguments": arguments,
                    "error": None,
                    "name": tool_name,
                    "output": result,
                    "server_label": "litellm",
                }
            ),
        )
        events.append(output_item_done_event)
    else:
        failed_event = MCPCallFailedEvent(
            type=ResponsesAPIStreamEvents.MCP_CALL_FAILED,
            sequence_number=sequence_start + 3,
            item_id=item_id,
            output_index=0,
        )
        events.append(failed_event)
    return events