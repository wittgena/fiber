# bound.bridge.protocol.mcp.parser.payload
## @lineage: bound.transport.protocol.mcp.parser.payload
import re
import traceback
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)
from copy import deepcopy
import json

from bound.adapter.switch.params import ResponsesAPIResponse, ModelResponse
from mcp_types import EmbeddedResource, ImageContent, TextContent
from bound.surface.legacy.types import Choices
from bound.surface.token.convert import convert_list_message_to_dict
from bound.surface.legacy.param.response import GenericResponseOutputItem, OutputText

from phase.gov.proto.gate import uuid
from watcher.plane.emitter import get_emitter

log = get_emitter("mcp.payload")

ToolParam = Any
LITELLM_PROXY_MCP_SERVER_URL = "litellm_proxy"
LITELLM_PROXY_MCP_SERVER_URL_PREFIX = f"{LITELLM_PROXY_MCP_SERVER_URL}/mcp/"
_PROXY_MCP_PATH_RE = re.compile(r"^https?://.+/mcp/([^/]+)$")

class MCPPayloadParser:
    @staticmethod
    def _parse_mcp_tools(
        tools: Optional[Iterable[ToolParam]],
    ) -> Tuple[List[ToolParam], List[Any]]:
        mcp_tools_with_litellm_proxy: List[ToolParam] = []
        other_tools: List[Any] = []

        if tools:
            for tool in tools:
                if isinstance(tool, dict) and tool.get("type") == "mcp":
                    server_url = tool.get("server_url", "")
                    if isinstance(server_url, str) and server_url.startswith(
                        LITELLM_PROXY_MCP_SERVER_URL
                    ):
                        mcp_tools_with_litellm_proxy.append(tool)
                    elif isinstance(server_url, str):
                        # Also intercept URLs like http://localhost:4000/mcp/atlassian_test
                        # by rewriting them to the internal litellm_proxy format.
                        m = _PROXY_MCP_PATH_RE.match(server_url)
                        if m:
                            rewritten = {
                                **tool,
                                "server_url": f"{LITELLM_PROXY_MCP_SERVER_URL_PREFIX}{m.group(1)}",
                            }
                            mcp_tools_with_litellm_proxy.append(rewritten)
                        else:
                            other_tools.append(tool)
                    else:
                        other_tools.append(tool)
                else:
                    other_tools.append(tool)

        return mcp_tools_with_litellm_proxy, other_tools

    @staticmethod
    def _extract_tool_calls_from_response(response: ResponsesAPIResponse) -> List[Any]:
        """Extract tool calls from the response output."""
        tool_calls: List[Any] = []
        for output_item in response.output:
            # Check if this is a function call output item
            if (
                isinstance(output_item, dict)
                and output_item.get("type") == "function_call"
            ):
                tool_calls.append(output_item)
            elif (
                hasattr(output_item, "type")
                and getattr(output_item, "type") == "function_call"
            ):
                # Handle pydantic model case
                tool_calls.append(output_item)

        return tool_calls

    @staticmethod
    def _extract_tool_calls_from_chat_response(response: ModelResponse) -> List[Any]:
        """Extract tool calls from a chat completion response."""
        tool_calls: List[Any] = []

        try:
            for choice in response.choices:
                message = getattr(choice, "message", None)
                if message is None:
                    continue
                tool_call_entries = getattr(message, "tool_calls", None)
                if tool_call_entries:
                    for tool_call in tool_call_entries:
                        if hasattr(tool_call, "model_dump"):
                            tool_calls.append(tool_call.model_dump())
                        else:
                            tool_calls.append(tool_call)
        except Exception:
            log.exception(
                "Failed to extract tool calls from chat completion response"
            )

        return tool_calls

    @staticmethod
    def _extract_tool_call_details(
        tool_call,
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Extract tool name, arguments, and call_id from a tool call."""
        if isinstance(tool_call, dict):
            tool_call_id = tool_call.get("call_id") or tool_call.get("id")

            # OpenAI chat completions wrap tool info under a `function` block
            function_block = tool_call.get("function")
            if isinstance(function_block, dict):
                tool_name = function_block.get("name")
                tool_arguments = function_block.get("arguments")
            else:
                tool_name = tool_call.get("name")
                tool_arguments = tool_call.get("arguments")
        else:
            tool_call_id = getattr(tool_call, "call_id", None) or getattr(
                tool_call, "id", None
            )

            function_obj = getattr(tool_call, "function", None)
            if function_obj is not None:
                tool_name = getattr(function_obj, "name", None)
                tool_arguments = getattr(function_obj, "arguments", None)
            else:
                tool_name = getattr(tool_call, "name", None)
                tool_arguments = getattr(tool_call, "arguments", None)

        return tool_name, tool_arguments, tool_call_id

    @staticmethod
    def _parse_tool_arguments(tool_arguments: Any) -> Dict[str, Any]:
        """Parse tool arguments, handling both string and dict formats."""
        import json

        if isinstance(tool_arguments, str):
            try:
                return json.loads(tool_arguments)
            except json.JSONDecodeError:
                return {}
        else:
            return tool_arguments or {}

    @staticmethod
    def _parse_mcp_result(result: Any) -> str:
        """Parse MCP tool call result and extract meaningful content."""
        if not result or not hasattr(result, "content") or not result.content:
            return "Tool executed successfully"

        text_parts = []
        other_content_types = []
        for content_item in result.content:
            if isinstance(content_item, TextContent):
                # Text content - extract the text
                text_parts.append(str(content_item.text))
            elif isinstance(content_item, ImageContent):
                # Image content
                other_content_types.append("Image")
            elif isinstance(content_item, EmbeddedResource):
                # Embedded resource
                other_content_types.append("EmbeddedResource")
            else:
                # Other unknown content types
                content_type = type(content_item).__name__
                other_content_types.append(content_type)

        # Combine text parts if any
        result_text = " ".join(text_parts) if text_parts else ""

        # Add info about other content types
        if other_content_types:
            other_info = f"[Generated {', '.join(other_content_types)}]"
            result_text = f"{result_text} {other_info}".strip()

        return result_text or "Tool executed successfully"

    @staticmethod
    def _create_follow_up_messages_for_chat(
        original_messages: List[Any],
        response: ModelResponse,
        tool_results: List[Dict[str, Any]],
    ) -> List[Any]:
        """Create follow-up chat messages that include tool execution results."""
        follow_up_messages: List[Any] = convert_list_message_to_dict(
            deepcopy(original_messages)
        )

        if not follow_up_messages:
            follow_up_messages = []

        message_to_append: Optional[dict] = None
        try:
            first_choice = response.choices[0]
            if isinstance(first_choice, Choices) and getattr(
                first_choice, "message", None
            ):
                message_to_append = first_choice.message.model_dump(exclude_none=True)
                # Ensure tool_calls have arguments field (required by OpenAI API)
                if message_to_append.get("tool_calls"):
                    for tool_call in message_to_append["tool_calls"]:
                        if isinstance(tool_call, dict) and "function" in tool_call:
                            if "arguments" not in tool_call["function"]:
                                tool_call["function"]["arguments"] = "{}"
        except Exception:
            log.exception("Failed to convert assistant message for MCP flow")

        if message_to_append:
            follow_up_messages.append(message_to_append)

        for tool_result in tool_results:
            follow_up_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_result.get("tool_call_id"),
                    "name": tool_result.get("name"),
                    "content": tool_result.get("result", ""),
                }
            )

        return follow_up_messages

    @staticmethod
    def _create_follow_up_input(
        response: ResponsesAPIResponse,
        tool_results: List[Dict[str, Any]],
        original_input: Any = None,
    ) -> List[Any]:
        """Create follow-up input with tool results in proper format."""
        follow_up_input: List[Any] = []

        # Add original user input if available to maintain conversation context
        if original_input:
            if isinstance(original_input, str):
                follow_up_input.append(
                    {"type": "message", "role": "user", "content": original_input}
                )
            elif isinstance(original_input, list):
                follow_up_input.extend(original_input)
            else:
                follow_up_input.append(original_input)

        # Add the assistant message with function calls
        assistant_message_content: List[Any] = []
        function_calls: List[Dict[str, Any]] = []

        for output_item in response.output:
            if not isinstance(output_item, dict) and hasattr(output_item, "model_dump"):
                output_item = output_item.model_dump()

            if isinstance(output_item, dict):
                if output_item.get("type") == "function_call":
                    call_id = output_item.get("call_id") or output_item.get("id")
                    name = output_item.get("name")
                    arguments = output_item.get("arguments")

                    # Only add if we have required fields
                    if call_id and name:
                        function_calls.append(
                            {
                                "type": "function_call",
                                "call_id": call_id,
                                "name": name,
                                "arguments": arguments,
                            }
                        )
                elif output_item.get("type") == "message":
                    # Extract content from message
                    content = output_item.get("content", [])
                    if isinstance(content, list):
                        assistant_message_content.extend(content)
                    else:
                        assistant_message_content.append(content)

        # Add assistant message only if there's actual content (not empty)
        # For example, gemini requires that function call turns come immediately after user turns,
        # so we should not add empty assistant messages
        if assistant_message_content:
            follow_up_input.append(
                {
                    "type": "message",
                    "role": "assistant",
                    "content": assistant_message_content,
                }
            )

        # Add function calls (these can come directly after user message for LLM)
        for function_call in function_calls:
            follow_up_input.append(function_call)

        # Add tool results (function call outputs)
        for tool_result in tool_results:
            follow_up_input.append(
                {
                    "type": "function_call_output",
                    "call_id": tool_result["tool_call_id"],
                    "output": tool_result["result"],
                }
            )

        return follow_up_input

    @staticmethod
    def _build_request_params(
        input: Union[str, Any],
        model: str,
        all_tools: Optional[List[Any]],
        call_params: Dict[str, Any],
        previous_response_id: Optional[str],
        **kwargs,
    ) -> Dict[str, Any]:
        request_params = {
            "input": input,
            "model": model,
            "tools": all_tools,
        }

        # Add previous_response_id if provided
        if previous_response_id is not None:
            request_params["previous_response_id"] = previous_response_id

        request_params.update(call_params)
        request_params.update(kwargs)
        return request_params

    @staticmethod
    def _prepare_initial_call_params(
        call_params: Dict[str, Any], should_auto_execute: bool
    ) -> Dict[str, Any]:
        initial_params = call_params.copy()
        if should_auto_execute:
            # Disable streaming for initial call when auto-executing tools
            initial_params["stream"] = False

        return initial_params

    @staticmethod
    def _prepare_follow_up_call_params(
        call_params: Dict[str, Any], original_stream_setting: bool
    ) -> Dict[str, Any]:
        follow_up_params = call_params.copy()

        # Restore original streaming setting for follow-up call
        follow_up_params["stream"] = original_stream_setting

        # Remove tool_choice since we're providing results, not requesting tool calls
        follow_up_params.pop("tool_choice", None)

        return follow_up_params

    @staticmethod
    def _add_mcp_output_elements_to_response(
        response: ResponsesAPIResponse,
        mcp_tools_fetched: List[Any],
        tool_results: List[Dict[str, Any]],
    ) -> ResponsesAPIResponse:
        """Add custom output elements to the final response for MCP tool execution."""
        mcp_tools_output = GenericResponseOutputItem(
            type="mcp_tools_fetched",
            id=f"mcp_tools_{uuid.uuid4().hex[:8]}",
            status="completed",
            role="system",
            content=[
                OutputText(
                    type="output_text",
                    text=json.dumps(mcp_tools_fetched, indent=2, default=str),
                    annotations=[],
                )
            ],
        )

        # Create output element for tool execution results
        tool_results_output = GenericResponseOutputItem(
            type="tool_execution_results",
            id=f"tool_results_{uuid.uuid4().hex[:8]}",
            status="completed",
            role="system",
            content=[
                OutputText(
                    type="output_text",
                    text=json.dumps(tool_results, indent=2, default=str),
                    annotations=[],
                )
            ],
        )

        # Add the new output elements to the response
        response.output.append(mcp_tools_output.model_dump())  # type: ignore
        response.output.append(tool_results_output.model_dump())  # type: ignore
        return response
