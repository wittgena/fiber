# atoa.call.action.mcp
## @lineage: agent.call.action.mcp
import json
from typing import Any
import mcp_types
from pydantic import Field
from rich.text import Text

from eco.call.action.message import ImageContent, TextContent
from eco.call.disc.action import Action, Observation
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

def display_dict(data) -> Text:
    content = Text()
    if isinstance(data, dict):
        for field_name, field_value in data.items():
            if field_value is None:
                continue
            content.append(f"\n  {field_name}: ", style="bold")
            if isinstance(field_value, str):
                if "\n" in field_value:
                    content.append("\n")
                    for line in field_value.split("\n"):
                        content.append(f"    {line}\n")
                else:
                    content.append(f'"{field_value}"')
            elif isinstance(field_value, (list, dict)):
                content.append(str(field_value))
            else:
                content.append(str(field_value))
    elif isinstance(data, list):
        content.append(f"[List with {len(data)} items]\n")
        for i, item in enumerate(data):
            content.append(f"  [{i}]: ", style="bold")
            if isinstance(item, str):
                content.append(f'"{item}"\n')
            else:
                content.append(f"{item}\n")
    elif isinstance(data, str):
        if "\n" in data:
            content.append("String:\n")
            for line in data.split("\n"):
                content.append(f"  {line}\n")
        else:
            content.append(f'"{data}"')
    elif data is None:
        content.append("null")
    else:
        content.append(str(data))
    return content

class MCPAction(Action):
    data: dict[str, Any] = Field(default_factory=dict, description="Dynamic data fields from the tool call")
    def to_mcp_arguments(self) -> dict:
        return self.data

class MCPObservation(Observation):
    tool_name: str = Field(description="Name of the tool that was called")

    @classmethod
    def from_call_tool_result(
        cls, tool_name: str, result: mcp_types.CallToolResult
    ) -> "MCPToolObservation":
        """Create an MCPToolObservation from a CallToolResult."""

        native_content: list[mcp_types.ContentBlock] = result.content
        content: list[TextContent | ImageContent] = [
            TextContent(text=f"[Tool '{tool_name}' executed.]")
        ]
        for block in native_content:
            if isinstance(block, mcp_types.TextContent):
                content.append(TextContent(text=block.text))
            elif isinstance(block, mcp_types.ImageContent):
                content.append(
                    ImageContent(
                        image_urls=[f"data:{block.mimeType};base64,{block.data}"],
                    )
                )
            else:
                log.warning(
                    f"Unsupported MCP content block type: {type(block)}. Ignoring."
                )

        return cls(
            content=content,
            is_error=result.isError,
            tool_name=tool_name,
        )

    @property
    def visualize(self) -> Text:
        """Return Rich Text representation of this observation."""
        text = Text()

        if self.is_error:
            text.append("❌ ", style="red bold")
            text.append(self.ERROR_MESSAGE_HEADER, style="bold red")

        text.append(f"[MCP Tool '{self.tool_name}' Observation]\n", style="bold")
        for block in self.content:
            if isinstance(block, TextContent):
                # try to see if block.text is a JSON
                try:
                    parsed = json.loads(block.text)
                    text.append(display_dict(parsed))
                    continue
                except (json.JSONDecodeError, TypeError):
                    text.append(block.text + "\n")
            elif isinstance(block, ImageContent):
                text.append(f"[Image with {len(block.image_urls)} URLs]\n")
        return text
