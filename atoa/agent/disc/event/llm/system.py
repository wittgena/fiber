# atoa.agent.disc.event.llm.system
## @lineage: atoa.disc.event.llm.system
## @lineage: agent.disc.event.llm.system
import __future__
import json
from pydantic import Field, model_validator
from rich.text import Text
from typing import TYPE_CHECKING, Any
from eco.agent.event.base import N_CHAR_PREVIEW, Event, LLMConvertibleEvent
from eco.agent.event.types import SourceType
from eco.agent.action.message import Message, TextContent
from atoa.agent.action.definition import ActionDefinition

class SystemPromptEvent(LLMConvertibleEvent):
    source: SourceType = "agent"
    system_prompt: TextContent = Field(..., description="The system prompt text")
    actions: list[ActionDefinition] = Field(
        default_factory=list, 
        description="List of actions as ActionDefinition objects"
    )
    tools: list[ActionDefinition] | None = Field(
        default=None, 
        description="Legacy tools input. Slowly migrating to 'actions'."
    )
    dynamic_context: TextContent | None = Field(
        default=None,
        description=(
            "Optional dynamic per-conversation context (runtime info, repo context, "
            "secrets). When provided, this is included as a second content block in "
            "the system message (not cached)."
        ),
    )

    @model_validator(mode='before')
    @classmethod
    def merge_tools_into_actions(cls, data: Any) -> Any:
        if isinstance(data, dict):
            legacy_tools = data.get('tools')
            current_actions = data.get('actions', [])
            if legacy_tools is not None:
                data['actions'] = current_actions + legacy_tools
                data['tools'] = None
        return data

    @property
    def visualize(self) -> Text:
        """Return Rich Text representation of this system prompt event."""
        content = Text()
        content.append("System Prompt:\n", style="bold")
        content.append(self.system_prompt.text)
        if self.dynamic_context:
            content.append("\n\nDynamic Context:\n", style="bold italic")
            content.append(self.dynamic_context.text)
        content.append(f"\n\nTools Available: {len(self.actions)}")
        for tool in self.actions:
            description = tool.description.split("\n")[0][:100]
            if len(description) < len(tool.description):
                description += "..."

            content.append(f"\n  - {tool.name}: {description}\n")
            try:
                params_dict = tool.action_type.to_mcp_schema()
                params_str = json.dumps(params_dict)
                if len(params_str) > 200:
                    params_str = params_str[:197] + "..."
                content.append(f"  Parameters: {params_str}")
            except Exception:
                content.append("  Parameters: <unavailable>")
        return content

    def to_llm_message(self) -> Message:
        if self.dynamic_context:
            return Message(role="system", content=[self.system_prompt, self.dynamic_context])
        return Message(role="system", content=[self.system_prompt])

    def __str__(self) -> str:
        """Plain text string representation for SystemPromptEvent."""
        base_str = f"{self.__class__.__name__} ({self.source})"
        prompt_preview = (
            self.system_prompt.text[:N_CHAR_PREVIEW] + "..."
            if len(self.system_prompt.text) > N_CHAR_PREVIEW
            else self.system_prompt.text
        )
        action_count = len(self.actions)
        context_info = ""
        if self.dynamic_context:
            context_info = (
                f"\n  Dynamic Context: {len(self.dynamic_context.text)} chars"
            )
        return (
            f"{base_str}\n  System: {prompt_preview}\n  "
            f"Actions: {action_count} available{context_info}"
        )

class TokenEvent(Event):
    source: SourceType
    prompt_token_ids: list[int] = Field(
        ..., description="The exact prompt token IDs for this message event"
    )
    response_token_ids: list[int] = Field(
        ..., description="The exact response token IDs for this message event"
    )

