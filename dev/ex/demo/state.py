# fiber.dev.ex.demo.state
import uuid
import json
from datetime import datetime
from typing import List, Literal, Union

from pydantic import BaseModel, Field

from fiber.llm.model.message import Message, TextContent, MessageToolCall

# =====================================================================
# 1. Event Definitions (Event Sourcing)
# =====================================================================

class BaseStateEvent(BaseModel):
    """Base class for trajectory events."""
    id: str = Field(default_factory=lambda: uuid.uuid4().hex, description="Unique event ID")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat(), description="ISO8601 timestamp")
    event_type: str = Field(..., description="Event type for routing")

class SystemPromptEvent(BaseStateEvent):
    event_type: Literal["system_prompt"] = "system_prompt"
    content: str

class UserMessageEvent(BaseStateEvent):
    event_type: Literal["user_message"] = "user_message"
    content: str

class AssistantTurnEvent(BaseStateEvent):
    """Preserves the raw LLM response including text and tool calls."""
    event_type: Literal["assistant_turn"] = "assistant_turn"
    message_dump: dict = Field(..., description="Dumped fiber Message dict")

class ToolObservationEvent(BaseStateEvent):
    """Successful tool execution result from the Gateway."""
    event_type: Literal["tool_observation"] = "tool_observation"
    tool_call_id: str = Field(..., description="Target tool call ID")
    tool_name: str
    result: dict

class ToolErrorEvent(BaseStateEvent):
    """Gateway rejection or parsing error mapped into a cognitive context."""
    event_type: Literal["tool_error"] = "tool_error"
    tool_call_id: str = Field(..., description="Target tool call ID")
    tool_name: str
    error_message: str

StateEvent = Union[
    SystemPromptEvent, 
    UserMessageEvent, 
    AssistantTurnEvent, 
    ToolObservationEvent, 
    ToolErrorEvent
]

# =====================================================================
# 2. Agent State Manager (Event Ledger)
# =====================================================================

class AgentState(BaseModel):
    """
    Core ledger for the stateless agent architecture.
    Manages the event trajectory without internal loop states.
    """
    conversation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    events: List[StateEvent] = Field(default_factory=list)

    def add_event(self, event: StateEvent) -> None:
        """Appends an event to the ledger."""
        self.events.append(event)

    def extract_pending_tool_calls(self) -> List[MessageToolCall]:
        """
        Scans the latest assistant turn for tool calls that have no matching
        observation or error events yet.
        """
        pending_calls = []
        resolved_tool_call_ids = {
            e.tool_call_id for e in self.events 
            if isinstance(e, (ToolObservationEvent, ToolErrorEvent))
        }

        # Reverse scan to check the latest turn efficiently
        for event in reversed(self.events):
            if isinstance(event, AssistantTurnEvent):
                msg = Message.model_validate(event.message_dump)
                for content in msg.content:
                    if isinstance(content, MessageToolCall):
                        if content.id not in resolved_tool_call_ids:
                            pending_calls.append(content)
                break  # Only evaluate the most recent turn

        return pending_calls

    def to_llm_messages(self) -> List[Message]:
        """
        Compiles the event ledger into standard LLM messages.
        Separates state management from view (prompt) generation.
        """
        messages: List[Message] = []
        
        for event in self.events:
            if isinstance(event, SystemPromptEvent):
                messages.append(Message(role="system", content=[TextContent(text=event.content)]))
            
            elif isinstance(event, UserMessageEvent):
                messages.append(Message(role="user", content=[TextContent(text=event.content)]))
            
            elif isinstance(event, AssistantTurnEvent):
                messages.append(Message.model_validate(event.message_dump))
            
            elif isinstance(event, ToolObservationEvent):
                messages.append(Message(
                    role="tool",
                    tool_call_id=event.tool_call_id,
                    content=[TextContent(text=json.dumps(event.result))]
                ))
            
            elif isinstance(event, ToolErrorEvent):
                messages.append(Message(
                    role="tool",
                    tool_call_id=event.tool_call_id,
                    content=[TextContent(text=f"Execution Error: {event.error_message}")]
                ))

        return messages