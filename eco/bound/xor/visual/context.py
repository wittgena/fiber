# engine.xor.visual.context
## @lineage: xor.visual.context
## @lineage: ator.context.visualizer
import logging
import re
from collections.abc import Callable
from typing import TYPE_CHECKING
from pydantic import BaseModel

from rich.console import Console, Group
from rich.rule import Rule
from rich.text import Text

from ator.conv.schema.event import Event
from ator.conv.event.acp import ACPToolCallEvent
from ator.conv.event.conv import PauseEvent, ConversationStateUpdateEvent, ConversationErrorEvent
from ator.conv.event.llm.action import ActionEvent
from ator.conv.event.llm.message import MessageEvent
from ator.conv.event.llm.observation import ObservationEvent, AgentErrorEvent, UserRejectObservation
from ator.conv.event.llm.system import SystemPromptEvent
from ator.conv.protocol.state import ConvStateProtocol

if TYPE_CHECKING:
    from ator.conv.context.stats import ConversationStats

logger = logging.getLogger(__name__)

_OBSERVATION_COLOR = "yellow"
_MESSAGE_USER_COLOR = "gold3"
_PAUSE_COLOR = "bright_yellow"
_SYSTEM_COLOR = "magenta"
_THOUGHT_COLOR = "bright_black"
_ERROR_COLOR = "red"
_ACTION_COLOR = "blue"
_MESSAGE_ASSISTANT_COLOR = _ACTION_COLOR

DEFAULT_HIGHLIGHT_REGEX = {
    r"^Reasoning:": f"bold {_THOUGHT_COLOR}",
    r"^Thought:": f"bold {_THOUGHT_COLOR}",
    r"^Action:": f"bold {_ACTION_COLOR}",
    r"^Arguments:": f"bold {_ACTION_COLOR}",
    r"^Tool:": f"bold {_OBSERVATION_COLOR}",
    r"^Result:": f"bold {_OBSERVATION_COLOR}",
    r"^Rejection Reason:": f"bold {_ERROR_COLOR}",
    r"\*\*(.*?)\*\*": "bold",
    r"\*(.*?)\*": "italic",
}

class EventVisualizationConfig(BaseModel):
    title: str | Callable[[Event], str]
    color: str | Callable[[Event], str]
    show_metrics: bool = False
    indent_content: bool = False
    skip: bool = False
    model_config = {"arbitrary_types_allowed": True}

def _abbr_number(n: int | float) -> str:
    """Format numbers into K, M, B formats."""
    n = int(n or 0)
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}".rstrip("0").rstrip(".") + "B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}".rstrip("0").rstrip(".") + "M"
    if n >= 1_000:
        return f"{n / 1_000:.2f}".rstrip("0").rstrip(".") + "K"
    return str(n)

def indent_content(content: Text, spaces: int = 4) -> Text:
    prefix = " " * spaces
    lines = content.split("\n")
    indented = Text()
    for i, line in enumerate(lines):
        if i > 0:
            indented.append("\n")
        indented.append(prefix)
        indented.append(line)
    return indented

def section_header(title: str, color: str) -> Rule:
    return Rule(f"[{color} bold]{title}[/{color} bold]", style=color, characters="─", align="left")

def build_event_block(content: Text, title: str, title_color: str, subtitle: str | None = None, indent: bool = False) -> Group:
    parts = [section_header(title, title_color), Text()]
    parts.append(indent_content(content) if indent else content)
    
    if subtitle:
        parts.append(Text())
        subtitle_text = Text.from_markup(subtitle)
        subtitle_text.stylize("dim")
        parts.append(subtitle_text)
        
    parts.append(Text())
    return Group(*parts)

def _get_action_title(event: Event) -> str:
    if isinstance(event, ActionEvent):
        return "Agent Action (Not Executed)" if event.action is None else "Agent Action"
    return "Action"

def _get_message_title(event: Event) -> str:
    if isinstance(event, MessageEvent) and event.llm_message:
        return "Message from User" if event.llm_message.role == "user" else "Message from Agent"
    return "Message"

def _get_message_color(event: Event) -> str:
    if isinstance(event, MessageEvent) and event.llm_message:
        return _MESSAGE_USER_COLOR if event.llm_message.role == "user" else _MESSAGE_ASSISTANT_COLOR
    return "white"

EVENT_VISUALIZATION_CONFIG: dict[type[Event], EventVisualizationConfig] = {
    ACPToolCallEvent: EventVisualizationConfig(title="ACP Tool Call", color=_ACTION_COLOR),
    SystemPromptEvent: EventVisualizationConfig(title="System Prompt", color=_SYSTEM_COLOR),
    ActionEvent: EventVisualizationConfig(title=_get_action_title, color=_ACTION_COLOR, show_metrics=True),
    ObservationEvent: EventVisualizationConfig(title="Observation", color=_OBSERVATION_COLOR),
    UserRejectObservation: EventVisualizationConfig(title="User Rejected Action", color=_ERROR_COLOR),
    MessageEvent: EventVisualizationConfig(title=_get_message_title, color=_get_message_color, show_metrics=True),
    AgentErrorEvent: EventVisualizationConfig(title="Agent Error", color=_ERROR_COLOR, show_metrics=True),
    PauseEvent: EventVisualizationConfig(title="User Paused", color=_PAUSE_COLOR),
    ConversationErrorEvent: EventVisualizationConfig(title="Conversation Error", color=_ERROR_COLOR, show_metrics=True),
}

class ConversationVisualizer:
    def __init__(self, highlight_regex: dict[str, str] | None = DEFAULT_HIGHLIGHT_REGEX, skip_user_messages: bool = False):
        self._state: "ConvStateProtocol | None" = None
        self._console = Console()
        self._skip_user_messages = skip_user_messages
        self._highlight_patterns = highlight_regex or {}

    def __call__(self, event: Event) -> None:
        """Allows the visualizer instance to be used directly as a callback hook."""
        self.on_event(event)

    def initialize(self, state: ConvStateProtocol) -> None:
        self._state = state

    @property
    def conversation_stats(self) -> "ConversationStats | None":
        return self._state.stats if self._state else None

    def create_sub_visualizer(self, agent_id: str) -> "ConversationVisualizer | None":
        return None

    def on_event(self, event: Event) -> None:
        output = self._create_event_block(event)
        if output:
            self._console.print(output)

    def _apply_highlighting(self, text: Text) -> Text:
        if not self._highlight_patterns:
            return text
        highlighted = text.copy()
        for pattern, style in self._highlight_patterns.items():
            pattern_compiled = re.compile(pattern, re.MULTILINE)
            highlighted.highlight_regex(pattern_compiled, style)
        return highlighted

    def _create_event_block(self, event: Event) -> Group | None:
        config = EVENT_VISUALIZATION_CONFIG.get(type(event))

        if not config:
            logger.warning("Event type %s is not registered in EVENT_VISUALIZATION_CONFIG.", event.__class__.__name__)
            return None

        if config.skip:
            return None

        if (self._skip_user_messages and isinstance(event, MessageEvent) 
            and event.llm_message and event.llm_message.role == "user"):
            return None

        content = event.visualize
        if not content.plain.strip():
            return None

        if self._highlight_patterns:
            content = self._apply_highlighting(content)

        title = config.title(event) if callable(config.title) else config.title
        title_color = config.color(event) if callable(config.color) else config.color
        subtitle = self._format_metrics_subtitle() if config.show_metrics else None
        
        return build_event_block(content=content, title=title, title_color=title_color, subtitle=subtitle)

    def _format_metrics_subtitle(self) -> str | None:
        stats = self.conversation_stats
        if not stats:
            return None

        combined_metrics = stats.get_combined_metrics()
        if not combined_metrics or not combined_metrics.accumulated_token_usage:
            return None

        usage = combined_metrics.accumulated_token_usage
        cost = combined_metrics.accumulated_cost or 0.0

        input_tokens = _abbr_number(usage.prompt_tokens)
        output_tokens = _abbr_number(usage.completion_tokens)
        prompt = usage.prompt_tokens or 0
        cache_read = usage.cache_read_tokens or 0
        cache_rate = f"{(cache_read / prompt * 100):.2f}%" if prompt > 0 else "N/A"
        reasoning_tokens = usage.reasoning_tokens or 0
        cost_str = f"{cost:.4f}" if cost > 0 else "0.00"

        parts = [
            f"[cyan]↑ input {input_tokens}[/cyan]",
            f"[magenta]cache hit {cache_rate}[/magenta]"
        ]
        if reasoning_tokens > 0:
            parts.append(f"[yellow] reasoning {_abbr_number(reasoning_tokens)}[/yellow]")
        parts.append(f"[blue]↓ output {output_tokens}[/blue]")
        parts.append(f"[green]$ {cost_str}[/green]")
        return "Tokens: " + " • ".join(parts)