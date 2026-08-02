# agent.state.parser.title
from collections.abc import Sequence

from engine.atoa.event.llm.message import MessageEvent
from engine.atoa.conv.event import Event
from engine.driver.llm.model import LLMModel

from engine.atoa.conv.message import Message, TextContent
from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

CATEGORY_MAP = [
    {"emoji": "💄", "name": "frontend", "description": "UI and style configuration"},
    {"emoji": "👔", "name": "backend", "description": "Core business logic"},
    {"emoji": "✅", "name": "test", "description": "Testing and validation"},
    {"emoji": "👷", "name": "devops", "description": "CI/CD and build systems"},
    {"emoji": "🚀", "name": "deployment", "description": "Release and deployment"},
    {"emoji": "📦️", "name": "dependencies", "description": "Package management"},
    {"emoji": "🗃️", "name": "database", "description": "Schema and data changes"},
    {"emoji": "🔧", "name": "chores", "description": "Routine maintenance"},
    {"emoji": "✨", "name": "features", "description": "New capabilities"},
    {"emoji": "🐛", "name": "bugfix", "description": "Issue resolution"},
    {"emoji": "⚡️", "name": "performance", "description": "Optimization"},
    {"emoji": "🔒️", "name": "security", "description": "Vulnerability patching"},
    {"emoji": "📝", "name": "documentation", "description": "Docs and guides"},
    {"emoji": "♻️", "name": "refactor", "description": "Structural code changes"},
]

def extract_message_text(event: MessageEvent) -> str | None:
    if not event.llm_message.content:
        return None

    text_parts = [
        content.text for content in event.llm_message.content 
        if isinstance(content, TextContent)
    ]
    return " ".join(text_parts).strip() or None


def extract_first_user_message(events: Sequence[Event]) -> str | None:
    ## @phase: extraction, @desc: Locate the initial user trigger in the event stream
    for event in events:
        if isinstance(event, MessageEvent) and event.source == "user":
            if text := extract_message_text(event):
                return text
    return None

def generate_fallback_title(message: str, max_length: int = 50) -> str:
    ## @phase: fallback, Simple text truncation when LLM is unavailable
    title = message.strip()
    return f"{title[:max_length - 3]}..." if len(title) > max_length else title