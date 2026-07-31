# phi.conver.state.parser.title
## @lineage: swarm.mesh.parser.title
## @lineage: swarm.mesh.conv.parser.title
## @lineage: swarm.mesh.engine.conv.parser.title
## @lineage: mesh.engine.conv.parser.title
## @lineage: gov.conv.parser.title
## @lineage: gov.atoa.parser.conv.title
## @lineage: bound.parser.atoa.conv.title
## @lineage: atoa.agent.conv.parser.title
## @lineage: agent.conv.parser.title
## @lineage: atoa.conv.parser.title
## @lineage: atoa.gov.context.message.parser.title
## @lineage: atoa.context.gov.message.parser.title
## @lineage: gov.conv.message.parser.title
## @lineage: gov.conv.message.title
"""
@desc: Module for generating concise, emoji-prefixed conversation titles using an LLM.
@flow: Extract initial user message -> Build contextual prompt -> Generate title via LLM -> Fallback to text truncation.
"""
from collections.abc import Sequence

from atoa.event.llm.message import MessageEvent
from atoa.conv.event import Event
from phi.engine.driver.tensor import Driver

from atoa.conv.message import Message, TextContent
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


def generate_title_with_llm(message: str, llm: Driver, max_length: int = 50) -> str | None:
    ## @phase: generation, @desc: Request LLM to synthesize a title based on context
    truncated_message = f"{message[:1000]}...(truncated)" if len(message) > 1000 else message
    category_prompt = "\n- ".join(
        f"{c['emoji']} {c['name']}: {c['description']}" for c in CATEGORY_MAP
    )

    try:
        messages = [
            Message(
                role="system",
                content=[
                    TextContent(
                        text=(
                            "You are a title generation engine. Your task is to analyze the "
                            "user's initial request and generate a concise, descriptive title. "
                            "The conversation involves an AI agent capable of executing terminal "
                            "commands, file editing, and browsing. "
                            "Return ONLY the raw title string, without quotes or explanations."
                        )
                    )
                ],
            ),
            Message(
                role="user",
                content=[
                    TextContent(
                        text=(
                            f"Generate a title (max {max_length} chars) for this initial message:\n\n"
                            f"\"{truncated_message}\"\n\n"
                            "Prefix the title with exactly ONE relevant emoji from the following list:\n"
                            f"{category_prompt}"
                        )
                    )
                ],
            ),
        ]

        response = llm.completion(messages)

        ## @phase: validation, @desc: Parse and enforce constraints on the LLM output
        if response.message.content and isinstance(response.message.content[0], TextContent):
            title = response.message.content[0].text.strip()
            return f"{title[:max_length - 3]}..." if len(title) > max_length else title

        logger.warning("LLM returned empty response for title generation")
        return None

    except Exception as e:
        logger.warning(f"Error generating conversation title with LLM: {e}")
        return None


def generate_fallback_title(message: str, max_length: int = 50) -> str:
    ## @phase: fallback, Simple text truncation when LLM is unavailable
    title = message.strip()
    return f"{title[:max_length - 3]}..." if len(title) > max_length else title


def generate_title_from_message(
    message: str, llm: Driver | None = None, max_length: int = 50
) -> str:
    ## @phase: routing
    llm_to_use = None if llm and getattr(llm, 'model', '') == "acp-managed" else llm
    
    if llm_to_use:
        if llm_title := generate_title_with_llm(message, llm_to_use, max_length):
            return llm_title

    return generate_fallback_title(message, max_length)


def generate_conversation_title(
    events: Sequence[Event], llm: Driver | None = None, max_length: int = 50
) -> str:
    ## @phase: execution - Main entry point to derive a title from a sequence
    first_user_message = extract_first_user_message(events)
    if not first_user_message:
        raise ValueError("No user messages found in conversation events")

    return generate_title_from_message(first_user_message, llm, max_length)