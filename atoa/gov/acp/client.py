# atoa.gov.acp.client
## @lineage: gov.acp.client
## @lineage: gov.policy.acp.client
## @lineage: gov.protocol.acp.client
## @lineage: gov.protocol.client.acp
from __future__ import annotations
import asyncio
import json
import os
import threading
import time
import uuid
from collections.abc import Generator
from typing import TYPE_CHECKING, Any
from pydantic import Field, PrivateAttr

from resolver.acp.schema import (
    AgentMessageChunk,
    AgentThoughtChunk,
    AllowedOutcome,
    RequestPermissionResponse,
    TextContentBlock,
    ToolCallProgress,
    ToolCallStart,
    UsageUpdate,
)

from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter(name="acp.client", phase="agent_execution")

# Minimum interval between on_activity heartbeat signals (seconds).
# Throttled to avoid excessive calls while still keeping the idle timer
# well below the ~20 min runtime-api kill threshold.
_ACTIVITY_SIGNAL_INTERVAL: float = 30.0

# Known ACP server name → bypass-permissions mode ID mappings.
_BYPASS_MODE_MAP: dict[str, str] = {
    "claude-agent": "bypassPermissions",
    "codex-acp": "full-access",
    "gemini-cli": "yolo",
}
_DEFAULT_BYPASS_MODE = "full-access"

# ACP auth method ID → environment variable that supplies the credential.
# When the server reports auth_methods, we pick the first method whose
# required env var is set.
# Note: claude-login is intentionally NOT included because Claude Code ACP
# uses bypassPermissions mode instead of API key authentication.
_AUTH_METHOD_ENV_MAP: dict[str, str] = {
    "codex-api-key": "CODEX_API_KEY",
    "openai-api-key": "OPENAI_API_KEY",
    "gemini-api-key": "GEMINI_API_KEY",
}

def _serialize_tool_content(content: list[Any] | None) -> list[dict[str, Any]] | None:
    """Serialize ACP tool call content blocks to plain dicts for JSON storage."""
    if not content:
        return None
    return [
        c.model_dump(mode="json") if hasattr(c, "model_dump") else c for c in content
    ]

class ACPClient:
    def __init__(self) -> None:
        self.accumulated_text: list[str] = []
        self.accumulated_thoughts: list[str] = []
        self.accumulated_tool_calls: list[dict[str, Any]] = []
        self.on_token: Any = None  # ConversationTokenCallbackType | None

        ## Activity heartbeat — called (throttled) during session_update to signal that the ACP subprocess is still actively working.
        self.on_activity: Any = None  # Callable[[], None] | None
        self._last_activity_signal: float = float("-inf")

        ## Telemetry state from UsageUpdate (persists across turns)
        self._last_cost: float = 0.0  # last cumulative cost seen
        self._last_cost_by_session: dict[str, float] = {}
        self._context_window: int = 0  # last context window seen
        self._context_window_by_session: dict[str, int] = {}

        ## Per-turn synchronization for UsageUpdate notifications
        self._turn_usage_updates: dict[str, Any] = {}
        self._usage_received: dict[str, asyncio.Event] = {}

        ## Fork session state for ask_agent() — guarded by _fork_lock to prevent concurrent ask_agent() calls from colliding.
        self._fork_lock = threading.Lock()
        self._fork_session_id: str | None = None
        self._fork_accumulated_text: list[str] = []

    def reset(self) -> None:
        self.accumulated_text.clear()
        self.accumulated_thoughts.clear()
        self.accumulated_tool_calls.clear()
        self.on_token = None
        self.on_activity = None
        self._turn_usage_updates.clear()
        self._usage_received.clear()
        # Note: telemetry state (_last_cost, _context_window, _last_activity_signal,
        # etc.) is intentionally NOT cleared — it accumulates across turns.

    def prepare_usage_sync(self, session_id: str) -> asyncio.Event:
        """Prepare per-turn UsageUpdate synchronization for a session."""
        event = asyncio.Event()
        self._usage_received[session_id] = event
        self._turn_usage_updates.pop(session_id, None)
        return event

    def get_turn_usage_update(self, session_id: str) -> Any:
        """Return the latest UsageUpdate observed for the current turn."""
        return self._turn_usage_updates.get(session_id)

    def pop_turn_usage_update(self, session_id: str) -> Any:
        """Consume per-turn UsageUpdate synchronization state for a session."""
        self._usage_received.pop(session_id, None)
        return self._turn_usage_updates.pop(session_id, None)

    # -- Client protocol methods ------------------------------------------

    async def session_update(
        self,
        session_id: str,
        update: Any,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        log.debug("ACP session_update: type=%s", type(update).__name__)

        # Route fork session updates to the fork accumulator
        if self._fork_session_id is not None and session_id == self._fork_session_id:
            if isinstance(update, AgentMessageChunk):
                if isinstance(update.content, TextContentBlock):
                    self._fork_accumulated_text.append(update.content.text)
            return

        if isinstance(update, AgentMessageChunk):
            if isinstance(update.content, TextContentBlock):
                text = update.content.text
                self.accumulated_text.append(text)
                if self.on_token is not None:
                    try:
                        self.on_token(text)
                    except Exception:
                        log.debug("on_token callback failed", exc_info=True)
            self._maybe_signal_activity()
        elif isinstance(update, AgentThoughtChunk):
            if isinstance(update.content, TextContentBlock):
                self.accumulated_thoughts.append(update.content.text)
        elif isinstance(update, UsageUpdate):
            # Store the update for step()/ask_agent() to process in one place.
            self._context_window = update.size
            self._context_window_by_session[session_id] = update.size
            self._turn_usage_updates[session_id] = update
            event = self._usage_received.get(session_id)
            if event is not None:
                event.set()
        elif isinstance(update, ToolCallStart):
            self.accumulated_tool_calls.append(
                {
                    "tool_call_id": update.tool_call_id,
                    "title": update.title,
                    "tool_kind": update.kind,
                    "status": update.status,
                    "raw_input": update.raw_input,
                    "raw_output": update.raw_output,
                    "content": _serialize_tool_content(update.content),
                }
            )
            log.debug("ACP tool call start: %s", update.tool_call_id)
            self._maybe_signal_activity()
        elif isinstance(update, ToolCallProgress):
            # Find the existing tool call entry and merge updates
            for tc in self.accumulated_tool_calls:
                if tc["tool_call_id"] == update.tool_call_id:
                    if update.title is not None:
                        tc["title"] = update.title
                    if update.kind is not None:
                        tc["tool_kind"] = update.kind
                    if update.status is not None:
                        tc["status"] = update.status
                    if update.raw_input is not None:
                        tc["raw_input"] = update.raw_input
                    if update.raw_output is not None:
                        tc["raw_output"] = update.raw_output
                    if update.content is not None:
                        tc["content"] = _serialize_tool_content(update.content)
                    break
            log.debug("ACP tool call progress: %s", update.tool_call_id)
            self._maybe_signal_activity()
        else:
            log.debug("ACP session update: %s", type(update).__name__)

    def _maybe_signal_activity(self) -> None:
        if self.on_activity is None:
            return
        now = time.monotonic()
        if now - self._last_activity_signal >= _ACTIVITY_SIGNAL_INTERVAL:
            self._last_activity_signal = now
            try:
                self.on_activity()
            except Exception:
                log.debug("on_activity callback failed", exc_info=True)

    async def request_permission(
        self,
        options: list[Any],
        session_id: str,  # noqa: ARG002
        tool_call: Any,
        **kwargs: Any,  # noqa: ARG002
    ) -> Any:
        """Auto-approve all permission requests from the ACP server."""
        # Pick the first option (usually "allow once")
        option_id = options[0].option_id if options else "allow_once"
        log.info(
            "ACP auto-approving permission: %s (option: %s)",
            tool_call,
            option_id,
        )
        return RequestPermissionResponse(
            outcome=AllowedOutcome(outcome="selected", option_id=option_id),
        )

    # fs/terminal methods — raise NotImplementedError; ACP server handles its own
    async def write_text_file(
        self, content: str, path: str, session_id: str, **kwargs: Any
    ) -> None:
        raise NotImplementedError("ACP server handles file operations")

    async def read_text_file(
        self,
        path: str,
        session_id: str,
        limit: int | None = None,
        line: int | None = None,
        **kwargs: Any,
    ) -> Any:
        raise NotImplementedError("ACP server handles file operations")

    async def create_terminal(
        self,
        command: str,
        session_id: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Any = None,
        output_byte_limit: int | None = None,
        **kwargs: Any,
    ) -> Any:
        raise NotImplementedError("ACP server handles terminal operations")

    async def terminal_output(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> Any:
        raise NotImplementedError("ACP server handles terminal operations")

    async def release_terminal(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> None:
        raise NotImplementedError("ACP server handles terminal operations")

    async def wait_for_terminal_exit(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> Any:
        raise NotImplementedError("ACP server handles terminal operations")

    async def kill_terminal(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> None:
        raise NotImplementedError("ACP server handles terminal operations")

    async def ext_method(
        self,
        method: str,  # noqa: ARG002
        params: dict[str, Any],  # noqa: ARG002
    ) -> dict[str, Any]:
        return {}

    async def ext_notification(
        self,
        method: str,  # noqa: ARG002
        params: dict[str, Any],  # noqa: ARG002
    ) -> None:
        pass

    def on_connect(self, conn: Any) -> None:  # noqa: ARG002
        pass