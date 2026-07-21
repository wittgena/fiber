# atoa.gov.acp.support
## @lineage: gov.acp.support
## @lineage: gov.policy.acp.support
## @lineage: gov.protocol.acp.support
## @lineage: gov.protocol.support
## @lineage: anchor.agent.client.support
## @lineage: gov.llm.protocol.acp.support
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

from atoa.gov.acp.conn import ClientSideConnection
from atoa.driver.tensor import Driver

from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter(name="acp.support", phase="agent_execution")

_USAGE_UPDATE_TIMEOUT: float = float(os.environ.get("ACP_USAGE_UPDATE_TIMEOUT", "2.0"))
_ACP_PROMPT_MAX_RETRIES: int = int(os.environ.get("ACP_PROMPT_MAX_RETRIES", "3"))
_ACP_PROMPT_RETRY_DELAYS: tuple[float, ...] = (5.0, 15.0, 30.0)  # seconds
_RETRIABLE_CONNECTION_ERRORS = (OSError, ConnectionError, BrokenPipeError, EOFError)
_RETRIABLE_SERVER_ERROR_CODES: frozenset[int] = frozenset({-32603})
_STREAM_READER_LIMIT: int = 100 * 1024 * 1024  # 100 MiB

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

def _select_auth_method(
    auth_methods: list[Any],
    env: dict[str, str],
) -> str | None:
    """Pick an auth method whose required env var is present.

    Returns the ``id`` of the first matching method, or ``None`` if no
    env-var-based method is available (the server may not require auth).
    """
    method_ids = {m.id for m in auth_methods}
    for method_id, env_var in _AUTH_METHOD_ENV_MAP.items():
        if method_id in method_ids and env_var in env:
            return method_id
    return None

def _resolve_bypass_mode(agent_name: str) -> str:
    """Return the session mode ID that bypasses all permission prompts.

    Different ACP servers use different mode IDs for the same concept:
    - claude-agent-acp → ``bypassPermissions``
    - codex-acp        → ``full-access``
    - gemini-cli       → ``yolo``

    Falls back to ``full-access`` for unknown servers.
    """
    for key, mode in _BYPASS_MODE_MAP.items():
        if key in agent_name.lower():
            return mode
    return _DEFAULT_BYPASS_MODE


def _build_session_meta(agent_name: str, acp_model: str | None) -> dict[str, Any]:
    """Build ACP session metadata for server-specific model selection."""
    if not acp_model:
        return {}
    # claude-agent-acp: model selection via session _meta (claudeCode.options.model)
    if "claude" in agent_name.lower():
        return {"claudeCode": {"options": {"model": acp_model}}}
    # codex-acp, gemini-cli: use protocol-level set_session_model instead (see below)
    return {}


async def _maybe_set_session_model(
    conn: ClientSideConnection,
    agent_name: str,
    session_id: str,
    acp_model: str | None,
) -> None:
    """Apply a protocol-level session model override when the server supports it."""
    if not acp_model:
        return
    # codex-acp, gemini-cli: model selection via set_session_model protocol method
    # claude-agent-acp: uses session _meta instead (see _build_session_meta)
    if "codex-acp" in agent_name.lower() or "gemini-cli" in agent_name.lower():
        await conn.set_session_model(model_id=acp_model, session_id=session_id)


def _extract_token_usage(
    response: Any,
) -> tuple[int, int, int, int, int]:
    """Extract token usage from an ACP PromptResponse.

    Returns (input_tokens, output_tokens, cache_read, cache_write, reasoning).

    Checks two locations:
    - claude-agent-acp, codex-acp: ``response.usage`` (standard ACP field)
    - gemini-cli: ``response._meta.quota.token_count`` (non-standard)
    """
    if response is not None and response.usage is not None:
        u = response.usage
        return (
            u.input_tokens,
            u.output_tokens,
            u.cached_read_tokens or 0,
            u.cached_write_tokens or 0,
            u.thought_tokens or 0,
        )
    if response is not None and response.field_meta is not None:
        quota = response.field_meta.get("quota", {})
        tc = quota.get("token_count", {})
        return (tc.get("input_tokens", 0), tc.get("output_tokens", 0), 0, 0, 0)
    return (0, 0, 0, 0, 0)


def _estimate_cost_from_tokens(
    model: str, input_tokens: int, output_tokens: int
) -> float:
    """Estimate cost from token counts using LiteLLM's pricing database.

    Returns 0.0 if pricing is unavailable for the model.
    """
    try:
        from bound.resolver.model.config.resolver import config

        cost_map = config.model_cost
        info = cost_map.get(model, {})
        input_cost = info.get("input_cost_per_token", 0) or 0
        output_cost = info.get("output_cost_per_token", 0) or 0
        return input_tokens * input_cost + output_tokens * output_cost
    except Exception:
        return 0.0

async def _filter_jsonrpc_lines(source: Any, dest: Any) -> None:
    """Read lines from *source* and forward only JSON-RPC lines to *dest*.

    Some ACP servers (e.g. ``claude-code-acp`` v0.1.x) emit log messages
    like ``[ACP] ...`` to stdout alongside JSON-RPC traffic.  This coroutine
    strips those non-protocol lines so the JSON-RPC connection is not confused.
    """
    try:
        while True:
            line = await source.readline()
            if not line:
                dest.feed_eof()
                break
            # JSON-RPC messages are single-line JSON objects containing
            # "jsonrpc". Filter out multi-line pretty-printed JSON from
            # debug logs that also start with '{'.
            stripped = line.lstrip()
            if stripped.startswith(b"{") and b'"jsonrpc"' in line:
                dest.feed_data(line)
            else:
                log.debug(
                    "ACP stdout (non-JSON): %s",
                    line.decode(errors="replace").rstrip(),
                )
    except Exception:
        log.debug("_filter_jsonrpc_lines stopped", exc_info=True)
        dest.feed_eof()