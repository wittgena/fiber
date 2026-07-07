# bound.channel.action.task.supervisor
## @lineage: bound.channel.client.action.task.supervisor
## @lineage: anchor.channel.client.action.task.supervisor
## @lineage: anchor.channel.action.task.supervisor
## @lineage: bound.channel.bridge.task.supervisor
## @lineage: bound.bridge.task.supervisor
## @lineage: bound.channel.supervisor
## @lineage: bound.bridge.supervisor
## @lineage: bound.client.bridge.supervisor
"""
@phase: MCP Task Supervisor Bridge
@desc: Manages long-running MCP tool executions. Handles timeouts, cancellation scopes,
and routes progress notifications back to the Agent's topological tracking state.
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Awaitable
import anyio

from mcp.client.client import Client
from mcp_types import CallToolResult

from watcher.plane.emitter import get_emitter

log = get_emitter("bridge.supervisor")

# Type alias for the agent's internal state tracker callback
ProgressTrackerT = Callable[[float, float | None, str | None], Awaitable[None]]

class TaskSupervisorBridge:
    """
    Wraps tool executions in a managed cancellation scope.
    """

    @classmethod
    async def execute_with_supervision(
        cls,
        client: Client,
        tool_name: str,
        arguments: dict[str, Any],
        timeout_seconds: float = 300.0,
        tracker: ProgressTrackerT | None = None
    ) -> CallToolResult:
        """
        Executes a tool with strict timeout enforcing and progress routing.
        
        Args:
            client: The established MCP Client.
            tool_name: Name of the tool to call.
            arguments: Tool arguments.
            timeout_seconds: Hard limit for execution time.
            tracker: Agent's internal callback to update the UI or state machine with progress.
        """
        log.info(f"Supervising tool execution: '{tool_name}' (Timeout: {timeout_seconds}s)")

        # 내부 Progress 콜백 (MCP 규격을 Agent 규격으로 매핑)
        async def _mcp_progress_handler(
            progress: float, 
            total: float | None = None, 
            message: str | None = None
        ):
            log.debug(f"[Task: {tool_name}] Progress: {progress}/{total} - {message}")
            if tracker:
                await tracker(progress, total, message)

        try:
            # anyio를 활용한 철저한 격리 및 타임아웃(Cancellation Scope) 강제
            with anyio.fail_after(timeout_seconds):
                result = await client.call_tool(
                    name=tool_name,
                    arguments=arguments,
                    read_timeout_seconds=timeout_seconds,
                    progress_callback=_mcp_progress_handler
                )
                log.info(f"Supervised execution of '{tool_name}' completed successfully.")
                return result

        except TimeoutError:
            log.error(f"⚠️ [SUPERVISOR KILLED] Tool '{tool_name}' exceeded {timeout_seconds}s limit.")
            # 에이전트의 Error/Residue 추적기로 실패 상태를 반환 (단순 크래시 방지)
            raise RuntimeError(f"Tool execution supervised timeout: {tool_name}")
        except Exception as e:
            log.error(f"Supervised execution failed: {str(e)}")
            raise