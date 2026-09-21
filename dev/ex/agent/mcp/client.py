# fiber.dev.ex.agent.mcp.client
import asyncio
import inspect
from collections.abc import Callable
from contextlib import AsyncExitStack
from typing import Any

from pydantic import ValidationError

import mcp_types
from mcp_types import LoggingMessageNotificationParams
from mcp.client.client import Client as AnchorClient
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters

from fiber.dev.ex.agent.protocol.executor import AsyncExecutorProtocol
from fiber.dev.ex.agent.config.mcp import MCPConfig

from xphi.arch.bound.event.next import LogEvent
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class MCPError(Exception):
    """Base exception for MCP-related errors."""
    pass

class MCPTimeoutError(MCPError):
    """Exception raised when MCP operations timeout."""
    timeout: float
    config: dict | None

    def __init__(self, message: str, timeout: float, config: dict | None = None):
        self.timeout = timeout
        self.config = config
        super().__init__(message)

# Default timeout for MCP tool execution in seconds
MCP_TOOL_TIMEOUT_SECONDS = 300

class MCPClient(AnchorClient):
    _executor: AsyncExecutorProtocol
    _closed: bool
    _config: MCPConfig | None

    def __init__(
        self, 
        executor: AsyncExecutorProtocol,
        config: MCPConfig | dict | None = None, 
        server: Any = None, 
        **kwargs
    ):
        self._executor = executor
        self._closed = False
        
        if isinstance(config, dict):
            config = MCPConfig.model_validate(config)
        self._config = config

        kwargs.pop("log_handler", None)
        target_server = server if server is not None else "stdio_config_override"
        super().__init__(server=target_server, **kwargs)

    async def __aenter__(self) -> "MCPClient":
        """Enter the async context manager and establish transport"""
        if self._session is not None:
            raise RuntimeError("Client is already entered; cannot reenter")

        if self._config and self._config.mcpServers:
            server_name = list(self._config.mcpServers.keys())[0]
            server_cfg = self._config.mcpServers[server_name]
            
            server_params = StdioServerParameters(
                command=server_cfg.command,
                args=server_cfg.args,
                env=server_cfg.env
            )

            async with AsyncExitStack() as exit_stack:
                read_stream, write_stream = await exit_stack.enter_async_context(
                    stdio_client(server_params)
                )

                self._session = await exit_stack.enter_async_context(
                    ClientSession(
                        read_stream=read_stream,
                        write_stream=write_stream,
                        read_timeout_seconds=self.read_timeout_seconds,
                        sampling_callback=self.sampling_callback,
                        list_roots_callback=self.list_roots_callback,
                        logging_callback=self.logging_callback,
                        message_handler=self.message_handler,
                        client_info=self.client_info,
                        elicitation_callback=self.elicitation_callback,
                    )
                )

                await self._session.initialize()
                self._exit_stack = exit_stack.pop_all()
            
            return self
        
        else:
            return await super().__aenter__()

    async def connect(self) -> None:
        try:
            await self.__aenter__()
        except RuntimeError as exc:
            raise MCPError("MCP Connection Failure") from exc

    def call_async_from_sync(self, awaitable_or_fn: Callable[..., Any] | Any, *args, timeout: float, **kwargs) -> Any:
        return self._executor.run_async(awaitable_or_fn, *args, timeout=timeout, **kwargs)

    async def call_sync_from_async(self, fn: Callable[..., Any], *args, **kwargs) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)

    def sync_close(self) -> None:
        if self._closed:
            return
            
        async def _async_close():
            if self._session is not None:
                await self.__aexit__(None, None, None)

        try:
            self._executor.run_async(_async_close, timeout=10.0)
        except Exception as e:
            log.warning(f"Error during MCP client sync_close: {e}")
            
        self._closed = True

    def __del__(self):
        try:
            self.sync_close()
        except Exception:
            pass

    def __enter__(self) -> "MCPClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.sync_close()


# ============================================================================
# Core MCP Interaction Logic (No Theoria Action Models)
# ============================================================================

async def mcp_log_callback(params: LoggingMessageNotificationParams) -> None:
    """Transforms logs transmitted from the MCP server"""
    level_map = {
        "error": "ERROR",
        "warning": "WARNING",
        "debug": "DEBUG",
        "info": "INFO"
    }
    normalized_level = level_map.get(str(params.level).lower(), "INFO")
    source_name = params.logger if params.logger else "unknown-module"

    event = LogEvent(
        level=normalized_level,
        message=params.data,
        source_id=f"mcp-server::{source_name}",
        context={
            "phase": "mcp_execution",
            "mcp_logger": params.logger,
            "mcp_level": params.level
        }
    )
    log.emit(event)

def create_mcp_client(executor: AsyncExecutorProtocol, config: dict | MCPConfig) -> MCPClient:
    """Factory function to create an MCPClient instance injected with the standard logging callback."""
    return MCPClient(
        executor=executor,
        config=config, 
        logging_callback=mcp_log_callback
    )

async def _connect_and_list_mcp_tools(client: MCPClient) -> list[mcp_types.Tool]:
    """Connects the client and returns raw MCP tool schemas."""
    await client.connect()
    return await client.list_tools()

async def call_mcp_tool_raw(
    client: MCPClient, 
    tool_name: str, 
    arguments: dict[str, Any]
) -> mcp_types.CallToolResult:
    """Executes a raw MCP tool call without wrapping in Theoria domain objects."""
    if not client.is_connected():
        raise RuntimeError(
            f"MCP client not connected for tool '{tool_name}'. "
            "The connection may have been closed or failed to establish."
        )
    return await client.call_tool_mcp(name=tool_name, arguments=arguments)

def fetch_mcp_tools_sync(
    client: MCPClient,
    timeout: float = 30.0,
) -> list[mcp_types.Tool]:
    """Synchronously fetches raw MCP tools, handling timeouts and cleanup."""
    try:
        tools = client.call_async_from_sync(_connect_and_list_mcp_tools, timeout=timeout, client=client)
        return tools
    except TimeoutError as e:
        client.sync_close()
        error_msg = f"MCP tool listing timed out after {timeout} seconds...\n"
        raise MCPTimeoutError(error_msg, timeout=timeout, config=client._config.model_dump() if client._config else None) from e
    except BaseException:
        try:
            client.sync_close()
        except Exception as close_exc:
            log.warning("Failed to close MCP client during error cleanup", exc_info=close_exc)
        raise