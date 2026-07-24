# eco.agent.mcp.client
## @lineage: eco.call.mcp.client
## @lineage: adapter.call.mcp.client
## @lineage: bound.adapter.call.mcp.client
## @lineage: gov.policy.mcp.client
## @lineage: gov.protocol.mcp.client
import asyncio
import inspect
from collections.abc import Callable
from contextlib import AsyncExitStack
from typing import Any

from mcp.client.client import Client as AnchorClient
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters

from eco.agent.mcp.exception import MCPError
from eco.agent.mcp.config import MCPConfig

from atoa.executor.base import AsyncExecutor

class MCPClient(AnchorClient):
    """@desc: Unified MCP Client that bridges AnchorClient with stdio execution via MCPConfig"""
    _executor: AsyncExecutor
    _closed: bool
    _config: MCPConfig | None

    def __init__(self, config: MCPConfig | dict | None = None, server: Any = None, **kwargs):
        self._executor = AsyncExecutor()
        self._closed = False
        
        ## @desc: Convert dictionary input to the internal Pydantic model.
        if isinstance(config, dict):
            config = MCPConfig.model_validate(config)
        self._config = config

        kwargs.pop("log_handler", None)

        ## @desc: AnchorClient strictly requires a 'server' argument
        target_server = server if server is not None else "stdio_config_override"
        super().__init__(server=target_server, **kwargs)

    async def __aenter__(self) -> "MCPClient":
        """@desc: Enter the async context manager and establish transport"""
        if self._session is not None:
            raise RuntimeError("Client is already entered; cannot reenter")

        if self._config and self._config.mcpServers:
            ## @phase: Configuration
            server_name = list(self._config.mcpServers.keys())[0]
            server_cfg = self._config.mcpServers[server_name]
            
            server_params = StdioServerParameters(
                command=server_cfg.command,
                args=server_cfg.args,
                env=server_cfg.env
            )

            async with AsyncExitStack() as exit_stack:
                ## @phase: Subprocess Execution
                read_stream, write_stream = await exit_stack.enter_async_context(
                    stdio_client(server_params)
                )

                ## @phase: Session Initialization
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

                ## @phase: Context Management
                self._exit_stack = exit_stack.pop_all()
                return self
        return await super().__aenter__()

    async def connect(self) -> None:
        try:
            await self.__aenter__()
        except RuntimeError as exc:
            raise MCPError("MCP Connection Failure") from exc

    def call_async_from_sync(self, awaitable_or_fn: Callable[..., Any] | Any, *args, timeout: float, **kwargs) -> Any:
        return self._executor.run_async(awaitable_or_fn, *args, timeout=timeout, **kwargs)

    async def call_sync_from_async(self, fn: Callable[..., Any], *args, **kwargs) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    def sync_close(self) -> None:
        if self._closed:
            return
            
        ## @desc: Execute the existing __aexit__ call as an async Task to properly clean up resources
        async def _async_close():
            if self._session is not None:
                await self.__aexit__(None, None, None)

        try:
            self._executor.run_async(_async_close, timeout=10.0)
        except Exception:
            pass
            
        self._executor.close()
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