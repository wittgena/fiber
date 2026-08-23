# phase.client.mcp.client
## @lineage: bound.client.mcp.client
## @lineage: bound.adapter.mcp.client
## @lineage: agent.protocol.mcp.client
import asyncio
import inspect
from collections.abc import Callable
from contextlib import AsyncExitStack
from typing import Any

from mcp.client.client import Client as AnchorClient
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters

from agent.space.terminal.session.builder import AsyncExecutorProtocol
from phase.client.mcp.exception import MCPError
from agent.anchor.config.mcp import MCPConfig

from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

class MCPClient(AnchorClient):
    _executor: AsyncExecutorProtocol
    _closed: bool
    _config: MCPConfig | None

    def __init__(
        self, 
        config: MCPConfig | dict | None = None, 
        server: Any = None, 
        # [개선 3] 의존성 주입(DI)을 통해 외부에서 Executor를 주입받을 수 있도록 변경
        executor: AsyncExecutorProtocol | None = None,
        **kwargs
    ):
        if executor is None:
            # 하위 호환성을 위해 주입되지 않은 경우에만 팩토리를 통해 가져옴
            from agent.space.terminal.session.builder import executor_factory
            self._executor = executor_factory.get_async_executor()
        else:
            self._executor = executor
            
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
        
        else:
            return await super().__aenter__()

    async def connect(self) -> None:
        try:
            await self.__aenter__()
        except RuntimeError as exc:
            raise MCPError("MCP Connection Failure") from exc

    def call_async_from_sync(self, awaitable_or_fn: Callable[..., Any] | Any, *args, timeout: float, **kwargs) -> Any:
        # 프로토콜에 정의된 run_async 메서드만 신뢰하고 호출
        return self._executor.run_async(awaitable_or_fn, *args, timeout=timeout, **kwargs)

    async def call_sync_from_async(self, fn: Callable[..., Any], *args, **kwargs) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)

    def sync_close(self) -> None:
        if self._closed:
            return
            
        ## @desc: Execute the existing __aexit__ call as an async Task to properly clean up resources
        async def _async_close():
            if self._session is not None:
                await self.__aexit__(None, None, None)

        try:
            self._executor.run_async(_async_close, timeout=10.0)
        except Exception as e:
            logger.warning(f"Error during MCP client sync_close: {e}")
            
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