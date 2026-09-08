# fiber.agent.engine.tool.mcp.factory
## @lineage: fiber.agent.client.tool.mcp.factory
## @lineage: surgent.client.mcp.factory
import asyncio
import inspect
import re
from collections.abc import Callable, Sequence
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from pydantic import Field, ValidationError

import mcp_types
from mcp_types import LoggingMessageNotificationParams
from mcp.client.client import Client as AnchorClient
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters

from fiber.llm.driver.config.mcp import MCPConfig
from fiber.llm.param import ChatCompletionToolParam

from fiber.infra.protocol.builder import AsyncExecutorProtocol
from fiber.agent.engine.tool.mcp.exception import MCPError, MCPTimeoutError
from fiber.agent.engine.tool.schema.action import Action, Observation, Schema
from fiber.agent.engine.tool.schema.builder import ActionAnnotations, ActionDefinition
from fiber.agent.engine.tool.schema.executor import ActionExecutor
from fiber.agent.engine.tool.schema.mcp import MCPAction, MCPObservation

from xphi.arch.event.next import LogEvent
from xphi.arch.model.surge.disc import DiscMixin
from xphi.watcher.plane.observer.span import observe
from xphi.watcher.plane.emitter import get_logger, get_emitter

logger = get_logger(__name__)
log = get_emitter(__name__)

# Default timeout for MCP tool execution in seconds
MCP_TOOL_TIMEOUT_SECONDS = 300


def to_camel_case(s: str) -> str:
    parts = re.split(r"[_\-\s]+", s)
    return "".join(word.capitalize() for word in parts if word)


# ============================================================================
# MCP Client Definitions
# ============================================================================

class MCPClient(AnchorClient):
    _executor: AsyncExecutorProtocol
    _closed: bool
    _config: MCPConfig | None

    def __init__(
        self, 
        config: MCPConfig | dict | None = None, 
        server: Any = None, 
        executor: AsyncExecutorProtocol | None = None,
        **kwargs
    ):
        if executor is None:
            from fiber.infra.protocol.builder import executor_factory
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


# ============================================================================
# MCP Action & Executor Definitions
# ============================================================================

class MCPExecutor(ActionExecutor):
    """Executor for MCP tools."""

    tool_name: str
    client: MCPClient
    timeout: float

    def __init__(
        self,
        tool_name: str,
        client: MCPClient,
        timeout: float = MCP_TOOL_TIMEOUT_SECONDS,
    ):
        self.tool_name = tool_name
        self.client = client
        self.timeout = timeout

    @observe(name="MCPExecutor.call_tool", span_type="TOOL")
    async def call_tool(self, action: MCPAction) -> MCPObservation:
        """Execute the MCP tool call using the already-connected client."""
        if not self.client.is_connected():
            raise RuntimeError(
                f"MCP client not connected for tool '{self.tool_name}'. "
                "The connection may have been closed or failed to establish."
            )
        try:
            logger.debug(
                f"Calling MCP tool {self.tool_name} with args: {action.model_dump()}"
            )
            result: mcp_types.CallToolResult = await self.client.call_tool_mcp(
                name=self.tool_name, arguments=action.to_mcp_arguments()
            )
            return MCPObservation.from_call_tool_result(
                tool_name=self.tool_name, result=result
            )
        except Exception as e:
            error_msg = f"Error calling MCP tool {self.tool_name}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return MCPObservation.from_text(
                text=error_msg,
                is_error=True,
                tool_name=self.tool_name,
            )

    def __call__(self, action: MCPAction, conversation: "Conv | None" = None) -> MCPObservation:
        try:
            return self.client.call_async_from_sync(self.call_tool, action=action, timeout=self.timeout)
        except TimeoutError:
            error_msg = (
                f"MCP tool '{self.tool_name}' timed out after {self.timeout} seconds. "
                "The tool server may be unresponsive or the operation is taking "
                "too long. Consider retrying or using an alternative approach."
            )
            logger.error(error_msg)
            return MCPObservation.from_text(
                text=error_msg,
                is_error=True,
                tool_name=self.tool_name,
            )


_mcp_dynamic_action_type: dict[str, type[Schema]] = {}


def _create_mcp_action_type(action_type: mcp_types.Tool) -> type[Schema]:
    mcp_action_type = _mcp_dynamic_action_type.get(action_type.name)
    if mcp_action_type:
        return mcp_action_type

    model_name = f"MCP{to_camel_case(action_type.name)}Action"
    mcp_action_type = Schema.from_mcp_schema(model_name, action_type.inputSchema)
    _mcp_dynamic_action_type[action_type.name] = mcp_action_type
    return mcp_action_type


class MCPActionDefinition(ActionDefinition[MCPAction, MCPObservation]):
    """MCP Tool that wraps an MCP client and provides tool functionality."""

    mcp_tool: mcp_types.Tool = Field(description="The MCP tool definition.")

    @property
    def name(self) -> str:  # type: ignore[override]
        """Return the MCP tool name instead of the class name."""
        return self.mcp_tool.name

    def __call__(self, action: Action, conv: "Conv | None" = None,) -> Observation:
        if not isinstance(action, MCPAction):
            raise ValueError(f"MCPAction can only execute MCPToolAction actions, got {type(action)}")
        assert self.name == self.mcp_tool.name
        mcp_action_type = _create_mcp_action_type(self.mcp_tool)

        try:
            mcp_action_type.model_validate(action.data)
        except ValidationError as e:
            error_msg = f"Validation error for MCP tool '{self.name}' args: {e}"
            logger.error(error_msg, exc_info=True)
            return MCPObservation.from_text(
                text=error_msg,
                is_error=True,
                tool_name=self.name,
            )

        return super().__call__(action, conv)

    def action_from_arguments(self, arguments: dict[str, Any]) -> MCPAction:
        prefiltered_args = {k: v for k, v in (arguments or {}).items() if v is not None}
        mcp_action_type = _create_mcp_action_type(self.mcp_tool)
        validated = mcp_action_type.model_validate(prefiltered_args)
        exclude_fields = set(DiscMixin.model_fields.keys()) | set(
            DiscMixin.model_computed_fields.keys()
        )
        sanitized = validated.model_dump(exclude_none=True, exclude=exclude_fields)
        return MCPAction(data=sanitized)

    @classmethod
    def create(
        cls,
        mcp_tool: mcp_types.Tool,
        mcp_client: MCPClient,
    ) -> Sequence["MCPActionDefinition"]:
        try:
            annotations = (
                ActionAnnotations.model_validate(
                    mcp_tool.annotations.model_dump(exclude_none=True)
                )
                if mcp_tool.annotations
                else None
            )

            tool_instance = cls(
                description=mcp_tool.description or "No description provided",
                action_type=MCPAction,
                observation_type=MCPObservation,
                annotations=annotations,
                meta=mcp_tool.meta,
                executor=MCPExecutor(tool_name=mcp_tool.name, client=mcp_client),
                # pass-through fields (enabled by **extra in Tool.create)
                mcp_tool=mcp_tool,
            )
            return [tool_instance]
        except ValidationError as e:
            logger.error(
                f"Validation error creating MCPAction for {mcp_tool.name}: "
                f"{e.json(indent=2)}",
                exc_info=True,
            )
            raise e

    def to_mcp_action(
        self,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if input_schema is not None or output_schema is not None:
            raise ValueError("MCPAction.to_mcp_tool does not support overriding schemas")

        return super().to_mcp_tool(
            input_schema=self.mcp_tool.inputSchema,
            output_schema=self.observation_type.to_mcp_schema()
            if self.observation_type
            else None,
        )

    def to_openai_tool(
        self,
        add_security_risk_prediction: bool = False,
        action_type: type[Schema] | None = None,
    ) -> ChatCompletionToolParam:
        if action_type is not None:
            raise ValueError("MCPAction.to_openai_tool does not support overriding action_type")

        assert self.name == self.mcp_tool.name
        mcp_action_type = _create_mcp_action_type(self.mcp_tool)
        return super().to_openai_tool(
            add_security_risk_prediction=add_security_risk_prediction,
            action_type=mcp_action_type,
        )


# ============================================================================
# MCP Factory / Creation Methods
# ============================================================================

async def mcp_log_callback(params: LoggingMessageNotificationParams) -> None:
    """@desc: Transforms logs transmitted from the MCP server"""
    level_map = {
        "error": "ERROR",
        "warning": "WARNING",
        "debug": "DEBUG",
        "info": "INFO"
    }
    normalized_level = level_map.get(str(params.level).lower(), "INFO")
    source_name = params.logger if params.logger else "unknown-module"

    ## @phase: Event Emission - Create and emit a standardized LogEvent object
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


def create_mcp_client_with_logs(config) -> MCPClient:
    """
    @desc: Factory function to create an MCPClient instance injected with the standard logging callback.
    """
    return MCPClient(
        config=config, 
        logging_callback=mcp_log_callback
    )


async def _connect_and_list_tools(client: MCPClient) -> list[MCPActionDefinition]:
    await client.connect()
    mcp_type_tools: list[mcp_types.Tool] = await client.list_tools()
    
    tools = []
    for mcp_tool in mcp_type_tools:
        tool_sequence = MCPActionDefinition.create(mcp_tool=mcp_tool, mcp_client=client)
        tools.extend(tool_sequence)
        
    return tools


def create_mcp_tools(
    config: dict | MCPConfig,
    timeout: float = 30.0,
) -> list[MCPActionDefinition]:
    if isinstance(config, dict):
        config = MCPConfig.model_validate(config)
        
    client = MCPClient(config, logging_callback=mcp_log_callback)
    try:
        ## @phase: Execution
        tools = client.call_async_from_sync(_connect_and_list_tools, timeout=timeout, client=client)
    except TimeoutError as e:
        ## @phase: Error Handling (Timeout)
        client.sync_close()
        server_names = list(config.mcpServers.keys()) if config.mcpServers else ["unknown"]
        error_msg = f"MCP tool listing timed out after {timeout} seconds...\n"
        raise MCPTimeoutError(error_msg, timeout=timeout, config=config.model_dump()) from e
    except BaseException:
        ## @phase: Cleanup
        try:
            client.sync_close()
        except Exception as close_exc:
            log.warning("Failed to close MCP client during error cleanup", exc_info=close_exc)
        raise

    log.info(f"Created {len(tools)} MCP tools: {[t.name for t in tools]}")
    return tools