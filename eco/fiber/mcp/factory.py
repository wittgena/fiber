# eco.fiber.mcp.factory
## @lineage: agent.eco.mcp.factory
## @lineage: eco.agent.mcp.factory
## @lineage: eco.call.mcp.factory
## @lineage: adapter.call.mcp.factory
## @lineage: bound.adapter.call.mcp.factory
## @lineage: gov.policy.mcp.factory
import mcp_types
from mcp_types import LoggingMessageNotificationParams

from atoa.disc.action.tool.mcp import MCPActionDefinition

from eco.fiber.mcp.config import MCPConfig
from eco.fiber.mcp.exception import MCPTimeoutError
from eco.fiber.mcp.client import MCPClient

from arch.contract.event.next import LogEvent
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

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
    emitter.emit(event)

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
    """
    @desc: Synchronously creates and lists MCP tools by wrapping the async connection and handling timeouts.
    @flow: Validate Config -> Initialize Client -> Execute Async Connection Synchronously -> Handle Errors -> Return Tools.
    """
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