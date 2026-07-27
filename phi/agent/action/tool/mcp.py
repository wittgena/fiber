# phi.agent.action.tool.mcp
## @lineage: swarm.phi.action.tool.mcp
## @lineage: agent.action.tool.mcp
## @lineage: gov.action.tool.mcp
## @lineage: atoa.disc.action.tool.mcp
## @lineage: atoa.gov.disc.action.tool.mcp
## @lineage: agent.atoa.action.tool.mcp
## @lineage: atoa.agent.action.tool.mcp
## @lineage: atoa.call.action.tool.mcp
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any
from pydantic import Field, ValidationError
import mcp_types
from eco.tenant.switch.params import ChatCompletionToolParam
from watcher.plane.observer.span import observe

from atoa.schema.action import Action, Observation, Schema

from swarm.mesh.mcp.client import MCPClient
from phi.agent.action.definition import ActionAnnotations, ActionDefinition
from phi.agent.action.executor import ActionExecutor
from phi.agent.action.mcp import MCPAction, MCPObservation

from arch.topos.bound.surge.disc import DiscMixin
from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

# Default timeout for MCP tool execution in seconds
MCP_TOOL_TIMEOUT_SECONDS = 300

def to_camel_case(s: str) -> str:
    parts = re.split(r"[_\-\s]+", s)
    return "".join(word.capitalize() for word in parts if word)

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
            raise ValueError(f"MCPAction can only execute MCPToolAction actions, got {type(action)}",)
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
