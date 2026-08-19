# ator.driver.tool.terminal.context
## @lineage: driver.tool.terminal.context
## @lineage: ator.bound.tool.terminal.context
## @lineage: eco.tool.terminal.context
## @lineage: engine.tool.terminal.context
## @lineage: engine.terminal.context
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from ator.conv.context.state.protocol import ToolExecutionContextProtocol
    from ator.conv.protocol.tool.terminal import TerminalAction, TerminalObservation
    from ator.driver.tool.terminal.session import TerminalSessionBase


class ExecutionContext:
    def __init__(
        self,
        session: "TerminalSessionBase",
        action: "TerminalAction",
        conversation: "ToolExecutionContextProtocol | None" = None,
    ):
        self.session = session
        self.action = action
        self.conversation = conversation
        self.metadata: dict[str, Any] = {}
        self.is_aborted: bool = False


class ExecutionEngine(ABC):
    """Abstract base class for terminal execution and pipeline orchestration."""

    @abstractmethod
    def execute(self, context: ExecutionContext) -> "TerminalObservation":
        """
        Executes the action within the provided context and returns an observation.
        """
        pass

class ExecutionMiddleware(ABC):
    """Abstract base class for pipeline interventions."""

    @abstractmethod
    def process(
        self, 
        context: ExecutionContext, 
        next_call: Callable[[ExecutionContext], "TerminalObservation"]
    ) -> "TerminalObservation":
        pass