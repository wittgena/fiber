# mesh.engine.executor.context
## @lineage: mesh.engine.execution.context
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from agent.disc.conv import ToolExecutionContextProtocol
    from gov.action.tool.terminal import TerminalAction, TerminalObservation
    from mesh.engine.session import TerminalSessionBase


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