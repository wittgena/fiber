# atoa.disc.action.executor
## @lineage: atoa.gov.disc.action.executor
## @lineage: agent.atoa.action.executor
## @lineage: atoa.agent.action.executor
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    TypeVar,
)
from atoa.disc.schema.action import Action, Observation

if TYPE_CHECKING:
    from atoa.disc.conv import ToolExecutionContextProtocol

ActionT = TypeVar("ActionT", bound=Action)
ObservationT = TypeVar("ObservationT", bound=Observation)

class ActionExecutor[ActionT, ObservationT](ABC):
    @abstractmethod
    def __call__(
        self, action: ActionT, conversation: "ToolExecutionContextProtocol | None" = None
    ) -> ObservationT:
        """Execute the tool with the given action and return an observation"""

    def close(self) -> None:
        """Close the executor and clean up resources"""
        pass

class ExecutableTool(Protocol):
    name: str
    executor: ActionExecutor[Any, Any]
    
    def __call__(self, action: Action, conversation: "ToolExecutionContextProtocol | None" = None) -> Observation:
        ...