# dphi.space.action.executor
## @lineage: agent.space.action.executor
## @lineage: bound.space.action.executor
## @lineage: bound.adapter.schema.executor
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    TypeVar,
)
from fiber.dphi.space.action.action import Action, Observation
from fiber.runtime.context import ToolExecutionContextProtocol

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