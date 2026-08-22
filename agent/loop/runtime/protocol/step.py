# agent.loop.runtime.protocol.step
## @lineage: agent.runtime.protocol.step
## @lineage: ator.driver.protocol.step
## @lineage: agent.protocol.step
from abc import ABC, abstractmethod
from typing import Any, Callable, Awaitable, Protocol
from dataclasses import dataclass

from agent.llm.driver.response import LLMResponse
from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

class SnapshotProtocol(Protocol):
    conversation_id: str | None
    iteration: int
    events: list[Any]


class ActivatorProtocol(Protocol):
    name: str
    tools_map: dict[str, Any]
    llm: Any
    
    def _maybe_emit_vllm_tokens(self, llm_response: LLMResponse) -> Any: ...
    def _get_action_event(self, tool_call: Any, **kwargs) -> Any: ...

@dataclass
class StepContext:
    llm_response: LLMResponse | None = None
    produced_aspects: set[str] | None = None
    node_attributes: dict[str, Any] | None = None

class StepHandler(ABC):
    @abstractmethod
    async def handle_async(
        self,
        activator: ActivatorProtocol,
        snapshot: SnapshotProtocol,
        on_event: Callable[[Any], Awaitable[None]],
        context: StepContext
    ) -> bool:
        pass