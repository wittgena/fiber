# atoa.gov.action.step
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Awaitable
from dataclasses import dataclass
from atoa.call.response import LLMResponse
from watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from atoa.activator import Activator, AgentStateSnapshot
    ActivatorType = Activator | Any
    SnapshotType = AgentStateSnapshot | Any
else:
    ActivatorType = Any
    SnapshotType = Any

logger = get_emitter(__name__)

@dataclass
class StepContext:
    llm_response: LLMResponse | None = None

class StepHandler(ABC):
    @abstractmethod
    async def handle_async(
        self,
        activator: ActivatorType,
        snapshot: SnapshotType,
        on_event: Callable[[Any], Awaitable[None]],
        context: StepContext
    ) -> bool:
        """
        @desc: 비동기 파이프라인 전용 핸들러. 
               상태 변경이나 도구 실행 대신, on_event 콜백을 통해 Gov(환경)로 의사를 전달합니다.
        """
        pass