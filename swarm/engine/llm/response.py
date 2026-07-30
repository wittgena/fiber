# swarm.engine.llm.response
import warnings
from typing import ClassVar
from pydantic import BaseModel, ConfigDict

from tenant.switch.params import ModelResponse
from swarm.atoa.conv.message import Message
from tenant.phi.cost.tracker.metric import MetricsSnapshot

warnings.filterwarnings("ignore", message="Pydantic serializer warnings")

class LLMResponse(BaseModel):
    """Result of an LLM completion request"""
    message: Message
    metrics: MetricsSnapshot
    raw_response: ModelResponse
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    @property
    def id(self) -> str:
        """Get the response ID from the underlying LLM response"""
        return self.raw_response.id
