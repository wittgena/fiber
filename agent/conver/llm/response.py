# agent.conver.llm.response
## @lineage: phi.conver.llm.response
## @lineage: swarm.conver.llm.response
## @lineage: swarm.engine.llm.response
import warnings
from typing import ClassVar
from pydantic import BaseModel, ConfigDict

from runtime.client.param import ModelResponse
from agent.atoa.conv.message import Message
from mesh.cost.tracker.metric import MetricsSnapshot

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
