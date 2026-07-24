# atoa.agent.response
## @lineage: atoa.call.response
## @lineage: agent.call.response
## @lineage: agent.llm.call.response
## @lineage: gov.llm.call.response
## @lineage: gov.llm.response
import warnings
from typing import ClassVar

from bound.gateway.switch.params import ResponsesAPIResponse
from bound.gateway.switch.params import ModelResponse
from pydantic import BaseModel, ConfigDict

from eco.agent.action.message import Message
from bound.watcher.snapshot.metrics import MetricsSnapshot

warnings.filterwarnings("ignore", message="Pydantic serializer warnings")

class LLMResponse(BaseModel):
    """Result of an LLM completion request"""
    message: Message
    metrics: MetricsSnapshot
    raw_response: ModelResponse | ResponsesAPIResponse
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    @property
    def id(self) -> str:
        """Get the response ID from the underlying LLM response"""
        return self.raw_response.id
