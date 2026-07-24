# agent.atoa.response
## @lineage: atoa.agent.response
## @lineage: atoa.call.response
## @lineage: agent.call.response
## @lineage: agent.llm.call.response
## @lineage: gov.llm.call.response
## @lineage: gov.llm.response
import warnings
from typing import ClassVar

from eco.tenant.switch.params import ResponsesAPIResponse
from eco.tenant.switch.params import ModelResponse
from pydantic import BaseModel, ConfigDict

from agent.eco.action.message import Message
from eco.watcher.snapshot.metrics import MetricsSnapshot

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
