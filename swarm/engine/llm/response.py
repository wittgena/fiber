# swarm.engine.llm.response
## @lineage: agent.driver.llm.response
## @lineage: atoa.response
import warnings
from typing import ClassVar
from pydantic import BaseModel, ConfigDict

from tenant.switch.params import ResponsesAPIResponse
from tenant.switch.params import ModelResponse
from atoa.conv.message import Message
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
