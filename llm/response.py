# fiber.llm.response
## @lineage: llm.response
import warnings
from typing import ClassVar
from pydantic import BaseModel, ConfigDict

from fiber.llm.model.message import Message
from fiber.llm.param import ModelResponse
from fiber.llm.model.metric import MetricsSnapshot

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
