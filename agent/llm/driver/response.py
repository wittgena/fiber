# agent.llm.driver.response
## @lineage: ator.driver.llm.response
import warnings
from typing import ClassVar
from pydantic import BaseModel, ConfigDict

from fiber.agent.anchor.llm.param import ModelResponse
from fiber.agent.space.action.message import Message
from fiber.agent.anchor.model.metric import MetricsSnapshot

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
