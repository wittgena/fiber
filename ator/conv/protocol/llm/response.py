# ator.conv.protocol.llm.response
## @lineage: engine.protocol.llm.response
## @lineage: agent.protocol.llm.response
## @lineage: engine.protocol.atoa.schema.llm.response
## @lineage: phi.agent.atoa.schema.llm.response
## @lineage: agent.atoa.schema.llm.response
## @lineage: agent.conver.llm.response
import warnings
from typing import ClassVar
from pydantic import BaseModel, ConfigDict

from ator.client.model.param import ModelResponse
from ator.conv.schema.message import Message
from bound.xor.model.metric import MetricsSnapshot

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
