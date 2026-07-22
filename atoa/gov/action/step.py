# atoa.gov.action.step
## @lineage: gov.action.step
## @lineage: gov.engine.action.step
from abc import ABC, abstractmethod
import json
from typing import TYPE_CHECKING, Any
from dataclasses import dataclass, field
from pydantic import PrivateAttr, ValidationError, model_validator
from atoa.call.types import ConversationCallbackType, ConversationTokenCallbackType
from atoa.call.response import LLMResponse
from eco.call.action.message import Message, TextContent
from watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from atoa.agent.disc.base.conv import ProtoConv
    from atoa.activator import Activator
    ActivatorType = Activator | Any
    ConvType = ProtoConv | Any
else:
    ActivatorType = Any
    ConvType = Any

logger = get_emitter(__name__)

@dataclass
class StepContext:
    llm_response: LLMResponse | None = None

class StepHandler(ABC):
    @abstractmethod
    def handle(
        self,
        activator: ActivatorType,
        conversation: ConvType,
        on_event: ConversationCallbackType,
        on_token: ConversationTokenCallbackType | None,
        context: StepContext
    ) -> bool:
        pass