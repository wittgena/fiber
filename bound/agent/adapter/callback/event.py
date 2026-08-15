# ext.router.adapter.callback.event
## @lineage: router.adapter.callback.event
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from bound.agent.adapter.pydantic import BaseModel, SerializeAsAny, ConfigDict
from bound.agent.llm.model.types.block import (
    ChatMessage,
    ChatResponse,
    CompletionResponse,
)
from bound.agent.adapter.pydantic import ConfigDict, Field
from bound.agent.llm.handle.template import BasePromptTemplate
from arch.model.phase.gate import uuid4

class BaseEvent(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    timestamp: datetime = Field(default_factory=lambda: datetime.now())
    id_: str = Field(default_factory=lambda: str(uuid4()))
    span_id: Optional[str] = Field(default=None)
    tags: Dict[str, Any] = Field(default={})

    @classmethod
    def class_name(cls) -> str:
        return "BaseEvent"

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        return self.model_dump(**kwargs)

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        data = super().model_dump(**kwargs)
        data["class_name"] = self.class_name()
        return data

class EmbeddingStartEvent(BaseEvent):
    model_config = ConfigDict(protected_namespaces=("pydantic_model_",))
    model_dict: dict

    @classmethod
    def class_name(cls) -> str:
        return "EmbeddingStartEvent"


class EmbeddingEndEvent(BaseEvent):
    chunks: List[str]
    embeddings: List[List[float]]

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "EmbeddingEndEvent"


class SparseEmbeddingStartEvent(EmbeddingStartEvent):
    @classmethod
    def class_name(cls) -> str:
        return "SparseEmbeddingStartEvent"


class SparseEmbeddingEndEvent(BaseEvent):
    chunks: List[str]
    embeddings: List[Dict[int, float]]
    @classmethod
    def class_name(cls) -> str:
        return "SparseEmbeddingEndEvent"

###
class ExceptionEvent(BaseEvent):
    exception: BaseException

    @classmethod
    def class_name(cls) -> str:
        return "ExceptionEvent"

class LLMPredictStartEvent(BaseEvent):
    template: SerializeAsAny[BasePromptTemplate]
    template_args: Optional[dict]

    @classmethod
    def class_name(cls) -> str:
        return "LLMPredictStartEvent"


class LLMPredictEndEvent(BaseEvent):
    output: str

    @classmethod
    def class_name(cls) -> str:
        return "LLMPredictEndEvent"


class LLMStructuredPredictStartEvent(BaseEvent):
    output_cls: Any
    template: SerializeAsAny[BasePromptTemplate]
    template_args: Optional[dict]

    @classmethod
    def class_name(cls) -> str:
        return "LLMStructuredPredictStartEvent"

class LLMStructuredPredictEndEvent(BaseEvent):
    output: SerializeAsAny[Any]

    @classmethod
    def class_name(cls) -> str:
        return "LLMStructuredPredictEndEvent"

class LLMStructuredPredictInProgressEvent(BaseEvent):
    output: SerializeAsAny[Any]
    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "LLMStructuredPredictInProgressEvent"

class LLMCompletionStartEvent(BaseEvent):
    model_config = ConfigDict(protected_namespaces=("pydantic_model_",))
    prompt: str
    additional_kwargs: dict
    model_dict: dict

    @classmethod
    def class_name(cls) -> str:
        return "LLMCompletionStartEvent"

class LLMCompletionInProgressEvent(BaseEvent):
    prompt: str
    response: CompletionResponse

    @classmethod
    def class_name(cls) -> str:
        return "LLMCompletionInProgressEvent"

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        if isinstance(self.response.raw, BaseModel):
            return self.model_copy(
                update={
                    "response": self.response.model_copy(
                        update={"raw": self.response.raw.model_dump()}
                    )
                }
            ).model_dump(**kwargs)
        return super().model_dump(**kwargs)


class LLMCompletionEndEvent(BaseEvent):
    prompt: str
    response: CompletionResponse

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "LLMCompletionEndEvent"

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        if isinstance(self.response.raw, BaseModel):
            return self.model_copy(
                update={
                    "response": self.response.model_copy(
                        update={"raw": self.response.raw.model_dump()}
                    )
                }
            ).model_dump(**kwargs)
        return super().model_dump(**kwargs)


class LLMChatStartEvent(BaseEvent):
    model_config = ConfigDict(protected_namespaces=("pydantic_model_",))
    messages: List[ChatMessage]
    additional_kwargs: dict
    model_dict: dict

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "LLMChatStartEvent"


class LLMChatInProgressEvent(BaseEvent):
    messages: List[ChatMessage]
    response: ChatResponse

    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "LLMChatInProgressEvent"

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        if isinstance(self.response.raw, BaseModel):
            return self.model_copy(
                update={
                    "response": self.response.model_copy(
                        update={"raw": self.response.raw.model_dump()}
                    )
                }
            ).model_dump(**kwargs)
        return super().model_dump(**kwargs)


class LLMChatEndEvent(BaseEvent):
    messages: List[ChatMessage]
    response: Optional[ChatResponse]

    @classmethod
    def class_name(cls) -> str:
        return "LLMChatEndEvent"

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        if self.response is not None and isinstance(self.response.raw, BaseModel):
            return self.model_copy(
                update={
                    "response": self.response.model_copy(
                        update={"raw": self.response.raw.model_dump()}
                    )
                }
            ).model_dump(**kwargs)
        return super().model_dump(**kwargs)

TIMESTAMP_FORMAT = "%m/%d/%Y, %H:%M:%S.%f"
BASE_TRACE_EVENT = "root"

class CBEventType(str, Enum):
    CHUNKING = "chunking"
    NODE_PARSING = "node_parsing"
    EMBEDDING = "embedding"
    LLM = "llm"
    QUERY = "query"
    RETRIEVE = "retrieve"
    SYNTHESIZE = "synthesize"
    TREE = "tree"
    SUB_QUESTION = "sub_question"
    TEMPLATING = "templating"
    FUNCTION_CALL = "function_call"
    RERANKING = "reranking"
    EXCEPTION = "exception"
    AGENT_STEP = "agent_step"

class EventPayload(str, Enum):
    DOCUMENTS = "documents"  # list of documents before parsing
    CHUNKS = "chunks"  # list of text chunks
    NODES = "nodes"  # list of nodes
    PROMPT = "formatted_prompt"  # formatted prompt sent to LLM
    MESSAGES = "messages"  # list of messages sent to LLM
    COMPLETION = "completion"  # completion from LLM
    RESPONSE = "response"  # message response from LLM
    QUERY_STR = "query_str"  # query used for query engine
    SUB_QUESTION = "sub_question"  # a sub question & answer + sources
    EMBEDDINGS = "embeddings"  # list of embeddings
    TOP_K = "top_k"  # top k nodes retrieved
    ADDITIONAL_KWARGS = "additional_kwargs"  # additional kwargs for event call
    SERIALIZED = "serialized"  # serialized object for event caller
    FUNCTION_CALL = "function_call"  # function call for the LLM
    FUNCTION_OUTPUT = "function_call_response"  # function call output
    TOOL = "tool"  # tool used in LLM call
    MODEL_NAME = "model_name"  # model name used in an event
    TEMPLATE = "template"  # template used in LLM call
    TEMPLATE_VARS = "template_vars"  # template variables used in LLM call
    SYSTEM_PROMPT = "system_prompt"  # system prompt used in LLM call
    QUERY_WRAPPER_PROMPT = "query_wrapper_prompt"  # query wrapper prompt used in LLM
    EXCEPTION = "exception"  # exception raised in an event

LEAF_EVENTS = (CBEventType.CHUNKING, CBEventType.LLM, CBEventType.EMBEDDING)

@dataclass
class CBEvent:
    event_type: CBEventType
    payload: Optional[Dict[str, Any]] = None
    time: str = ""
    id_: str = ""

    def __post_init__(self) -> None:
        """Init time and id if needed."""
        if not self.time:
            self.time = datetime.now().strftime(TIMESTAMP_FORMAT)
        if not self.id_:
            self.id = str(uuid.uuid4())


@dataclass
class EventStats:
    """Time-based Statistics for events."""

    total_secs: float
    average_secs: float
    total_count: int
