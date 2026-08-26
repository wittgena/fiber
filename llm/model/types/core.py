# llm.model.types.core
import time
from typing import Any, Dict, List, Literal, Optional, Union
from typing_extensions import Required, TypedDict
from pydantic import Field
from xphi.arch.model.surge.model import DynamicSurgeModel

Phase = Optional[Literal["commentary", "final_answer"]]

class ChatCompletionToolParamFunctionChunk(TypedDict, total=False):
    name: Required[str]
    description: str
    parameters: dict
    strict: bool

class ChatCompletionToolParam(TypedDict):
    type: Literal["function"]
    function: ChatCompletionToolParamFunctionChunk

class ChatCompletionUserMessageParam(TypedDict):
    role: Literal["user"]
    content: Union[str, List[Dict[str, Any]]] 

class Function(DynamicSurgeModel):
    name: str = ""
    arguments: str = ""

class ChatCompletionMessageToolCall(DynamicSurgeModel):
    id: str = Field(default_factory=lambda: f"call_{int(time.time())}")
    type: Literal["function"] = "function"
    function: Function = Field(default_factory=Function)

class Message(DynamicSurgeModel):
    role: str = "assistant"
    content: Optional[str] = None
    tool_calls: Optional[List[ChatCompletionMessageToolCall]] = None

class Delta(DynamicSurgeModel):
    role: Optional[str] = None
    content: Optional[str] = None
    tool_calls: Optional[List[ChatCompletionMessageToolCall]] = None

class Usage(DynamicSurgeModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

class Choices(DynamicSurgeModel):
    index: int = 0
    message: Message = Field(default_factory=Message)
    finish_reason: Optional[str] = None

class StreamingChoices(DynamicSurgeModel):
    index: int = 0
    delta: Delta = Field(default_factory=Delta)
    finish_reason: Optional[str] = None

class ModelResponse(DynamicSurgeModel):
    id: str = Field(default_factory=lambda: f"chatcmpl-{int(time.time())}")
    model: Optional[str] = None
    choices: List[Choices] = Field(default_factory=lambda: [Choices()])
    usage: Optional[Usage] = None
    provider_metadata: Optional[Dict[str, Any]] = None 

class EmbeddingResponse(DynamicSurgeModel):
    data: List[Dict[str, Any]] = Field(default_factory=list)
    model: Optional[str] = None
    usage: Optional[Usage] = None

class FunctionCall(DynamicSurgeModel):
    arguments: str = ""
    name: Optional[str] = None

class OutputFunctionToolCall(DynamicSurgeModel):
    """A tool call to run a function"""
    arguments: Optional[str] = None
    call_id: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None # "function_call"
    id: Optional[str] = None
    status: Literal["in_progress", "completed", "incomplete"] = "in_progress"
    phase: Phase = None