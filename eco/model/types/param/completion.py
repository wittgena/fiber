# eco.model.types.param.completion
## @lineage: engine.model.types.param.completion
## @lineage: bound.model.types.param.completion
## @lineage: llm.types.param.completion
## @lineage: eco.mesh.model.types.param.completion
## @lineage: runtime.mesh.model.types.param.completion
## @lineage: mesh.model.types.param.completion
## @lineage: mesh.mapper.param.completion
## @lineage: bound.mapper.param.completion
"""
bound.mapper.param.completion
@desc: Lightweight type definitions for Chat Completion Parameters.
"""
from typing import Iterable, Union, Optional
from typing_extensions import Literal, Required, TypedDict


class ChatCompletionSystemMessageParam(TypedDict, total=False):
    content: Required[str]
    role: Required[Literal["system"]]
    name: str

class ChatCompletionContentPartTextParam(TypedDict, total=False):
    text: Required[str]
    type: Required[Literal["text"]]

class ImageURL(TypedDict, total=False):
    url: Required[str]
    detail: Literal["auto", "low", "high"]

class ChatCompletionContentPartImageParam(TypedDict, total=False):
    image_url: Required[ImageURL]
    type: Required[Literal["image_url"]]

ChatCompletionContentPartParam = Union[
    ChatCompletionContentPartTextParam, ChatCompletionContentPartImageParam
]

class ChatCompletionUserMessageParam(TypedDict, total=False):
    content: Required[Union[str, Iterable[ChatCompletionContentPartParam]]]
    role: Required[Literal["user"]]
    name: str

class FunctionCall(TypedDict, total=False):
    arguments: Required[str]
    name: Required[str]

class Function(TypedDict, total=False):
    arguments: Required[str]
    name: Required[str]

class ChatCompletionToolMessageParam(TypedDict, total=False):
    content: Required[Union[str, Iterable[ChatCompletionContentPartParam]]]
    role: Required[Literal["tool"]]
    tool_call_id: Required[str]

class ChatCompletionFunctionMessageParam(TypedDict, total=False):
    content: Required[Union[str, Iterable[ChatCompletionContentPartParam]]]
    name: Required[str]
    role: Required[Literal["function"]]

class ChatCompletionMessageToolCallParam(TypedDict, total=False):
    id: Required[str]
    function: Required[Function]
    type: Required[Literal["function"]]

class ChatCompletionAssistantMessageParam(TypedDict, total=False):
    role: Required[Literal["assistant"]]
    content: Optional[str]
    function_call: FunctionCall
    name: str
    tool_calls: Iterable[ChatCompletionMessageToolCallParam]

ChatCompletionMessageParam = Union[
    ChatCompletionSystemMessageParam,
    ChatCompletionUserMessageParam,
    ChatCompletionAssistantMessageParam,
    ChatCompletionFunctionMessageParam,
    ChatCompletionToolMessageParam,
]