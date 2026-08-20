# bound.xor.model.types.anthropic
## @lineage: eco.model.types.anthropic
## @lineage: engine.model.types.anthropic
## @lineage: bound.model.types.anthropic
## @lineage: llm.types.anthropic
## @lineage: eco.mesh.model.types.anthropic
## @lineage: runtime.mesh.model.types.anthropic
## @lineage: mesh.model.types.anthropic
## @lineage: tenant.model.types.anthropic
## @lineage: tenant.legacy.anthropic
## @lineage: bound.resolver.legacy.anthropic
## @lineage: bound.legacy.anthropic
from typing import Iterable, Optional, Union
from typing_extensions import Literal, Required, TypedDict
from bound.xor.model.types.openai import ChatCompletionCachedContent

class AnthropicThinkingParam(TypedDict, total=False):
    type: Literal["enabled", "adaptive"]
    budget_tokens: int

class DirectToolCaller(TypedDict, total=False):
    type: Required[Literal["direct"]]

class CodeExecutionToolCaller(TypedDict, total=False):
    type: Required[Literal["code_execution_20250825"]]
    tool_id: Required[str]

ToolCaller = Union[DirectToolCaller, CodeExecutionToolCaller]

class AnthropicMessagesToolUseParam(TypedDict, total=False):
    type: Required[Literal["tool_use"]]
    id: str
    name: str
    input: dict
    cache_control: Optional[Union[dict, ChatCompletionCachedContent]]
    caller: Optional[ToolCaller]

class AnthropicMessagesToolResultContent(TypedDict, total=False):
    type: Required[Literal["text"]]
    text: Required[str]
    cache_control: Optional[Union[dict, ChatCompletionCachedContent]]

class AnthropicContentParamSource(TypedDict):
    type: Literal["base64"]
    media_type: str
    data: str

class AnthropicContentParamSourceUrl(TypedDict):
    type: Literal["url"]
    url: str

class AnthropicContentParamSourceFileId(TypedDict):
    type: Literal["file"]
    file_id: str

class AnthropicMessagesImageParam(TypedDict, total=False):
    type: Required[Literal["image"]]
    source: Required[
        Union[
            AnthropicContentParamSource,
            AnthropicContentParamSourceFileId,
            AnthropicContentParamSourceUrl,
        ]
    ]
    cache_control: Optional[Union[dict, ChatCompletionCachedContent]]

class CitationsObject(TypedDict):
    enabled: bool

class AnthropicMessagesDocumentParam(TypedDict, total=False):
    type: Required[Literal["document"]]
    source: Required[
        Union[
            AnthropicContentParamSource,
            AnthropicContentParamSourceFileId,
            AnthropicContentParamSourceUrl,
        ]
    ]
    cache_control: Optional[Union[dict, ChatCompletionCachedContent]]
    title: str
    context: str
    citations: Optional[CitationsObject]

class AnthropicMessagesToolResultParam(TypedDict, total=False):
    type: Required[Literal["tool_result"]]
    tool_use_id: Required[str]
    is_error: bool
    content: Union[
        str,
        Iterable[
            Union[
                AnthropicMessagesToolResultContent,
                AnthropicMessagesImageParam,
                AnthropicMessagesDocumentParam,
            ]
        ],
    ]
    cache_control: Optional[Union[dict, ChatCompletionCachedContent]]