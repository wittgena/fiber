# eco.model.types.openai
## @lineage: engine.model.types.openai
## @lineage: bound.model.types.openai
## @lineage: llm.types.openai
## @lineage: eco.mesh.model.types.openai
## @lineage: runtime.mesh.model.types.openai
## @lineage: mesh.model.types.openai
## @lineage: tenant.model.types.openai
from typing import Any, Dict, Iterable, List, Literal, Optional, Union
from typing_extensions import Annotated, NotRequired, Required, TypedDict, override
from openai.types.chat import ChatCompletionChunk
from openai.types.chat.chat_completion_content_part_input_audio_param import ChatCompletionContentPartInputAudioParam

ValidUserMessageContentTypesLiteral = Literal[
    "text",
    "image_url",
    "input_audio",
    "audio_url",
    "document",
    "guarded_text",
    "video_url",
    "file",
]

ValidUserMessageContentTypes = [
    "text",
    "image_url",
    "input_audio",
    "audio_url",
    "document",
    "guarded_text",
    "video_url",
    "file",
]

ValidAssistantMessageContentTypesLiteral = Literal[
    "text",
    "thinking",
    "redacted_thinking",
    "image_url",
]

ValidAssistantMessageContentTypes = [
    "text",
    "thinking",
    "redacted_thinking",
    "image_url",
]

ValidChatCompletionMessageContentTypesLiteral = Literal[
    "text",
    "image_url",
    "input_audio",
    "audio_url",
    "document",
    "guarded_text",
    "video_url",
    "file",
    "thinking",
    "redacted_thinking",
]

ValidChatCompletionMessageContentTypes = [
    "text",
    "image_url",
    "input_audio",
    "audio_url",
    "document",
    "guarded_text",
    "video_url",
    "file",
    "thinking",
    "redacted_thinking",
]

## Base Primitives & Caching
class ChatCompletionCachedContent(TypedDict):
    type: Literal["ephemeral"]

class Function(TypedDict, total=False):
    name: Required[str]

class ChatCompletionNamedToolChoiceParam(TypedDict, total=False):
    function: Required[Function]
    type: Required[Literal["function"]]

class ChatCompletionToolChoiceFunctionParam(TypedDict):
    name: str

class ChatCompletionToolChoiceObjectParam(TypedDict):
    type: Literal["function"]
    function: ChatCompletionToolChoiceFunctionParam

## Tool & Function Declarations
class ChatCompletionToolCallFunctionChunk(TypedDict, total=False):
    name: Optional[str]
    arguments: str
    provider_specific_fields: Optional[Dict[str, Any]]

class ChatCompletionToolParamFunctionChunk(TypedDict, total=False):
    name: Required[str]
    description: str
    parameters: dict
    strict: bool

class OpenAIChatCompletionToolParam(TypedDict):
    type: Union[Literal["function"], str]
    function: ChatCompletionToolCallFunctionChunk

class ChatCompletionToolParam(OpenAIChatCompletionToolParam, total=False):
    cache_control: ChatCompletionCachedContent

class ChatCompletionAssistantToolCall(TypedDict):
    id: Optional[str]
    type: Literal["function"]
    function: ChatCompletionToolCallFunctionChunk

class ChatCompletionToolCallChunk(TypedDict):
    id: Optional[str]
    type: Literal["function"]
    function: ChatCompletionToolCallFunctionChunk
    index: int

class ChatCompletionDeltaToolCallChunk(TypedDict, total=False):
    id: str
    type: Literal["function"]
    function: ChatCompletionToolCallFunctionChunk
    index: int

## Reasoning & Thinking Blocks
class ChatCompletionThinkingBlock(TypedDict, total=False):
    type: Required[Literal["thinking"]]
    thinking: str
    signature: str
    cache_control: Optional[Union[dict, ChatCompletionCachedContent]]

class ChatCompletionRedactedThinkingBlock(TypedDict, total=False):
    type: Required[Literal["redacted_thinking"]]
    data: str
    cache_control: Optional[Union[dict, ChatCompletionCachedContent]]

class ChatCompletionReasoningSummaryTextBlock(TypedDict, total=False):
    type: Required[Literal["summary_text"]]
    text: str

class ChatCompletionReasoningItem(TypedDict, total=False):
    type: Required[Literal["reasoning"]]
    id: str
    encrypted_content: Optional[str]
    summary: List[ChatCompletionReasoningSummaryTextBlock]

## Message Content Blocks
class OpenAIChatCompletionTextObject(TypedDict):
    type: Literal["text"]
    text: str

class ChatCompletionTextObject(OpenAIChatCompletionTextObject, total=False):
    cache_control: ChatCompletionCachedContent

class ChatCompletionImageUrlObject(TypedDict, total=False):
    url: Required[str]
    detail: str
    format: str

class ChatCompletionImageObject(TypedDict):
    type: Literal["image_url"]
    image_url: Union[str, ChatCompletionImageUrlObject]

class ChatCompletionVideoUrlObject(TypedDict, total=False):
    url: Required[str]
    detail: str

class ChatCompletionVideoObject(TypedDict):
    type: Literal["video_url"]
    video_url: Union[str, ChatCompletionVideoUrlObject]

class ChatCompletionAudioObject(ChatCompletionContentPartInputAudioParam):
    pass

class DocumentObject(TypedDict):
    type: Literal["text"]
    media_type: str
    data: str

class CitationsObject(TypedDict):
    enabled: bool

class ChatCompletionDocumentObject(TypedDict):
    type: Literal["document"]
    source: DocumentObject
    title: str
    context: str
    citations: Optional[CitationsObject]

class ChatCompletionFileObjectFile(TypedDict, total=False):
    file_data: str
    file_id: str
    filename: str
    format: str
    detail: str
    video_metadata: Dict[str, Any]

class ChatCompletionFileObject(TypedDict):
    type: Literal["file"]
    file: ChatCompletionFileObjectFile

OpenAIMessageContentListBlock = Union[
    ChatCompletionTextObject,
    ChatCompletionImageObject,
    ChatCompletionAudioObject,
    ChatCompletionDocumentObject,
    ChatCompletionVideoObject,
    ChatCompletionFileObject,
]

OpenAIMessageContent = Union[
    str,
    Iterable[OpenAIMessageContentListBlock],
]

## Messages
class OpenAIChatCompletionSystemMessage(TypedDict, total=False):
    role: Required[Literal["system"]]
    content: Required[Union[str, List]]
    name: str

class ChatCompletionSystemMessage(OpenAIChatCompletionSystemMessage, total=False):
    cache_control: ChatCompletionCachedContent

class OpenAIChatCompletionDeveloperMessage(TypedDict, total=False):
    role: Required[Literal["developer"]]
    content: Required[Union[str, List]]
    name: str

class ChatCompletionDeveloperMessage(OpenAIChatCompletionDeveloperMessage, total=False):
    cache_control: ChatCompletionCachedContent

class OpenAIChatCompletionUserMessage(TypedDict):
    role: Literal["user"]
    content: OpenAIMessageContent

class ChatCompletionUserMessage(OpenAIChatCompletionUserMessage, total=False):
    cache_control: ChatCompletionCachedContent

class OpenAIChatCompletionAssistantMessage(TypedDict, total=False):
    role: Required[Literal["assistant"]]
    content: Optional[
        Union[
            str,
            Iterable[
                Union[
                    ChatCompletionTextObject,
                    ChatCompletionThinkingBlock,
                    ChatCompletionRedactedThinkingBlock,
                    ChatCompletionImageObject,
                ]
            ],
        ]
    ]
    name: Optional[str]
    tool_calls: Optional[List[ChatCompletionAssistantToolCall]]
    function_call: Optional[ChatCompletionToolCallFunctionChunk]
    reasoning_content: Optional[str]

class ChatCompletionAssistantMessage(OpenAIChatCompletionAssistantMessage, total=False):
    cache_control: ChatCompletionCachedContent
    thinking_blocks: Optional[
        List[Union[ChatCompletionThinkingBlock, ChatCompletionRedactedThinkingBlock]]
    ]
    reasoning_items: Optional[List[ChatCompletionReasoningItem]]

class ChatCompletionToolMessage(TypedDict):
    role: Literal["tool"]
    content: Union[str, Iterable[ChatCompletionTextObject]]
    tool_call_id: str

class ChatCompletionFunctionMessage(TypedDict):
    role: Literal["function"]
    content: Optional[Union[str, Iterable[ChatCompletionTextObject]]]
    name: str
    tool_call_id: Optional[str]

AllMessageValues = Union[
    ChatCompletionUserMessage,
    ChatCompletionAssistantMessage,
    ChatCompletionToolMessage,
    ChatCompletionSystemMessage,
    ChatCompletionFunctionMessage,
    ChatCompletionDeveloperMessage,
]

## Streams & Processing (Chunks, Usage, Outcomes)
class ChatCompletionDeltaChunk(TypedDict, total=False):
    content: Optional[str]
    tool_calls: List[ChatCompletionDeltaToolCallChunk]
    role: str

OpenAIChatCompletionFinishReason = Literal[
    "stop",
    "content_filter",
    "function_call",
    "tool_calls",
    "length",
    "guardrail_intervened",
    "eos",
    "finish_reason_unspecified",
    "malformed_function_call",
]

class ChatCompletionUsageBlock(TypedDict, total=False):
    prompt_tokens: Required[int]
    completion_tokens: Required[int]
    total_tokens: Required[int]
    prompt_tokens_details: Optional[dict]
    completion_tokens_details: Optional[dict]

class OpenAIChatCompletionChunk(ChatCompletionChunk):
    def __init__(self, **kwargs):
        # Set the 'object' kwarg to 'chat.completion.chunk'
        kwargs["object"] = "chat.completion.chunk"
        super().__init__(**kwargs)

## Web Search Options
class OpenAIWebSearchUserLocationApproximate(TypedDict):
    city: str
    country: str
    region: str
    timezone: str

class OpenAIWebSearchUserLocation(TypedDict):
    approximate: OpenAIWebSearchUserLocationApproximate
    type: Literal["approximate"]

class OpenAIWebSearchOptions(TypedDict, total=False):
    search_context_size: Optional[Literal["low", "medium", "high"]]
    user_location: Optional[OpenAIWebSearchUserLocation]

WebSearchOptions = OpenAIWebSearchOptions