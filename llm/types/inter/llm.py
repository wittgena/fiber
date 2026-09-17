# fiber.llm.types.inter.llm
from abc import abstractmethod
from typing import (
    Any,
    List,
    Optional,
    Sequence,
)

# Pydantic Imports
from fiber.llm.mapper.pydantic import (
    ConfigDict,
    Field,
    model_validator,
)

from fiber.llm.router.manager import CallbackManager
from fiber.llm.types.llm.block import (
    ChatMessage,
    ChatResponse,
    ChatResponseAsyncGen,
    ChatResponseGen,
    CompletionResponse,
    CompletionResponseAsyncGen,
    CompletionResponseGen,
    LLMMetadata,
    TextBlock,
)
from fiber.llm.types.inter.schema import BaseComponent

class BaseLLM(BaseComponent):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    callback_manager: CallbackManager = Field(
        default_factory=lambda: CallbackManager([]), exclude=True
    )
    rate_limiter: Optional[Any] = Field(
        default=None,
        description="Rate limiter instance to throttle API calls.",
        exclude=True,
    )

    @model_validator(mode="after")
    def check_callback_manager(self) -> "BaseLLM":
        if self.callback_manager is None:
            self.callback_manager = CallbackManager([])
        return self

    @property
    @abstractmethod
    def metadata(self) -> LLMMetadata:
        ...

    def convert_chat_messages(self, messages: Sequence[ChatMessage]) -> List[Any]:
        """Convert chat messages to an LLM specific message format."""
        converted_messages = []
        for message in messages:
            if isinstance(message.content, str):
                converted_messages.append(message)
            elif isinstance(message.content, List):
                content_string = ""
                for block in message.content:
                    if isinstance(block, TextBlock):
                        content_string += block.text
                    else:
                        raise ValueError("LLM only supports text inputs")
                message.content = content_string
                converted_messages.append(message)
            else:
                raise ValueError(f"Invalid message content: {message.content!s}")

        return converted_messages

    @abstractmethod
    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        ...

    @abstractmethod
    def complete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponse:
        ...

    @abstractmethod
    def stream_chat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseGen:
        ...

    @abstractmethod
    def stream_complete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponseGen:
        ...

    # ===== Async Endpoints =====
    @abstractmethod
    async def achat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponse:
        ...

    @abstractmethod
    async def acomplete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponse:
        ...

    @abstractmethod
    async def astream_chat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseAsyncGen:
        ...

    @abstractmethod
    async def astream_complete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponseAsyncGen:
        ...