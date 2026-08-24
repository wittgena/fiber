# dphi.model.ext.llm.model.base
## @lineage: phase.client.model.llm.model.base
## @lineage: phase.client.ext.llm.model.base
## @lineage: bound.client.ext.llm.model.base
## @lineage: ator.client.ext.llm.model.base
## @lineage: bound.eco.agent.llm.model.base
## @lineage: eco.bound.agent.llm.model.base
## @lineage: bound.agent.llm.model.base
## @lineage: ext.router.llm.model.base
## @lineage: router.llm.model.base
## @lineage: engine.router.llm.model.base
## @lineage: engine.eco.llm.model.base
## @lineage: runtime.engine.eco.llm.model.base
## @lineage: eco.llms.model.base
## @lineage: eco.llms.base
## @lineage: eco.llama.llms.base
from abc import abstractmethod
from typing import (
    Any,
    List,
    Optional,
    Sequence,
)

from fiber.dphi.model.ext.llm.model.types.block import (
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
from fiber.dphi.model.mapper.pydantic import Field, model_validator, ConfigDict
from fiber.dphi.model.ext.callback.manager import CallbackManager
from fiber.dphi.model.ext.types.schema import BaseComponent


class BaseLLM(BaseComponent):
    """BaseLLM interface."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    callback_manager: CallbackManager = Field(
        default_factory=lambda: CallbackManager([]), exclude=True
    )
    # Expected type: BaseRateLimiter (from llama_index.core.rate_limiter)
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