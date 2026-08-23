# phase.client.ext.llm.openlike
## @lineage: bound.client.ext.llm.openlike
## @lineage: ator.client.ext.llm.openlike
## @lineage: bound.eco.agent.llm.openlike
## @lineage: eco.bound.agent.llm.openlike
## @lineage: bound.agent.llm.openlike
## @lineage: ext.router.llm.openlike
## @lineage: router.llm.openlike
## @lineage: engine.router.llm.openlike
## @lineage: engine.eco.llm.openlike
## @lineage: runtime.engine.eco.llm.openlike
## @lineage: eco.llms.openlike
from typing import Any, Optional, Sequence, Union
from fiber.phase.client.ext.llm.model.types.block import (
    ChatMessage,
    ChatResponse,
    ChatResponseAsyncGen,
    ChatResponseGen,
    CompletionResponse,
    CompletionResponseAsyncGen,
    CompletionResponseGen,
    LLMMetadata,
)
from fiber.agent.llm.router.pydantic import Field
from fiber.agent.llm.router.constants import DEFAULT_CONTEXT_WINDOW
from fiber.phase.client.ext.llm.handle.converter import async_stream_completion_response_to_chat_response, completion_response_to_chat_response, stream_completion_response_to_chat_response
from llama_index.llms.openai.base import OpenAI, Tokenizer

class OpenAILike(OpenAI):
    context_window: int = Field(
        default=DEFAULT_CONTEXT_WINDOW,
        description=LLMMetadata.model_fields["context_window"].description,
    )
    is_chat_model: bool = Field(
        default=False,
        description=LLMMetadata.model_fields["is_chat_model"].description,
    )
    is_function_calling_model: bool = Field(
        default=False,
        description=LLMMetadata.model_fields["is_function_calling_model"].description,
    )
    should_use_structured_outputs: bool = Field(
        default=False,
        # https://platform.openai.com/docs/guides/structured-outputs
        description=(
            "Set True if the model supports structured output through response_format."
        ),
    )
    tokenizer: Union[Tokenizer, str, None] = Field(
        default=None,
        description=(
            "An instance of a tokenizer object that has an encode method, or the name"
            " of a tokenizer model from Hugging Face. If left as None, then this"
            " disables inference of max_tokens."
        ),
    )

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if isinstance(self.tokenizer, str):
            try:
                import transformers  # noqa: F401
            except ImportError:
                raise ImportError(
                    "The `transformers` package is required when passing a string "
                    "tokenizer name. Install it with: "
                    "`pip install llama-index-llms-openai-like[transformers]`"
                )

    @property
    def metadata(self) -> LLMMetadata:
        return LLMMetadata(
            context_window=self.context_window,
            num_output=self.max_tokens or -1,
            is_chat_model=self.is_chat_model,
            is_function_calling_model=self.is_function_calling_model,
            model_name=self.model,
        )

    @property
    def _tokenizer(self) -> Optional[Tokenizer]:
        if isinstance(self.tokenizer, str):
            from transformers import AutoTokenizer

            return AutoTokenizer.from_pretrained(self.tokenizer)
        return self.tokenizer

    @classmethod
    def class_name(cls) -> str:
        return "OpenAILike"

    def _should_use_structure_outputs(self) -> bool:
        return self.should_use_structured_outputs

    def complete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponse:
        """Complete the prompt."""
        if not formatted:
            prompt = self.completion_to_prompt(prompt)

        return super().complete(prompt, **kwargs)

    def stream_complete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponseGen:
        """Stream complete the prompt."""
        if not formatted:
            prompt = self.completion_to_prompt(prompt)

        return super().stream_complete(prompt, **kwargs)

    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        """Chat with the model."""
        if not self.metadata.is_chat_model:
            prompt = self.messages_to_prompt(messages)
            completion_response = self.complete(prompt, formatted=True, **kwargs)
            return completion_response_to_chat_response(completion_response)

        return super().chat(messages, **kwargs)

    def stream_chat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseGen:
        if not self.metadata.is_chat_model:
            prompt = self.messages_to_prompt(messages)
            completion_response = self.stream_complete(prompt, formatted=True, **kwargs)
            return stream_completion_response_to_chat_response(completion_response)

        return super().stream_chat(messages, **kwargs)

    # -- Async methods --

    async def acomplete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponse:
        """Complete the prompt."""
        if not formatted:
            prompt = self.completion_to_prompt(prompt)

        return await super().acomplete(prompt, **kwargs)

    async def astream_complete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponseAsyncGen:
        """Stream complete the prompt."""
        if not formatted:
            prompt = self.completion_to_prompt(prompt)

        return await super().astream_complete(prompt, **kwargs)

    async def achat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponse:
        """Chat with the model."""
        if not self.metadata.is_chat_model:
            prompt = self.messages_to_prompt(messages)
            completion_response = await self.acomplete(prompt, formatted=True, **kwargs)
            return completion_response_to_chat_response(completion_response)

        return await super().achat(messages, **kwargs)

    async def astream_chat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseAsyncGen:
        if not self.metadata.is_chat_model:
            prompt = self.messages_to_prompt(messages)
            completion_response = await self.astream_complete(
                prompt, formatted=True, **kwargs
            )
            return async_stream_completion_response_to_chat_response(
                completion_response
            )

        return await super().astream_chat(messages, **kwargs)
