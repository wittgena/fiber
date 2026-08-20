# ator.client.ext.llm.handle.converter
## @lineage: bound.eco.agent.llm.handle.converter
## @lineage: eco.bound.agent.llm.handle.converter
## @lineage: bound.agent.llm.handle.converter
## @lineage: ext.router.llm.handle.converter
## @lineage: router.llm.handle.converter
## @lineage: engine.router.llm.handle.converter
import base64
import json
import os
from binascii import Error as BinasciiError
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
)
from ator.client.ext.llm.model.types.block import (
    ChatMessage,
    ChatResponse,
    ChatResponseAsyncGen,
    ChatResponseGen,
    CompletionResponse,
    CompletionResponseAsyncGen,
    CompletionResponseGen,
    ImageBlock,
    MessageRole,
)
from bound.agent.types.schema import ImageNode
from bound.agent.callback.manager import CallbackManager

def parse_partial_json(s: str) -> Dict:
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass

    new_s = ""
    stack = []
    is_inside_string = False
    escaped = False
    for char in s:
        if is_inside_string:
            if char == '"' and not escaped:
                is_inside_string = False
            elif char == "\n" and not escaped:
                char = "\\n"  # Replace the newline character with the escape sequence.
            elif char == "\\":
                escaped = not escaped
            else:
                escaped = False
        else:
            if char == '"':
                is_inside_string = True
                escaped = False
            elif char == "{":
                stack.append("}")
            elif char == "[":
                stack.append("]")
            elif char == "}" or char == "]":
                if stack and stack[-1] == char:
                    stack.pop()
                else:
                    # Mismatched closing character; the input is malformed.
                    raise ValueError("Malformed partial JSON encountered.")

        # Append the processed character to the new string.
        new_s += char

    if is_inside_string and '"' in new_s and ":" not in new_s[new_s.rindex('"') :]:
        new_s = new_s[: new_s.rindex('"')]
    elif is_inside_string:
        new_s += '"'

    new_s = new_s.rstrip()
    if new_s.endswith(":"):
        new_s += " null"  # Add a default value for incomplete value
    elif new_s.endswith(","):
        new_s = new_s[:-1]  # Remove the trailing comma

    for closing_char in reversed(stack):
        new_s += closing_char

    try:
        return json.loads(new_s)
    except json.JSONDecodeError:
        # If we still can't parse the string as JSON, raise error to indicate failure.
        raise ValueError("Malformed partial JSON encountered.")

def messages_to_history_str(messages: Sequence[ChatMessage]) -> str:
    """Convert messages to a history string."""
    string_messages = []
    for message in messages:
        role = message.role
        content = message.content
        string_message = f"{role.value}: {content}"

        additional_kwargs = message.additional_kwargs
        if additional_kwargs:
            string_message += f"\n{additional_kwargs}"
        string_messages.append(string_message)
    return "\n".join(string_messages)


def messages_to_prompt(messages: Sequence[ChatMessage]) -> str:
    """Convert messages to a prompt string."""
    string_messages = []
    for message in messages:
        role = message.role
        content = message.content
        string_message = f"{role.value}: {content}"

        additional_kwargs = message.additional_kwargs
        if additional_kwargs:
            string_message += f"\n{additional_kwargs}"
        string_messages.append(string_message)

    string_messages.append(f"{MessageRole.ASSISTANT.value}: ")
    return "\n".join(string_messages)


def prompt_to_messages(prompt: str) -> List[ChatMessage]:
    """Convert a string prompt to a sequence of messages."""
    return [ChatMessage(role=MessageRole.USER, content=prompt)]

def completion_response_to_chat_response(
    completion_response: CompletionResponse,
) -> ChatResponse:
    return ChatResponse(
        message=ChatMessage(
            role=MessageRole.ASSISTANT,
            content=completion_response.text,
            additional_kwargs=completion_response.additional_kwargs,
        ),
        raw=completion_response.raw,
    )

def stream_completion_response_to_chat_response(
    completion_response_gen: CompletionResponseGen,
) -> ChatResponseGen:
    """Convert a stream completion response to a stream chat response."""
    def gen() -> ChatResponseGen:
        for response in completion_response_gen:
            yield ChatResponse(
                message=ChatMessage(
                    role=MessageRole.ASSISTANT,
                    content=response.text,
                    additional_kwargs=response.additional_kwargs,
                ),
                delta=response.delta,
                raw=response.raw,
            )

    return gen()

def chat_response_to_completion_response(
    chat_response: ChatResponse,
) -> CompletionResponse:
    """Convert a chat response to a completion response."""
    additional_kwargs = chat_response.message.additional_kwargs
    additional_kwargs.update(chat_response.additional_kwargs)

    return CompletionResponse(
        text=chat_response.message.content or "",
        additional_kwargs=additional_kwargs,
        raw=chat_response.raw,
    )


def stream_chat_response_to_completion_response(
    chat_response_gen: ChatResponseGen,
) -> CompletionResponseGen:
    """Convert a stream chat response to a completion response."""

    def gen() -> CompletionResponseGen:
        for response in chat_response_gen:
            additional_kwargs = response.message.additional_kwargs
            additional_kwargs.update(response.additional_kwargs)

            yield CompletionResponse(
                text=response.message.content or "",
                additional_kwargs=additional_kwargs,
                delta=response.delta,
                raw=response.raw,
            )

    return gen()

def completion_to_chat_decorator(
    func: Callable[..., CompletionResponse],
) -> Callable[..., ChatResponse]:
    def wrapper(messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        prompt = messages_to_prompt(messages)
        completion_response = func(prompt, **kwargs)
        return completion_response_to_chat_response(completion_response)

    return wrapper


def stream_completion_to_chat_decorator(
    func: Callable[..., CompletionResponseGen],
) -> Callable[..., ChatResponseGen]:
    def wrapper(messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponseGen:
        prompt = messages_to_prompt(messages)
        completion_response = func(prompt, **kwargs)
        return stream_completion_response_to_chat_response(completion_response)

    return wrapper


def chat_to_completion_decorator(
    func: Callable[..., ChatResponse],
) -> Callable[..., CompletionResponse]:
    def wrapper(prompt: str, **kwargs: Any) -> CompletionResponse:
        messages = prompt_to_messages(prompt)
        chat_response = func(messages, **kwargs)
        return chat_response_to_completion_response(chat_response)

    return wrapper

def stream_chat_to_completion_decorator(
    func: Callable[..., ChatResponseGen],
) -> Callable[..., CompletionResponseGen]:
    def wrapper(prompt: str, **kwargs: Any) -> CompletionResponseGen:
        messages = prompt_to_messages(prompt)
        chat_response = func(messages, **kwargs)
        return stream_chat_response_to_completion_response(chat_response)

    return wrapper

def astream_completion_response_to_chat_response(
    completion_response_gen: CompletionResponseAsyncGen,
) -> ChatResponseAsyncGen:
    """Convert an async stream completion to an async stream chat response."""

    async def gen() -> ChatResponseAsyncGen:
        async for response in completion_response_gen:
            yield ChatResponse(
                message=ChatMessage(
                    role=MessageRole.ASSISTANT,
                    content=response.text,
                    additional_kwargs=response.additional_kwargs,
                ),
                delta=response.delta,
                raw=response.raw,
            )

    return gen()

def async_stream_completion_response_to_chat_response(
    completion_response_gen: CompletionResponseAsyncGen,
) -> ChatResponseAsyncGen:
    """Alias for astream_completion_response_to_chat_response."""
    return astream_completion_response_to_chat_response(completion_response_gen)


def astream_chat_response_to_completion_response(
    chat_response_gen: ChatResponseAsyncGen,
) -> CompletionResponseAsyncGen:
    """Convert an async stream chat response to a completion response."""

    async def gen() -> CompletionResponseAsyncGen:
        async for response in chat_response_gen:
            additional_kwargs = response.message.additional_kwargs
            additional_kwargs.update(response.additional_kwargs)

            yield CompletionResponse(
                text=response.message.content or "",
                additional_kwargs=additional_kwargs,
                delta=response.delta,
                raw=response.raw,
            )

    return gen()

def acompletion_to_chat_decorator(
    func: Callable[..., Awaitable[CompletionResponse]],
) -> Callable[..., Awaitable[ChatResponse]]:
    async def wrapper(messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        prompt = messages_to_prompt(messages)
        completion_response = await func(prompt, **kwargs)
        return completion_response_to_chat_response(completion_response)

    return wrapper

def achat_to_completion_decorator(
    func: Callable[..., Awaitable[ChatResponse]],
) -> Callable[..., Awaitable[CompletionResponse]]:
    async def wrapper(prompt: str, **kwargs: Any) -> CompletionResponse:
        messages = prompt_to_messages(prompt)
        chat_response = await func(messages, **kwargs)
        return chat_response_to_completion_response(chat_response)

    return wrapper


def astream_completion_to_chat_decorator(
    func: Callable[..., Awaitable[CompletionResponseAsyncGen]],
) -> Callable[..., Awaitable[ChatResponseAsyncGen]]:
    async def wrapper(
        messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseAsyncGen:
        prompt = messages_to_prompt(messages)
        completion_response = await func(prompt, **kwargs)
        return astream_completion_response_to_chat_response(completion_response)

    return wrapper

def astream_chat_to_completion_decorator(
    func: Callable[..., Awaitable[ChatResponseAsyncGen]],
) -> Callable[..., Awaitable[CompletionResponseAsyncGen]]:
    async def wrapper(prompt: str, **kwargs: Any) -> CompletionResponseAsyncGen:
        messages = prompt_to_messages(prompt)
        chat_response = await func(messages, **kwargs)
        return astream_chat_response_to_completion_response(chat_response)

    return wrapper

def get_from_param_or_env(
    key: str,
    param: Optional[str] = None,
    env_key: Optional[str] = None,
    default: Optional[str] = None,
) -> str:
    """Get a value from a param or an environment variable."""
    if param is not None:
        return param
    elif env_key and env_key in os.environ and os.environ[env_key]:
        return os.environ[env_key]
    elif default is not None:
        return default
    else:
        raise ValueError(
            f"Did not find {key}, please add an environment variable"
            f" `{env_key}` which contains it, or pass"
            f"  `{key}` as a named parameter."
        )

def image_node_to_image_block(image_node: ImageNode) -> ImageBlock:
    if isinstance(image_node.image, str):
        try:
            return ImageBlock(image=base64.b64decode(image_node.image, validate=True))
        except BinasciiError:
            raise ValueError("The provided image string is not base64-encoded")
    elif image_node.image is None:
        if image_node.image_path is not None:
            image_path: Optional[Path] = Path(image_node.image_path)
        elif "file_path" in image_node.metadata:
            image_path = image_node.metadata["file_path"]
        else:
            image_path = image_node.image_path
            
        return ImageBlock(
            image=image_node.image,
            url=image_node.image_url,
            image_mimetype=image_node.image_mimetype,
            path=image_path,
        )
    else:
        raise ValueError("image_node.image is neither a string or None.")