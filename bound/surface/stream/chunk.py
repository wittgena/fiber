# bound.surface.stream.chunk
## @lineage: bound.transport.stream.chunk
## @lineage: bound.transport.stream.chunk.builder
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
    get_args,
)
import dotenv
import httpx
import openai
import tiktoken

if TYPE_CHECKING:
    from bound.watcher.delegator import LogDelegator

from bound.surface.legacy.types import TextChoices, TextCompletionResponse
from bound.surface.exception import APIError

from anchor.registry.model.config.resolver import config
from bound.surface.token.counter import token_counter
from bound.surface.stream.processor.builder import ChunkBuilderProcessor
from bound.adapter.switch.params import Choices, Message, ModelResponse, Usage
from watcher.plane.emitter import get_emitter

log = get_emitter("stream.chunk")

def print_verbose(print_statement):
    try:
        log.debug(print_statement)
        if config.set_verbose:
            print(print_statement)  # noqa
    except Exception:
        pass

def stream_chunk_builder_text_completion(
    chunks: list, messages: Optional[List] = None
) -> TextCompletionResponse:
    id = chunks[0]["id"]
    object = chunks[0]["object"]
    created = chunks[0]["created"]
    model = chunks[0]["model"]
    system_fingerprint = chunks[0].get("system_fingerprint", None)
    finish_reason = chunks[-1]["choices"][0]["finish_reason"]
    logprobs = chunks[-1]["choices"][0]["logprobs"]

    response = {
        "id": id,
        "object": object,
        "created": created,
        "model": model,
        "system_fingerprint": system_fingerprint,
        "choices": [
            {
                "text": None,
                "index": 0,
                "logprobs": logprobs,
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": None,
            "completion_tokens": None,
            "total_tokens": None,
        },
    }
    content_list = []
    for chunk in chunks:
        choices = chunk["choices"]
        for choice in choices:
            if (
                choice is not None
                and hasattr(choice, "text")
                and choice.get("text") is not None
            ):
                _choice = choice.get("text")
                content_list.append(_choice)

    # Combine the "content" strings into a single string || combine the 'function' strings into a single string
    combined_content = "".join(content_list)

    # Update the "content" field within the response dictionary
    response["choices"][0]["text"] = combined_content

    if len(combined_content) > 0:
        pass
    else:
        pass
    # # Update usage information if needed
    try:
        response["usage"]["prompt_tokens"] = token_counter(
            model=model, messages=messages
        )
    except (
        Exception
    ):  # don't allow this failing to block a complete streaming response from being returned
        print_verbose("token_counter failed, assuming prompt tokens is 0")
        response["usage"]["prompt_tokens"] = 0
    response["usage"]["completion_tokens"] = token_counter(
        model=model,
        text=combined_content,
        count_response_tokens=True,  # count_response_tokens is a Flag to tell token counter this is a response, No need to add extra tokens we do for input messages
    )
    response["usage"]["total_tokens"] = (
        response["usage"]["prompt_tokens"] + response["usage"]["completion_tokens"]
    )
    return TextCompletionResponse(**response)


def stream_chunk_builder(  # noqa: PLR0915
    chunks: list,
    messages: Optional[list] = None,
    start_time=None,
    end_time=None,
    logging_obj: Optional["Logging"] = None,
) -> Optional[Union[ModelResponse, TextCompletionResponse]]:
    try:
        if chunks is None:
            raise APIError(
                status_code=500,
                message="Error building chunks for logging/streaming usage calculation",
                llm_provider="",
                model="",
            )
        if not chunks:
            return None

        processor = ChunkBuilderProcessor(chunks, messages)
        chunks = processor.chunks

        ### BASE-CASE ###
        if len(chunks) == 0:
            return None
        ## Route to the text completion logic
        first_chunk_with_choices = next((c for c in chunks if c["choices"]), None)
        if first_chunk_with_choices is not None and isinstance(
            first_chunk_with_choices["choices"][0], TextChoices
        ):  # route to the text completion logic
            return stream_chunk_builder_text_completion(
                chunks=chunks, messages=messages
            )

        model = chunks[0]["model"]
        # Initialize the response dictionary
        response = processor.build_base_response(chunks)

        # Fast path for the common text-only streaming case:
        # avoid repeated multi-pass list scans over chunks.
        simple_content_parts: List[str] = []
        is_simple_text_stream = True
        for chunk in chunks:
            if len(chunk["choices"]) == 0:
                continue

            choice = chunk["choices"][0]
            delta_obj = (
                choice.get("delta", {})
                if isinstance(choice, dict)
                else getattr(choice, "delta", {})
            )
            if isinstance(delta_obj, dict):
                delta = delta_obj
            elif hasattr(delta_obj, "model_dump"):
                delta = cast(Dict[str, Any], delta_obj.model_dump())
            else:
                delta = {}

            if (
                delta.get("tool_calls") is not None
                or delta.get("function_call") is not None
                or delta.get("reasoning_content") is not None
                or delta.get("thinking_blocks") is not None
                or delta.get("annotations") is not None
                or delta.get("audio") is not None
                or delta.get("images") is not None
                or delta.get("provider_specific_fields") is not None
            ):
                is_simple_text_stream = False
                break

            content = delta.get("content")
            if isinstance(content, str) and content:
                simple_content_parts.append(content)

        if is_simple_text_stream:
            if simple_content_parts:
                response["choices"][0]["message"]["content"] = "".join(
                    simple_content_parts
                )
            completion_output = get_content_from_model_response(response)
            usage = processor.calculate_usage(
                chunks=chunks,
                model=model,
                completion_output=completion_output,
                messages=messages,
                reasoning_tokens=0,
            )
            setattr(response, "usage", usage)

            # Propagate provider_specific_fields from chunk hidden params when present.
            for chunk in reversed(chunks):
                if isinstance(chunk, dict):
                    hidden = chunk.get("_hidden_params")
                else:
                    hidden = getattr(chunk, "_hidden_params", None)
                if isinstance(hidden, dict) and "provider_specific_fields" in hidden:
                    response._hidden_params.setdefault(
                        "provider_specific_fields", {}
                    ).update(hidden["provider_specific_fields"])
                    break

            if config.include_cost_in_streaming_usage and logging_obj is not None:
                setattr(
                    usage,
                    "cost",
                    logging_obj._response_cost_calculator(result=response),
                )
            return response

        tool_call_chunks = [
            chunk
            for chunk in chunks
            if len(chunk["choices"]) > 0
            and "tool_calls" in chunk["choices"][0]["delta"]
            and chunk["choices"][0]["delta"]["tool_calls"] is not None
        ]

        if len(tool_call_chunks) > 0:
            tool_calls_list = processor.get_combined_tool_content(tool_call_chunks)
            _choice = cast(Choices, response.choices[0])
            _choice.message.content = None
            _choice.message.tool_calls = tool_calls_list

        function_call_chunks = [
            chunk
            for chunk in chunks
            if len(chunk["choices"]) > 0
            and "function_call" in chunk["choices"][0]["delta"]
            and chunk["choices"][0]["delta"]["function_call"] is not None
        ]

        if len(function_call_chunks) > 0:
            _choice = cast(Choices, response.choices[0])
            _choice.message.content = None
            _choice.message.function_call = (
                processor.get_combined_function_call_content(function_call_chunks)
            )

        content_chunks = [
            chunk
            for chunk in chunks
            if len(chunk["choices"]) > 0
            and "content" in chunk["choices"][0]["delta"]
            and chunk["choices"][0]["delta"]["content"] is not None
        ]

        if len(content_chunks) > 0:
            response["choices"][0]["message"]["content"] = (
                processor.get_combined_content(content_chunks)
            )

        thinking_blocks = [
            chunk
            for chunk in chunks
            if len(chunk["choices"]) > 0
            and "thinking_blocks" in chunk["choices"][0]["delta"]
            and chunk["choices"][0]["delta"]["thinking_blocks"] is not None
        ]

        if len(thinking_blocks) > 0:
            response["choices"][0]["message"]["thinking_blocks"] = (
                processor.get_combined_thinking_content(thinking_blocks)
            )

        reasoning_chunks = [
            chunk
            for chunk in chunks
            if len(chunk["choices"]) > 0
            and "reasoning_content" in chunk["choices"][0]["delta"]
            and chunk["choices"][0]["delta"]["reasoning_content"] is not None
        ]

        if len(reasoning_chunks) > 0:
            response["choices"][0]["message"]["reasoning_content"] = (
                processor.get_combined_reasoning_content(reasoning_chunks)
            )

        annotation_chunks = [
            chunk
            for chunk in chunks
            if len(chunk["choices"]) > 0
            and "annotations" in chunk["choices"][0]["delta"]
            and chunk["choices"][0]["delta"]["annotations"] is not None
        ]

        if len(annotation_chunks) > 0:
            # Merge annotations from ALL chunks — providers may spread
            # them across multiple streaming chunks or send them only in
            # the final chunk.
            all_annotations: list = []
            for ac in annotation_chunks:
                all_annotations.extend(ac["choices"][0]["delta"]["annotations"])
            response["choices"][0]["message"]["annotations"] = all_annotations

        audio_chunks = [
            chunk
            for chunk in chunks
            if len(chunk["choices"]) > 0
            and "audio" in chunk["choices"][0]["delta"]
            and chunk["choices"][0]["delta"]["audio"] is not None
        ]

        if len(audio_chunks) > 0:
            _choice = cast(Choices, response.choices[0])
            _choice.message.audio = processor.get_combined_audio_content(audio_chunks)

        # Handle image chunks from models like gemini-2.5-flash-image
        # See: https://github.com/BerriAI/litellm/issues/19478
        image_chunks = [
            chunk
            for chunk in chunks
            if len(chunk["choices"]) > 0
            and "images" in chunk["choices"][0]["delta"]
            and chunk["choices"][0]["delta"]["images"] is not None
        ]

        if len(image_chunks) > 0:
            # Images come complete in a single chunk, collect all images from all chunks
            all_images = []
            for chunk in image_chunks:
                all_images.extend(chunk["choices"][0]["delta"]["images"])
            response["choices"][0]["message"]["images"] = all_images

        # Combine provider_specific_fields from streaming chunks (e.g., web_search_results, citations)
        # See: https://github.com/BerriAI/litellm/issues/17737
        provider_specific_chunks = [
            chunk
            for chunk in chunks
            if len(chunk["choices"]) > 0
            and "provider_specific_fields" in chunk["choices"][0]["delta"]
            and chunk["choices"][0]["delta"]["provider_specific_fields"] is not None
        ]

        if len(provider_specific_chunks) > 0:
            combined_provider_fields: Dict[str, Any] = {}
            for chunk in provider_specific_chunks:
                fields = chunk["choices"][0]["delta"]["provider_specific_fields"]
                if isinstance(fields, dict):
                    for key, value in fields.items():
                        if key not in combined_provider_fields:
                            combined_provider_fields[key] = value
                        elif isinstance(value, list) and isinstance(
                            combined_provider_fields[key], list
                        ):
                            # For lists like web_search_results, take the last (most complete) one
                            combined_provider_fields[key] = value
                        else:
                            combined_provider_fields[key] = value

            if combined_provider_fields:
                _choice = cast(Choices, response.choices[0])
                _choice.message.provider_specific_fields = combined_provider_fields

        completion_output = get_content_from_model_response(response)

        reasoning_tokens = processor.count_reasoning_tokens(response)

        usage = processor.calculate_usage(
            chunks=chunks,
            model=model,
            completion_output=completion_output,
            messages=messages,
            reasoning_tokens=reasoning_tokens,
        )

        setattr(response, "usage", usage)

        # Propagate provider_specific_fields from the last chunk (contains provider
        # metadata like traffic_type set during streaming)
        for chunk in reversed(chunks):
            if isinstance(chunk, dict):
                hidden = chunk.get("_hidden_params")
            else:
                hidden = getattr(chunk, "_hidden_params", None)
            if isinstance(hidden, dict) and "provider_specific_fields" in hidden:
                response._hidden_params.setdefault(
                    "provider_specific_fields", {}
                ).update(hidden["provider_specific_fields"])
                break

        # Add cost to usage object if include_cost_in_streaming_usage is True
        if config.include_cost_in_streaming_usage and logging_obj is not None:
            setattr(usage, "cost", logging_obj._response_cost_calculator(result=response))
        return response
    except Exception as e:
        log.exception("blm.main: stream_chunk_builder() - Exception occurred - {}".format(str(e)))
        raise APIError(
            status_code=500,
            message="Error building chunks for logging/streaming usage calculation",
            llm_provider="",
            model="",
        )

def get_content_from_model_response(response: Union[ModelResponse, dict]) -> str:
    if isinstance(response, dict):
        new_response = ModelResponse(**response)
    else:
        new_response = response

    content = ""
    for choice in new_response.choices:
        if isinstance(choice, Choices):
            content += choice.message.content if choice.message.content else ""
            if choice.message.function_call:
                content += choice.message.function_call.model_dump_json()
            if choice.message.tool_calls:
                for tc in choice.message.tool_calls:
                    content += tc.model_dump_json()
        elif isinstance(choice, StreamingChoices):
            content += getattr(choice, "delta", {}).get("content", "") or ""
    return content