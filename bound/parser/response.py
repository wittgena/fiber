# bound.parser.response
## @lineage: bound.gateway.parser.response
## @lineage: bound.gateway.response
## @lineage: bound.gateway.adapter.response
## @lineage: gateway.adapter.response
## @lineage: bound.adapter.response
import asyncio
import json
import time
import traceback
import re
from collections import defaultdict
from typing import Dict, Iterable, List, Literal, Optional, Tuple, Union, cast
from typing_extensions import Required, TypedDict

from bound.resolver.model.config.constants import RESPONSE_FORMAT_TOOL_NAME
from bound.parser.header import get_response_headers
from bound.resolver.openai.types import (
    ChatCompletionThinkingBlock,
    ImageURLListItem,
    OpenAIModerationResponse,
)
from eco.tenant.switch.params import (
    ChatCompletionDeltaToolCall,
    ChatCompletionRedactedThinkingBlock,
    CompletionTokensDetailsWrapper,
    EmbeddingResponse,
    Function,
    HiddenParams,
    ImageResponse,
    PromptTokensDetailsWrapper,
    TranscriptionUsageDurationObject,
    TranscriptionUsageTokensObject,
)
from eco.tenant.switch.params import (
    TextChoices,
    TextCompletionResponse,
    TranscriptionResponse,
    RerankResponse,
    ModelResponse,
    ModelResponseStream, 
    StreamingChoices,
    Usage,
    Message,
    ChatCompletionMessageToolCall,
    Choices, 
    Delta
)
from eco.exception import APIError
from bound.resolver.legacy.types import Logprobs as TextCompletionLogprobs
from watcher.plane.emitter import get_emitter 

log = get_emitter("response.converter")

_MESSAGE_FIELDS: frozenset = frozenset(Message.model_fields.keys())
_CHOICES_FIELDS: frozenset = frozenset(Choices.model_fields.keys())
_MODEL_RESPONSE_FIELDS: frozenset = frozenset(ModelResponse.model_fields.keys()) | {"usage"}

class DatabricksFunction(TypedDict, total=False):
    name: Required[str]
    description: Union[dict, str]
    parameters: dict
    strict: bool


class DatabricksTool(TypedDict):
    function: DatabricksFunction
    type: Literal["function"]


# =========================================================================
# Utility Functions
# =========================================================================

def _normalize_images_for_message(images: Optional[List[dict]]) -> Optional[List[ImageURLListItem]]:
    if not images:
        return cast(Optional[List[ImageURLListItem]], images)
    normalized: List[ImageURLListItem] = []
    for i, img in enumerate(images):
        if isinstance(img, dict) and "index" not in img:
            normalized.append(cast(ImageURLListItem, {**img, "index": i}))
        else:
            normalized.append(cast(ImageURLListItem, img))
    return normalized


def _safe_convert_created_field(created_value) -> int:
    if created_value is None:
        return int(time.time())
    elif isinstance(created_value, (int, float)):
        return int(created_value)
    else:
        try:
            return int(float(created_value))
        except (ValueError, TypeError):
            return int(time.time())


def _should_convert_tool_call_to_json_mode(
    tool_calls: Optional[Union[List[ChatCompletionMessageToolCall], List[DatabricksTool]]] = None,
    convert_tool_call_to_json_mode: Optional[bool] = None,
) -> bool:
    return bool(
        convert_tool_call_to_json_mode
        and tool_calls
        and len(tool_calls) == 1
        and tool_calls[0]["function"]["name"] == RESPONSE_FORMAT_TOOL_NAME
    )


def convert_tool_call_to_json_mode(
    tool_calls: List[ChatCompletionMessageToolCall],
    convert_tool_call_to_json_mode: bool,
) -> Tuple[Optional[Message], Optional[str]]:
    if _should_convert_tool_call_to_json_mode(tool_calls, convert_tool_call_to_json_mode):
        json_mode_content_str: Optional[str] = tool_calls[0]["function"].get("arguments")
        if json_mode_content_str is not None:
            return Message(content=json_mode_content_str), "stop"
    return None, None


def _handle_invalid_parallel_tool_calls(tool_calls: List[ChatCompletionMessageToolCall]):
    if not tool_calls:
        return tool_calls
    try:
        replacements: Dict[int, List[ChatCompletionMessageToolCall]] = defaultdict(list)
        for i, tool_call in enumerate(tool_calls):
            current_function = tool_call.function.name
            function_args = json.loads(tool_call.function.arguments)
            
            if current_function == "multi_tool_use.parallel":
                log.debug("OpenAI did a weird pseudo-multi-tool-use call, fixing call structure..")
                for _fake_i, _fake_tool_use in enumerate(function_args["tool_uses"]):
                    _function_args = _fake_tool_use["parameters"]
                    _current_function = _fake_tool_use["recipient_name"]
                    if _current_function.startswith("functions."):
                        _current_function = _current_function[len("functions.") :]

                    fixed_tc = ChatCompletionMessageToolCall(
                        id=f"{tool_call.id}_{_fake_i}",
                        type="function",
                        function=Function(
                            name=_current_function, arguments=json.dumps(_function_args)
                        ),
                    )
                    replacements[i].append(fixed_tc)

        shift = 0
        for i, replacement in replacements.items():
            tool_calls[:] = tool_calls[: i + shift] + replacement + tool_calls[i + shift + 1 :]
            shift += len(replacement)

        return tool_calls
    except json.JSONDecodeError:
        return tool_calls


def _extract_reasoning_content(message: dict) -> Tuple[Optional[str], Optional[str]]:
    message_content = message.get("content")
    if "reasoning_content" in message:
        return message["reasoning_content"], message_content
    elif "reasoning" in message:
        return message["reasoning"], message_content
    elif isinstance(message_content, str):
        return _parse_content_for_reasoning(message_content)
    return None, message_content


def _parse_content_for_reasoning(message_text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not message_text:
        return None, message_text
    reasoning_match = re.match(
        r"<(?:think|thinking|budget:thinking)>(.*?)</(?:think|thinking|budget:thinking)>(.*)",
        message_text,
        re.DOTALL,
    )
    if reasoning_match:
        return reasoning_match.group(1), reasoning_match.group(2)
    return None, message_text


# =========================================================================
# Streaming Converters
# =========================================================================

async def convert_to_streaming_response_async(response_object: Optional[dict] = None):
    if response_object is None:
        raise Exception("Error in response object format")

    model_response_object = ModelResponseStream()
    choice_list: List[StreamingChoices] = []
    
    if not response_object.get("choices"):
        raise APIError(
            status_code=500,
            message=f"LiteLLM: provider returned a response with no 'choices'. Raw keys: {list(response_object.keys())}",
            llm_provider="",
            model="",
        )

    for idx, choice in enumerate(response_object["choices"]):
        msg = choice.get("message", {})
        if (
            msg.get("tool_calls") is not None
            and isinstance(msg["tool_calls"], list)
            and len(msg["tool_calls"]) > 0
            and isinstance(msg["tool_calls"][0], dict)
        ):
            pydantic_tool_calls = []
            for index, t in enumerate(msg["tool_calls"]):
                if "index" not in t:
                    t["index"] = index
                pydantic_tool_calls.append(ChatCompletionDeltaToolCall(**t))
            choice["message"]["tool_calls"] = pydantic_tool_calls
            
        delta = Delta(
            content=msg.get("content", None),
            role=msg.get("role"),
            function_call=msg.get("function_call", None),
            tool_calls=msg.get("tool_calls", None),
        )
        finish_reason = choice.get("finish_reason") or choice.get("finish_details")

        choice_list.append(
            StreamingChoices(
                finish_reason=finish_reason, 
                index=idx, 
                delta=delta, 
                logprobs=choice.get("logprobs")
            )
        )

    model_response_object.choices = choice_list

    if response_object.get("usage"):
        model_response_object.usage = Usage(
            completion_tokens=response_object["usage"].get("completion_tokens", 0),
            prompt_tokens=response_object["usage"].get("prompt_tokens", 0),
            total_tokens=response_object["usage"].get("total_tokens", 0),
        )

    model_response_object.id = response_object.get("id", "")
    model_response_object.created = _safe_convert_created_field(response_object.get("created"))
    model_response_object.system_fingerprint = response_object.get("system_fingerprint")
    model_response_object.model = response_object.get("model", "")

    yield model_response_object
    await asyncio.sleep(0)


def convert_to_streaming_response(response_object: Optional[dict] = None):
    if response_object is None:
        raise Exception("Error in response object format")

    model_response_object = ModelResponseStream()
    choice_list: List[StreamingChoices] = []

    if not response_object.get("choices"):
        raise APIError(
            status_code=500,
            message=f"LiteLLM: provider returned a response with no 'choices'. Raw keys: {list(response_object.keys())}",
            llm_provider="",
            model="",
        )

    for idx, choice in enumerate(response_object["choices"]):
        delta = Delta(**choice["message"])
        finish_reason = choice.get("finish_reason") or choice.get("finish_details")
        
        choice_list.append(
            StreamingChoices(
                finish_reason=finish_reason,
                index=idx,
                delta=delta,
                logprobs=choice.get("logprobs"),
                enhancements=choice.get("enhancements"),
            )
        )

    model_response_object.choices = choice_list

    if response_object.get("usage"):
        model_response_object.usage = Usage(
            completion_tokens=response_object["usage"].get("completion_tokens", 0),
            prompt_tokens=response_object["usage"].get("prompt_tokens", 0),
            total_tokens=response_object["usage"].get("total_tokens", 0),
        )

    model_response_object.id = response_object.get("id", "")
    model_response_object.created = _safe_convert_created_field(response_object.get("created"))
    model_response_object.system_fingerprint = response_object.get("system_fingerprint")
    model_response_object.model = response_object.get("model", "")
    yield model_response_object


# =========================================================================
# Handlers & Builders
# =========================================================================

class LiteLLMResponseObjectHandler:
    @staticmethod
    def convert_to_image_response(
        response_object: dict,
        model_response_object: Optional[ImageResponse] = None,
        hidden_params: Optional[dict] = None,
    ) -> ImageResponse:
        response_object.update({"hidden_params": hidden_params})

        if response_object.get("usage"):
            usage = response_object["usage"]
            usage.setdefault("input_tokens", 0)
            usage.setdefault("output_tokens", 0)
            usage.setdefault("total_tokens", usage["input_tokens"] + usage["output_tokens"])
            usage.setdefault("input_tokens_details", {"image_tokens": 0, "text_tokens": 0})
            usage.setdefault("prompt_tokens", usage["input_tokens"])
            usage.setdefault("completion_tokens", usage["output_tokens"])

            if isinstance(usage.get("input_tokens_details"), dict):
                usage["prompt_tokens_details"] = PromptTokensDetailsWrapper(**usage["input_tokens_details"])
            if isinstance(usage.get("output_tokens_details"), dict):
                usage["completion_tokens_details"] = CompletionTokensDetailsWrapper(**usage["output_tokens_details"])

        if model_response_object is None:
            return ImageResponse(**response_object)
        else:
            model_response_dict = model_response_object.model_dump()
            model_response_dict.update(response_object)
            return ImageResponse(**model_response_dict)

    @staticmethod
    def convert_to_moderation_response(response_object: dict) -> OpenAIModerationResponse:
        return OpenAIModerationResponse(**response_object)

    @staticmethod
    def convert_chat_to_text_completion(
        response: ModelResponse,
        text_completion_response: TextCompletionResponse,
        custom_llm_provider: Optional[str] = None,
    ) -> TextCompletionResponse:
        transformed_logprobs = LiteLLMResponseObjectHandler._convert_provider_response_logprobs_to_text_completion_logprobs(
            response=response, custom_llm_provider=custom_llm_provider
        )
        
        text_completion_response["id"] = response.get("id")
        text_completion_response["object"] = "text_completion"
        text_completion_response["created"] = response.get("created")
        text_completion_response["model"] = response.get("model")
        
        choices_list: List[TextChoices] = []
        for choice in response["choices"]:
            text_choices = TextChoices()
            text_choices["text"] = choice["message"]["content"]
            text_choices["index"] = choice["index"]
            text_choices["logprobs"] = transformed_logprobs
            text_choices["finish_reason"] = choice["finish_reason"]
            choices_list.append(text_choices)

        text_completion_response["choices"] = choices_list
        text_completion_response["usage"] = response.get("usage")
        text_completion_response._hidden_params = HiddenParams(**response._hidden_params)
        return text_completion_response

    @staticmethod
    def _convert_provider_response_logprobs_to_text_completion_logprobs(
        response: ModelResponse, custom_llm_provider: Optional[str] = None
    ) -> Optional[TextCompletionLogprobs]:
        return None


# -------------------------------------------------------------------------
# Internal Builder Delegates for convert_to_model_response_object
# -------------------------------------------------------------------------

def _prepare_metadata(hidden_params: Optional[dict], _response_headers: Optional[dict]) -> Tuple[dict, dict]:
    hidden_params = hidden_params or {}
    additional_headers = get_response_headers(_response_headers)
    
    existing_additional_headers = hidden_params.get("additional_headers", {})
    if existing_additional_headers and _response_headers is None:
        additional_headers = existing_additional_headers
    elif existing_additional_headers:
        additional_headers.update(existing_additional_headers)

    hidden_params["additional_headers"] = additional_headers
    return hidden_params, additional_headers


def _check_and_raise_errors(response_object: dict):
    error_obj = response_object.get("error")
    if not error_obj:
        return

    has_meaningful_error = False
    if isinstance(error_obj, dict):
        has_meaningful_error = bool(error_obj.get("message")) or error_obj.get("code") is not None
    elif isinstance(error_obj, str):
        has_meaningful_error = bool(error_obj)
    else:
        has_meaningful_error = True

    if has_meaningful_error:
        error_args = {"status_code": 422, "message": "Error in response object"}
        if isinstance(error_obj, dict):
            error_args["status_code"] = error_obj.get("code", 422)
            msg = error_obj.get("message")
            error_args["message"] = json.dumps(msg) if isinstance(msg, dict) else str(msg)
            
        raised_exception = Exception()
        setattr(raised_exception, "status_code", error_args["status_code"])
        setattr(raised_exception, "message", error_args["message"])
        raise raised_exception


def _build_completion_response(
    response_object: dict,
    model_response_object: Optional[ModelResponse],
    start_time: Optional[float],
    end_time: Optional[float],
    hidden_params: dict,
    _response_headers: Optional[dict],
    convert_tool_call_to_json_mode_flag: Optional[bool]
) -> ModelResponse:
    
    if model_response_object is None:
        model_response_object = ModelResponse()

    if not response_object.get("choices") or not isinstance(response_object["choices"], Iterable):
        raise APIError(
            status_code=500,
            message=f"LiteLLM: provider returned a response with no 'choices'. Raw keys: {list(response_object.keys())}",
            llm_provider="", model=""
        )

    choice_list: List[Choices] = []
    for idx, choice in enumerate(response_object["choices"]):
        tool_calls = choice["message"].get("tool_calls")
        if tool_calls is not None:
            _openai_tool_calls = [ChatCompletionMessageToolCall(**tc) for tc in tool_calls]
            tool_calls = _handle_invalid_parallel_tool_calls(_openai_tool_calls) or tool_calls

        message: Optional[Message] = None
        finish_reason: Optional[str] = None
        
        # Check JSON mode conversion
        if _should_convert_tool_call_to_json_mode(tool_calls, convert_tool_call_to_json_mode_flag):
            json_mode_content_str = tool_calls[0]["function"].get("arguments")
            if json_mode_content_str is not None:
                message = Message(content=json_mode_content_str)
                finish_reason = "stop"

        if message is None:
            provider_specific_fields = dict(choice["message"].get("provider_specific_fields") or {})
            for f in choice["message"].keys() - _MESSAGE_FIELDS:
                provider_specific_fields[f] = choice["message"][f]

            reasoning_content, content = _extract_reasoning_content(choice["message"])
            thinking_blocks = choice["message"].get("thinking_blocks")
            
            if thinking_blocks:
                provider_specific_fields["thinking_blocks"] = thinking_blocks
            if reasoning_content:
                provider_specific_fields["reasoning_content"] = reasoning_content

            message = Message(
                content=content,
                role=choice["message"].get("role") or "assistant",
                function_call=choice["message"].get("function_call"),
                tool_calls=tool_calls,
                audio=choice["message"].get("audio"),
                provider_specific_fields=provider_specific_fields,
                reasoning_content=reasoning_content,
                thinking_blocks=thinking_blocks,
                annotations=choice["message"].get("annotations"),
                images=_normalize_images_for_message(choice["message"].get("images")),
            )
            
            finish_reason = choice.get("finish_reason") or choice.get("finish_details") or "stop"
            if finish_reason == "stop" and message.tool_calls and len(message.tool_calls) > 0:
                finish_reason = "tool_calls"

        provider_specific_fields_choice = {f: choice[f] for f in choice.keys() - _CHOICES_FIELDS}

        choice_list.append(Choices(
            finish_reason=finish_reason,
            index=idx,
            message=message,
            logprobs=choice.get("logprobs"),
            enhancements=choice.get("enhancements"),
            provider_specific_fields=provider_specific_fields_choice,
        ))

    model_response_object.choices = choice_list 

    if response_object.get("usage"):
        setattr(model_response_object, "usage", Usage(**response_object["usage"]))
    
    if "created" in response_object:
        model_response_object.created = _safe_convert_created_field(response_object["created"])
    
    if "id" in response_object:
        model_response_object.id = response_object["id"] or model_response_object.id
    
    if "system_fingerprint" in response_object:
        model_response_object.system_fingerprint = response_object["system_fingerprint"]

    if "model" in response_object:
        if model_response_object.model is None:
            model_response_object.model = response_object["model"]
        elif "/" in model_response_object.model and response_object["model"] is not None:
            provider = model_response_object.model.split("/")[0]
            model_response_object.model = f"{provider}/{response_object['model']}"

    if start_time and end_time and isinstance(start_time, type(end_time)):
        model_response_object._response_ms = (end_time - start_time).total_seconds() * 1000

    if model_response_object._hidden_params is None:
        model_response_object._hidden_params = {}
    model_response_object._hidden_params.update(hidden_params)

    if _response_headers is not None:
        model_response_object._response_headers = _response_headers

    for k, v in response_object.items():
        if k not in _MODEL_RESPONSE_FIELDS:
            setattr(model_response_object, k, v)

    return model_response_object


def _build_embedding_response(
    response_object: dict,
    model_response_object: Optional[EmbeddingResponse],
    start_time: Optional[float],
    end_time: Optional[float],
    hidden_params: dict,
    _response_headers: Optional[dict],
) -> EmbeddingResponse:
    
    if model_response_object is None:
        model_response_object = EmbeddingResponse()

    if "model" in response_object:
        model_response_object.model = response_object["model"]
    if "object" in response_object:
        model_response_object.object = response_object["object"]
        
    model_response_object.data = response_object.get("data", [])

    if response_object.get("usage"):
        model_response_object.usage.completion_tokens = response_object["usage"].get("completion_tokens", 0) 
        model_response_object.usage.prompt_tokens = response_object["usage"].get("prompt_tokens", 0) 
        model_response_object.usage.total_tokens = response_object["usage"].get("total_tokens", 0)

    if start_time and end_time:
        model_response_object._response_ms = (end_time - start_time).total_seconds() * 1000

    model_response_object._hidden_params = hidden_params
    if _response_headers is not None:
        model_response_object._response_headers = _response_headers

    return model_response_object


def _build_audio_transcription_response(
    response_object: dict,
    model_response_object: Optional[TranscriptionResponse],
    hidden_params: dict,
    _response_headers: Optional[dict],
) -> TranscriptionResponse:
    
    if model_response_object is None:
        model_response_object = TranscriptionResponse()

    if "text" in response_object:
        model_response_object.text = response_object["text"]

    for key in ["language", "task", "duration", "words", "segments"]:
        if key in response_object:
            setattr(model_response_object, key, response_object[key])

    if response_object.get("usage"):
        usage_type = response_object["usage"].get("type")
        if usage_type == "duration":
            setattr(model_response_object, "usage", TranscriptionUsageDurationObject(**response_object["usage"]))
        elif usage_type == "tokens":
            setattr(model_response_object, "usage", TranscriptionUsageTokensObject(**response_object["usage"]))

    model_response_object._hidden_params = hidden_params
    if "_audio_transcription_duration" in response_object:
        model_response_object._hidden_params["audio_transcription_duration"] = response_object["_audio_transcription_duration"]

    if _response_headers is not None:
        model_response_object._response_headers = _response_headers

    return model_response_object


# =========================================================================
# Main Exported Function
# =========================================================================

def convert_to_model_response_object(
    response_object: Optional[dict] = None,
    model_response_object: Optional[Union[ModelResponse, EmbeddingResponse, ImageResponse, TranscriptionResponse, RerankResponse]] = None,
    response_type: Literal["completion", "embedding", "image_generation", "audio_transcription", "rerank"] = "completion",
    stream=False,
    start_time=None,
    end_time=None,
    hidden_params: Optional[dict] = None,
    _response_headers: Optional[dict] = None,
    convert_tool_call_to_json_mode: Optional[bool] = None,
):
    """
    Normalizes provider-specific API responses into standardized Pydantic models.
    Delegates to specific builders based on the `response_type`.
    """
    if response_object is None:
        raise Exception("Error in response object format (None provided)")

    hidden_params, _response_headers = _prepare_metadata(hidden_params, _response_headers)
    _check_and_raise_errors(response_object)

    try:
        if response_type == "completion":
            if stream:
                return convert_to_streaming_response(response_object=response_object)
            return _build_completion_response(
                response_object, cast(Optional[ModelResponse], model_response_object), 
                start_time, end_time, hidden_params, _response_headers, convert_tool_call_to_json_mode
            )
            
        elif response_type == "embedding":
            return _build_embedding_response(
                response_object, cast(Optional[EmbeddingResponse], model_response_object),
                start_time, end_time, hidden_params, _response_headers
            )
            
        elif response_type == "image_generation":
            return LiteLLMResponseObjectHandler.convert_to_image_response(
                response_object, cast(Optional[ImageResponse], model_response_object), hidden_params
            )
            
        elif response_type == "audio_transcription":
            return _build_audio_transcription_response(
                response_object, cast(Optional[TranscriptionResponse], model_response_object),
                hidden_params, _response_headers
            )
            
        elif response_type == "rerank":
            if model_response_object is None:
                model_response_object = RerankResponse(**response_object)
                return model_response_object
                
            if "id" in response_object:
                model_response_object.id = response_object["id"]
            if "meta" in response_object:
                model_response_object.meta = response_object["meta"]
            if "results" in response_object:
                model_response_object.results = response_object["results"]
            return model_response_object

    except Exception as e:
        if isinstance(e, APIError):
            raise

        received_args = {
            "response_object": response_object,
            "model_response_object": model_response_object,
            "response_type": response_type,
            "stream": stream,
            "start_time": start_time,
            "end_time": end_time,
            "convert_tool_call_to_json_mode": convert_tool_call_to_json_mode,
        }
        raise Exception(f"Invalid response object {traceback.format_exc()}\n\nreceived_args={received_args}")