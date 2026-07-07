# bound.channel.action.support.logging
## @lineage: bound.channel.client.action.support.logging
## @lineage: anchor.channel.client.action.support.logging
## @lineage: anchor.channel.action.support.logging
## @lineage: bound.channel.support.api.logging
## @lineage: bound.channel.api.logging
## @lineage: bound.bridge.api.logging
## @lineage: bound.client.api.logging
## @lineage: bound.handler.support.utils
from typing import (
    Any,
    Dict,
    Mapping,
    Iterable,
    List,
    Optional,
    Type,
    Union,
)
from bound.surface.legacy.openai.types import ResponseAPIUsage
from bound.surface.legacy.types import CompletionTokensDetailsWrapper, PromptTokensDetailsWrapper, Usage
from watcher.plane.emitter import get_emitter

log = get_emitter("api.logging")

class ResponseAPILoggingUtils:
    @staticmethod
    def _is_response_api_usage(usage: Union[dict, ResponseAPIUsage]) -> bool:
        """returns True if usage is from OpenAI Response API"""
        if isinstance(usage, ResponseAPIUsage):
            return True
        if "input_tokens" in usage and "output_tokens" in usage:
            return True
        return False

    @staticmethod
    def _transform_response_api_usage_to_chat_usage(
        usage_input: Optional[Union[dict, ResponseAPIUsage]],
    ) -> Usage:
        if usage_input is None:
            return Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0)

        response_api_usage: ResponseAPIUsage
        if isinstance(usage_input, dict):
            usage_input = dict(usage_input)  # shallow copy; avoid mutating caller
            # Realtime *_token_details → *_tokens_details when unset.
            if (
                usage_input.get("input_tokens_details") is None
                and "input_token_details" in usage_input
            ):
                usage_input["input_tokens_details"] = usage_input["input_token_details"]
            if (
                usage_input.get("output_tokens_details") is None
                and "output_token_details" in usage_input
            ):
                usage_input["output_tokens_details"] = usage_input["output_token_details"]
            total_tokens = usage_input.get("total_tokens")
            if total_tokens is None:
                input_tokens = usage_input.get("input_tokens")
                output_tokens = usage_input.get("output_tokens")
                if input_tokens is not None and output_tokens is not None:
                    total_tokens = input_tokens + output_tokens
                    usage_input["total_tokens"] = total_tokens
            response_api_usage = ResponseAPIUsage(**usage_input)
        else:
            response_api_usage = usage_input
        prompt_tokens: int = response_api_usage.input_tokens or 0
        completion_tokens: int = response_api_usage.output_tokens or 0
        prompt_tokens_details: Optional[PromptTokensDetailsWrapper] = None
        if response_api_usage.input_tokens_details:
            if isinstance(response_api_usage.input_tokens_details, dict):
                prompt_tokens_details = PromptTokensDetailsWrapper(**response_api_usage.input_tokens_details)
            else:
                prompt_tokens_details = PromptTokensDetailsWrapper(
                    cached_tokens=getattr(response_api_usage.input_tokens_details, "cached_tokens", None),
                    audio_tokens=getattr(response_api_usage.input_tokens_details, "audio_tokens", None),
                    text_tokens=getattr(response_api_usage.input_tokens_details, "text_tokens", None),
                    image_tokens=getattr(response_api_usage.input_tokens_details, "image_tokens", None),
                )
        completion_tokens_details: Optional[CompletionTokensDetailsWrapper] = None
        output_tokens_details = getattr(response_api_usage, "output_tokens_details", None)

        if output_tokens_details:
            completion_tokens_details = CompletionTokensDetailsWrapper(
                reasoning_tokens=getattr(output_tokens_details, "reasoning_tokens", None),
                image_tokens=getattr(output_tokens_details, "image_tokens", None),
                text_tokens=getattr(output_tokens_details, "text_tokens", None),
                audio_tokens=getattr(output_tokens_details, "audio_tokens", None),
            )

        chat_usage = Usage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
            prompt_tokens_details=prompt_tokens_details,
            completion_tokens_details=completion_tokens_details,
        )

        # Preserve cost attribute if it exists on ResponseAPIUsage
        if hasattr(response_api_usage, "cost") and response_api_usage.cost is not None:
            setattr(chat_usage, "cost", response_api_usage.cost)
        return chat_usage
