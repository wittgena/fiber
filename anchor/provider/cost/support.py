# anchor.provider.cost.support
import time
from typing import TYPE_CHECKING, Any, List, Literal, Optional, Tuple, Union, cast
from httpx import Response
from pydantic import BaseModel
from functools import lru_cache

from bound.channel.config.resolver import config
from bound.channel.config.constants import DEFAULT_MAX_LRU_CACHE_SIZE, DEFAULT_REPLICATE_GPU_PRICE_PER_SECOND
from anchor.provider.registry import model_cost
from anchor.provider.cost.transform import UsageTransform
from anchor.provider.types import ProviderTypesSet

from anchor.provider.cost.unit import generic_cost_per_token
from anchor.provider.router.locator import get_llm_provider
from anchor.provider.model.token.counter import token_counter
from anchor.surface.switch.params import ModelResponse, ModelResponseStream

from anchor.provider.legacy.openai.types import (
    HttpxBinaryResponseContent,
    OpenAIModerationResponse,
    OpenAIRealtimeStreamList,
    OpenAIRealtimeStreamResponseBaseObject,
    OpenAIRealtimeStreamSessionEvents,
    ResponseAPIUsage,
    ResponsesAPIResponse,
)
from anchor.provider.model.param.rerank import RerankBilledUnits, RerankResponse
from anchor.provider.legacy.types import CallTypesLiteral, LiteLLMRealtimeStreamLoggingObject, StandardBuiltInToolsParams, Usage
from anchor.provider.legacy.types import CallTypes, CostPerToken, EmbeddingResponse, ImageResponse, TextCompletionResponse, TranscriptionResponse

from watcher.plane.emitter import get_emitter

log = get_emitter("cost.support")

LitellmLoggingObject = Any

def _cost_per_token_custom_pricing_helper(
    prompt_tokens: float = 0,
    completion_tokens: float = 0,
    response_time_ms: Optional[float] = 0.0,
    cached_tokens: float = 0,
    cache_creation_tokens: float = 0,
    ### CUSTOM PRICING ###
    custom_cost_per_token: Optional[CostPerToken] = None,
    custom_cost_per_second: Optional[float] = None,
) -> Optional[Tuple[float, float]]:
    """Internal helper function for calculating cost, if custom pricing given.

    prompt_tokens is assumed to include both cached_tokens and cache_creation_tokens
    (OpenAI-compatible convention). Anthropic-style usage where prompt_tokens excludes
    cache tokens is handled at the caller (cost_per_token) before invoking this helper.
    """
    if custom_cost_per_token is None and custom_cost_per_second is None:
        return None

    if custom_cost_per_token is not None:
        input_cost_per_token = custom_cost_per_token["input_cost_per_token"]
        output_cost_per_token = custom_cost_per_token["output_cost_per_token"]

        cache_read_input_token_cost = custom_cost_per_token.get(
            "cache_read_input_token_cost",
            input_cost_per_token,
        )
        cache_creation_input_token_cost = custom_cost_per_token.get(
            "cache_creation_input_token_cost",
            input_cost_per_token,
        )

        regular_prompt_tokens = max(
            prompt_tokens - cached_tokens - cache_creation_tokens,
            0,
        )

        input_cost = (
            regular_prompt_tokens * input_cost_per_token
            + cached_tokens * cache_read_input_token_cost
            + cache_creation_tokens * cache_creation_input_token_cost
        )
        output_cost = completion_tokens * output_cost_per_token
        return input_cost, output_cost
    elif custom_cost_per_second is not None:
        output_cost = custom_cost_per_second * response_time_ms / 1000  # type: ignore
        return 0, output_cost

    return None


def _get_additional_costs(
    model: str,
    custom_llm_provider: Optional[str],
    prompt_tokens: int,
    completion_tokens: int,
) -> Optional[dict]:
    if not custom_llm_provider:
        return None

    try:
        config_class = None
        if config_class and hasattr(config_class, "calculate_additional_costs"):
            return config_class.calculate_additional_costs(
                model=model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )
    except Exception as e:
        log.debug(f"Error calculating additional costs: {e}")

    return None


def _transcription_usage_has_token_details(
    usage_block: Optional[Usage],
) -> bool:
    if usage_block is None:
        return False

    prompt_tokens_val = getattr(usage_block, "prompt_tokens", 0) or 0
    completion_tokens_val = getattr(usage_block, "completion_tokens", 0) or 0
    prompt_details = getattr(usage_block, "prompt_tokens_details", None)

    if prompt_details is not None:
        audio_token_count = getattr(prompt_details, "audio_tokens", 0) or 0
        text_token_count = getattr(prompt_details, "text_tokens", 0) or 0
        if audio_token_count > 0 or text_token_count > 0:
            return True

    return (prompt_tokens_val > 0) or (completion_tokens_val > 0)

def get_replicate_completion_pricing(completion_response: dict, total_time=0.0):
    # see https://replicate.com/pricing
    # for all litellm currently supported LLMs, almost all requests go to a100_80gb
    a100_80gb_price_per_second_public = DEFAULT_REPLICATE_GPU_PRICE_PER_SECOND  # assume all calls sent to A100 80GB for now
    if total_time == 0.0:  # total time is in ms
        start_time = completion_response.get("created", time.time())
        end_time = getattr(completion_response, "ended", time.time())
        total_time = end_time - start_time

    return a100_80gb_price_per_second_public * total_time / 1000


def has_hidden_params(obj: Any) -> bool:
    return hasattr(obj, "_hidden_params")


def _get_provider_for_cost_calc(
    model: Optional[str],
    custom_llm_provider: Optional[str] = None,
) -> Optional[str]:
    if custom_llm_provider is not None:
        return custom_llm_provider
    if model is None:
        return None
    try:
        _, custom_llm_provider, _, _ = get_llm_provider(model=model)
    except Exception as e:
        log.debug(
            f"calculator.calc::_get_provider_for_cost_calc() - Error inferring custom_llm_provider - {str(e)}"
        )
        return None

    return custom_llm_provider


def _select_model_name_for_cost_calc(
    model: Optional[str],
    completion_response: Optional[Any],
    base_model: Optional[str] = None,
    custom_pricing: Optional[bool] = None,
    custom_llm_provider: Optional[str] = None,
    router_model_id: Optional[str] = None,
) -> Optional[str]:
    return_model: Optional[str] = None
    region_name: Optional[str] = None
    custom_llm_provider = _get_provider_for_cost_calc(
        model=model, custom_llm_provider=custom_llm_provider
    )

    completion_response_model: Optional[str] = None
    if completion_response is not None:
        if isinstance(completion_response, BaseModel):
            completion_response_model = getattr(completion_response, "model", None)
        elif isinstance(completion_response, dict):
            completion_response_model = completion_response.get("model", None)
    hidden_params: Optional[dict] = getattr(completion_response, "_hidden_params", None)

    if custom_pricing is True:
        if router_model_id is not None and router_model_id in model_cost:
            entry = model_cost[router_model_id]
            if (
                entry.get("input_cost_per_token") is not None
                or entry.get("input_cost_per_second") is not None
            ):
                return_model = router_model_id
            else:
                return_model = model
        else:
            return_model = model

    elif base_model is not None:
        return_model = base_model

    elif completion_response_model is None and hidden_params is not None:
        if (
            hidden_params.get("model", None) is not None
            and len(hidden_params["model"]) > 0
        ):
            return_model = hidden_params.get("model", model)
    elif (
        hidden_params is not None and hidden_params.get("region_name", None) is not None
    ):
        region_name = hidden_params.get("region_name", None)

    if return_model is None and completion_response_model is not None:
        return_model = completion_response_model

    if return_model is None and model is not None:
        return_model = model

    if (
        return_model is not None
        and custom_llm_provider is not None
        and not _model_contains_known_llm_provider(return_model)
    ):  # add provider prefix if not already present, to match model_cost
        if region_name is not None:
            return_model = f"{custom_llm_provider}/{region_name}/{return_model}"
        else:
            return_model = f"{custom_llm_provider}/{return_model}"

    return return_model


@lru_cache(maxsize=DEFAULT_MAX_LRU_CACHE_SIZE)
def _model_contains_known_llm_provider(model: str) -> bool:
    """
    Check if the model contains a known llm provider
    """
    _provider_prefix = model.split("/")[0]
    return _provider_prefix in ProviderTypesSet


def _get_response_model(completion_response: Any) -> Optional[str]:
    """
    Extract the model name from a completion response object.

    Used as a fallback for cost calculation when the input model name
    doesn't exist in model_cost (e.g., Azure Model Router).
    """
    if completion_response is None:
        return None

    if isinstance(completion_response, BaseModel):
        return getattr(completion_response, "model", None)
    elif isinstance(completion_response, dict):
        return completion_response.get("model", None)

    return None


_GEMINI_TRAFFIC_TYPE_TO_SERVICE_TIER: dict = {
    # ON_DEMAND_PRIORITY maps to "priority" — selects input_cost_per_token_priority, etc.
    "ON_DEMAND_PRIORITY": "priority",
    # FLEX / BATCH maps to "flex" — selects input_cost_per_token_flex, etc.
    "FLEX": "flex",
    "BATCH": "flex",
    # ON_DEMAND is standard pricing — no service_tier suffix applied
    "ON_DEMAND": None,
}


def _map_traffic_type_to_service_tier(traffic_type: Optional[str]) -> Optional[str]:
    """
    Map a Gemini usageMetadata.trafficType value to a LiteLLM service_tier string.

    This allows the same `_priority` / `_flex` cost-key suffix logic used for
    OpenAI/Azure to work for Gemini and Vertex AI models.

    trafficType values seen in practice
    ------------------------------------
    ON_DEMAND          -> standard pricing  (service_tier = None)
    ON_DEMAND_PRIORITY -> priority pricing  (service_tier = "priority")
    FLEX / BATCH       -> batch/flex pricing (service_tier = "flex")
    """
    if traffic_type is None:
        return None
    service_tier = _GEMINI_TRAFFIC_TYPE_TO_SERVICE_TIER.get(str(traffic_type).upper())
    return service_tier


def _get_usage_object(
    completion_response: Any,
) -> Optional[Usage]:
    usage_obj = cast(
        Union[Usage, ResponseAPIUsage, dict, BaseModel],
        (
            completion_response.get("usage")
            if isinstance(completion_response, dict)
            else getattr(completion_response, "get", lambda x: None)("usage")
        ),
    )

    if usage_obj is None:
        return None
    if isinstance(usage_obj, Usage):
        return usage_obj
    elif isinstance(usage_obj, dict):
        return Usage(**usage_obj)
    elif isinstance(usage_obj, BaseModel):
        return Usage(**usage_obj.model_dump())
    else:
        log.debug(
            f"Unknown usage object type: {type(usage_obj)}, usage_obj: {usage_obj}"
        )
        return None

def _apply_cost_discount(
    base_cost: float,
    custom_llm_provider: Optional[str],
) -> Tuple[float, float, float]:
    """
    Apply provider-specific cost discount from module-level config.

    Args:
        base_cost: The base cost before discount
        custom_llm_provider: The LLM provider name

    Returns:
        Tuple of (final_cost, discount_percent, discount_amount)
    """
    original_cost = base_cost
    discount_percent = 0.0
    discount_amount = 0.0

    if custom_llm_provider and custom_llm_provider in config.cost_discount_config:
        discount_percent = litellm.cost_discount_config[custom_llm_provider]
        discount_amount = original_cost * discount_percent
        final_cost = original_cost - discount_amount
        log.debug(
            f"Applied {discount_percent*100}% discount to {custom_llm_provider}: "
            f"${original_cost:.6f} -> ${final_cost:.6f} (saved ${discount_amount:.6f})"
        )
        return final_cost, discount_percent, discount_amount
    return base_cost, discount_percent, discount_amount


def _apply_cost_margin(
    base_cost: float,
    custom_llm_provider: Optional[str],
) -> Tuple[float, float, float, float]:
    """
    Apply provider-specific or global cost margin from module-level config.

    Args:
        base_cost: The base cost before margin (after discount if applicable)
        custom_llm_provider: The LLM provider name

    Returns:
        Tuple of (final_cost, margin_percent, margin_fixed_amount, margin_total_amount)
    """
    original_cost = base_cost
    margin_percent = 0.0
    margin_fixed_amount = 0.0
    margin_total_amount = 0.0

    # Get margin config - check provider-specific first, then global
    margin_config = None
    if custom_llm_provider and custom_llm_provider in config.cost_margin_config:
        margin_config = config.cost_margin_config[custom_llm_provider]
        log.debug(f"Found provider-specific margin config for {custom_llm_provider}: {margin_config}")
    elif "global" in config.cost_margin_config:
        margin_config = config.cost_margin_config["global"]
        log.debug(f"Using global margin config: {margin_config}")
    else:
        log.debug(
            f"No margin config found. Provider: {custom_llm_provider}, Available configs: {list(config.cost_margin_config.keys())}"
        )

    if margin_config is not None:
        # Handle different margin config formats
        if isinstance(margin_config, (int, float)):
            # Simple percentage: {"openai": 0.10}
            margin_percent = float(margin_config)
            margin_total_amount = original_cost * margin_percent
        elif isinstance(margin_config, dict):
            # Complex config: {"percentage": 0.08, "fixed_amount": 0.0005}
            if "percentage" in margin_config:
                margin_percent = float(margin_config["percentage"])
                margin_total_amount += original_cost * margin_percent
            if "fixed_amount" in margin_config:
                margin_fixed_amount = float(margin_config["fixed_amount"])
                margin_total_amount += margin_fixed_amount

        final_cost = original_cost + margin_total_amount
        log.debug(
            f"Applied margin to {custom_llm_provider or 'global'}: "
            f"${original_cost:.6f} -> ${final_cost:.6f} "
            f"(margin: {margin_percent*100 if margin_percent > 0 else 0}% + ${margin_fixed_amount:.6f} = ${margin_total_amount:.6f})"
        )

        return final_cost, margin_percent, margin_fixed_amount, margin_total_amount

    return base_cost, margin_percent, margin_fixed_amount, margin_total_amount

def get_response_cost_from_hidden_params(
    hidden_params: Union[dict, BaseModel],
) -> Optional[float]:
    if isinstance(hidden_params, BaseModel):
        _hidden_params_dict = cast(BaseModel, hidden_params).model_dump()
    else:
        _hidden_params_dict = hidden_params

    additional_headers = _hidden_params_dict.get("additional_headers", {})
    if (
        additional_headers
        and "llm_provider-x-litellm-response-cost" in additional_headers
    ):
        response_cost = additional_headers["llm_provider-x-litellm-response-cost"]
        if response_cost is None:
            return None
        return float(additional_headers["llm_provider-x-litellm-response-cost"])
    return None

class BaseTokenUsageProcessor:
    @staticmethod
    def combine_usage_objects(usage_objects: List[Usage]) -> Usage:
        """
        Combine multiple Usage objects into a single Usage object, checking model keys for nested values.
        """
        from anchor.provider.legacy.types import (
            CompletionTokensDetailsWrapper,
            PromptTokensDetailsWrapper,
            Usage,
        )

        combined = Usage()

        # Sum basic token counts
        for usage in usage_objects:
            # Handle direct attributes by checking what exists in the model
            for attr in dir(usage):
                if not attr.startswith("_") and not callable(getattr(usage, attr)):
                    current_val = getattr(combined, attr, 0)
                    new_val = getattr(usage, attr, 0)
                    if (
                        new_val is not None
                        and isinstance(new_val, (int, float))
                        and isinstance(current_val, (int, float))
                    ):
                        setattr(combined, attr, current_val + new_val)
            # Handle nested prompt_tokens_details
            if hasattr(usage, "prompt_tokens_details") and usage.prompt_tokens_details:
                if (
                    not hasattr(combined, "prompt_tokens_details")
                    or not combined.prompt_tokens_details
                ):
                    combined.prompt_tokens_details = PromptTokensDetailsWrapper()

                # Check what keys exist in the model's prompt_tokens_details
                # Access model_fields on the class, not the instance, to avoid Pydantic 2.11+ deprecation warnings
                for attr in type(usage.prompt_tokens_details).model_fields:
                    if (
                        hasattr(usage.prompt_tokens_details, attr)
                        and not attr.startswith("_")
                        and not callable(getattr(usage.prompt_tokens_details, attr))
                    ):
                        current_val = (
                            getattr(combined.prompt_tokens_details, attr, 0) or 0
                        )
                        new_val = getattr(usage.prompt_tokens_details, attr, 0) or 0
                        if new_val is not None and isinstance(new_val, (int, float)):
                            setattr(
                                combined.prompt_tokens_details,
                                attr,
                                current_val + new_val,
                            )

            # Handle nested completion_tokens_details
            if (
                hasattr(usage, "completion_tokens_details")
                and usage.completion_tokens_details
            ):
                if (
                    not hasattr(combined, "completion_tokens_details")
                    or not combined.completion_tokens_details
                ):
                    combined.completion_tokens_details = (
                        CompletionTokensDetailsWrapper()
                    )

                # Check what keys exist in the model's completion_tokens_details
                # Access model_fields on the class, not the instance, to avoid Pydantic 2.11+ deprecation warnings
                for attr in type(usage.completion_tokens_details).model_fields:
                    if not attr.startswith("_") and not callable(
                        getattr(usage.completion_tokens_details, attr)
                    ):
                        current_val = (
                            getattr(combined.completion_tokens_details, attr, 0) or 0
                        )
                        new_val = getattr(usage.completion_tokens_details, attr, 0) or 0
                        if isinstance(new_val, (int, float)):
                            setattr(
                                combined.completion_tokens_details,
                                attr,
                                current_val + new_val,
                            )

        return combined


class RealtimeAPITokenUsageProcessor(BaseTokenUsageProcessor):
    @staticmethod
    def collect_usage_from_realtime_stream_results(
        results: OpenAIRealtimeStreamList,
    ) -> List[Usage]:
        response_done_events: List[OpenAIRealtimeStreamResponseBaseObject] = cast(
            List[OpenAIRealtimeStreamResponseBaseObject],
            [result for result in results if result["type"] == "response.done"],
        )
        usage_objects: List[Usage] = []
        return usage_objects

    @staticmethod
    def collect_and_combine_usage_from_realtime_stream_results(
        results: OpenAIRealtimeStreamList,
    ) -> Usage:
        """
        Collect and combine usage from realtime stream results
        """
        collected_usage_objects = (
            RealtimeAPITokenUsageProcessor.collect_usage_from_realtime_stream_results(
                results
            )
        )
        combined_usage_object = RealtimeAPITokenUsageProcessor.combine_usage_objects(
            collected_usage_objects
        )
        return combined_usage_object

    @staticmethod
    def create_logging_realtime_object(
        usage: Usage, results: OpenAIRealtimeStreamList
    ) -> LiteLLMRealtimeStreamLoggingObject:
        return LiteLLMRealtimeStreamLoggingObject(
            usage=usage,
            results=results,
        )


def handle_realtime_stream_cost_calculation(
    results: OpenAIRealtimeStreamList,
    combined_usage_object: Usage,
    custom_llm_provider: str,
    litellm_model_name: str,
    data_residency: Optional[str] = None,
) -> float:
    """
    Handles the cost calculation for realtime stream responses.

    Pick the 'response.done' events. Calculate total cost across all 'response.done' events.

    Args:
        results: A list of OpenAIRealtimeStreamBaseObject objects
    """
    received_model = None
    potential_model_names = []
    for result in results:
        if result["type"] == "session.created":
            received_model = cast(OpenAIRealtimeStreamSessionEvents, result)[
                "session"
            ].get("model", None)
            potential_model_names.append(received_model)

    potential_model_names.append(litellm_model_name)
    input_cost_per_token = 0.0
    output_cost_per_token = 0.0

    for model_name in potential_model_names:
        try:
            if model_name is None:
                continue
            _input_cost_per_token, _output_cost_per_token = generic_cost_per_token(
                model=model_name,
                usage=combined_usage_object,
                custom_llm_provider=custom_llm_provider,
                data_residency=data_residency,
            )
        except Exception:
            continue
        input_cost_per_token += _input_cost_per_token
        output_cost_per_token += _output_cost_per_token
        break  # exit if we find a valid model
    total_cost = input_cost_per_token + output_cost_per_token

    return total_cost

def _infer_call_type(
    call_type: Optional[CallTypesLiteral], completion_response: Any
) -> Optional[CallTypesLiteral]:
    if call_type is not None:
        return call_type

    if completion_response is None:
        return None

    if isinstance(completion_response, ModelResponse) or isinstance(
        completion_response, ModelResponseStream
    ):
        return "completion"
    elif isinstance(completion_response, EmbeddingResponse):
        return "embedding"
    elif isinstance(completion_response, TranscriptionResponse):
        return "transcription"
    elif isinstance(completion_response, HttpxBinaryResponseContent):
        return "speech"
    elif isinstance(completion_response, RerankResponse):
        return "rerank"
    elif isinstance(completion_response, ImageResponse):
        return "image_generation"
    elif isinstance(completion_response, TextCompletionResponse):
        return "text_completion"
    elif isinstance(completion_response, LiteLLMSendMessageResponse):
        return "send_message"

    return call_type