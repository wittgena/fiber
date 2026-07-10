# anchor.registry.provider.cost.calculator
import time
from typing import TYPE_CHECKING, Any, List, Literal, Optional, Tuple, Union, cast
from httpx import Response
from pydantic import BaseModel
from functools import lru_cache

from anchor.bind.switch.params import ModelResponse, ModelResponseStream

from anchor.registry.model.cost import model_cost, lookup_base_model_info
from anchor.registry.router.locator import get_llm_provider
from anchor.provider.cost.builtin import BuiltInToolCostTracker
from anchor.provider.token.counter import token_counter
from anchor.provider.cost.unit import UnitCostCalculator, UsageTransform, _IMAGE_RESPONSE_CALL_TYPES

from anchor.provider.param.rerank import RerankBilledUnits, RerankResponse
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.config.constants import DEFAULT_MAX_LRU_CACHE_SIZE, DEFAULT_REPLICATE_GPU_PRICE_PER_SECOND
from bound.surface.legacy.openai.types import (
    HttpxBinaryResponseContent,
    OpenAIModerationResponse,
    OpenAIRealtimeStreamList,
    OpenAIRealtimeStreamResponseBaseObject,
    OpenAIRealtimeStreamSessionEvents,
    ResponseAPIUsage,
    ResponsesAPIResponse,
)
from bound.surface.legacy.types import (
    CallTypesLiteral, LiteLLMRealtimeStreamLoggingObject,
    StandardBuiltInToolsParams, Usage, CallTypes, CostPerToken, 
    EmbeddingResponse, ImageResponse, TextCompletionResponse, TranscriptionResponse
)

from anchor.provider.cost.support import (
    PricingPolicyManager,
    ProviderContextResolver,
    UsageTelemetryParser
)

from watcher.plane.emitter import get_emitter

log = get_emitter("cost.calculator")

LitellmLoggingObject = Any

## @cleanup: Pre-resolved CallTypes enum values
_A2A_CALL_TYPES = frozenset({CallTypes.asend_message.value, CallTypes.send_message.value})
_VIDEO_CALL_TYPES = frozenset({
    CallTypes.create_video.value, CallTypes.acreate_video.value,
    CallTypes.video_edit.value, CallTypes.avideo_edit.value,
    CallTypes.video_remix.value, CallTypes.avideo_remix.value,
})
_SPEECH_CALL_TYPES = frozenset({CallTypes.speech.value, CallTypes.aspeech.value})
_TRANSCRIPTION_CALL_TYPES = frozenset({CallTypes.atranscription.value, CallTypes.transcription.value})
_RERANK_CALL_TYPES = frozenset({CallTypes.rerank.value, CallTypes.arerank.value})
_SEARCH_CALL_TYPES = frozenset({CallTypes.search.value, CallTypes.asearch.value})
_AREALTIME_CALL_TYPE = CallTypes.arealtime.value
_MCP_CALL_TYPE = CallTypes.call_mcp_tool.value

# =============================================================================
# Entry 1: cost_per_token 
# =============================================================================
def cost_per_token(  # noqa: PLR0915
    model: str = "",
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    response_time_ms: Optional[float] = 0.0,
    custom_llm_provider: Optional[str] = None,
    region_name=None,
    prompt_characters: Optional[int] = None,
    completion_characters: Optional[int] = None,
    cache_creation_input_tokens: Optional[int] = 0,
    cache_read_input_tokens: Optional[int] = 0,
    custom_cost_per_token: Optional[CostPerToken] = None,
    custom_cost_per_second: Optional[float] = None,
    number_of_queries: Optional[int] = None,
    usage_object: Optional[Usage] = None,
    rerank_billed_units: Optional[RerankBilledUnits] = None,
    call_type: CallTypesLiteral = "completion",
    audio_transcription_file_duration: float = 0.0,
    service_tier: Optional[str] = None,
    data_residency: Optional[str] = None,
    response: Optional[Any] = None,
    request_model: Optional[str] = None,
) -> Tuple[float, float]:
    if model is None:
        raise Exception("Invalid arg. Model cannot be none.")

    if usage_object is not None:
        usage_block = usage_object
    else:
        usage_block = Usage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
        )

    _cache_read_tokens: float = 0
    _cache_creation_tokens: float = 0
    _is_anthropic_style = False

    if usage_object is not None:
        _pt_details = getattr(usage_object, "prompt_tokens_details", None)
        if _pt_details is not None:
            _cache_read_tokens = float(getattr(_pt_details, "cached_tokens", 0) or 0)
            _cache_creation_tokens = float(
                getattr(_pt_details, "cache_write_tokens", 0)
                or getattr(_pt_details, "cache_creation_tokens", 0)
                or 0
            )

        _anthropic_read = getattr(usage_object, "cache_read_input_tokens", None)
        _anthropic_create = getattr(usage_object, "cache_creation_input_tokens", None)
        if _anthropic_read is not None or _anthropic_create is not None:
            _is_anthropic_style = True
            if _anthropic_read is not None: _cache_read_tokens = float(_anthropic_read)
            if _anthropic_create is not None: _cache_creation_tokens = float(_anthropic_create)

    if not _cache_read_tokens and cache_read_input_tokens:
        _cache_read_tokens = float(cache_read_input_tokens)
        _is_anthropic_style = True
    if not _cache_creation_tokens and cache_creation_input_tokens:
        _cache_creation_tokens = float(cache_creation_input_tokens)
        _is_anthropic_style = True

    _normalized_prompt_tokens = float(prompt_tokens)
    if _is_anthropic_style:
        _normalized_prompt_tokens += _cache_read_tokens + _cache_creation_tokens

    # [REFACTORED] PricingPolicyManager 사용
    response_cost = PricingPolicyManager.calculate_custom_pricing(
        prompt_tokens=_normalized_prompt_tokens,
        completion_tokens=completion_tokens,
        response_time_ms=response_time_ms,
        cached_tokens=_cache_read_tokens,
        cache_creation_tokens=_cache_creation_tokens,
        custom_cost_per_second=custom_cost_per_second,
        custom_cost_per_token=custom_cost_per_token,
    )
    if response_cost is not None:
        return response_cost[0], response_cost[1]

    prompt_tokens_cost_usd_dollar: float = 0
    completion_tokens_cost_usd_dollar: float = 0
    caller_supplied_provider = custom_llm_provider is not None
    model_is_str = isinstance(model, str)

    if caller_supplied_provider and model_is_str:
        _dup_prefix = f"{custom_llm_provider}/"
        while model.startswith(_dup_prefix):
            _remainder = model[len(_dup_prefix) :]
            if _remainder.startswith(_dup_prefix):
                model = _remainder
            else:
                break

    model_with_provider = model
    if caller_supplied_provider:
        _prov_prefix = f"{custom_llm_provider}/"
        if not (model_is_str and model.startswith(_prov_prefix)):
            model_with_provider = f"{custom_llm_provider}/{model}"
        if region_name is not None:
            model_with_provider_and_region = f"{custom_llm_provider}/{region_name}/{model}"
            if model_with_provider_and_region in model_cost:
                model_with_provider = model_with_provider_and_region
    else:
        _, custom_llm_provider, _, _ = get_llm_provider(model=model)

    assert custom_llm_provider is not None

    model_without_prefix = model
    model_parts = model.split("/", 1)
    if len(model_parts) > 1:
        model_without_prefix = model_parts[1]

    if model_with_provider in model_cost:
        model = model_with_provider
    elif model in model_cost:
        model = model
    elif model_without_prefix in model_cost:
        model = model_without_prefix

    model_info = lookup_base_model_info(model=model, custom_llm_provider=custom_llm_provider)

    if (model_info.get("input_cost_per_token") or 0.0) > 0 or (model_info.get("output_cost_per_token") or 0.0) > 0:
        # [KEY CHANGE 2] cost.util.generic_cost_per_token 대신 UnitCostCalculator 사용
        return UnitCostCalculator.generic_cost_per_token(
            model=model,
            usage=usage_block,
            custom_llm_provider=custom_llm_provider,
            service_tier=service_tier,
            data_residency=data_residency,
        )

    if model_info.get("input_cost_per_second") is not None and response_time_ms is not None:
        prompt_tokens_cost_usd_dollar = model_info["input_cost_per_second"] * response_time_ms / 1000  # type: ignore

    if model_info.get("output_cost_per_second") is not None and response_time_ms is not None:
        completion_tokens_cost_usd_dollar = model_info["output_cost_per_second"] * response_time_ms / 1000  # type: ignore

    return prompt_tokens_cost_usd_dollar, completion_tokens_cost_usd_dollar


# =============================================================================
# Entry 2: completion_cost
# =============================================================================
def completion_cost(  # noqa: PLR0915
    completion_response=None,
    model: Optional[str] = None,
    prompt="",
    messages: List = [],
    completion="",
    total_time: Optional[float] = 0.0,
    call_type: Optional[CallTypesLiteral] = None,
    custom_llm_provider=None,
    region_name=None,
    size: Optional[str] = None,
    quality: Optional[str] = None,
    n: Optional[int] = None,
    custom_cost_per_token: Optional[CostPerToken] = None,
    custom_cost_per_second: Optional[float] = None,
    optional_params: Optional[dict] = None,
    custom_pricing: Optional[bool] = None,
    base_model: Optional[str] = None,
    standard_built_in_tools_params: Optional[StandardBuiltInToolsParams] = None,
    litellm_model_name: Optional[str] = None,
    router_model_id: Optional[str] = None,
    log_delegator: Optional[LitellmLoggingObject] = None,
    service_tier: Optional[str] = None,
    data_residency: Optional[str] = None,
) -> float:
    try:
        # [REFACTORED] UsageTelemetryParser 활용
        call_type = UsageTelemetryParser.infer_call_type(call_type, completion_response) or "completion"

        if call_type in ("aimage_generation", "image_generation") and isinstance(model, str) and not model and custom_llm_provider == "azure":
            model = "dall-e-2"

        prompt_tokens = 0
        prompt_characters: Optional[int] = None
        completion_tokens = 0
        completion_characters: Optional[int] = None
        cache_creation_input_tokens: Optional[int] = None
        cache_read_input_tokens: Optional[int] = None
        audio_transcription_file_duration: float = 0.0
        
        # [REFACTORED] UsageTelemetryParser 활용
        cost_per_token_usage_object: Optional[Usage] = UsageTelemetryParser.extract_usage(completion_response)
        rerank_billed_units: Optional[RerankBilledUnits] = None

        if service_tier is None and optional_params is not None:
            service_tier = optional_params.get("service_tier")
        if service_tier is None and completion_response is not None:
            service_tier = getattr(completion_response, "service_tier", None) if isinstance(completion_response, BaseModel) else completion_response.get("service_tier")
        if service_tier is None and cost_per_token_usage_object is not None:
            service_tier = getattr(cost_per_token_usage_object, "service_tier", None) if isinstance(cost_per_token_usage_object, BaseModel) else cost_per_token_usage_object.get("service_tier")

        # [REFACTORED] ProviderContextResolver 활용
        selected_model = ProviderContextResolver.resolve_selected_model(
            model=model,
            completion_response=completion_response,
            custom_llm_provider=custom_llm_provider,
            custom_pricing=custom_pricing,
            base_model=base_model,
            router_model_id=router_model_id,
        )

        potential_model_names = [selected_model, ProviderContextResolver.resolve_response_model(completion_response)]
        if model is not None:
            potential_model_names.append(model)

        for idx, current_model in enumerate(potential_model_names):
            try:
                if completion_response is not None and isinstance(completion_response, (BaseModel, dict)):
                    usage_obj = completion_response.get("usage", {}) if isinstance(completion_response, dict) else getattr(completion_response, "usage", {})
                    
                    if isinstance(usage_obj, BaseModel) and not _is_known_usage_objects(usage_obj):
                        setattr(completion_response, "usage", config.Usage(**usage_obj.model_dump()))
                    
                    _usage = usage_obj.model_dump() if isinstance(usage_obj, BaseModel) else (usage_obj or {})

                    prompt_tokens = _usage.get("prompt_tokens", 0)
                    completion_tokens = _usage.get("completion_tokens", 0)
                    cache_creation_input_tokens = _usage.get("cache_creation_input_tokens", 0)
                    cache_read_input_tokens = _usage.get("cache_read_input_tokens", 0)
                    
                    if _usage.get("prompt_tokens_details"):
                        cache_read_input_tokens = _usage["prompt_tokens_details"].get("cached_tokens", 0)

                    total_time = getattr(completion_response, "_response_ms", 0)
                    hidden_params = getattr(completion_response, "_hidden_params", None)
                    
                    if hidden_params is not None:
                        custom_llm_provider = hidden_params.get("custom_llm_provider", custom_llm_provider)
                        region_name = hidden_params.get("region_name", region_name)
                        
                        if service_tier is None:
                            provider_specific = hidden_params.get("provider_specific_fields") or {}
                            raw_traffic_type = provider_specific.get("traffic_type")
                            if raw_traffic_type:
                                # [REFACTORED] ProviderContextResolver 활용
                                service_tier = ProviderContextResolver.map_traffic_to_tier(raw_traffic_type)
                else:
                    if current_model is None:
                        raise ValueError(f"Model is None. Passed completion_response={completion_response}")
                    if messages:
                        prompt_tokens = token_counter(model=current_model, messages=messages)
                    elif prompt:
                        prompt_tokens = token_counter(model=current_model, text=prompt)
                    completion_tokens = token_counter(model=current_model, text=completion)

                if current_model is None:
                    raise ValueError(f"Model is None.")

                if custom_llm_provider is None:
                    try:
                        current_model, custom_llm_provider, _, _ = get_llm_provider(model=current_model)
                    except Exception as e:
                        log.debug(f"Error inferring custom_llm_provider - {str(e)}")

                # [KEY CHANGE 3] CostCalculatorUtils 대신 _IMAGE_RESPONSE_CALL_TYPES 상수와 UnitCostCalculator 직접 사용
                if call_type in _IMAGE_RESPONSE_CALL_TYPES and isinstance(completion_response, ImageResponse):
                    return UnitCostCalculator.route_image_generation_cost_calculator(
                        model=current_model,
                        custom_llm_provider=custom_llm_provider,
                        completion_response=completion_response,
                        quality=quality, n=n, size=size,
                        optional_params=optional_params, call_type=call_type,
                    )
                elif call_type in _SPEECH_CALL_TYPES:
                    prompt_characters = config.utils._count_characters(text=prompt)
                elif call_type in _TRANSCRIPTION_CALL_TYPES:
                    _hidden = getattr(completion_response, "_hidden_params", {}) or {}
                    audio_transcription_file_duration = _hidden.get("audio_transcription_duration", getattr(completion_response, "duration", 0.0))
                elif call_type in _RERANK_CALL_TYPES:
                    if isinstance(completion_response, RerankResponse):
                        meta_obj = completion_response.meta
                        billed_units = (meta_obj.get("billed_units", {}) if meta_obj else {}) or {}
                        rerank_billed_units = RerankBilledUnits(search_units=billed_units.get("search_units"), total_tokens=billed_units.get("total_tokens"))
                        completion_tokens = billed_units.get("search_units") or 1 
                elif call_type == _AREALTIME_CALL_TYPE and isinstance(completion_response, LiteLLMRealtimeStreamLoggingObject):
                    if cost_per_token_usage_object is None or custom_llm_provider is None:
                        raise ValueError("usage object and custom_llm_provider required for realtime streams.")
                    
                    # [REFACTORED] UsageTelemetryParser 활용
                    return UsageTelemetryParser.process_realtime_stream(
                        results=completion_response.results,
                        combined_usage_object=cost_per_token_usage_object,
                        custom_llm_provider=custom_llm_provider,
                        litellm_model_name=current_model,
                        data_residency=data_residency,
                    )

                if "togethercomputer" in current_model or "together_ai" in current_model or custom_llm_provider == "together_ai":
                    pass 

                elif (current_model in getattr(config, "replicate_models", []) or "replicate" in current_model) and current_model not in model_cost:
                    # [REFACTORED] PricingPolicyManager 활용
                    return PricingPolicyManager.get_replicate_pricing(completion_response, total_time)

                request_model_for_cost = log_delegator.model if log_delegator else None

                prompt_cost, comp_cost = cost_per_token(
                    model=current_model,
                    prompt_tokens=prompt_tokens or 0,
                    completion_tokens=completion_tokens or 0,
                    custom_llm_provider=custom_llm_provider,
                    response_time_ms=total_time,
                    region_name=region_name,
                    custom_cost_per_second=custom_cost_per_second,
                    custom_cost_per_token=custom_cost_per_token,
                    prompt_characters=prompt_characters,
                    completion_characters=completion_characters,
                    cache_creation_input_tokens=cache_creation_input_tokens,
                    cache_read_input_tokens=cache_read_input_tokens,
                    usage_object=cost_per_token_usage_object,
                    call_type=call_type,
                    audio_transcription_file_duration=audio_transcription_file_duration,
                    rerank_billed_units=rerank_billed_units,
                    service_tier=service_tier,
                    data_residency=data_residency,
                    response=completion_response,
                    request_model=request_model_for_cost,
                )

                _final_cost = prompt_cost + comp_cost
                cost_for_built_in_tools = BuiltInToolCostTracker.get_cost_for_built_in_tools(
                    model=current_model,
                    response_object=completion_response,
                    usage=cost_per_token_usage_object,
                    standard_built_in_tools_params=standard_built_in_tools_params,
                    custom_llm_provider=custom_llm_provider,
                )
                _final_cost += cost_for_built_in_tools

                model_for_additional_costs = current_model
                if custom_llm_provider == "azure_ai":
                    model_for_additional_costs = request_model_for_cost
                    if completion_response:
                        hidden_params = getattr(completion_response, "_hidden_params", None) or {}
                        hidden_model = hidden_params.get("model") or hidden_params.get("litellm_model_name")
                        if hidden_model and "model_router" in hidden_model.lower():
                            model_for_additional_costs = hidden_model

                # [REFACTORED] PricingPolicyManager 활용
                additional_costs = PricingPolicyManager.get_additional_costs(
                    model=model_for_additional_costs or current_model,
                    custom_llm_provider=custom_llm_provider,
                    prompt_tokens=prompt_tokens or 0,
                    completion_tokens=completion_tokens or 0,
                )
                if additional_costs:
                    _final_cost += sum(additional_costs.values())

                original_cost = _final_cost
                discount_percent = discount_amount = margin_percent = margin_fixed_amount = margin_total_amount = 0.0

                if getattr(config, "cost_discount_config", None):
                    # [REFACTORED] PricingPolicyManager 활용
                    _final_cost, discount_percent, discount_amount = PricingPolicyManager.apply_discount(
                        base_cost=_final_cost, custom_llm_provider=custom_llm_provider
                    )

                if getattr(config, "cost_margin_config", None):
                    # [REFACTORED] PricingPolicyManager 활용
                    _final_cost, margin_percent, margin_fixed_amount, margin_total_amount = PricingPolicyManager.apply_margin(
                        base_cost=_final_cost, custom_llm_provider=custom_llm_provider
                    )
                return _final_cost
            except Exception as e:
                log.debug(f"Error calculating cost for model={current_model} - {str(e)}")
                if idx == len(potential_model_names) - 1:
                    raise e
        raise Exception(f"Unable to calculate cost for received potential model names - {potential_model_names}")
    except Exception as e:
        raise e


# =============================================================================
# Entry 3: response_cost_calculator
# =============================================================================
def response_cost_calculator(
    response_object: Union[
        ModelResponse, EmbeddingResponse, ImageResponse, TranscriptionResponse,
        TextCompletionResponse, HttpxBinaryResponseContent, RerankResponse,
        ResponsesAPIResponse, LiteLLMRealtimeStreamLoggingObject,
        OpenAIModerationResponse, Response
    ],
    model: str,
    custom_llm_provider: Optional[str],
    call_type: CallTypesLiteral,
    optional_params: dict,
    cache_hit: Optional[bool] = None,
    base_model: Optional[str] = None,
    custom_pricing: Optional[bool] = None,
    prompt: str = "",
    standard_built_in_tools_params: Optional[StandardBuiltInToolsParams] = None,
    litellm_model_name: Optional[str] = None,
    router_model_id: Optional[str] = None,
    log_delegator: Optional[LitellmLoggingObject] = None,
    service_tier: Optional[str] = None,
    data_residency: Optional[str] = None,
) -> float:
    try:
        if cache_hit:
            return 0.0

        if isinstance(response_object, BaseModel) and hasattr(response_object, "_hidden_params"):
            response_object._hidden_params["optional_params"] = optional_params
            # [REFACTORED] PricingPolicyManager 활용
            provider_response_cost = PricingPolicyManager.extract_cost_from_headers(response_object._hidden_params)
            if provider_response_cost is not None:
                return provider_response_cost

        return completion_cost(
            completion_response=response_object,
            model=model,
            call_type=call_type,
            custom_llm_provider=custom_llm_provider,
            optional_params=optional_params,
            custom_pricing=custom_pricing,
            base_model=base_model,
            prompt=prompt,
            standard_built_in_tools_params=standard_built_in_tools_params,
            litellm_model_name=litellm_model_name,
            router_model_id=router_model_id,
            log_delegator=log_delegator,
            service_tier=service_tier,
            data_residency=data_residency,
        )
    except Exception as e:
        raise e

def _is_known_usage_objects(usage_obj):
    return (
        isinstance(usage_obj, Usage)
        or isinstance(usage_obj, ResponseAPIUsage)
        or UsageTransform.is_transcription_usage_object(
            usage_obj
        )
    )