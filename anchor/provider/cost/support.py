# anchor.registry.provider.cost.support
import time
from typing import TYPE_CHECKING, Any, List, Literal, Optional, Tuple, Union, cast
from httpx import Response
from pydantic import BaseModel
from functools import lru_cache

from anchor.bind.switch.params import ModelResponse, ModelResponseStream
from anchor.registry.model.cost import model_cost
from anchor.registry.router.locator import get_llm_provider
from anchor.provider.token.counter import token_counter
from anchor.provider.cost.unit import UsageTransform, UnitCostCalculator

from anchor.provider.param.rerank import RerankBilledUnits, RerankResponse
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.config.constants import DEFAULT_MAX_LRU_CACHE_SIZE, DEFAULT_REPLICATE_GPU_PRICE_PER_SECOND
from bound.surface.legacy.provider import ProviderTypesSet
from bound.surface.legacy.openai.types import (
    HttpxBinaryResponseContent,
    OpenAIModerationResponse,
    OpenAIRealtimeStreamList,
    OpenAIRealtimeStreamResponseBaseObject,
    OpenAIRealtimeStreamSessionEvents,
    ResponseAPIUsage,
    ResponsesAPIResponse,
)
from bound.surface.legacy.types import CallTypesLiteral, LiteLLMRealtimeStreamLoggingObject, StandardBuiltInToolsParams, Usage
from bound.surface.legacy.types import CallTypes, CostPerToken, EmbeddingResponse, ImageResponse, TextCompletionResponse, TranscriptionResponse
try:
    from bound.surface.legacy.types import LiteLLMSendMessageResponse
except ImportError:
    LiteLLMSendMessageResponse = Any

from watcher.plane.emitter import get_emitter

log = get_emitter("cost.support")
LitellmLoggingObject = Any

_GEMINI_TRAFFIC_TYPE_TO_SERVICE_TIER: dict = {
    "ON_DEMAND_PRIORITY": "priority",
    "FLEX": "flex",
    "BATCH": "flex",
    "ON_DEMAND": None,
}

class PricingPolicyManager:
    """비용 조정 및 단가 정책"""
    @staticmethod
    def calculate_custom_pricing(
        prompt_tokens: float = 0,
        completion_tokens: float = 0,
        response_time_ms: Optional[float] = 0.0,
        cached_tokens: float = 0,
        cache_creation_tokens: float = 0,
        custom_cost_per_token: Optional[CostPerToken] = None,
        custom_cost_per_second: Optional[float] = None,
    ) -> Optional[Tuple[float, float]]:
        if custom_cost_per_token is None and custom_cost_per_second is None:
            return None

        if custom_cost_per_token is not None:
            input_cost_per_token = custom_cost_per_token["input_cost_per_token"]
            output_cost_per_token = custom_cost_per_token["output_cost_per_token"]

            cache_read_input_token_cost = custom_cost_per_token.get(
                "cache_read_input_token_cost", input_cost_per_token,
            )
            cache_creation_input_token_cost = custom_cost_per_token.get(
                "cache_creation_input_token_cost", input_cost_per_token,
            )

            regular_prompt_tokens = max(
                prompt_tokens - cached_tokens - cache_creation_tokens, 0,
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

    @staticmethod
    def get_additional_costs(
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

    @staticmethod
    def get_replicate_pricing(completion_response: dict, total_time=0.0):
        a100_80gb_price_per_second_public = DEFAULT_REPLICATE_GPU_PRICE_PER_SECOND
        if total_time == 0.0:
            start_time = completion_response.get("created", time.time())
            end_time = getattr(completion_response, "ended", time.time())
            total_time = end_time - start_time
        return a100_80gb_price_per_second_public * total_time / 1000

    @staticmethod
    def extract_cost_from_headers(hidden_params: Union[dict, BaseModel]) -> Optional[float]:
        if isinstance(hidden_params, BaseModel):
            _hidden_params_dict = cast(BaseModel, hidden_params).model_dump()
        else:
            _hidden_params_dict = hidden_params

        additional_headers = _hidden_params_dict.get("additional_headers", {})
        if additional_headers and "llm_provider-x-litellm-response-cost" in additional_headers:
            response_cost = additional_headers["llm_provider-x-litellm-response-cost"]
            if response_cost is None:
                return None
            return float(response_cost)
        return None

    @staticmethod
    def apply_discount(base_cost: float, custom_llm_provider: Optional[str]) -> Tuple[float, float, float]:
        original_cost = base_cost
        discount_percent = 0.0
        discount_amount = 0.0

        if custom_llm_provider and custom_llm_provider in getattr(config, "cost_discount_config", {}):
            discount_percent = config.cost_discount_config[custom_llm_provider]
            discount_amount = original_cost * discount_percent
            final_cost = original_cost - discount_amount
            log.debug(
                f"Applied {discount_percent*100}% discount to {custom_llm_provider}: "
                f"${original_cost:.6f} -> ${final_cost:.6f} (saved ${discount_amount:.6f})"
            )
            return final_cost, discount_percent, discount_amount
        return base_cost, discount_percent, discount_amount

    @staticmethod
    def apply_margin(base_cost: float, custom_llm_provider: Optional[str]) -> Tuple[float, float, float, float]:
        original_cost = base_cost
        margin_percent = 0.0
        margin_fixed_amount = 0.0
        margin_total_amount = 0.0

        margin_config = None
        cost_margin_config = getattr(config, "cost_margin_config", {})
        
        if custom_llm_provider and custom_llm_provider in cost_margin_config:
            margin_config = cost_margin_config[custom_llm_provider]
            log.debug(f"Found provider-specific margin config for {custom_llm_provider}: {margin_config}")
        elif "global" in cost_margin_config:
            margin_config = cost_margin_config["global"]
            log.debug(f"Using global margin config: {margin_config}")
        else:
            log.debug(
                f"No margin config found. Provider: {custom_llm_provider}, Available configs: {list(cost_margin_config.keys())}"
            )

        if margin_config is not None:
            if isinstance(margin_config, (int, float)):
                margin_percent = float(margin_config)
                margin_total_amount = original_cost * margin_percent
            elif isinstance(margin_config, dict):
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

class ProviderContextResolver:
    """ProviderContextResolver (실행 컨텍스트 및 제공자 식별)"""
    @staticmethod
    @lru_cache(maxsize=DEFAULT_MAX_LRU_CACHE_SIZE)
    def _model_contains_known_llm_provider(model: str) -> bool:
        _provider_prefix = model.split("/")[0]
        return _provider_prefix in ProviderTypesSet

    @staticmethod
    def resolve_provider(model: Optional[str], custom_llm_provider: Optional[str] = None) -> Optional[str]:
        if custom_llm_provider is not None:
            return custom_llm_provider
        if model is None:
            return None
        try:
            _, infered_provider, _, _ = get_llm_provider(model=model)
            return infered_provider
        except Exception as e:
            log.debug(f"calculator.calc::resolve_provider() - Error inferring custom_llm_provider - {str(e)}")
            return None

    @staticmethod
    def resolve_selected_model(
        model: Optional[str],
        completion_response: Optional[Any],
        base_model: Optional[str] = None,
        custom_pricing: Optional[bool] = None,
        custom_llm_provider: Optional[str] = None,
        router_model_id: Optional[str] = None,
    ) -> Optional[str]:
        return_model: Optional[str] = None
        region_name: Optional[str] = None
        
        provider = ProviderContextResolver.resolve_provider(model=model, custom_llm_provider=custom_llm_provider)
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
                if entry.get("input_cost_per_token") is not None or entry.get("input_cost_per_second") is not None:
                    return_model = router_model_id
                else:
                    return_model = model
            else:
                return_model = model
        elif base_model is not None:
            return_model = base_model
        elif completion_response_model is None and hidden_params is not None:
            if hidden_params.get("model", None) is not None and len(hidden_params["model"]) > 0:
                return_model = hidden_params.get("model", model)
        elif hidden_params is not None and hidden_params.get("region_name", None) is not None:
            region_name = hidden_params.get("region_name", None)

        if return_model is None and completion_response_model is not None:
            return_model = completion_response_model
        if return_model is None and model is not None:
            return_model = model

        if (
            return_model is not None
            and provider is not None
            and not ProviderContextResolver._model_contains_known_llm_provider(return_model)
        ):
            if region_name is not None:
                return_model = f"{provider}/{region_name}/{return_model}"
            else:
                return_model = f"{provider}/{return_model}"

        return return_model

    @staticmethod
    def resolve_response_model(completion_response: Any) -> Optional[str]:
        if completion_response is None:
            return None
        if isinstance(completion_response, BaseModel):
            return getattr(completion_response, "model", None)
        elif isinstance(completion_response, dict):
            return completion_response.get("model", None)
        return None

    @staticmethod
    def map_traffic_to_tier(traffic_type: Optional[str]) -> Optional[str]:
        if traffic_type is None:
            return None
        return _GEMINI_TRAFFIC_TYPE_TO_SERVICE_TIER.get(str(traffic_type).upper())

class UsageTelemetryParser:
    """사용량 및 응답 데이터 파싱"""
    @staticmethod
    def has_hidden_params(obj: Any) -> bool:
        return hasattr(obj, "_hidden_params")

    @staticmethod
    def has_token_details(usage_block: Optional[Usage]) -> bool:
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

    @staticmethod
    def extract_usage(completion_response: Any) -> Optional[Usage]:
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
            log.debug(f"Unknown usage object type: {type(usage_obj)}, usage_obj: {usage_obj}")
            return None

    @staticmethod
    def infer_call_type(call_type: Optional[CallTypesLiteral], completion_response: Any) -> Optional[CallTypesLiteral]:
        if call_type is not None:
            return call_type
        if completion_response is None:
            return None

        if isinstance(completion_response, (ModelResponse, ModelResponseStream)):
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

    @staticmethod
    def combine_usage_objects(usage_objects: List[Usage]) -> Usage:
        from bound.surface.legacy.types import CompletionTokensDetailsWrapper, PromptTokensDetailsWrapper
        combined = Usage()

        for usage in usage_objects:
            for attr in dir(usage):
                if not attr.startswith("_") and not callable(getattr(usage, attr)):
                    current_val = getattr(combined, attr, 0)
                    new_val = getattr(usage, attr, 0)
                    if new_val is not None and isinstance(new_val, (int, float)) and isinstance(current_val, (int, float)):
                        setattr(combined, attr, current_val + new_val)
                        
            if hasattr(usage, "prompt_tokens_details") and usage.prompt_tokens_details:
                if not hasattr(combined, "prompt_tokens_details") or not combined.prompt_tokens_details:
                    combined.prompt_tokens_details = PromptTokensDetailsWrapper()

                for attr in type(usage.prompt_tokens_details).model_fields:
                    if hasattr(usage.prompt_tokens_details, attr) and not attr.startswith("_") and not callable(getattr(usage.prompt_tokens_details, attr)):
                        current_val = getattr(combined.prompt_tokens_details, attr, 0) or 0
                        new_val = getattr(usage.prompt_tokens_details, attr, 0) or 0
                        if new_val is not None and isinstance(new_val, (int, float)):
                            setattr(combined.prompt_tokens_details, attr, current_val + new_val)

            if hasattr(usage, "completion_tokens_details") and usage.completion_tokens_details:
                if not hasattr(combined, "completion_tokens_details") or not combined.completion_tokens_details:
                    combined.completion_tokens_details = CompletionTokensDetailsWrapper()

                for attr in type(usage.completion_tokens_details).model_fields:
                    if not attr.startswith("_") and not callable(getattr(usage.completion_tokens_details, attr)):
                        current_val = getattr(combined.completion_tokens_details, attr, 0) or 0
                        new_val = getattr(usage.completion_tokens_details, attr, 0) or 0
                        if isinstance(new_val, (int, float)):
                            setattr(combined.completion_tokens_details, attr, current_val + new_val)

        return combined

    @staticmethod
    def _collect_usage_from_realtime_stream_results(results: OpenAIRealtimeStreamList) -> List[Usage]:
        response_done_events: List[OpenAIRealtimeStreamResponseBaseObject] = cast(
            List[OpenAIRealtimeStreamResponseBaseObject],
            [result for result in results if result["type"] == "response.done"],
        )
        usage_objects: List[Usage] = []
        return usage_objects

    @staticmethod
    def create_logging_realtime_object(usage: Usage, results: OpenAIRealtimeStreamList) -> LiteLLMRealtimeStreamLoggingObject:
        return LiteLLMRealtimeStreamLoggingObject(usage=usage, results=results)

    @staticmethod
    def process_realtime_stream(
        results: OpenAIRealtimeStreamList,
        combined_usage_object: Usage,
        custom_llm_provider: str,
        litellm_model_name: str,
        data_residency: Optional[str] = None,
    ) -> float:
        received_model = None
        potential_model_names = []
        for result in results:
            if result["type"] == "session.created":
                received_model = cast(OpenAIRealtimeStreamSessionEvents, result)["session"].get("model", None)
                potential_model_names.append(received_model)

        potential_model_names.append(litellm_model_name)
        input_cost_per_token = 0.0
        output_cost_per_token = 0.0

        for model_name in potential_model_names:
            try:
                if model_name is None:
                    continue
                # [KEY CHANGE 2] cost.util 대신 UnitCostCalculator에 직접 요청합니다.
                _input_cost, _output_cost = UnitCostCalculator.generic_cost_per_token(
                    model=model_name,
                    usage=combined_usage_object,
                    custom_llm_provider=custom_llm_provider,
                    data_residency=data_residency,
                )
            except Exception:
                continue
            input_cost_per_token += _input_cost
            output_cost_per_token += _output_cost
            break 
            
        return input_cost_per_token + output_cost_per_token