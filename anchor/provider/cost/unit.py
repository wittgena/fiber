# anchor.registry.provider.cost.unit
from typing import Literal, Optional, Tuple, TypedDict, cast, Any, Union
from bound.surface.legacy.types import (
    TranscriptionUsageDurationObject,
    TranscriptionUsageTokensObject,
    CacheCreationTokenDetails,
    CallTypes,
    CompletionTokensDetailsWrapper,
    ImageResponse,
    ModelInfo,
    PassthroughCallTypes,
    PromptTokensDetailsWrapper,
    ServiceTier,
    Usage,
    DataResidency
)
from bound.surface.model.info import get_model_info
from watcher.plane.emitter import get_emitter

log = get_emitter("cost.unit")

_VALID_DATA_RESIDENCIES = frozenset(r.value for r in DataResidency)
_IMAGE_RESPONSE_CALL_TYPES = frozenset(
    {
        CallTypes.image_generation.value,
        CallTypes.aimage_generation.value,
        PassthroughCallTypes.passthrough_image_generation.value,
        CallTypes.image_edit.value,
        CallTypes.aimage_edit.value,
    }
)

# =============================================================================
# TypedDict Definitions
# =============================================================================
class PromptTokensDetailsResult(TypedDict):
    cache_hit_tokens: int
    cache_creation_tokens: int
    cache_creation_token_details: Optional[CacheCreationTokenDetails]
    text_tokens: int
    audio_tokens: int
    image_tokens: int
    character_count: int
    image_count: int
    video_length_seconds: float

class CompletionTokensDetailsResult(TypedDict):
    audio_tokens: int
    text_tokens: int
    reasoning_tokens: int
    image_tokens: int


# =============================================================================
# 1. UnitValueRetriever: 데이터 조회, 추출, 파싱을 전담하는 클래스
# =============================================================================
class UnitValueRetriever:
    @staticmethod
    def get_token_detail_value(details: object, key: str) -> Optional[int]:
        if isinstance(details, dict):
            value = details.get(key)
        else:
            value = getattr(details, key, None)
        return value if isinstance(value, int) else None

    @staticmethod
    def get_service_tier_cost_key(base_key: str, service_tier: Optional[str]) -> str:
        if service_tier is None:
            return base_key
        if service_tier.lower() in [ServiceTier.FLEX.value, ServiceTier.PRIORITY.value]:
            return f"{base_key}_{service_tier.lower()}"
        return base_key

    @staticmethod
    def get_cost_per_unit(model_info: ModelInfo, cost_key: str, default_value: Optional[float] = 0.0) -> Optional[float]:
        cost_per_unit = model_info.get(cost_key)
        if isinstance(cost_per_unit, float):
            return cost_per_unit
        if isinstance(cost_per_unit, int):
            return float(cost_per_unit)
        if isinstance(cost_per_unit, str):
            try:
                return float(cost_per_unit)
            except ValueError:
                log.exception(f"Exception occured - {cost_per_unit}\nDefaulting to 0.0")

        if cost_per_unit is None:
            for service_tier in ServiceTier:
                suffix = f"_{service_tier.value}"
                if suffix in cost_key:
                    base_key = cost_key.replace(suffix, "")
                    fallback_cost = model_info.get(base_key)
                    if isinstance(fallback_cost, float): return fallback_cost
                    if isinstance(fallback_cost, int): return float(fallback_cost)
                    if isinstance(fallback_cost, str):
                        try:
                            return float(fallback_cost)
                        except ValueError:
                            log.exception(f"Exception occured - {fallback_cost}\nDefaulting to 0.0")
                    break
        return default_value

    @staticmethod
    def get_token_base_cost(model_info: ModelInfo, usage: Usage, service_tier: Optional[str] = None) -> Tuple[float, float, float, float, float]:
        input_cost_key = UnitValueRetriever.get_service_tier_cost_key("input_cost_per_token", service_tier)
        output_cost_key = UnitValueRetriever.get_service_tier_cost_key("output_cost_per_token", service_tier)
        cache_creation_cost_key = UnitValueRetriever.get_service_tier_cost_key("cache_creation_input_token_cost", service_tier)
        cache_read_cost_key = UnitValueRetriever.get_service_tier_cost_key("cache_read_input_token_cost", service_tier)
        
        prompt_base_cost = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, input_cost_key))
        completion_base_cost = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, output_cost_key))
        
        if completion_base_cost == 0.0 or completion_base_cost is None:
            output_image_cost = UnitValueRetriever.get_cost_per_unit(model_info, "output_cost_per_image_token", None)
            if output_image_cost is not None:
                completion_base_cost = cast(float, output_image_cost)
                
        cache_creation_cost = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, cache_creation_cost_key))
        cache_creation_cost_above_1hr = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, "cache_creation_input_token_cost_above_1hr"))
        cache_read_cost = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, cache_read_cost_key))
        
        threshold_keys = [k for k in model_info if k.startswith("input_cost_per_token_above_") and not any(k.endswith(f"_{st.value}") for st in ServiceTier)]
        
        if not threshold_keys:
            return prompt_base_cost, completion_base_cost, cache_creation_cost, cache_creation_cost_above_1hr, cache_read_cost

        for key in sorted(threshold_keys, reverse=True):
            value = model_info.get(key)
            if value is not None:
                try:
                    threshold_str = key.split("_above_")[1].split("_tokens")[0]
                    threshold = float(threshold_str.replace("k", "")) * (1000 if "k" in threshold_str else 1)
                    if usage.prompt_tokens > threshold:
                        tiered_input_key = UnitValueRetriever.get_service_tier_cost_key(f"input_cost_per_token_above_{threshold_str}_tokens", service_tier) if service_tier else key
                        prompt_base_cost = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, tiered_input_key, prompt_base_cost))
                        
                        tiered_output_key = UnitValueRetriever.get_service_tier_cost_key(f"output_cost_per_token_above_{threshold_str}_tokens", service_tier) if service_tier else f"output_cost_per_token_above_{threshold_str}_tokens"
                        completion_base_cost = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, tiered_output_key, completion_base_cost))

                        cache_creation_tiered_key = f"cache_creation_input_token_cost_above_{threshold_str}_tokens"
                        cache_creation_1hr_tiered_key = f"cache_creation_input_token_cost_above_1hr_above_{threshold_str}_tokens"
                        cache_read_tiered_key = f"cache_read_input_token_cost_above_{threshold_str}_tokens"

                        if cache_creation_tiered_key in model_info:
                            cache_creation_cost = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, cache_creation_tiered_key, cache_creation_cost))
                        if cache_creation_1hr_tiered_key in model_info:
                            cache_creation_cost_above_1hr = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, cache_creation_1hr_tiered_key, cache_creation_cost_above_1hr))
                        if cache_read_tiered_key in model_info:
                            cache_read_cost = cast(float, UnitValueRetriever.get_cost_per_unit(model_info, cache_read_tiered_key, cache_read_cost))
                    break
                except (IndexError, ValueError):
                    continue
                except Exception:
                    continue

        return prompt_base_cost, completion_base_cost, cache_creation_cost, cache_creation_cost_above_1hr, cache_read_cost

    @staticmethod
    def get_regional_uplift_multiplier(model_info: ModelInfo, data_residency: Optional[str]) -> float:
        if data_residency is None:
            return 1.0
        residency = data_residency.lower()
        if residency not in _VALID_DATA_RESIDENCIES:
            return 1.0
        multiplier = model_info.get(f"regional_processing_uplift_multiplier_{residency}")
        if multiplier is None:
            return 1.0
        try:
            return float(cast(float, multiplier))
        except (TypeError, ValueError):
            log.exception(f"Invalid regional_processing_uplift_multiplier_{residency} for model; defaulting to 1.0")
            return 1.0

    @staticmethod
    def parse_prompt_tokens_details(usage: Usage) -> PromptTokensDetailsResult:
        cache_hit_tokens = cast(Optional[int], getattr(usage.prompt_tokens_details, "cached_tokens", 0)) or 0
        cache_creation_tokens = cast(Optional[int], getattr(usage.prompt_tokens_details, "cache_creation_tokens", 0)) or 0
        cache_creation_token_details = cast(Optional[CacheCreationTokenDetails], getattr(usage.prompt_tokens_details, "cache_creation_token_details", None)) or None
        text_tokens = cast(Optional[int], getattr(usage.prompt_tokens_details, "text_tokens", None)) or 0
        audio_tokens = cast(Optional[int], getattr(usage.prompt_tokens_details, "audio_tokens", 0)) or 0
        image_tokens = cast(Optional[int], getattr(usage.prompt_tokens_details, "image_tokens", 0)) or 0
        character_count = cast(Optional[int], getattr(usage.prompt_tokens_details, "character_count", 0)) or 0
        image_count = cast(Optional[int], getattr(usage.prompt_tokens_details, "image_count", 0)) or 0
        video_length_seconds = cast(Optional[float], getattr(usage.prompt_tokens_details, "video_length_seconds", 0)) or 0.0

        return PromptTokensDetailsResult(
            cache_hit_tokens=cache_hit_tokens,
            cache_creation_tokens=cache_creation_tokens,
            cache_creation_token_details=cache_creation_token_details,
            text_tokens=text_tokens,
            audio_tokens=audio_tokens,
            image_tokens=image_tokens,
            character_count=character_count,
            image_count=image_count,
            video_length_seconds=float(video_length_seconds),
        )

    @staticmethod
    def parse_completion_tokens_details(usage: Usage) -> CompletionTokensDetailsResult:
        audio_tokens = cast(Optional[int], getattr(usage.completion_tokens_details, "audio_tokens", 0)) or 0
        text_tokens = cast(Optional[int], getattr(usage.completion_tokens_details, "text_tokens", None)) or 0
        reasoning_tokens = cast(Optional[int], getattr(usage.completion_tokens_details, "reasoning_tokens", 0)) or 0
        image_tokens = cast(Optional[int], getattr(usage.completion_tokens_details, "image_tokens", 0)) or 0

        return CompletionTokensDetailsResult(
            audio_tokens=audio_tokens,
            text_tokens=text_tokens,
            reasoning_tokens=reasoning_tokens,
            image_tokens=image_tokens,
        )

    @staticmethod
    def get_billable_input_tokens(usage: Usage) -> int:
        details = UnitValueRetriever.parse_prompt_tokens_details(usage)
        return usage.prompt_tokens - details["cache_hit_tokens"]

    @staticmethod
    def select_cost_metric_for_model(model_info: ModelInfo) -> Literal["cost_per_character", "cost_per_token"]:
        if model_info.get("input_cost_per_character"):
            return "cost_per_character"
        elif model_info.get("input_cost_per_token"):
            return "cost_per_token"
        else:
            raise ValueError(f"Model {model_info['key']} does not have 'input_cost_per_character' or 'input_cost_per_token'")


# =============================================================================
# 2. UnitCostCalculator: 비용 곱셈, 합산 및 최종 연산을 전담하는 클래스
# =============================================================================
class UnitCostCalculator:
    @staticmethod
    def calculate_cost_component(model_info: ModelInfo, cost_key: str, usage_value: Optional[float]) -> float:
        cost_per_unit = UnitValueRetriever.get_cost_per_unit(model_info, cost_key)
        if cost_per_unit is not None and isinstance(cost_per_unit, float) and usage_value is not None and usage_value > 0:
            return float(usage_value) * cost_per_unit
        return 0.0

    @staticmethod
    def calculate_cache_writing_cost(
        cache_creation_tokens: int,
        cache_creation_token_details: Optional[CacheCreationTokenDetails],
        cache_creation_cost_above_1hr: float,
        cache_creation_cost: float,
    ) -> float:
        total_cost: float = 0.0
        if cache_creation_token_details is not None:
            cache_creation_tokens_5m = cache_creation_token_details.ephemeral_5m_input_tokens
            cache_creation_tokens_1h = cache_creation_token_details.ephemeral_1h_input_tokens
            total_cost += (cache_creation_tokens_5m * cache_creation_cost if cache_creation_tokens_5m is not None else 0.0)
            total_cost += (cache_creation_tokens_1h * cache_creation_cost_above_1hr if cache_creation_tokens_1h is not None else 0.0)
        else:
            total_cost += cache_creation_tokens * cache_creation_cost
        return total_cost

    @staticmethod
    def calculate_input_cost(
        prompt_tokens_details: PromptTokensDetailsResult,
        model_info: ModelInfo,
        prompt_base_cost: float,
        cache_read_cost: float,
        cache_creation_cost: float,
        cache_creation_cost_above_1hr: float,
        service_tier: Optional[str] = None,
    ) -> float:
        prompt_cost = float(prompt_tokens_details["text_tokens"]) * prompt_base_cost
        prompt_cost += float(prompt_tokens_details["cache_hit_tokens"]) * cache_read_cost

        if prompt_tokens_details["audio_tokens"]:
            audio_cost_key = UnitValueRetriever.get_service_tier_cost_key("input_cost_per_audio_token", service_tier)
            prompt_cost += UnitCostCalculator.calculate_cost_component(model_info, audio_cost_key, prompt_tokens_details["audio_tokens"])

        if prompt_tokens_details["image_tokens"]:
            image_token_cost_key = "input_cost_per_image_token"
            if model_info.get(image_token_cost_key) is None:
                image_token_cost_key = "input_cost_per_token"
            prompt_cost += UnitCostCalculator.calculate_cost_component(model_info, image_token_cost_key, prompt_tokens_details["image_tokens"])

        if prompt_tokens_details["cache_creation_tokens"] or prompt_tokens_details["cache_creation_token_details"] is not None:
            prompt_cost += UnitCostCalculator.calculate_cache_writing_cost(
                cache_creation_tokens=prompt_tokens_details["cache_creation_tokens"],
                cache_creation_token_details=prompt_tokens_details["cache_creation_token_details"],
                cache_creation_cost_above_1hr=cache_creation_cost_above_1hr,
                cache_creation_cost=cache_creation_cost,
            )

        if prompt_tokens_details["character_count"]:
            prompt_cost += UnitCostCalculator.calculate_cost_component(model_info, "input_cost_per_character", prompt_tokens_details["character_count"])

        if prompt_tokens_details["image_count"]:
            prompt_cost += UnitCostCalculator.calculate_cost_component(model_info, "input_cost_per_image", prompt_tokens_details["image_count"])

        if prompt_tokens_details["video_length_seconds"]:
            prompt_cost += UnitCostCalculator.calculate_cost_component(model_info, "input_cost_per_video_per_second", prompt_tokens_details["video_length_seconds"])

        return prompt_cost

    @staticmethod
    def generic_cost_per_character(
        model: str,
        custom_llm_provider: str,
        prompt_characters: float,
        completion_characters: float,
        custom_prompt_cost: Optional[float],
        custom_completion_cost: Optional[float],
    ) -> Tuple[Optional[float], Optional[float]]:
        model_info = get_model_info(model=model, custom_llm_provider=custom_llm_provider)
        try:
            if custom_prompt_cost is None:
                assert "input_cost_per_character" in model_info and model_info["input_cost_per_character"] is not None
                custom_prompt_cost = model_info["input_cost_per_character"]
            prompt_cost = prompt_characters * custom_prompt_cost
        except Exception as e:
            log.exception(f"cost_per_character() prompt error: {str(e)}")
            prompt_cost = None

        try:
            if custom_completion_cost is None:
                assert "output_cost_per_character" in model_info and model_info["output_cost_per_character"] is not None
                custom_completion_cost = model_info["output_cost_per_character"]
            completion_cost = completion_characters * custom_completion_cost
        except Exception as e:
            log.exception(f"cost_per_character() completion error: {str(e)}")
            completion_cost = None

        return prompt_cost, completion_cost

    @staticmethod
    def generic_cost_per_token(
        model: str,
        usage: Usage,
        custom_llm_provider: str,
        service_tier: Optional[str] = None,
        data_residency: Optional[str] = None,
    ) -> Tuple[float, float]:
        model_info = get_model_info(model=model, custom_llm_provider=custom_llm_provider)
        prompt_tokens_details = PromptTokensDetailsResult(
            cache_hit_tokens=0, cache_creation_tokens=0, cache_creation_token_details=None,
            text_tokens=usage.prompt_tokens, audio_tokens=0, image_tokens=0,
            character_count=0, image_count=0, video_length_seconds=0.0,
        )
        if usage.prompt_tokens_details:
            prompt_tokens_details = UnitValueRetriever.parse_prompt_tokens_details(usage)

        cache_hit = prompt_tokens_details["cache_hit_tokens"]
        text_tokens = prompt_tokens_details["text_tokens"]
        audio_tokens = prompt_tokens_details["audio_tokens"]
        cache_creation = prompt_tokens_details["cache_creation_tokens"]
        image_tokens = prompt_tokens_details["image_tokens"]
        
        total_details = text_tokens + cache_hit + audio_tokens + cache_creation + image_tokens
        has_double_counting = cache_hit > 0 and total_details > usage.prompt_tokens

        if (text_tokens == 0 and prompt_tokens_details["image_count"] == 0) or has_double_counting:
            text_tokens = usage.prompt_tokens - cache_hit - audio_tokens - cache_creation - image_tokens
            if text_tokens < 0: text_tokens = 0
            prompt_tokens_details["text_tokens"] = text_tokens

        (
            prompt_base_cost, completion_base_cost,
            cache_creation_cost, cache_creation_cost_above_1hr, cache_read_cost,
        ) = UnitValueRetriever.get_token_base_cost(model_info=model_info, usage=usage, service_tier=service_tier)
        
        prompt_cost = UnitCostCalculator.calculate_input_cost(
            prompt_tokens_details=prompt_tokens_details,
            model_info=model_info, prompt_base_cost=prompt_base_cost,
            cache_read_cost=cache_read_cost, cache_creation_cost=cache_creation_cost,
            cache_creation_cost_above_1hr=cache_creation_cost_above_1hr, service_tier=service_tier,
        )

        text_tokens = audio_tokens = reasoning_tokens = image_tokens = 0
        is_text_tokens_total = False
        if usage.completion_tokens_details is not None:
            completion_tokens_details = UnitValueRetriever.parse_completion_tokens_details(usage)
            audio_tokens = completion_tokens_details["audio_tokens"]
            text_tokens = completion_tokens_details["text_tokens"]
            reasoning_tokens = completion_tokens_details["reasoning_tokens"]
            image_tokens = completion_tokens_details["image_tokens"]

        has_token_breakdown = image_tokens > 0 or audio_tokens > 0 or reasoning_tokens > 0
        if text_tokens == 0:
            if has_token_breakdown:
                text_tokens = max(0, usage.completion_tokens - reasoning_tokens - audio_tokens - image_tokens)
            else:
                text_tokens = usage.completion_tokens
                is_text_tokens_total = True

        completion_cost = float(text_tokens) * completion_base_cost

        if not is_text_tokens_total and audio_tokens > 0:
            _out_audio_token = UnitValueRetriever.get_cost_per_unit(model_info, "output_cost_per_audio_token", completion_base_cost)
            completion_cost += float(audio_tokens) * _out_audio_token

        if not is_text_tokens_total and reasoning_tokens > 0:
            _out_reasoning_token = UnitValueRetriever.get_cost_per_unit(model_info, "output_cost_per_reasoning_token", completion_base_cost)
            completion_cost += float(reasoning_tokens) * _out_reasoning_token

        if not is_text_tokens_total and image_tokens > 0:
            _out_img_token = UnitValueRetriever.get_cost_per_unit(model_info, "output_cost_per_image_token", completion_base_cost)
            completion_cost += float(image_tokens) * _out_img_token

        uplift = UnitValueRetriever.get_regional_uplift_multiplier(model_info, data_residency)
        if uplift != 1.0:
            prompt_cost *= uplift
            completion_cost *= uplift

        return prompt_cost, completion_cost

    @staticmethod
    def default_image_cost_calculator(
        model: str,
        quality: Optional[str] = None,
        custom_llm_provider: Optional[str] = None,
        n: Optional[int] = None,
        size: Optional[str] = None,
        optional_params: Optional[dict] = None,
    ) -> float:
        try:
            model_info = get_model_info(model=model, custom_llm_provider=custom_llm_provider)
            image_count = float(n) if n is not None else 1.0
            potential_cost_keys = []
            
            if size and quality:
                potential_cost_keys.append(f"cost_per_image_{size}_{quality}")
                potential_cost_keys.append(f"output_cost_per_image_{size}_{quality}")
            if size:
                potential_cost_keys.append(f"cost_per_image_{size}")
                potential_cost_keys.append(f"output_cost_per_image_{size}")
            potential_cost_keys.extend(["cost_per_image", "output_cost_per_image"])

            cost_per_image = 0.0
            for key in potential_cost_keys:
                extracted_cost = UnitValueRetriever.get_cost_per_unit(model_info, key, None)
                if extracted_cost is not None and extracted_cost > 0.0:
                    cost_per_image = extracted_cost
                    break
                    
            if cost_per_image == 0.0:
                log.debug(f"[{model}] 이미지 생성 단가를 찾을 수 없습니다. (탐색 키: {potential_cost_keys})")

            base_cost = cost_per_image * image_count
            optional_params = optional_params or {}
            data_residency = optional_params.get("data_residency")
            
            if data_residency:
                uplift = UnitValueRetriever.get_regional_uplift_multiplier(model_info, data_residency)
                if uplift != 1.0:
                    base_cost *= uplift
            return base_cost
        except Exception as e:
            log.exception(f"default_image_cost_calculator 오류: {str(e)}")
            return 0.0

    @staticmethod
    def route_image_generation_cost_calculator(
        model: str,
        completion_response: ImageResponse,
        custom_llm_provider: Optional[str] = None,
        quality: Optional[str] = None,
        n: Optional[int] = None,
        size: Optional[str] = None,
        optional_params: Optional[dict] = None,
        call_type: Optional[str] = None,
    ) -> float:
        if call_type not in _IMAGE_RESPONSE_CALL_TYPES:
            return 0.0
        return UnitCostCalculator.default_image_cost_calculator(
            model=model, quality=quality, custom_llm_provider=custom_llm_provider,
            n=n, size=size, optional_params=optional_params,
        )

    @staticmethod
    def calculate_image_response_cost_from_usage(
        model: str,
        image_response: ImageResponse,
        custom_llm_provider: str,
    ) -> Optional[float]:
        usage = image_response.usage
        if usage is None: return None
        
        prompt_tokens = usage.input_tokens
        completion_tokens = usage.output_tokens
        total_tokens = usage.total_tokens

        if None in (prompt_tokens, completion_tokens, total_tokens) or (prompt_tokens == 0 and completion_tokens == 0 and total_tokens == 0):
            return None

        input_tokens_details = getattr(usage, "input_tokens_details", None)
        prompt_tokens_details: Optional[PromptTokensDetailsWrapper] = None
        if input_tokens_details is not None:
            prompt_tokens_details = PromptTokensDetailsWrapper(
                text_tokens=getattr(input_tokens_details, "text_tokens", None),
                image_tokens=getattr(input_tokens_details, "image_tokens", None),
                cached_tokens=0,
            )

        output_tokens_details = getattr(usage, "completion_tokens_details", None) or getattr(usage, "output_tokens_details", None)

        if output_tokens_details is None:
            completion_tokens_details = CompletionTokensDetailsWrapper(text_tokens=0, image_tokens=completion_tokens, reasoning_tokens=0, audio_tokens=0)
        else:
            text_tokens = UnitValueRetriever.get_token_detail_value(output_tokens_details, "text_tokens") or 0
            image_tokens = UnitValueRetriever.get_token_detail_value(output_tokens_details, "image_tokens") or 0
            audio_tokens = UnitValueRetriever.get_token_detail_value(output_tokens_details, "audio_tokens") or 0
            reasoning_tokens = UnitValueRetriever.get_token_detail_value(output_tokens_details, "reasoning_tokens") or 0
            
            known_output_tokens = text_tokens + image_tokens + audio_tokens + reasoning_tokens
            if completion_tokens > known_output_tokens:
                text_tokens += completion_tokens - known_output_tokens

            completion_tokens_details = CompletionTokensDetailsWrapper(
                text_tokens=text_tokens, image_tokens=image_tokens, reasoning_tokens=reasoning_tokens, audio_tokens=audio_tokens,
            )

        normalized_usage = Usage(
            prompt_tokens=prompt_tokens, completion_tokens=completion_tokens, total_tokens=total_tokens,
            prompt_tokens_details=prompt_tokens_details, completion_tokens_details=completion_tokens_details,
        )

        prompt_cost, completion_cost = UnitCostCalculator.generic_cost_per_token(
            model=model, usage=normalized_usage, custom_llm_provider=custom_llm_provider,
        )
        return prompt_cost + completion_cost


# =============================================================================
# 3. UsageTransform: 사용량 페이로드 객체 변환 클래스
# =============================================================================
class UsageTransform:
    @staticmethod
    def is_transcription_usage_object(usage_object: Any) -> bool:
        return isinstance(usage_object, TranscriptionUsageDurationObject) or isinstance(usage_object, TranscriptionUsageTokensObject)

    @staticmethod
    def transform_transcription_usage_object(
        usage_object: Union[TranscriptionUsageDurationObject, TranscriptionUsageTokensObject],
    ) -> Optional[Usage]:
        if isinstance(usage_object, TranscriptionUsageDurationObject):
            return None
        elif isinstance(usage_object, TranscriptionUsageTokensObject):
            return Usage(
                prompt_tokens=usage_object.input_tokens,
                completion_tokens=usage_object.output_tokens,
                total_tokens=usage_object.total_tokens,
                prompt_tokens_details=PromptTokensDetailsWrapper(
                    text_tokens=usage_object.input_token_details.text_tokens,
                    audio_tokens=usage_object.input_token_details.audio_tokens,
                ),
            )
        return None