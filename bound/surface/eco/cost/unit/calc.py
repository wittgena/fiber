# bound.surface.eco.cost.unit.calc
## @lineage: bound.surface.cost.unit.calc
## @lineage: bound.surface.cost.unit
from typing import Any, Callable, Dict, Literal, Optional, Tuple, TypedDict, cast
from bound.surface.legacy.types import (
    CacheCreationTokenDetails,
    CompletionTokensDetailsWrapper,
    ImageResponse,
    ModelInfo,
    PromptTokensDetailsWrapper,
    ServiceTier,
    Usage,
    DataResidency
)
from watcher.plane.emitter import get_emitter

log = get_emitter("cost.unit")

_VALID_DATA_RESIDENCIES = frozenset(r.value for r in DataResidency)

class CostMetricKeys:
    """비용 산정에 사용되는 모델 레지스트리의 키 문자열 테이블"""
    INPUT = "input_cost_per_token"
    OUTPUT = "output_cost_per_token"
    CACHE_CREATION = "cache_creation_input_token_cost"
    CACHE_CREATION_1HR = "cache_creation_input_token_cost_above_1hr"
    CACHE_READ = "cache_read_input_token_cost"
    
    AUDIO_INPUT = "input_cost_per_audio_token"
    AUDIO_OUTPUT = "output_cost_per_audio_token"
    IMAGE_INPUT = "input_cost_per_image_token"
    IMAGE_OUTPUT = "output_cost_per_image_token"
    REASONING_OUTPUT = "output_cost_per_reasoning_token"

    CHAR_INPUT = "input_cost_per_character"
    CHAR_OUTPUT = "output_cost_per_character"
    IMAGE_COUNT_INPUT = "input_cost_per_image"
    VIDEO_SEC_INPUT = "input_cost_per_video_per_second"

_PROMPT_DETAIL_MAPPING = {
    "cache_hit_tokens": ("cached_tokens", 0, int),
    "cache_creation_tokens": ("cache_creation_tokens", 0, int),
    "cache_creation_token_details": ("cache_creation_token_details", None, lambda x: x),
    "text_tokens": ("text_tokens", 0, int),
    "audio_tokens": ("audio_tokens", 0, int),
    "image_tokens": ("image_tokens", 0, int),
    "character_count": ("character_count", 0, int),
    "image_count": ("image_count", 0, int),
    "video_length_seconds": ("video_length_seconds", 0.0, float),
}

_COMPLETION_DETAIL_MAPPING = {
    "audio_tokens": ("audio_tokens", 0, int),
    "text_tokens": ("text_tokens", 0, int),
    "reasoning_tokens": ("reasoning_tokens", 0, int),
    "image_tokens": ("image_tokens", 0, int),
}

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

class UsageDetailParser:
    @staticmethod
    def _extract_from_mapping(details_obj: Any, mapping_table: dict) -> dict:
        """매핑 테이블을 순회하여 안전하게 데이터를 추출합니다."""
        result = {}
        for result_key, (source_key, default_val, type_caster) in mapping_table.items():
            if not details_obj:
                result[result_key] = default_val
                continue
                
            raw_val = getattr(details_obj, source_key, None)
            if raw_val is None:
                result[result_key] = default_val
            else:
                try:
                    result[result_key] = type_caster(raw_val)
                except (ValueError, TypeError):
                    result[result_key] = default_val
        return result

    @staticmethod
    def parse_prompt(usage: Usage) -> PromptTokensDetailsResult:
        result = UsageDetailParser._extract_from_mapping(usage.prompt_tokens_details, _PROMPT_DETAIL_MAPPING)
        return cast(PromptTokensDetailsResult, result)

    @staticmethod
    def parse_completion(usage: Usage) -> CompletionTokensDetailsResult:
        result = UsageDetailParser._extract_from_mapping(usage.completion_tokens_details, _COMPLETION_DETAIL_MAPPING)
        return cast(CompletionTokensDetailsResult, result)

class ModelCostRetriever:
    @staticmethod
    def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
        if value is None: return default
        if isinstance(value, (float, int)): return float(value)
        try:
            return float(value)
        except ValueError:
            log.exception(f"Failed to parse float from '{value}'. Defaulting to {default}")
            return default

    @staticmethod
    def get_tier_key(base_key: str, service_tier: Optional[str]) -> str:
        if service_tier and service_tier.lower() in (ServiceTier.FLEX.value, ServiceTier.PRIORITY.value):
            return f"{base_key}_{service_tier.lower()}"
        return base_key

    @staticmethod
    def get_cost(model_info: ModelInfo, cost_key: str, default_value: Optional[float] = 0.0) -> Optional[float]:
        if cost_key in model_info:
            return ModelCostRetriever._safe_float(model_info[cost_key], default_value)

        # Fallback to base key without service tier suffix
        for tier in ServiceTier:
            suffix = f"_{tier.value}"
            if suffix in cost_key:
                base_key = cost_key.replace(suffix, "")
                if base_key in model_info:
                    return ModelCostRetriever._safe_float(model_info[base_key], default_value)
                break
        return default_value

    @staticmethod
    def _get_applicable_threshold_string(model_info: ModelInfo, prompt_tokens: int) -> Optional[str]:
        """주어진 토큰량에 맞는 가장 높은 임계값 문자열(예: '128k')을 반환합니다."""
        threshold_keys = [k for k in model_info if k.startswith("input_cost_per_token_above_") and not any(k.endswith(f"_{st.value}") for st in ServiceTier)]
        for key in sorted(threshold_keys, reverse=True):
            try:
                threshold_str = key.split("_above_")[1].split("_tokens")[0]
                threshold_val = float(threshold_str.replace("k", "")) * (1000 if "k" in threshold_str else 1)
                if prompt_tokens > threshold_val:
                    return threshold_str
            except (IndexError, ValueError, Exception):
                continue
        return None

    @staticmethod
    def get_token_base_costs(model_info: ModelInfo, usage: Usage, service_tier: Optional[str] = None) -> Tuple[float, float, float, float, float]:
        ## 임계값(Threshold) 확인 및 키 조립
        threshold_str = ModelCostRetriever._get_applicable_threshold_string(model_info, usage.prompt_tokens)
        threshold_suffix = f"_above_{threshold_str}_tokens" if threshold_str else ""
        
        ## 동적 키 생성
        in_key = ModelCostRetriever.get_tier_key(f"{CostMetricKeys.INPUT}{threshold_suffix}", service_tier)
        out_key = ModelCostRetriever.get_tier_key(f"{CostMetricKeys.OUTPUT}{threshold_suffix}", service_tier)
        cache_key = f"{CostMetricKeys.CACHE_CREATION}{threshold_suffix}"
        cache_1hr_key = f"{CostMetricKeys.CACHE_CREATION_1HR}{threshold_suffix}"
        cache_read_key = f"{CostMetricKeys.CACHE_READ}{threshold_suffix}"

        ## 데이터 추출 (Fallback: Threshold가 적용 안 될 경우 원본 키 사용)
        prompt_cost = cast(float, ModelCostRetriever.get_cost(model_info, in_key, 
                           ModelCostRetriever.get_cost(model_info, ModelCostRetriever.get_tier_key(CostMetricKeys.INPUT, service_tier))))
        
        completion_cost = cast(float, ModelCostRetriever.get_cost(model_info, out_key, 
                              ModelCostRetriever.get_cost(model_info, ModelCostRetriever.get_tier_key(CostMetricKeys.OUTPUT, service_tier))))
        
        if completion_cost == 0.0:
            completion_cost = cast(float, ModelCostRetriever.get_cost(model_info, CostMetricKeys.IMAGE_OUTPUT))
        
        cache_creation_cost = cast(float, ModelCostRetriever.get_cost(model_info, cache_key, 
                                      ModelCostRetriever.get_cost(model_info, CostMetricKeys.CACHE_CREATION)))
        cache_creation_1hr = cast(float, ModelCostRetriever.get_cost(model_info, cache_1hr_key, 
                                     ModelCostRetriever.get_cost(model_info, CostMetricKeys.CACHE_CREATION_1HR)))
        cache_read_cost = cast(float, ModelCostRetriever.get_cost(model_info, cache_read_key, 
                                  ModelCostRetriever.get_cost(model_info, CostMetricKeys.CACHE_READ)))
        return prompt_cost, completion_cost, cache_creation_cost, cache_creation_1hr, cache_read_cost

class UnitCostCalculator:
    @staticmethod
    def calculate_cost_component(model_info: ModelInfo, cost_key: str, usage_value: Optional[float]) -> float:
        cost_per_unit = ModelCostRetriever.get_cost(model_info, cost_key)
        if cost_per_unit and usage_value and usage_value > 0:
            return float(usage_value) * cost_per_unit
        return 0.0

    @staticmethod
    def calculate_cache_writing_cost(
        cache_creation_tokens: int,
        cache_creation_token_details: Optional[CacheCreationTokenDetails],
        cache_creation_cost_above_1hr: float,
        cache_creation_cost: float,
    ) -> float:
        if cache_creation_token_details is not None:
            c_5m = cache_creation_token_details.ephemeral_5m_input_tokens or 0
            c_1h = cache_creation_token_details.ephemeral_1h_input_tokens or 0
            return (c_5m * cache_creation_cost) + (c_1h * cache_creation_cost_above_1hr)
        return cache_creation_tokens * cache_creation_cost

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
        cost = float(prompt_tokens_details["text_tokens"]) * prompt_base_cost
        cost += float(prompt_tokens_details["cache_hit_tokens"]) * cache_read_cost

        if prompt_tokens_details["audio_tokens"]:
            audio_key = ModelCostRetriever.get_tier_key(CostMetricKeys.AUDIO_INPUT, service_tier)
            cost += UnitCostCalculator.calculate_cost_component(model_info, audio_key, prompt_tokens_details["audio_tokens"])

        if prompt_tokens_details["image_tokens"]:
            img_key = CostMetricKeys.IMAGE_INPUT if model_info.get(CostMetricKeys.IMAGE_INPUT) else CostMetricKeys.INPUT
            cost += UnitCostCalculator.calculate_cost_component(model_info, img_key, prompt_tokens_details["image_tokens"])

        if prompt_tokens_details["cache_creation_tokens"] or prompt_tokens_details["cache_creation_token_details"]:
            cost += UnitCostCalculator.calculate_cache_writing_cost(
                prompt_tokens_details["cache_creation_tokens"],
                prompt_tokens_details["cache_creation_token_details"],
                cache_creation_cost_above_1hr, cache_creation_cost
            )

        if prompt_tokens_details["character_count"]:
            cost += UnitCostCalculator.calculate_cost_component(model_info, CostMetricKeys.CHAR_INPUT, prompt_tokens_details["character_count"])

        if prompt_tokens_details["image_count"]:
            cost += UnitCostCalculator.calculate_cost_component(model_info, CostMetricKeys.IMAGE_COUNT_INPUT, prompt_tokens_details["image_count"])

        if prompt_tokens_details["video_length_seconds"]:
            cost += UnitCostCalculator.calculate_cost_component(model_info, CostMetricKeys.VIDEO_SEC_INPUT, prompt_tokens_details["video_length_seconds"])

        return cost

    @staticmethod
    def generic_cost_per_character(
        model_info: ModelInfo,
        prompt_chars: float,
        completion_chars: float,
        custom_prompt_cost: Optional[float] = None,
        custom_completion_cost: Optional[float] = None,
    ) -> Tuple[Optional[float], Optional[float]]:
        
        p_cost = custom_prompt_cost or model_info.get(CostMetricKeys.CHAR_INPUT)
        c_cost = custom_completion_cost or model_info.get(CostMetricKeys.CHAR_OUTPUT)
        
        prompt_total = (prompt_chars * p_cost) if p_cost else None
        completion_total = (completion_chars * c_cost) if c_cost else None
        
        return prompt_total, completion_total

    @staticmethod
    def generic_cost_per_token(
        model_info: ModelInfo,
        usage: Usage,
        service_tier: Optional[str] = None,
        data_residency: Optional[str] = None,
    ) -> Tuple[float, float]:
        
        p_details = UsageDetailParser.parse_prompt(usage)
        
        total_details = p_details["text_tokens"] + p_details["cache_hit_tokens"] + p_details["audio_tokens"] + p_details["cache_creation_tokens"] + p_details["image_tokens"]
        has_double_counting = p_details["cache_hit_tokens"] > 0 and total_details > usage.prompt_tokens

        if (p_details["text_tokens"] == 0 and p_details["image_count"] == 0) or has_double_counting:
            calculated_text = usage.prompt_tokens - p_details["cache_hit_tokens"] - p_details["audio_tokens"] - p_details["cache_creation_tokens"] - p_details["image_tokens"]
            p_details["text_tokens"] = max(0, calculated_text)

        base_costs = ModelCostRetriever.get_token_base_costs(model_info, usage, service_tier)
        prompt_cost = UnitCostCalculator.calculate_input_cost(p_details, model_info, *base_costs, service_tier)

        c_details = UsageDetailParser.parse_completion(usage)
        text_tokens = c_details["text_tokens"]
        
        has_breakdown = c_details["image_tokens"] > 0 or c_details["audio_tokens"] > 0 or c_details["reasoning_tokens"] > 0
        is_total = False
        
        if text_tokens == 0:
            if has_breakdown:
                text_tokens = max(0, usage.completion_tokens - c_details["reasoning_tokens"] - c_details["audio_tokens"] - c_details["image_tokens"])
            else:
                text_tokens = usage.completion_tokens
                is_total = True

        completion_cost = float(text_tokens) * base_costs[1]

        if not is_total:
            if c_details["audio_tokens"] > 0:
                completion_cost += float(c_details["audio_tokens"]) * ModelCostRetriever.get_cost(model_info, CostMetricKeys.AUDIO_OUTPUT, base_costs[1])
            if c_details["reasoning_tokens"] > 0:
                completion_cost += float(c_details["reasoning_tokens"]) * ModelCostRetriever.get_cost(model_info, CostMetricKeys.REASONING_OUTPUT, base_costs[1])
            if c_details["image_tokens"] > 0:
                completion_cost += float(c_details["image_tokens"]) * ModelCostRetriever.get_cost(model_info, CostMetricKeys.IMAGE_OUTPUT, base_costs[1])

        if data_residency and data_residency.lower() in _VALID_DATA_RESIDENCIES:
            uplift = ModelCostRetriever.get_cost(model_info, f"regional_processing_uplift_multiplier_{data_residency.lower()}", 1.0)
            if uplift and uplift != 1.0:
                prompt_cost *= uplift
                completion_cost *= uplift

        return prompt_cost, completion_cost

    @staticmethod
    def default_image_cost_calculator(
        model_info: ModelInfo,
        quality: Optional[str] = None,
        n: Optional[int] = None,
        size: Optional[str] = None,
        optional_params: Optional[dict] = None,
    ) -> float:
        image_count = float(n) if n is not None else 1.0
        keys_to_check = []
        
        if size and quality:
            keys_to_check.extend([f"cost_per_image_{size}_{quality}", f"output_cost_per_image_{size}_{quality}"])
        if size:
            keys_to_check.extend([f"cost_per_image_{size}", f"output_cost_per_image_{size}"])
        keys_to_check.extend(["cost_per_image", "output_cost_per_image"])

        cost_per_image = 0.0
        for key in keys_to_check:
            extracted = ModelCostRetriever.get_cost(model_info, key, None)
            if extracted and extracted > 0.0:
                cost_per_image = extracted
                break

        base_cost = cost_per_image * image_count
        
        data_residency = (optional_params or {}).get("data_residency")
        if data_residency and data_residency.lower() in _VALID_DATA_RESIDENCIES:
            uplift = ModelCostRetriever.get_cost(model_info, f"regional_processing_uplift_multiplier_{data_residency.lower()}", 1.0)
            if uplift and uplift != 1.0:
                base_cost *= uplift
                
        return base_cost