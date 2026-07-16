# bound.proxy.eco.cost.policy
## @lineage: bound.surface.eco.cost.policy
## @lineage: bound.surface.cost.policy
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, Literal, Optional, Tuple, Union, cast
from pydantic import BaseModel
from anchor.registry.model.config.resolver import config as global_config
from anchor.registry.model.config.constants import DEFAULT_REPLICATE_GPU_PRICE_PER_SECOND
from bound.surface.legacy.types import CostPerToken
from watcher.plane.emitter import get_emitter

log = get_emitter("cost.policy")


class CostModifier(BaseModel):
    """비용 변동 내역을 추적하기 위한 데이터 모델"""
    name: str                   # 예: 'discount', 'margin', 'additional_cost'
    percentage: float = 0.0     # 적용된 비율 (0.0 ~ 1.0)
    fixed_amount: float = 0.0   # 고정 추가 금액
    total_amount: float = 0.0   # 최종 변동된 금액 (+는 증가, -는 감소)


class CostPolicyBreakdown(BaseModel):
    """파이프라인을 거친 최종 비용 명세서"""
    original_cost: float
    final_cost: float
    modifiers: list[CostModifier] = []


class CostPolicy:
    """
    DI(의존성 주입)와 파이프라인 패턴이 적용된 개선된 비용 단가 정책 클래스.
    """
    
    def __init__(self, config_instance: Any = None):
        # 전역 config에 직접 의존하지 않고 주입받아 사용 (테스트 용이성 확보)
        self.config = config_instance or global_config

    def calculate_custom_pricing(
        self,
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

            cache_read_input_token_cost = custom_cost_per_token.get("cache_read_input_token_cost", input_cost_per_token)
            cache_creation_input_token_cost = custom_cost_per_token.get("cache_creation_input_token_cost", input_cost_per_token)

            regular_prompt_tokens = max(prompt_tokens - cached_tokens - cache_creation_tokens, 0)

            input_cost = (
                regular_prompt_tokens * input_cost_per_token
                + cached_tokens * cache_read_input_token_cost
                + cache_creation_tokens * cache_creation_input_token_cost
            )
            output_cost = completion_tokens * output_cost_per_token
            return input_cost, output_cost
            
        elif custom_cost_per_second is not None:
            output_cost = custom_cost_per_second * (response_time_ms or 0) / 1000
            return 0, output_cost

        return None

    def apply_cost_modifiers(
        self, 
        base_cost: float, 
        custom_llm_provider: Optional[str]
    ) -> CostPolicyBreakdown:
        """할인과 마진 등 등록된 모든 정책을 순차적으로 적용하고 상세 내역을 반환합니다."""
        current_cost = base_cost
        modifiers = []

        # 단계 1: 할인 정책 적용
        discount_config = getattr(self.config, "cost_discount_config", {})
        if custom_llm_provider and custom_llm_provider in discount_config:
            discount_pct = discount_config[custom_llm_provider]
            discount_amt = current_cost * discount_pct
            current_cost -= discount_amt
            
            modifiers.append(CostModifier(
                name=f"discount ({custom_llm_provider})", 
                percentage=discount_pct, 
                total_amount=-discount_amt
            ))
            log.debug(f"Applied {discount_pct*100}% discount: ${base_cost:.6f} -> ${current_cost:.6f}")

        # 단계 2: 마진 정책 적용
        margin_config = getattr(self.config, "cost_margin_config", {})
        provider_margin = margin_config.get(custom_llm_provider) if custom_llm_provider else None
        active_margin_config = provider_margin or margin_config.get("global")

        if active_margin_config is not None:
            margin_pct, margin_fixed = 0.0, 0.0
            
            if isinstance(active_margin_config, (int, float)):
                margin_pct = float(active_margin_config)
            elif isinstance(active_margin_config, dict):
                margin_pct = float(active_margin_config.get("percentage", 0.0))
                margin_fixed = float(active_margin_config.get("fixed_amount", 0.0))

            margin_amt = (current_cost * margin_pct) + margin_fixed
            current_cost += margin_amt
            
            modifiers.append(CostModifier(
                name="margin", 
                percentage=margin_pct, 
                fixed_amount=margin_fixed,
                total_amount=margin_amt
            ))
            log.debug(f"Applied margin: +${margin_amt:.6f} -> Final: ${current_cost:.6f}")

        return CostPolicyBreakdown(
            original_cost=base_cost,
            final_cost=current_cost,
            modifiers=modifiers
        )

    def get_additional_costs(
        self,
        model: str,
        custom_llm_provider: Optional[str],
        prompt_tokens: int,
        completion_tokens: int,
    ) -> Optional[dict]:
        if not custom_llm_provider:
            return None
        try:
            # 설정 객체에 주입된 추가 비용 리졸버를 동적으로 조회
            additional_cost_resolver = getattr(self.config, "additional_cost_resolver", None)
            
            if additional_cost_resolver and hasattr(additional_cost_resolver, "calculate_additional_costs"):
                return additional_cost_resolver.calculate_additional_costs(
                    model=model,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                )
        except Exception as e:
            log.debug(f"Error calculating additional costs: {e}")
        return None

    def get_provider_specific_pricing(self, provider: str, model: str, completion_response: dict, total_time_ms: float) -> Optional[float]:
        """특정 프로바이더에 종속적인 과금 방식(예: Replicate 초당 과금)을 매핑합니다."""
        # TODO: 프로바이더별 Strategy 클래스로 분리 가능
        if provider == "replicate" or "replicate" in model:
            return self._calculate_replicate_pricing(completion_response, total_time_ms)
        
        return None

    def _calculate_replicate_pricing(self, completion_response: dict, total_time: float) -> float:
        price_per_sec = getattr(self.config, "replicate_gpu_price_per_second", DEFAULT_REPLICATE_GPU_PRICE_PER_SECOND)
        if total_time == 0.0:
            start_time = completion_response.get("created", time.time())
            end_time = getattr(completion_response, "ended", time.time())
            total_time = end_time - start_time
        return price_per_sec * total_time / 1000

    @staticmethod
    def extract_cost_from_headers(hidden_params: Union[dict, BaseModel]) -> Optional[float]:
        _hidden_params_dict = hidden_params.model_dump() if isinstance(hidden_params, BaseModel) else hidden_params
        
        additional_headers = _hidden_params_dict.get("additional_headers", {})
        response_cost = additional_headers.get("llm_provider-x-litellm-response-cost")
        
        return float(response_cost) if response_cost is not None else None