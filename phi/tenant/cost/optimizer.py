# phi.tenant.cost.optimizer
## @lineage: tenant.cost.optimizer
## @lineage: eco.tenant.cost.optimizer
## @lineage: bound.gateway.cost.optimizer
## @lineage: gateway.cost.optimizer
## @lineage: bound.eco.optimizer
## @lineage: bound.proxy.eco.optimizer
## @lineage: bound.surface.eco.optimizer
## @lineage: bound.surface.cost.eco.optimizer
# bound.surface.cost.eco.optimizer (Completed Bridgehead)
"""
@desc: Universal Unit Economics & Token Cost Tracker for Heterogeneous LLM Services.
@role: Protocol-agnostic pricing model & Legacy Cost Absorber.
"""
from typing import Dict, Any, List
from abc import ABC, abstractmethod
from watcher.plane.emitter import get_emitter

log = get_emitter("surface.cost.optimizer")

class BaseCostOptimizer(ABC):
    @abstractmethod
    def calculate_efficiency(self, raw_input: Any, structural_input: Any) -> float:
        pass

class UniversalAIEconomics:
    """
    @role: Pluggable Financial Control Plane.
    @desc: 새로운 자체 과금 모델을 처리함과 동시에, 기존 LiteLLM 레거시에서 
           계산된 비용(Legacy Cost)을 흡수하여 단일 통합 재무 리포트를 발행합니다.
    """
    def __init__(self, 
                 pricing_registry: Dict[str, Any], 
                 optimizer: BaseCostOptimizer = None):
        self.registry = pricing_registry
        self.optimizer = optimizer
        
        self.metrics = {
            "total_calls": 0,
            "accumulated_input_tokens": 0,
            "accumulated_output_tokens": 0,
            "accumulated_revenue": 0.0,
            "legacy_llm_cost_absorbed": 0.0,
            "dynamic_infra_costs": {}
        }

    def inject_legacy_cost(self, final_cost_from_litellm: float):
        """
        @step: Legacy Absorption (교두보의 핵심)
        @desc: 기존 bound.surface.cost.calculator에서 복잡하게 계산된 
               최종 달러($) 비용을 던져주면, 여기서 단순 '원가' 중 하나로 합산합니다.
        """
        self.metrics["legacy_llm_cost_absorbed"] += final_cost_from_litellm
        self.metrics["total_calls"] += 1

    def track_call(self, 
                   model_name: str, 
                   input_tokens: int, 
                   output_tokens: int, 
                   custom_revenue: float = None):
        """@step: Native Surgent Transaction Tracking (미래의 표준)"""
        self.metrics["total_calls"] += 1
        self.metrics["accumulated_input_tokens"] += input_tokens
        self.metrics["accumulated_output_tokens"] += output_tokens
        
        # Revenue 산출
        rev_model = self.registry.get("REVENUE_MODEL", {})
        if custom_revenue is not None:
            self.metrics["accumulated_revenue"] += custom_revenue
        elif rev_model.get("TYPE") == "PAY_AS_YOU_GO_TOKEN":
            self.metrics["accumulated_revenue"] += (
                (input_tokens / 1000) * rev_model.get("PRICE_PER_1K_IN", 0) +
                (output_tokens / 1000) * rev_model.get("PRICE_PER_1K_OUT", 0)
            )
        else:
            self.metrics["accumulated_revenue"] += rev_model.get("FIXED_PRICE_PER_CALL", 0.0)

    def add_infra_vector(self, cost_name: str, total_cost: float):
        """@desc: 외부 DB, Vector Store, 로드밸런서 등의 비용을 동적으로 추가"""
        self.metrics["dynamic_infra_costs"][cost_name] = total_cost

    def generate_synthesis(self) -> Dict[str, Any]:
        """
        @flow: Aggregate heterogeneous cost vectors (Legacy + Native + Infra) and return ROI.
        """
        # 1. 자체 토큰 비용 계산 (Registry 기반)
        native_llm_cost = 0.0
        if "LLM_COST_MODEL" in self.registry:
            cost_model = self.registry["LLM_COST_MODEL"]
            native_llm_cost = (
                (self.metrics["accumulated_input_tokens"] / 1_000_000) * cost_model.get("INPUT_1M", 0) +
                (self.metrics["accumulated_output_tokens"] / 1_000_000) * cost_model.get("OUTPUT_1M", 0)
            )

        # 2. 총 LLM 비용 = (자체 계산 비용) + (레거시 LiteLLM에서 주입된 비용)
        total_llm_cost = native_llm_cost + self.metrics["legacy_llm_cost_absorbed"]

        # 3. 인프라 비용 합산
        total_infra_cost = sum(self.metrics["dynamic_infra_costs"].values())

        # 4. 공헌이익 및 최종 Net Margin 도출
        total_cost = total_llm_cost + total_infra_cost
        revenue = self.metrics["accumulated_revenue"]
        net_profit = revenue - total_cost
        margin_pct = (net_profit / revenue * 100) if revenue > 0 else 0.0

        synthesis = {
            "transactions": self.metrics["total_calls"],
            "revenue": revenue,
            "cost_breakdown": {
                "native_llm": native_llm_cost,
                "legacy_llm_absorbed": self.metrics["legacy_llm_cost_absorbed"],
                "infrastructure": total_infra_cost
            },
            "total_cost": total_cost,
            "net_profit": net_profit,
            "margin_pct": margin_pct
        }
        
        log.info(f"[Economics Synthesis] Rev: ${revenue:.4f} | Cost: ${total_cost:.4f} | Margin: {margin_pct:.1f}%")
        return synthesis