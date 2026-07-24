# eco.tenant.cost.estimator
## @lineage: bound.gateway.cost.estimator
## @lineage: gateway.cost.estimator
## @lineage: bound.eco.estimator
## @lineage: bound.proxy.eco.estimator
## @lineage: bound.surface.eco.estimator
## @lineage: bound.surface.cost.eco.estimator
"""
@role: FinOps Control Plane. Tracks transactions, applies Autophagy ratio, resolves LLM costs dynamically via registry, and reports net profit.
"""
import time
from typing import Dict, Any, Optional
from watcher.plane.emitter import get_emitter
from bound.resolver.model.cost import ModelCostRegistry

log = get_emitter("eco.esitmator")

ECONOMICS_CONFIG = {
    "REVENUE": {
        "TYPE": "FIXED_PER_TRANSACTION", 
        "PRICE_PER_REQUEST": 0.01       # 건당 과금 모델 (예: $0.01)
    },
    "LLM_DEFAULTS": {
        "TOKENS_PER_LINE_AVG": 15,      # 평균 1줄당 토큰 수
        "FALLBACK_INPUT_COST": 0.075 / 1_000_000,  # 레지스트리 조회 실패 시 1토큰당 기본값
        "FALLBACK_OUTPUT_COST": 0.30 / 1_000_000,
    },
    "INFRA": {
        "VM_HOURLY_RATE": 0.16,         # $0.16/hr (GCP e2-standard-4 기준, 월 ~$115)
        "NETWORK_EGRESS_PER_GB": 0.08,  # $0.08/GB (아웃바운드 트래픽)
    }
}

class Economics:
    """
    @role: Business FinOps Control Plane & Margin Analyzer.
    @desc: LiteLLM 레지스트리를 활용해 동적으로 LLM 단가를 가져오고, 인프라 비용과 
           Autophagy(자가포식) 효율을 반영해 최종 비즈니스 마진을 계산합니다.
    """
    def __init__(self, config: Dict[str, Any] = ECONOMICS_CONFIG):
        self.cfg = config
        self.start_time = time.time()
        
        # Unified Metrics State
        self.metrics = {
            "total_transactions": 0,
            "raw_lines_ingested": 0,
            "lines_retained": 0,
            "egress_bytes": 0,
            "total_revenue": 0.0,
            "total_llm_cost": 0.0
        }

    def _resolve_pricing(self, model_name: str, custom_provider: Optional[str] = None) -> Dict[str, float]:
        """
        @desc: 외부 `ModelCostRegistry`를 호출하여 현재 모델의 1토큰당 단가를 추출합니다.
               안정성을 위해 조회 실패 시 Fallback 단가를 반환합니다.
        """
        try:
            model_info = ModelCostRegistry.lookup_base_model_info(
                model=model_name, 
                custom_llm_provider=custom_provider
            )
            return {
                "input_cost_per_token": float(model_info.get("input_cost_per_token", self.cfg["LLM_DEFAULTS"]["FALLBACK_INPUT_COST"])),
                "output_cost_per_token": float(model_info.get("output_cost_per_token", self.cfg["LLM_DEFAULTS"]["FALLBACK_OUTPUT_COST"]))
            }
        except Exception as e:
            log.warning(f"Cost registry lookup failed for '{model_name}', using fallback pricing. Error: {e}")
            return {
                "input_cost_per_token": self.cfg["LLM_DEFAULTS"]["FALLBACK_INPUT_COST"],
                "output_cost_per_token": self.cfg["LLM_DEFAULTS"]["FALLBACK_OUTPUT_COST"]
            }

    def record_transaction(self, 
                           model_name: str,
                           raw_lines: int, 
                           retained_lines: int, 
                           output_tokens: int,
                           custom_provider: Optional[str] = None,
                           egress_bytes: int = 512):
        """
        @step: 1회 API 호출 사이클이 끝났을 때 이 함수를 호출합니다.
        @desc: Autophagy 효율을 반영하여 실제 발생한 LLM 비용과 매출을 기록합니다.
        """
        # 1. 지표 누적 계산
        self.metrics["total_transactions"] += 1
        self.metrics["raw_lines_ingested"] += raw_lines
        self.metrics["lines_retained"] += retained_lines
        self.metrics["egress_bytes"] += egress_bytes
        
        # 2. 매출 계산
        if self.cfg["REVENUE"]["TYPE"] == "FIXED_PER_TRANSACTION":
            self.metrics["total_revenue"] += self.cfg["REVENUE"]["PRICE_PER_REQUEST"]

        # 3. LLM 비용 계산 (Retained Lines 기준)
        pricing = self._resolve_pricing(model_name, custom_provider)
        input_tokens = retained_lines * self.cfg["LLM_DEFAULTS"]["TOKENS_PER_LINE_AVG"]
        
        actual_llm_cost = (
            (input_tokens * pricing["input_cost_per_token"]) + 
            (output_tokens * pricing["output_cost_per_token"])
        )
        self.metrics["total_llm_cost"] += actual_llm_cost

    def _calc_infra_cost(self, uptime_hours: float) -> float:
        """@flow: Calculates VM uptime and Network Egress costs."""
        infra_cfg = self.cfg["INFRA"]
        vm_cost = uptime_hours * infra_cfg["VM_HOURLY_RATE"]
        egress_gb = self.metrics["egress_bytes"] / (1024 ** 3)
        network_cost = egress_gb * infra_cfg["NETWORK_EGRESS_PER_GB"]
        return vm_cost + network_cost

    def generate_report(self, mock_uptime_hours: Optional[float] = None) -> Dict[str, Any]:
        """
        @desc: Generates a comprehensive financial synthesis of the fulfillment pipeline.
        """
        uptime = mock_uptime_hours if mock_uptime_hours else (time.time() - self.start_time) / 3600.0
        
        revenue = self.metrics["total_revenue"]
        llm_cost = self.metrics["total_llm_cost"]
        infra_cost = self._calc_infra_cost(uptime)
        
        total_cost = llm_cost + infra_cost
        net_profit = revenue - total_cost
        margin_pct = (net_profit / revenue * 100) if revenue > 0 else 0.0
        
        # Autophagy (필터링) 효율성 계산
        autophagy_rate = 0.0
        raw_lines = self.metrics["raw_lines_ingested"]
        retained = self.metrics["lines_retained"]
        if raw_lines > 0:
            autophagy_rate = ((raw_lines - retained) / raw_lines) * 100

        log.info(f"=== [Fulfillment Economics Report] ===")
        log.info(f" ↳ Transactions: {self.metrics['total_transactions']:,} reqs")
        log.info(f" ↳ Autophagy Efficiency: {autophagy_rate:.1f}% noise discarded")
        log.info(f" ↳ Gross Revenue: ${revenue:.4f}")
        log.info(f" ↳ Total Costs: ${total_cost:.4f} (LLM: ${llm_cost:.4f}, Infra: ${infra_cost:.4f})")
        log.info(f" ↳ Net Profit: ${net_profit:.4f} [Margin: {margin_pct:.1f}%]")
        log.info(f"=======================================")

        return {
            "transactions": self.metrics["total_transactions"],
            "autophagy_rate_pct": autophagy_rate,
            "revenue": revenue,
            "cost_breakdown": {
                "llm_cost": llm_cost,
                "infra_cost": infra_cost
            },
            "net_profit": net_profit,
            "margin_pct": margin_pct
        }