# fiber.infra.agent.worker.margin
## @lineage: fiber.a2a.worker.margin
## @lineage: fiber.infra.worker.agent.margin
import sys
import json
import logging
from typing import Dict, Any, List

import numpy as np
from pydantic import BaseModel, Field, ValidationError

from fiber.infra.agent.observer.intent.trajectory import (
    FundingRateComparator,
    RiskPolicy,
    SpreadSnapshot,
    ArbitrageIntent
)
from fiber.infra.agent.bridge.protocol import AgentProtocol

# ---------------------------------------------------------
# Pydantic Schemas (도메인 데이터 검증)
# ---------------------------------------------------------
class ExecutionPricingModel(BaseModel):
    base_l402_fee_usd: float = Field(0.002, description="Base L402 invocation fee")
    profit_share_ratio: float = Field(0.05, ge=0.0, le=1.0, description="Take-rate on net arbitrage profit")

class ExecutionInfraModel(BaseModel):
    monthly_fixed_cost_usd: float = Field(30.0, description="Fixed infrastructure cost")
    compute_cost_per_sec_usd: float = Field(0.00001667, description="Compute cost rate")
    avg_latency_sec: float = Field(0.05, description="Average execution latency")

class RealtimeMarginRequest(BaseModel):
    symbol: str = Field(..., description="Target asset pair (e.g. BTC-USDT)")
    observations: Dict[str, Dict[str, Any]] = Field(
        ..., 
        description="Raw funding rates from distinct venues: {arn: {'rate': float, 'time': int}}"
    )
    trade_size_usd: float = Field(10000.0, description="Hypothetical trade size for margin sizing")
    pricing: ExecutionPricingModel = Field(default_factory=ExecutionPricingModel)
    infra: ExecutionInfraModel = Field(default_factory=ExecutionInfraModel)
    tps_range: List[float] = Field(default=[1.0, 10.0, 50.0, 100.0], description="TPS evaluation points")


# =====================================================================
# Legacy Agent -> Protocol Inherited
# =====================================================================
class MarginCalcAgent(AgentProtocol):
    def __init__(self):
        # [적용] 부모 클래스 초기화 - 자동으로 stdout 격리 및 로깅 설정이 적용됩니다.
        super().__init__(agent_name="agent.margin")
        
        self.MONTHLY_SECONDS = 30 * 24 * 60 * 60
        self.risk_policy = RiskPolicy()
        self.log.info("Production MarginCalcAgent online. Awaiting margin computation requests...")

    # [삭제됨] 지저분했던 serve_forever, _dispatch 메서드는 모두 부모 클래스(AgentProtocol)로 이관되었습니다.

    # =====================================================================
    # AgentProtocol 추상 메서드 구현
    # =====================================================================
    def handle_tools_list(self, req_id: Any):
        """MCP 2026 규격의 tools/list 요청 처리"""
        tools = [{
            "name": "calculate_trajectory_margin",
            "description": "Calculates real unit economics and breakeven matrices by binding live market spread to risk policy.",
            "inputSchema": RealtimeMarginRequest.model_json_schema()
        }]
        self.send_response(req_id, {"tools": tools})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        """
        MCP 2026 규격의 tools/call 요청 처리
        [적용] 코어망이 주입한 _meta는 파라미터에서 완전히 분리되어 전달되므로 
        Pydantic의 엄격한 스키마 검증(ValidationError)을 우회할 수 있습니다.
        """
        if tool_name == "calculate_trajectory_margin":
            try:
                # 1. 강력한 Pydantic 파라미터 검증
                req_data = RealtimeMarginRequest(**arguments)
                
                # 2. 순수 도메인 로직 실행
                result = self._execute_margin_analysis(req_data)
                
                # 3. 안전한 IPC 응답 반환
                self.send_response(req_id, {
                    "content": [{"type": "text", "text": json.dumps(result)}],
                    "isError": False
                })
                
            except ValidationError as ve:
                self.log.warning(f"Pydantic Validation failed: {ve}")
                self.send_error(req_id, -32602, f"Validation failed: {ve.json()}")
            except Exception as e:
                self.log.error(f"Domain Logic Fracture: {e}", exc_info=True)
                self.send_error(req_id, -32000, str(e))
        else:
            self.send_error(req_id, -32601, f"Method not found: {tool_name}")

    # =====================================================================
    # 순수 비즈니스 로직 (수익성 및 손익분기 분석)
    # =====================================================================
    def _execute_margin_analysis(self, req: RealtimeMarginRequest) -> Dict[str, Any]:
        # 1. trajectory.py의 핵심 도메인 로직 직접 호출 (스프레드 평가)
        snapshot, intent = FundingRateComparator.evaluate(req.symbol, req.observations)

        # 2. 거래 마찰 및 순익 산출 (RiskPolicy 적용)
        friction_rate = self.risk_policy.base_friction_bps / 10000.0
        gross_spread_yield = intent.expected_yield
        net_spread_yield = max(0.0, gross_spread_yield - friction_rate)
        
        projected_gross_profit = req.trade_size_usd * net_spread_yield
        
        # 3. L402 수취 모델 결정
        effective_l402_fee = max(
            req.pricing.base_l402_fee_usd,
            projected_gross_profit * req.pricing.profit_share_ratio
        )
        
        # 4. 연산 인프라 원가
        var_cost_per_call = req.infra.avg_latency_sec * req.infra.compute_cost_per_sec_usd
        marginal_profit = effective_l402_fee - var_cost_per_call

        if marginal_profit <= 0:
            return {
                "actionable": False,
                "reason": "Negative marginal profit under current compute cost and spread.",
                "net_spread": net_spread_yield
            }

        # 5. Numpy 고속 벡터화 (손익 매트릭스)
        tps_arr = np.array(req.tps_range, dtype=np.float64)
        monthly_volume = tps_arr * self.MONTHLY_SECONDS
        
        revenue_vec = monthly_volume * effective_l402_fee
        cost_vec = req.infra.monthly_fixed_cost_usd + (monthly_volume * var_cost_per_call)
        profit_vec = revenue_vec - cost_vec
        margin_pct_vec = (profit_vec / revenue_vec) * 100.0

        bep_calls = req.infra.monthly_fixed_cost_usd / marginal_profit
        bep_tps = bep_calls / self.MONTHLY_SECONDS

        return {
            "market_state": {
                "symbol": snapshot.symbol,
                "net_spread": round(snapshot.net_spread, 6),
                "is_actionable": intent.is_actionable,
                "long_venue": intent.optimal_long_venue,
                "short_venue": intent.optimal_short_venue
            },
            "unit_economics": {
                "projected_trade_profit_usd": round(projected_gross_profit, 4),
                "effective_fee_usd": round(effective_l402_fee, 6),
                "compute_cost_usd": round(var_cost_per_call, 8),
                "marginal_profit_usd": round(marginal_profit, 6)
            },
            "break_even": {
                "bep_tps": round(bep_tps, 4),
                "bep_monthly_calls": int(bep_calls)
            },
            "matrix": {
                "tps": tps_arr.tolist(),
                "monthly_revenue": np.round(revenue_vec, 2).tolist(),
                "monthly_cost": np.round(cost_vec, 2).tolist(),
                "monthly_net_profit": np.round(profit_vec, 2).tolist(),
                "margin_percent": np.round(margin_pct_vec, 2).tolist()
            }
        }

def main():
    server = MarginCalcAgent()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Agent Terminated by Interrupt.")

if __name__ == "__main__":
    main()