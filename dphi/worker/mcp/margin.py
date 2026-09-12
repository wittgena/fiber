# fiber.dphi.worker.mcp.margin
import sys
import json
import logging
from typing import Dict, Any, List

from pydantic import BaseModel, Field, ValidationError

from xphi.watcher.receptor.trajectory import (
    FundingRateComparator,
    RiskPolicy,
    SpreadSnapshot,
    ArbitrageIntent
)
from xphi.arch.contract.protocol.agent import AsyncAgentProtocol

"""Pydantic Schemas (Financial vs Compute)"""
class ExecutionPricingModel(BaseModel):
    base_x402_fee_usd: float = Field(0.002, description="Base X402 invocation fee")
    profit_share_ratio: float = Field(0.05, ge=0.0, le=1.0, description="Take-rate on net arbitrage profit")

class ExecutionInfraModel(BaseModel):
    monthly_fixed_cost_usd: float = Field(30.0, description="Fixed infrastructure cost")
    compute_cost_per_sec_usd: float = Field(0.00001667, description="Compute cost rate")
    avg_latency_sec: float = Field(0.05, description="Average execution latency")

class RealtimeMarginRequest(BaseModel):
    symbol: str = Field(..., description="Target asset pair (e.g. BTC-USDT)")
    observations: Dict[str, Dict[str, Any]] = Field(..., description="Raw funding rates from distinct venues")
    trade_size_usd: float = Field(10000.0, description="Hypothetical trade size for margin sizing")
    pricing: ExecutionPricingModel = Field(default_factory=ExecutionPricingModel)
    infra: ExecutionInfraModel = Field(default_factory=ExecutionInfraModel)
    tps_range: List[float] = Field(default=[1.0, 10.0, 50.0, 100.0])

# 클라이언트용 껍데기 스키마 (실제 연산은 rpc.margin의 ComputeMarginRequest가 담당)
class ComputeMarginRequestParams(BaseModel):
    target_worker: str = Field(..., description="The ID of the worker that executed the task")
    compute_time_sec: float = Field(0.0, description="Total CPU compute execution time in seconds")
    io_consumed_mb: float = Field(0.0, description="Total memory/disk I/O throughput in MB")
    value_units_extracted: int = Field(0, description="Business value units extracted (events, rows, etc.)")


# =====================================================================
# 2. Asynchronous Margin Calculation Agent (MCP Proxy)
# =====================================================================
class MarginCalcAgent(AsyncAgentProtocol):
    def __init__(self):
        super().__init__(agent_name="agent.margin")
        
        self.MONTHLY_SECONDS = 30 * 24 * 60 * 60
        self.risk_policy = RiskPolicy()
        self.log.info("Production Universal Margin Oracle online. Awaiting computation requests...")

    async def handle_tools_list(self, req_id: Any):
        tools = [
            {
                "name": "calculate_trajectory_margin",
                "description": "Calculates real unit economics and breakeven matrices by binding live market spread to risk policy.",
                "inputSchema": RealtimeMarginRequest.model_json_schema()
            },
            {
                "name": "calculate_compute_margin",
                "description": "Calculates the dynamic X402 execution fee based on consumed Universal Infrastructure Telemetry (CPU/IO).",
                "inputSchema": ComputeMarginRequestParams.model_json_schema()
            }
        ]
        await self.send_response(req_id, {"tools": tools})

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        try:
            if tool_name == "calculate_trajectory_margin":
                req_data = RealtimeMarginRequest(**arguments)
                result = self._execute_trajectory_analysis(req_data)
                
            elif tool_name == "calculate_compute_margin":
                # [핵심 아키텍처 변경] 
                # 워커 내부에서 도메인 로직을 처리하지 않습니다.
                # Connector가 열어둔 백도어(rpc_delegate)를 통해 내부 순수 RPC 망(eco.margin.calculate)으로 위임(Delegation)합니다.
                
                try:
                    # AsyncAgentProtocol이 제공하는 내부 통신 브릿지 활용 (또는 Connector가 후킹하는 포맷 방출)
                    result = await self.request_core_rpc(
                        target_method="eco.margin.calculate", 
                        payload=arguments
                    )
                except Exception as rpc_err:
                    self.log.error(f"Failed to delegate margin calculation to core RPC: {rpc_err}")
                    await self.send_error(req_id, -32000, f"Core Delegation Failed: {rpc_err}")
                    return
                
            else:
                await self.send_error(req_id, -32601, f"Method not found: {tool_name}")
                return
                
            # 최종 결과만 MCP 규격(content/text) 껍데기를 씌워 반환
            await self.send_response(req_id, {
                "content": [{"type": "text", "text": json.dumps(result)}],
                "isError": False
            })
            
        except ValidationError as ve:
            self.log.warning(f"Pydantic Validation failed: {ve}")
            await self.send_error(req_id, -32602, f"Validation failed: {ve.json()}")
        except Exception as e:
            self.log.error(f"Domain Logic Fracture: {e}", exc_info=True)
            await self.send_error(req_id, -32000, str(e))

    # =====================================================================
    # [기존] 순수 비즈니스 로직 (수익성 및 손익분기 분석) 
    # - 이 부분은 차익거래 도메인에 특화되어 있으므로 워커에 잔존
    # =====================================================================
    def _execute_trajectory_analysis(self, req: RealtimeMarginRequest) -> Dict[str, Any]:
        snapshot, intent = FundingRateComparator.evaluate(req.symbol, req.observations)

        friction_rate = self.risk_policy.base_friction_bps / 10000.0
        gross_spread_yield = intent.expected_yield
        net_spread_yield = max(0.0, gross_spread_yield - friction_rate)
        
        projected_gross_profit = req.trade_size_usd * net_spread_yield
        
        effective_x402_fee = max(
            req.pricing.base_x402_fee_usd,
            projected_gross_profit * req.pricing.profit_share_ratio
        )
        
        var_cost_per_call = req.infra.avg_latency_sec * req.infra.compute_cost_per_sec_usd
        marginal_profit = effective_x402_fee - var_cost_per_call

        if marginal_profit <= 0:
            return {
                "actionable": False,
                "reason": "Negative marginal profit under current compute cost and spread.",
                "net_spread": net_spread_yield
            }

        import numpy as np
        tps_arr = np.array(req.tps_range, dtype=np.float64)
        monthly_volume = tps_arr * self.MONTHLY_SECONDS
        
        revenue_vec = monthly_volume * effective_x402_fee
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
                "effective_fee_usd": round(effective_x402_fee, 6),
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
        import asyncio
        asyncio.run(server.serve_forever_async())
    except KeyboardInterrupt:
        logging.info("Agent Terminated by Interrupt.")

if __name__ == "__main__":
    main()