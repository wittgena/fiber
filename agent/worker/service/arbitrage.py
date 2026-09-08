# fiber.agent.worker.service.arbitrage
## @lineage: fiber.agent.infra.worker.service.arbitrage
## @lineage: fiber.infra.agent.worker.service.arbitrage
import json
from typing import Dict, Any
from pydantic import BaseModel, Field, ValidationError
from fiber.infra.oracle.observer.trajectory import FundingRateComparator
from fiber.agent.gateway.protocol import AgentProtocol

class ObservationPoint(BaseModel):
    rate: float = Field(...)
    time: int = Field(...)

class ArbitrageMarginRequest(BaseModel):
    symbol: str = Field(...)
    observations: Dict[str, ObservationPoint] = Field(...)
    trade_size_usd: float = Field(10000.0)
    base_friction_bps: float = Field(15.0)

class ArbitrageMarginAgent(AgentProtocol):
    def __init__(self):
        super().__init__(agent_name="agent.margin.arbitrage")

    def handle_tools_list(self, req_id: Any):
        self.send_response(req_id, {"tools": [{
            "name": "evaluate_arbitrage_intent",
            "description": "Calculates purely financial unit economics.",
            "inputSchema": ArbitrageMarginRequest.model_json_schema()
        }]})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        if tool_name == "evaluate_arbitrage_intent":
            try:
                req_data = ArbitrageMarginRequest(**arguments)
                result = self._execute_domain_logic(req_data)
                self.send_response(req_id, {"content": [{"type": "text", "text": json.dumps(result)}], "isError": False})
            except ValidationError as ve:
                self.send_error(req_id, -32602, f"Validation failed: {ve.json()}")
            except Exception as e:
                self.send_error(req_id, -32000, str(e))
        else:
            self.send_error(req_id, -32601, "Method not found")

    def _execute_domain_logic(self, req: ArbitrageMarginRequest) -> Dict[str, Any]:
        # 1. 딕셔너리 변환 및 스프레드 평가
        raw_obs = {arn: {"rate": obs.rate, "time": obs.time} for arn, obs in req.observations.items()}
        snapshot, intent = FundingRateComparator.evaluate(req.symbol, raw_obs)

        # 2. 금융적 마찰(Friction) 적용 후 순수익 확정
        friction_rate = req.base_friction_bps / 10000.0
        net_spread_yield = intent.expected_yield - friction_rate
        
        is_financially_actionable = intent.is_actionable and (net_spread_yield > 0)
        projected_gross_profit = req.trade_size_usd * net_spread_yield if is_financially_actionable else 0.0

        return {
            "domain": "arbitrage",
            "symbol": snapshot.symbol,
            "financial_feasibility": {
                "is_actionable": is_financially_actionable,
                "net_spread_yield": round(net_spread_yield, 6)
            },
            "value_projection": {
                "projected_profit_usd": round(projected_gross_profit, 4)
            }
        }

def main():
    server = ArbitrageMarginAgent()
    try: server.serve_forever()
    except KeyboardInterrupt: pass

if __name__ == "__main__": main()