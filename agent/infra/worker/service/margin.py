# fiber.agent.infra.worker.service.margin
## @lineage: fiber.infra.agent.worker.service.margin
import json
from typing import Dict, Any, List, Optional
import numpy as np
from pydantic import BaseModel, Field, ValidationError
from fiber.agent.infra.bridge.protocol import AgentProtocol

class ComputeMetrics(BaseModel):
    wasm_instantiation_ms: float = Field(...)
    execution_time_ms: float = Field(...)
    memory_peak_mb: float = Field(...)
    
class CostFactors(BaseModel):
    cost_per_ms_usd: float = Field(0.0000002)
    cost_per_mb_usd: float = Field(0.0000015)
    l402_routing_fee_usd: float = Field(0.0001)
    fixed_infra_monthly_usd: float = Field(30.0)

class ValueCapturePolicy(BaseModel):
    base_toll_fee_usd: float = Field(0.002)
    dynamic_take_rate: float = Field(0.05)
    client_projected_value_usd: Optional[float] = Field(0.0)

class EdgeMarginRequest(BaseModel):
    caller_id: str = Field(...)
    metrics: ComputeMetrics
    costs: CostFactors = Field(default_factory=CostFactors)
    policy: ValueCapturePolicy
    tps_range: List[float] = Field(default=[1.0, 10.0, 50.0, 100.0, 500.0])

class ServiceMarginAgent(AgentProtocol):
    def __init__(self):
        super().__init__(agent_name="agent.service_margin")
        self.MONTHLY_SECONDS = 30 * 24 * 60 * 60

    def handle_tools_list(self, req_id: Any):
        self.send_response(req_id, {"tools": [{
            "name": "calculate_edge_economics",
            "description": "Calculates L402 pricing based on physical V8 compute metrics.",
            "inputSchema": EdgeMarginRequest.model_json_schema()
        }]})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        if tool_name == "calculate_edge_economics":
            try:
                req_data = EdgeMarginRequest(**arguments)
                result = self._execute_metering_logic(req_data)
                self.send_response(req_id, {"content": [{"type": "text", "text": json.dumps(result)}], "isError": False})
            except ValidationError as ve:
                self.send_error(req_id, -32602, f"Validation failed: {ve.json()}")
            except Exception as e:
                self.send_error(req_id, -32000, str(e))
        else:
            self.send_error(req_id, -32601, "Method not found")

    def _execute_metering_logic(self, req: EdgeMarginRequest) -> Dict[str, Any]:
        # 1. 물리적 원가 산출
        total_time_ms = req.metrics.wasm_instantiation_ms + req.metrics.execution_time_ms
        absolute_cost = (total_time_ms * req.costs.cost_per_ms_usd) + (req.metrics.memory_peak_mb * req.costs.cost_per_mb_usd) + req.costs.l402_routing_fee_usd

        # 2. 가치 포획 및 청구 금액 확정
        value_share_fee = req.policy.client_projected_value_usd * req.policy.dynamic_take_rate
        final_l402_fee = max(req.policy.base_toll_fee_usd, value_share_fee)
        marginal_profit = final_l402_fee - absolute_cost

        if marginal_profit <= 0:
            return {"actionable": False, "absolute_cost": absolute_cost, "proposed_fee": final_l402_fee}

        # 3. 매트릭스 도출
        tps_arr = np.array(req.tps_range, dtype=np.float64)
        monthly_volume = tps_arr * self.MONTHLY_SECONDS
        revenue_vec = monthly_volume * final_l402_fee
        profit_vec = revenue_vec - (req.costs.fixed_infra_monthly_usd + (monthly_volume * absolute_cost))

        return {
            "metering_target": req.caller_id,
            "unit_economics": {
                "l402_invoice_usd": round(final_l402_fee, 6),
                "physical_cost_usd": round(absolute_cost, 8),
                "marginal_profit_usd": round(marginal_profit, 6),
            },
            "matrix": {
                "tps": tps_arr.tolist(),
                "monthly_net_profit_usd": np.round(profit_vec, 2).tolist()
            }
        }

def main():
    server = ServiceMarginAgent()
    try: server.serve_forever()
    except KeyboardInterrupt: pass

if __name__ == "__main__": main()