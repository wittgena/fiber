# fiber.infra.worker.agent.margin
import sys
import json
import logging
from typing import Dict, Any

import numpy as np
from pydantic import BaseModel, Field, ValidationError

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s [%(levelname)s] [agent.margin] %(message)s")
log = logging.getLogger("agent.margin")

# =====================================================================
# 1. Pydantic Input Schemas (엄격한 파라미터 검증 및 스키마 자동 생성)
# =====================================================================
class PricingModel(BaseModel):
    base_fee_usd: float = Field(..., description="L402 base charge per API call")
    dynamic_multiplier: float = Field(1.0, description="Multiplier for surge pricing")

class InfraModel(BaseModel):
    monthly_fixed_cost_usd: float = Field(..., description="Server/DB base cost (e.g., $30)")
    cost_per_compute_ms_usd: float = Field(..., description="Variable cost per ms of execution")
    avg_compute_time_ms: float = Field(..., description="Average processing time in ms")

class TrafficScenarios(BaseModel):
    min_tps: float = Field(..., description="Minimum Transactions Per Second")
    max_tps: float = Field(..., description="Maximum Transactions Per Second")
    cache_hit_ratio: float = Field(0.0, ge=0.0, le=1.0, description="0.0 to 1.0 cache success rate")

class MarginSimulationRequest(BaseModel):
    pricing_model: PricingModel
    infra_model: InfraModel
    traffic_scenarios: TrafficScenarios

# =====================================================================
# 2. Core Agent Logic (순수 결정론적 연산 데몬)
# =====================================================================
class MarginCalcAgent:
    """
    [Unit Economics & Margin Simulator]
    A2A 생태계 참여자들이 자신의 에이전트를 배포하기 전, 최적의 L402 단가를 계산하는 BI 도구입니다.
    과금 영수증(Receipt) 생성은 코어망에 위임하고, 오직 Numpy 벡터 연산 최적화에만 집중합니다.
    """
    def __init__(self):
        self.MONTHLY_SECONDS = 30 * 24 * 60 * 60  # 2,592,000초

    def serve_forever(self):
        log.info("Margin Simulator Ignited (Daemon Mode). Listening on stdin.")
        for line in sys.stdin:
            if not line.strip(): continue
            try:
                self._dispatch(json.loads(line))
            except json.JSONDecodeError:
                self._send_error(None, -32700, "Parse error: Invalid JSON")
            except Exception as e:
                log.error(f"Daemon Loop Fracture: {e}", exc_info=True)
                self._send_error(None, -32603, f"Internal Server Error: {e}")

    def _dispatch(self, req: Dict[str, Any]):
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})
        
        if method == "initialize":
            self._send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
            
        elif method == "tools/list":
            tools = [{
                "name": "simulate_unit_economics",
                "description": "Calculates break-even points and revenue matrices for L402 API pricing.",
                "inputSchema": MarginSimulationRequest.model_json_schema()
            }]
            self._send_response(req_id, {"tools": tools})
            
        elif method == "tools/call" and params.get("name") == "simulate_unit_economics":
            try:
                # 1. Pydantic을 이용한 스키마 검증 및 파싱 (입력 무결성 보장)
                req_data = MarginSimulationRequest(**params.get("arguments", {}))
                
                # 2. 핵심 연산 (Numpy Vectorization) 실행
                result = self._simulate(req_data)
                
                # 3. 순수 결과 반환 (MCP 규격 준수, 과금 로직 배제)
                self._send_response(req_id, {
                    "content": [{"type": "text", "text": json.dumps(result)}],
                    "isError": False
                })
                
            except ValidationError as ve:
                # 사용자/LLM의 스키마 입력 오류는 명확하게 Invalid Params(-32602)로 반환
                log.warning(f"Validation Error: {ve.errors()}")
                self._send_error(req_id, -32602, f"Invalid params: {ve.json()}")
            except Exception as e:
                log.error(f"Simulation Execution Failed: {e}", exc_info=True)
                self._send_error(req_id, -32000, str(e))
        else:
            self._send_error(req_id, -32601, f"Unknown method/tool: {method}")

    def _simulate(self, data: MarginSimulationRequest) -> Dict[str, Any]:
        """
        Numpy 벡터 연산을 활용하여 최소/평균/최대 트래픽에 대한 재무 매트릭스를 단일 연산으로 도출합니다.
        """
        # 단위 변수 추출
        rev_per_call = data.pricing_model.base_fee_usd * data.pricing_model.dynamic_multiplier
        # 캐시 적중률을 반영한 실질 호출 원가 계산
        var_cost_per_call = (data.infra_model.avg_compute_time_ms * data.infra_model.cost_per_compute_ms_usd) * (1.0 - data.traffic_scenarios.cache_hit_ratio)
        fixed_cost = data.infra_model.monthly_fixed_cost_usd
        
        profit_per_call = rev_per_call - var_cost_per_call
        
        if profit_per_call <= 0:
            return {"error": "Structural Deficit: Variable cost exceeds revenue per call. Increase base_fee or optimize compute."}

        # 1. 손익 분기점 (BEP) 계산
        bep_calls_per_month = fixed_cost / profit_per_call
        bep_tps = bep_calls_per_month / self.MONTHLY_SECONDS

        # 2. 다중 시나리오 매트릭스 생성 (Numpy Array Vectorization)
        tps_scenarios = np.array([
            data.traffic_scenarios.min_tps,
            (data.traffic_scenarios.min_tps + data.traffic_scenarios.max_tps) / 2.0,
            data.traffic_scenarios.max_tps
        ])
        
        # 행렬(Matrix) 기반 고속 재무 연산
        monthly_calls_matrix = tps_scenarios * self.MONTHLY_SECONDS
        revenue_matrix = monthly_calls_matrix * rev_per_call
        cost_matrix = fixed_cost + (monthly_calls_matrix * var_cost_per_call)
        profit_matrix = revenue_matrix - cost_matrix
        margin_rate_matrix = (profit_matrix / revenue_matrix) * 100.0

        return {
            "unit_economics": {
                "revenue_per_call_usd": round(rev_per_call, 6),
                "variable_cost_per_call_usd": round(var_cost_per_call, 6),
                "marginal_profit_per_call_usd": round(profit_per_call, 6)
            },
            "break_even_point": {
                "bep_tps": round(bep_tps, 4),
                "bep_monthly_calls": int(bep_calls_per_month)
            },
            "monthly_scenarios": {
                "labels": ["MIN_TRAFFIC", "AVG_TRAFFIC", "MAX_TRAFFIC"],
                "tps": np.round(tps_scenarios, 2).tolist(),
                "revenue_usd": np.round(revenue_matrix, 2).tolist(),
                "total_cost_usd": np.round(cost_matrix, 2).tolist(),
                "net_profit_usd": np.round(profit_matrix, 2).tolist(),
                "margin_rate_percent": np.round(margin_rate_matrix, 2).tolist()
            },
            "recommendation": "Highly Profitable" if profit_matrix[1] > (fixed_cost * 3) else "Marginal. Increase cache hit ratio or base fee."
        }

    # =====================================================================
    # 4. IPC Communicators (Stdout Writers)
    # =====================================================================
    def _send_response(self, req_id: Any, result: Dict[str, Any]):
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result}) + "\n")
        sys.stdout.flush()

    def _send_error(self, req_id: Any, code: int, message: str):
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}) + "\n")
        sys.stdout.flush()

if __name__ == "__main__":
    MarginCalcAgent().serve_forever()