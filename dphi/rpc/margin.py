# fiber.dphi.rpc.margin
from typing import Dict, Any
from pydantic import BaseModel, Field, ValidationError

from fiber.dphi.rpc.handler import WorkerContext, _build_error
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("rpc.margin")

# [순수 내부 도메인 모델] 범용 클라우드 종량제 메트릭
class ComputeMarginRequest(BaseModel):
    target_worker: str = Field(..., description="The ID of the worker that executed the task")
    compute_time_sec: float = Field(0.0, description="Total CPU compute execution time in seconds")
    io_consumed_mb: float = Field(0.0, description="Total memory/disk I/O throughput in MB")
    value_units_extracted: int = Field(0, description="Business value units extracted (events, rows, etc.)")
    
    base_fee_usd: float = Field(0.002, description="Base API invocation cost")
    cost_per_mb_usd: float = Field(0.00005, description="Cost per MB of memory/disk I/O")
    cost_per_cpu_sec_usd: float = Field(0.0001, description="Cost per second of CPU time")

async def handle_compute_margin_calculate(params: dict, ctx: WorkerContext) -> dict:
    """
    [순수 내부망 RPC 핸들러]
    워커가 발생시킨 범용 클라우드 텔레메트리(CPU/IO)를 기반으로
    X402 종량제 단가를 정밀하게 산출합니다. (MCP 껍데기 없음)
    """
    try:
        target = params.get("target") or params.get("target_worker", "unknown_worker")
        
        # DuckDB 등 레거시 워커의 특정 메트릭을 범용 클라우드 메트릭으로 안전하게 자동 치환
        compute_time = params.get("compute_time_sec", 0.0)
        if "duckdb_sql_time_sec" in params:
            compute_time += params.get("duckdb_sql_time_sec", 0.0) + params.get("python_regex_time_sec", 0.0)
            
        io_mb = params.get("io_consumed_mb", 0.0)
        if "scanned_file_mb" in params:
            io_mb += params.get("scanned_file_mb", 0.0)
            
        value_units = params.get("value_units_extracted", 0)
        if "total_events_parsed" in params:
            value_units += params.get("total_events_parsed", 0)

        # 안전한 DTO 구성
        mapped_args = {
            "target_worker": target,
            "compute_time_sec": compute_time,
            "io_consumed_mb": io_mb,
            "value_units_extracted": value_units,
            "base_fee_usd": params.get("base_fee_usd", 0.002),
            "cost_per_mb_usd": params.get("cost_per_mb_usd", 0.00005),
            "cost_per_cpu_sec_usd": params.get("cost_per_cpu_sec_usd", 0.0001),
        }
        
        req = ComputeMarginRequest(**mapped_args)
        
        # 원가 계산 비즈니스 로직
        cpu_cost = req.compute_time_sec * req.cost_per_cpu_sec_usd
        io_cost = req.io_consumed_mb * req.cost_per_mb_usd
        value_premium = req.value_units_extracted * 0.00001
        
        total_calculated_fee = req.base_fee_usd + cpu_cost + io_cost + value_premium
        safe_fee_usd = round(max(req.base_fee_usd, total_calculated_fee), 5)
        
        log.info(f"[Universal Pricing] {req.target_worker} -> Base: \({req.base_fee_usd} | CPU:\){cpu_cost:.5f} | IO: \({io_cost:.5f} | Total:\){safe_fee_usd:.5f}")

        # MCP 규격(content/text)이 아닌 순수 JSON 딕셔너리 리턴
        return {
            "worker_id": req.target_worker,
            "unit_economics": {
                "effective_fee_usd": safe_fee_usd,
                "base_fee": req.base_fee_usd,
                "variable_costs": {
                    "cpu_cost": round(cpu_cost, 6),
                    "io_cost": round(io_cost, 6),
                    "value_premium": round(value_premium, 6)
                }
            },
            "telemetry_echo": {
                "io_consumed_mb": round(req.io_consumed_mb, 2),
                "compute_time_sec": round(req.compute_time_sec, 4)
            }
        }
        
    except ValidationError as ve:
        log.warning(f"[Margin] Pydantic Validation failed: {ve}")
        return _build_error(422, "Margin Request Validation failed")
    except Exception as e:
        log.error(f"[Margin] Domain Logic Fracture: {e}", exc_info=True)
        return _build_error(500, "Internal Margin Calculation Error")