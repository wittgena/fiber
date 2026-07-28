# topos.xelog.edge.a2a.profile
from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel
from typing import Dict, Any, Optional

from topos.xelog.bench.profile import BenchProfile

from arch.topos.bound.exchange.config import billing_config
from watcher.dphi.cgroup import Tier

profile_edge = APIRouter()

class BilledExecutionRequest(BaseModel):
    agent_schema: Dict[str, Any]
    context_depth: int = 2
    target_entry: str

class QuotationResponse(BaseModel):
    status: str
    tier_applied: str
    fuel_estimated: int
    estimated_cost_usd: float
    reason: Optional[str] = None

class BilledExecutionResponse(BaseModel):
    status: str
    tier_applied: str
    fuel_billed: int
    billed_cost_usd: float
    reason: Optional[str] = None

class ProfileBillingState:
    QUOTE_READY = "QUOTE_READY"
    QUOTE_REJECTED = "QUOTE_REJECTED"
    BILLED_SUCCESS = "BILLED_EXECUTION_SUCCESS"
    BILLED_FAILED = "BILLED_EXECUTION_FAILED"

async def extract_client_project(api_key: str = "test_key") -> str:
    # TODO: 실제 API Key를 기반으로 프로젝트/에이전트 ID 추출 로직 연동
    return "generative-language-client-1234" 

def get_billing_profile_service() -> BenchProfile:
    """FastAPI Depends를 위한 BenchProfile(Billing/Resource 서비스) 인스턴스 주입기"""
    return BenchProfile()

@profile_edge.post(
    "/quote", 
    summary="[Billing] Request execution quotation (Dry-run)",
    response_model=QuotationResponse
)
async def request_quotation(
    req: BilledExecutionRequest, 
    client_project_id: str = Depends(extract_client_project),
    profile_service: BenchProfile = Depends(get_billing_profile_service)
):
    try:
        # bench.profile에 구현된 dry_run=True 플래그를 통해 실제 과금을 방지 (견적용 실행)
        result = await profile_service.execute(
            client_project_id=client_project_id,
            schema=req.agent_schema,
            entry=req.target_entry,
            depth=req.context_depth,
            dry_run=True 
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        
    is_success = (result.status == "COHERENCE")
    api_status = ProfileBillingState.QUOTE_READY if is_success else ProfileBillingState.QUOTE_REJECTED
    estimated_cost = (result.fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
    return QuotationResponse(
        status=api_status, 
        tier_applied=result.tier_applied,
        fuel_estimated=result.fuel_consumed,
        estimated_cost_usd=estimated_cost,
        reason=result.reason
    )

@profile_edge.post(
    "/execute/billed", 
    summary="[Billing] Execute workload with account charging",
    response_model=BilledExecutionResponse
)
async def execute_billed_workload(
    req: BilledExecutionRequest, 
    client_project_id: str = Depends(extract_client_project),
    profile_service: BenchProfile = Depends(get_billing_profile_service)
):
    """@desc: 할당된 리소스 티어 내에서 코드를 실행하고, 소모된 자원(Fuel)만큼 클라이언트 계정에서 실제로 비용을 차감(Billing)"""
    try:
        result = await profile_service.execute(
            client_project_id=client_project_id,
            schema=req.agent_schema,
            entry=req.target_entry,
            depth=req.context_depth,
            dry_run=False
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        
    is_success = (result.status == "COHERENCE")
    api_status = ProfileBillingState.BILLED_SUCCESS if is_success else ProfileBillingState.BILLED_FAILED
    billed_cost = (result.fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
    return BilledExecutionResponse(
        status=api_status, 
        tier_applied=result.tier_applied,
        fuel_billed=result.fuel_consumed,
        billed_cost_usd=billed_cost,
        reason=result.reason
    )