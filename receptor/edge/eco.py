# receptor.edge.eco
import json
import time
from typing import Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from arch.contract.interface import ContractRouter
from arch.contract.model.receptor import (
    EdgeState,
    IntentValidationRequest, IntentValidationResponse,
    ExecuteComputeRequest, ExecuteComputeResponse,
    ProofGenerationRequest, ProofGenerationResponse,
    TradeIngressRequest, TradeIngressResponse,
    EpochInitPayload,
    ClearingReceiptRequest, ClearingReceiptResponse
)
from receptor.ingress.gov.policy import IngressPolicyEngine, get_ingress_policy

from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.exchange import ExchangeAdapter
from kernel.dphi.broker import DphiBroker
from kernel.dphi.cgroup import Tier
from kernel.dphi.exchange.config import tier_config, billing_config

from receptor.xe.depend import get_wasm_broker, get_exchange_adapter
from receptor.xe.profile import BenchProfile

compute_edge = ContractRouter(namespace="eco.compute", prefix="/compute", tags=["Eco Compute"])
exchange_edge = ContractRouter(namespace="eco.exchange", prefix="/exchange", tags=["Eco Exchange"])
profile_edge = ContractRouter(namespace="eco.profile", prefix="/profile", tags=["Eco Profile"])

@compute_edge.post(
    "/intent/validate", 
    summary="1. Validate Intent", 
    response_model=IntentValidationResponse
)
async def validate_intent(req: IntentValidationRequest, broker: DphiBroker = Depends(get_wasm_broker)):
    raw_payload = {**req.model_dump(), "timestamp": int(time.time() * 1000)}
    canonical_payload = StateAdapter.to_canonical_bytes(raw_payload).decode('utf-8')
    res = await broker.invoke("validate_intent", canonical_payload)
    
    if not res.success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=res.error.message)
        
    return IntentValidationResponse(status=EdgeState.INTENT_VALIDATED, clearance=json.loads(res.output))

@compute_edge.post(
    "/execute", 
    summary="2. Execute Compute", 
    response_model=ExecuteComputeResponse
)
async def execute_compute(req: ExecuteComputeRequest, broker: DphiBroker = Depends(get_wasm_broker)):
    res = await broker.execute(code=req.code, variables=req.variables)
    
    if not res.success:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=res.error.message)
        
    return ExecuteComputeResponse(status=EdgeState.EXECUTION_SUCCESS, output=res.output)

@compute_edge.post(
    "/proof/generate", 
    summary="3. Generate Proof", 
    response_model=ProofGenerationResponse
)
async def generate_proof(req: ProofGenerationRequest, broker: DphiBroker = Depends(get_wasm_broker)):
    canonical_payload = StateAdapter.to_canonical_bytes(req.model_dump()).decode('utf-8')
    res = await broker.invoke("generate_proof", canonical_payload)
    
    if not res.success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.error.message)
        
    return ProofGenerationResponse(status=EdgeState.PROOF_GENERATED, zk_receipt=json.loads(res.output))

# =====================================================================
# 2. Eco Exchange
# =====================================================================
@exchange_edge.post(
    "/order/ingress", 
    summary="거래 인텐트 인입 및 Session 발급", 
    response_model=TradeIngressResponse
)
async def submit_trade_intent(
    req: TradeIngressRequest,
    broker: DphiBroker = Depends(get_wasm_broker),
    policy_engine: IngressPolicyEngine = Depends(get_ingress_policy)
):
    context = await policy_engine.resolve_context(agent_id=req.agent_id, action=req.action)

    if context.is_ruptured:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Topology Ruptured: {context.reason}")

    press_limit = context.press_limit if hasattr(context, 'press_limit') and context.press_limit > 0 else tier_config.fallback_fuel
    payload_obj = EpochInitPayload(
        ts=int(time.time() * 1000), topo=context.topo_id, press=press_limit,
        rupture=context.is_ruptured, injected_intent=req
    )
    
    canonical_payload = StateAdapter.to_canonical_bytes(payload_obj.model_dump()).decode('utf-8')
    res = await broker.invoke("init_epoch", canonical_payload)
    
    if not res.success:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
        
    return TradeIngressResponse(status=EdgeState.INTENT_ACCEPTED, session=json.loads(res.output))

@exchange_edge.post(
    "/clearing/receipt/generate", 
    summary="외부 네트워크용 정산 영수증 발급", 
    response_model=ClearingReceiptResponse
)
async def generate_external_receipt(
    req: ClearingReceiptRequest,
    exchange: ExchangeAdapter = Depends(get_exchange_adapter)
):
    receipt = exchange.finalize_settlement(
        entangled_state=req.entangled_state, 
        signatures=req.signatures,
        cost_metrics=req.cost_metrics, 
        tier=Tier.SYSTEM  
    )
    
    external_payload = exchange.generate_settlement_payload(receipt)
    return ClearingReceiptResponse(status=EdgeState.RECEIPT_GENERATED, rollup_payload=external_payload)

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

eco_router = APIRouter(prefix="/v1/eco")
eco_router.include_router(compute_edge)
eco_router.include_router(exchange_edge)
eco_router.include_router(profile_edge)