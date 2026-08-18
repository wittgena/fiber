# receptor.edge.internal
import json
import time
import uuid
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Body, status, Depends, HTTPException, Query
from pydantic import BaseModel

from phase.anchor.adapter.dphi import NexusAnchor, AnchorProposal
from receptor.stream.store import LogStreamStore
from receptor.ingress.gov.policy import IngressPolicyEngine, get_ingress_policy
from receptor.xe.depend import get_wasm_broker, get_logstream_store, get_nexus_anchor, get_exchange_adapter
from receptor.xe.profile import BenchProfile

from arch.contract.interface import ContractRouter
from arch.contract.model.receptor import (
    EdgeState,
    AnchorProposalRequest, AnchorSealResponse,
    IntentValidationRequest, IntentValidationResponse,
    ExecuteComputeRequest, ExecuteComputeResponse,
    ProofGenerationRequest, ProofGenerationResponse,
    TradeIngressRequest, TradeIngressResponse,
    EpochInitPayload,
    ClearingReceiptRequest, ClearingReceiptResponse
)

from kernel.dphi.broker import DphiBroker, DphiMethod
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.exchange import ExchangeAdapter
from kernel.dphi.cgroup import Tier
from kernel.dphi.exchange.config import tier_config, billing_config
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.internal")

internal_router = APIRouter(prefix="/v1")

# Sub-routers
core_edge = ContractRouter(namespace="core.internal", prefix="/core", tags=["Internal Core (Ledger & Anchor)"])
compute_edge = ContractRouter(namespace="eco.compute", prefix="/eco/compute", tags=["Internal Eco Compute"])
exchange_edge = ContractRouter(namespace="eco.exchange", prefix="/eco/exchange", tags=["Internal Eco Exchange"])
profile_edge = ContractRouter(namespace="eco.profile", prefix="/eco/profile", tags=["Internal Eco Profile"])


# =====================================================================
# [Data Models] Internal Data Structures
# =====================================================================
class LedgerEventSchema(BaseModel):
    action: str
    user_id: str
    pii_data: Optional[Dict[str, Any]] = None
    details: str

class StreamAppendRequest(BaseModel):
    stream_name: str
    events: List[LedgerEventSchema]
    verbose: bool = False

class StreamAppendResult(BaseModel):
    hash: str
    membership_proof: Optional[str] = None

class StreamAppendResponse(BaseModel):
    request_id: str
    status: str
    result: StreamAppendResult

class MandateRegisterRequest(BaseModel):
    agent_id: str
    max_spend_usdc: str
    expiration_ts: int
    signature: str

class MandateRegisterResponse(BaseModel):
    receipt_id: str
    status: str

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


# =====================================================================
# 1. Core (Ledger & Anchor)
# =====================================================================
@core_edge.post(
    "/ledger/stream/append", 
    status_code=status.HTTP_200_OK,
    summary="[Internal] Immutable Ledger Stream Bulk Append",
    response_model=StreamAppendResponse
)
async def append_to_stream(
    req: StreamAppendRequest = Body(...),
    broker: DphiBroker = Depends(get_wasm_broker),
    store: LogStreamStore = Depends(get_logstream_store)
):
    """내부 마이크로서비스들이 불변 원장 스트림에 이벤트를 안전하게 추가하기 위해 호출"""
    request_id = f"ledg_{uuid.uuid4().hex[:8]}"
    with flow_scope(phase="LEDGER_INTERNAL_APPEND", bound="edge.internal", req_id=request_id):
        events_dicts = [e.model_dump(exclude_none=True) for e in req.events]
        
        is_authorized = await store.bulk_append(stream_name=req.stream_name, events=events_dicts)
        if not is_authorized:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Kernel Blocked Stream Append")
            
        payload_to_hash = {
            "stream_name": req.stream_name,
            "timestamp": int(time.time() * 1000),
            "events": events_dicts
        }
        canonical_payload = StateAdapter.to_canonical_bytes(payload_to_hash).decode('utf-8')
        fp_res = await broker.invoke(DphiMethod.COMPUTE_ROOT_FINGERPRINT, canonical_payload)
        
        if not fp_res.success:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"WASM Fingerprint Failed: {fp_res.error}")
            
        event_hash = json.loads(fp_res.output)["fingerprint"]
        merkle_proof = None
        if req.verbose:
            proof_res = await broker.invoke(DphiMethod.GENERATE_PROOF, canonical_payload)
            if proof_res.success:
                merkle_proof = json.loads(proof_res.output).get("current_hash")
                
        return StreamAppendResponse(
            request_id=request_id, status="success",
            result=StreamAppendResult(hash=event_hash, membership_proof=merkle_proof)
        )


@core_edge.post(
    "/anchor/seal", 
    summary="[Internal] 상태 합의 및 영수증 방출 (Seal Epoch)",
    response_model=AnchorSealResponse
)
async def seal_state(req: AnchorProposalRequest, nexus: NexusAnchor = Depends(get_nexus_anchor)):
    """Public Gateway가 연산을 마치고 원장에 기록 및 영수증을 발행할 때 호출하는 내부 API"""
    proposal = AnchorProposal(
        receptor_id=req.receptor_id, proposed_parity=req.proposed_parity.model_dump(),
        parent_nexus_id=req.parent_nexus_id, self_parent_state=req.self_parent_state,
        repos=req.repos, signers=req.signers, signatures=req.signatures, timestamp=req.timestamp
    )
    result = await nexus.anchor_state(proposal)
    if not result.is_sealed:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=f"Consensus Failed: {result.rupture_reason}")
        
    return AnchorSealResponse(
        status=EdgeState.SEALED_AND_COMMITTED, nexus_id=result.nexus_id,
        commit_hash=result.commit_hash, 
        receipt=result.receipt.__dict__ if hasattr(result.receipt, "__dict__") else dict(result.receipt)
    )


# =====================================================================
# 2. Eco Compute (Execution & Validation)
# =====================================================================
@compute_edge.get("/budget/verify", summary="[Internal] Tollgate용 X402 예산 잔고 확인")
async def verify_budget(
    receipt: str = Query(..., description="X402 영수증 해시"),
    cost: float = Query(..., description="차감 예정 예상 비용 (USDC)"),
    broker: DphiBroker = Depends(get_wasm_broker)
):
    """edge.public의 톨게이트가 호출하여 롤업 원장 내 예산을 검증"""
    payload = json.dumps({"receipt_id": receipt, "required_cost": cost})
    res = await broker.invoke("verify_ledger_budget", payload)
    
    if not res.success or not json.loads(res.output).get("is_sufficient"):
        raise HTTPException(status.HTTP_402_PAYMENT_REQUIRED, detail="Insufficient internal budget")
        
    return {"status": "BUDGET_SUFFICIENT"}


@compute_edge.post("/intent/validate", summary="[Internal] Validate Intent", response_model=IntentValidationResponse)
async def validate_intent(req: IntentValidationRequest, broker: DphiBroker = Depends(get_wasm_broker)):
    raw_payload = {**req.model_dump(), "timestamp": int(time.time() * 1000)}
    canonical_payload = StateAdapter.to_canonical_bytes(raw_payload).decode('utf-8')
    res = await broker.invoke("validate_intent", canonical_payload)
    if not res.success:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return IntentValidationResponse(status=EdgeState.INTENT_VALIDATED, clearance=json.loads(res.output))


@compute_edge.post("/execute", summary="[Internal] Execute Compute", response_model=ExecuteComputeResponse)
async def execute_compute(req: ExecuteComputeRequest, broker: DphiBroker = Depends(get_wasm_broker)):
    res = await broker.execute(code=req.code, variables=req.variables)
    if not res.success:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=res.error.message)
    return ExecuteComputeResponse(status=EdgeState.EXECUTION_SUCCESS, output=res.output)


@compute_edge.post("/proof/generate", summary="[Internal] Generate Proof", response_model=ProofGenerationResponse)
async def generate_proof(req: ProofGenerationRequest, broker: DphiBroker = Depends(get_wasm_broker)):
    canonical_payload = StateAdapter.to_canonical_bytes(req.model_dump()).decode('utf-8')
    res = await broker.invoke("generate_proof", canonical_payload)
    if not res.success:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.error.message)
    return ProofGenerationResponse(status=EdgeState.PROOF_GENERATED, zk_receipt=json.loads(res.output))


# =====================================================================
# 3. Eco Exchange (Mandate & Clearing)
# =====================================================================
@exchange_edge.post("/mandate/register", summary="[Internal] 에이전트 서명 검증 및 원장 예산 충전", response_model=MandateRegisterResponse)
async def register_mandate(req: MandateRegisterRequest, broker: DphiBroker = Depends(get_wasm_broker)):
    """edge.public에서 넘어온 EIP-712 서명을 검증하고, 성공 시 Ledger에 Deposit 후 Capability Token 발급"""
    raw_payload = req.model_dump()
    canonical_payload = StateAdapter.to_canonical_bytes(raw_payload).decode('utf-8')
    
    res = await broker.invoke("register_mandate_and_deposit", canonical_payload)
    if not res.success:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
        
    receipt_data = json.loads(res.output)
    return MandateRegisterResponse(
        receipt_id=receipt_data["receipt_id"],
        status="REGISTERED_AND_FUNDED"
    )


@exchange_edge.post("/order/ingress", summary="[Internal] 거래 인텐트 인입 및 Session 발급", response_model=TradeIngressResponse)
async def submit_trade_intent(
    req: TradeIngressRequest, broker: DphiBroker = Depends(get_wasm_broker),
    policy_engine: IngressPolicyEngine = Depends(get_ingress_policy)
):
    context = await policy_engine.resolve_context(agent_id=req.agent_id, action=req.action)
    if context.is_ruptured:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Topology Ruptured: {context.reason}")

    press_limit = context.press_limit if hasattr(context, 'press_limit') and context.press_limit > 0 else tier_config.fallback_fuel
    payload_obj = EpochInitPayload(
        ts=int(time.time() * 1000), topo=context.topo_id, press=press_limit,
        rupture=context.is_ruptured, injected_intent=req
    )
    res = await broker.invoke("init_epoch", StateAdapter.to_canonical_bytes(payload_obj.model_dump()).decode('utf-8'))
    if not res.success:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return TradeIngressResponse(status=EdgeState.INTENT_ACCEPTED, session=json.loads(res.output))


@exchange_edge.post("/clearing/receipt/generate", summary="[Internal] 외부 네트워크용 정산 영수증 발급", response_model=ClearingReceiptResponse)
async def generate_external_receipt(req: ClearingReceiptRequest, exchange: ExchangeAdapter = Depends(get_exchange_adapter)):
    receipt = exchange.finalize_settlement(
        entangled_state=req.entangled_state, signatures=req.signatures,
        cost_metrics=req.cost_metrics, tier=Tier.SYSTEM  
    )
    return ClearingReceiptResponse(status=EdgeState.RECEIPT_GENERATED, rollup_payload=exchange.generate_settlement_payload(receipt))


# =====================================================================
# 4. Eco Profile (Billing & Quota)
# =====================================================================
async def extract_client_project(api_key: str = "test_key") -> str:
    return "generative-language-client-1234" 

def get_billing_profile_service() -> BenchProfile:
    return BenchProfile()

@profile_edge.post("/quote", summary="[Internal] Request execution quotation (Dry-run)", response_model=QuotationResponse)
async def request_quotation(
    req: BilledExecutionRequest, client_project_id: str = Depends(extract_client_project),
    profile_service: BenchProfile = Depends(get_billing_profile_service)
):
    try:
        result = await profile_service.execute(
            client_project_id=client_project_id, schema=req.agent_schema,
            entry=req.target_entry, depth=req.context_depth, dry_run=True 
        )
    except ValueError as e:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        
    estimated_cost = (result.fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
    return QuotationResponse(
        status="QUOTE_READY" if result.status == "COHERENCE" else "QUOTE_REJECTED", 
        tier_applied=result.tier_applied, fuel_estimated=result.fuel_consumed,
        estimated_cost_usd=estimated_cost, reason=result.reason
    )


@profile_edge.post("/execute/billed", summary="[Internal] Execute workload with account charging", response_model=BilledExecutionResponse)
async def execute_billed_workload(
    req: BilledExecutionRequest, client_project_id: str = Depends(extract_client_project),
    profile_service: BenchProfile = Depends(get_billing_profile_service)
):
    try:
        result = await profile_service.execute(
            client_project_id=client_project_id, schema=req.agent_schema,
            entry=req.target_entry, depth=req.context_depth, dry_run=False
        )
    except ValueError as e:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        
    billed_cost = (result.fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
    return BilledExecutionResponse(
        status="BILLED_EXECUTION_SUCCESS" if result.status == "COHERENCE" else "BILLED_EXECUTION_FAILED", 
        tier_applied=result.tier_applied, fuel_billed=result.fuel_consumed,
        billed_cost_usd=billed_cost, reason=result.reason
    )


# =====================================================================
# Main Router Inclusion
# =====================================================================
internal_router.include_router(core_edge)
internal_router.include_router(compute_edge)
internal_router.include_router(exchange_edge)
internal_router.include_router(profile_edge)