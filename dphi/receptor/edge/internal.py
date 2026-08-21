# dphi.receptor.edge.internal
## @lineage: receptor.edge.internal
import json
import time
import uuid
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Body, status, Depends, HTTPException, Query
from pydantic import BaseModel

from dphi.adapter.anchor import NexusAnchor, AnchorProposal, StreamAppendRequest, LedgerEventSchema
from arch.xor.stream.edge import LogStreamStore
from dphi.receptor.ingress.gov.policy import IngressPolicyEngine, get_ingress_policy
from dphi.receptor.edge.depend import get_wasm_broker, get_logstream_store, get_nexus_anchor, get_exchange_adapter
from bound.space.sandbox.profile import BenchProfile, VerificationError

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

from watcher.receptor.contract.model import (
    BilledExecutionRequest,
    BilledExecutionResponse,
    KernelLedgerAppendRecord
)

from kernel.dphi.broker import DphiBroker, DphiMethod
from kernel.dphi.exchange.transaction import ExchangeAdapter
from kernel.dphi.cgroup import Tier
from kernel.dphi.exchange.config import tier_config, billing_config
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.internal")

internal_router = APIRouter(prefix="/v1")

core_edge = ContractRouter(namespace="core.internal", prefix="/core", tags=["Internal Core (Ledger & Anchor)"])
compute_edge = ContractRouter(namespace="eco.compute", prefix="/eco/compute", tags=["Internal Eco Compute"])
exchange_edge = ContractRouter(namespace="eco.exchange", prefix="/eco/exchange", tags=["Internal Eco Exchange"])
profile_edge = ContractRouter(namespace="eco.profile", prefix="/eco/profile", tags=["Internal Eco Profile"])

class StreamAppendResult(BaseModel):
    hash: str
    membership_proof: Optional[str] = None

class StreamAppendResponse(BaseModel):
    request_id: str
    status: str
    result: StreamAppendResult

class QuotationResponse(BaseModel):
    status: str
    tier_applied: str
    fuel_estimated: int
    estimated_cost_usd: float
    reason: Optional[str] = None


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
    request_id = f"ledg_{uuid.uuid4().hex[:8]}"
    with flow_scope(phase="LEDGER_INTERNAL_APPEND", bound="edge.internal", req_id=request_id):
        events_dicts = [e.model_dump(exclude_none=True) for e in req.events]
        
        is_authorized = await store.bulk_append(stream_name=req.stream_name, events=events_dicts)
        if not is_authorized:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Kernel Blocked Stream Append")
            
        payload_to_hash = KernelLedgerAppendRecord(
            stream_name=req.stream_name,
            timestamp=int(time.time() * 1000),
            events=events_dicts
        ).model_dump(exclude_none=True)
        
        fp_res = await broker.invoke(DphiMethod.COMPUTE_ROOT_FINGERPRINT, payload_to_hash)
        if not fp_res.success:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"WASM Fingerprint Failed: {fp_res.error}")
            
        event_hash = json.loads(fp_res.output)["fingerprint"]
        merkle_proof = None
        if req.verbose:
            proof_res = await broker.invoke(DphiMethod.GENERATE_PROOF, payload_to_hash)
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

@compute_edge.post("/intent/validate", summary="[Internal] Validate Intent", response_model=IntentValidationResponse)
async def validate_intent(req: IntentValidationRequest):
    # 1. 필수 경계값 검증
    if not req.requester_id or not req.action:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Missing Critical Boundaries (Agent ID or Action)")

    if req.max_fuel_budget and req.max_fuel_budget > 10_000_000:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Topological Fuel Limit Exceeded (> 10M)")

    if not getattr(req, 'signature', None):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Signature missing. Request must be cryptographically signed.")

    # 2. Edge와 Client 간 약속된 검증용 평문 메시지 조립
    expected_msg = f"EXECUTE:{req.requester_id}:{req.action}:{req.max_fuel_budget or 1000000}"
    
    try:
        # 3. 알고리즘 식별 및 서명 검증 로직 분기
        sig_algo = getattr(req, 'sig_algo', 'ECDSA_SECP256K1').upper()
        
        if sig_algo == "ECDSA_SECP256K1":
            from eth_account import Account
            from eth_account.messages import encode_defunct
            
            msg_hash = encode_defunct(text=expected_msg)
            # ecrecover를 통해 메시지에 서명한 주소 도출
            recovered_address = Account.recover_message(msg_hash, signature=req.signature)
            
            # 서명자 주소와 페이로드의 요청자 ID(지갑 주소)가 일치하는지 대조
            if recovered_address.lower() != req.requester_id.lower():
                log.warning(f"Signature mismatch: Recovered {recovered_address} != Expected {req.requester_id}")
                raise ValueError("Address mismatch")
                
        elif sig_algo == "ED25519":
            # TODO: CosmWasm 등 Ed25519 체계 검증 로직 확장을 위한 Placeholder
            log.warning("Ed25519 verification is currently bypassed in mock mode.")
            pass
            
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=f"Unsupported signature algorithm: {sig_algo}")
            
    except ValueError as ve:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="WASM Rejected Intent: CRYPTOGRAPHIC_SIGNATURE_MISMATCH")
    except Exception as e:
        log.error(f"Intent validation crashed: {str(e)}")
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Malformed cryptographic signature")

    # 4. 검증 통과 시 인가 데이터 생성
    clearance_data = {
        "is_valid": True,
        "verified_at": int(time.time() * 1000),
        "agent": req.requester_id,
        "fuel_authorized": req.max_fuel_budget
    }

    return IntentValidationResponse(status=EdgeState.INTENT_VALIDATED, clearance=clearance_data)


@compute_edge.post("/execute", summary="[Internal] Execute Compute", response_model=ExecuteComputeResponse)
async def execute_compute(req: ExecuteComputeRequest, broker: DphiBroker = Depends(get_wasm_broker)):
    res = await broker.execute(code=req.code, variables=req.variables)
    if not res.success:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(res.error))
    return ExecuteComputeResponse(status=EdgeState.EXECUTION_SUCCESS, output=res.output)


@compute_edge.post("/proof/generate", summary="[Internal] Generate Proof", response_model=ProofGenerationResponse)
async def generate_proof(req: ProofGenerationRequest, broker: DphiBroker = Depends(get_wasm_broker)):
    res = await broker.invoke(DphiMethod.GENERATE_PROOF, req.model_dump(exclude_none=True))
    if not res.success:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(res.error))
    return ProofGenerationResponse(status=EdgeState.PROOF_GENERATED, zk_receipt=json.loads(res.output))


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
    
    res = await broker.invoke(DphiMethod.INIT_EPOCH, payload_obj.model_dump(exclude_none=True))
    if not res.success:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(res.error))
    return TradeIngressResponse(status=EdgeState.INTENT_ACCEPTED, session=json.loads(res.output))


@exchange_edge.post("/clearing/receipt/generate", summary="[Internal] 외부 네트워크용 정산 영수증 발급", response_model=ClearingReceiptResponse)
async def generate_external_receipt(req: ClearingReceiptRequest, exchange: ExchangeAdapter = Depends(get_exchange_adapter)):
    receipt = exchange.finalize_settlement(
        entangled_state=req.entangled_state, signatures=req.signatures,
        cost_metrics=req.cost_metrics, tier=Tier.SYSTEM  
    )
    return ClearingReceiptResponse(status=EdgeState.RECEIPT_GENERATED, rollup_payload=exchange.generate_settlement_payload(receipt))


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
    except VerificationError as ve:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail=str(ve))
        
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
    except VerificationError as ve:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail=str(ve))
        
    billed_cost = (result.fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
    return BilledExecutionResponse(
        status="BILLED_EXECUTION_SUCCESS" if result.status == "COHERENCE" else "BILLED_EXECUTION_FAILED", 
        tier_applied=result.tier_applied, fuel_billed=result.fuel_consumed,
        billed_cost_usd=billed_cost, reason=result.reason
    )

internal_router.include_router(core_edge)
internal_router.include_router(compute_edge)
internal_router.include_router(exchange_edge)
internal_router.include_router(profile_edge)