# fiber.dphi.receptor.edge.public
## @lineage: dphi.receptor.edge.public
"""
@desc: DPHI Edge Public Gateway
- Orchestrates WASM kernels and internal nodes for intent validation, metered execution, and immutable cryptographic receipts.
- Exposes a symmetric Zero-Trust interface for Autonomous AI Agents.
"""

import os
import json
import time
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import orjson
import httpx

from fastapi import APIRouter, Body, Header, Response, status, Depends, BackgroundTasks, HTTPException, Request, Query
from pydantic import BaseModel

from fiber.dphi.adapter.config import dphi_env
from fiber.dphi.adapter.anchor import NotarySwarm
from fiber.dphi.receptor.edge.depend import get_wasm_broker, get_pubsub, get_otlp_engine

from xphi.arch.contract.interface import ContractRouter
from xphi.arch.contract.model.receptor import EdgeState, EdgeHeader, IntentValidationRequest
from xphi.kernel.space.topos.tunnel.subs import DistributedPubSub
from xphi.arch.xor.parser.otlp import StrictOtlpExtractionEngine

from xphi.kernel.dphi.broker import DphiBroker, DphiMethod
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.adapter.sign import NodeSigner
from xphi.watcher.receptor.contract.model import (
    CodebotIntent, 
    AuditReceipt,
    ExportLogsServiceRequest, 
    AuditLogRequest, 
    AuditLogResponse, 
    AuditResult, 
    AuditEnvelope,
    BilledExecutionRequest,
    KernelExecutionRecord,
    KernelOtlpRecord
)
from xphi.watcher.receptor.audit.secret import SecretAuditor, get_secret_auditor
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.public")

public_edge = ContractRouter(
    namespace="public", 
    prefix="/v1/public", 
    tags=["Public Gateway"],
    description="Deterministic Zero-Trust Gateway for Agentic Workloads"
)

def get_internal_edge_url(request: Request) -> str:
    return request.app.state.config.internal_edge_url

"""DATA TRANSFER OBJECTS (DTO)"""
class InvoiceIssueRequest(BaseModel):
    payee_address: str
    amount_usdc: str
    resource_id: str

class AgentHandshakeResponse(BaseModel):
    status: str
    estimated_fuel: int
    estimated_cost_usd: float
    invoice: Dict[str, Any]
    macaroon: Optional[str] = None
    next_action: str = "POST /v1/public/agent/execute with X-X402-Receipt header"


"""BASE INFRASTRUCTURE (TRUST ANCHOR)"""
@public_edge.get(
    "/keys", 
    summary="Get Trusted Signer Keys (Strictly Pre-Signed)",
    description="Returns the list of active Edge nodes' public keys."
)
async def get_public_keys():
    """
    @desc: 서명자 목록 엔드포인트
    - 클라이언트 SDK의 VerifiedHttpClient가 서버 응답 검증을 위해 캐싱합니다.
    - 노드는 절대 Root Key를 갖지 않으며, 주입된 환경변수(사전 생성된 서명)만 서빙합니다.
    """
    active_signers_env = os.getenv("DPHI_ACTIVE_SIGNERS")
    root_signature = os.getenv("DPHI_PRE_SIGNED_ROOT_SIG")

    if not active_signers_env or not root_signature:
        log.critical("[Security] DPHI_ACTIVE_SIGNERS or DPHI_PRE_SIGNED_ROOT_SIG not configured. Rejecting request.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Security misconfiguration: Trusted registry is offline."
        )

    payload_dict = {"active_signers": [key.strip() for key in active_signers_env.split(",")]}
    
    return Response(
        content=orjson.dumps(payload_dict),
        media_type="application/json",
        headers={"X-Dphi-Root-Signature": root_signature}
    )


"""COMPUTE SYMMETRY (QUOTE ↔ EXECUTE)"""
@public_edge.post(
    "/agent/quote", 
    summary="Get Pre-flight Execution Quotation (Dry-run)"
)
async def public_agent_quote(
    intent: CodebotIntent,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt", description="Payment proof receipt (L402/X402)"),
    internal_url: str = Depends(get_internal_edge_url)
):
    """
    @desc: WASM 샌드박스에서 시뮬레이션을 돌려 예상되는 연료(Fuel) 소모량과 과금액을 사전 확인합니다.
    - 무료 티어 (영수증 없음): 서명 검증을 생략하고 기본 견적 시뮬레이션만 수행
    - 유료 티어 (영수증 있음): 사전 인텐트 및 서명 진위 여부 엄격 검증 (실패 시 422)
    """
    async with httpx.AsyncClient(base_url=internal_url, timeout=15.0) as internal_client:
        # 🌟 1. 유료/프리미엄 티어: 결제 영수증이 첨부된 경우 서명 및 인텐트 엄격 검증
        if x_x402_receipt:
            val_req = IntentValidationRequest(
                requester_id=intent.agent_id,
                responder_id=intent.responder_id or "edge-gateway-01",
                action=intent.action,
                max_fuel_budget=intent.max_fuel,
                signature=intent.signature,
                payment_receipt=x_x402_receipt
            )
            val_res = await internal_client.post(
                "/v1/eco/compute/intent/validate", 
                json=val_req.model_dump(exclude_none=True)
            )
            if val_res.status_code != 200:
                log.warning(f"[Quote] Premium intent verification failed: {val_res.text}")
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, 
                    detail=f"Intent Validation Failed: {val_res.text}"
                )

        # 🌟 2. 견적 시뮬레이션 요청 조립
        exec_req = {
            "agent_schema": {
                "runtime": "python3.11-wasm",
                "files": {"main.py": intent.source_code}, 
                "limits": {"max_fuel": intent.max_fuel}
            },
            "target_entry": "main.py",
            "context_depth": 2
        }
        
        try:
            res = await internal_client.post("/v1/eco/profile/quote", json=exec_req)
            if res.status_code != 200:
                raise HTTPException(status_code=res.status_code, detail=f"Quotation Failed: {res.text}")
            
            quote_data = res.json()
            
            # 🌟 3. 내부 샌드박스에서 거절(REJECTED)된 경우 422 상태코드로 방어
            if quote_data.get("status") == "QUOTE_REJECTED":
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Quotation Rejected: {quote_data.get('reason', 'Unknown execution policy violation')}"
                )
                
            return quote_data
        except httpx.RequestError as e:
            log.error(f"Internal Network Error to {internal_url}: {e}")
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, f"Internal Edge Network Down ({internal_url})")


@public_edge.post(
    "/agent/execute", 
    summary="Execute Billed AI Agent Intent & Issue Cryptographic Proof-of-Action",
    response_model=AuditReceipt
)
async def public_agent_execute(
    intent: CodebotIntent,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt", description="Payment proof receipt (L402/X402)"),
    internal_url: str = Depends(get_internal_edge_url),
    broker: DphiBroker = Depends(get_wasm_broker)
):
    """
    @desc: Orchestrates metered AI intent execution and WASM state transitions.
    - L402 결제 영수증 확인 후 샌드박스 연산을 수행하고, 불변의 AuditReceipt를 발행합니다.
    """
    request_id = f"cbot_{uuid.uuid4().hex[:8]}"
    with flow_scope(phase="GATEWAY_ORCHESTRATION", bound="edge.public", req_id=request_id):
        async with httpx.AsyncClient(base_url=internal_url, timeout=15.0) as internal_client:
            try:
                val_req = IntentValidationRequest(
                    requester_id=intent.agent_id,
                    responder_id=intent.responder_id or "edge-gateway-01",
                    action=intent.action,
                    max_fuel_budget=intent.max_fuel,
                    signature=intent.signature,
                    payment_receipt=x_x402_receipt
                )
                val_res = await internal_client.post("/v1/eco/compute/intent/validate", json=val_req.model_dump(exclude_none=True))
                if val_res.status_code != 200:
                    raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"Intent Rejected: {val_res.text}")

                exec_req = BilledExecutionRequest(
                    agent_schema={
                        "runtime": "python3.11-wasm",
                        "files": {"main.py": intent.source_code}, 
                        "limits": {"max_fuel": intent.max_fuel}
                    },
                    target_entry="main.py",
                    context_depth=2
                )
                exec_res = await internal_client.post("/v1/eco/profile/execute/billed", json=exec_req.model_dump())
                if exec_res.status_code != 200:
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"Compute Failed: {exec_res.text}")
                
                exec_data = exec_res.json()
                fuel_metered = exec_data.get("fuel_billed", 0)
                cost_usd = exec_data.get("billed_cost_usd", 0.0)
                exec_hash = hashlib.sha256(json.dumps(exec_data).encode()).hexdigest()
                repos = {"vm_trace_hash": exec_hash, "metered_fuel": str(fuel_metered)}

                swarm = NotarySwarm(size=3)
                canonical_hash = StateAdapter.to_canonical_bytes(repos)
                commit_hash_bytes = hashlib.sha256(canonical_hash).digest()
                attested_signatures = swarm.attest_payload(commit_hash_bytes)

                kernel_req_dict = KernelExecutionRecord(
                    receipt_id=request_id,
                    repos=repos,
                    signatures=attested_signatures,
                    timestamp=float(time.time() * 1000)
                ).model_dump(exclude_none=True)
                evo_ctx = StateAdapter.build_evolution_context(phase_root={})
                transition_payload = StateAdapter.build_transition_payload(
                    intent_action="record_agent_execution",
                    intent_payload=kernel_req_dict,
                    evolution_ctx=evo_ctx
                )

                canonical_payload = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
                fp_res = await broker.invoke(DphiMethod.COMPUTE_ROOT_FINGERPRINT, canonical_payload)
                if not fp_res.success:
                    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Kernel Record Failed: {fp_res.error}")
                
                commit_hash = json.loads(fp_res.output).get("fingerprint", "0x_sealed_root")
                return AuditReceipt(
                    receipt_id=request_id,
                    receipt_type="Proof-of-Agent-Action",
                    status="SUCCESS",
                    fuel_consumed=fuel_metered,
                    metered_cost_usd=cost_usd,
                    state_root=commit_hash,
                    audit_trail=["Gateway", "PolicyEngine", "WasmSandbox", "CoreLedger"]
                )
            except httpx.RequestError as e:
                log.error(f"Internal Network Error to {internal_url}: {e}")
                raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, f"Internal Edge Network Down ({internal_url})")


"""ECONOMY SYMMETRY (INVOICE ↔ BALANCE & HANDSHAKE)"""
@public_edge.post(
    "/agent/handshake", 
    summary="Agent Pre-flight Handshake (Quote & Invoice)",
    response_model=AgentHandshakeResponse
)
async def public_agent_handshake(
    intent: CodebotIntent,
    internal_url: str = Depends(get_internal_edge_url)
):
    """
    @desc: Orchestrates Quotation and Invoice Issue.
    - 실행할 인텐트의 비용을 사전 계산하고, 지불을 위한 L402 청구서를 통합 발급합니다.
    """
    async with httpx.AsyncClient(base_url=internal_url, timeout=15.0) as internal_client:
        try:
            # 1. Quote
            quote_req = {
                "agent_schema": {
                    "runtime": "python3.11-wasm",
                    "files": {"main.py": intent.source_code}, 
                    "limits": {"max_fuel": intent.max_fuel}
                },
                "target_entry": "main.py",
                "context_depth": 2
            }
            quote_res = await internal_client.post("/v1/eco/profile/quote", json=quote_req)
            if quote_res.status_code != 200:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Quotation Failed: {quote_res.text}")
            
            quote_data = quote_res.json()
            cost_usd = quote_data.get("estimated_cost_usd", 0.0)
            fuel = quote_data.get("fuel_estimated", 0)

            # 2. Invoice
            invoice_req = {
                "payee_address": "0x000000000000000000000000000000000000dEaD",
                "amount_usdc": str(cost_usd),
                "resource_id": f"res_intent_{uuid.uuid4().hex[:8]}"
            }
            invoice_res = await internal_client.post("/v1/eco/exchange/invoice/issue", json=invoice_req)
            if invoice_res.status_code != 200:
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Invoice Issue Failed: {invoice_res.text}")
            
            invoice_data = invoice_res.json()

            return AgentHandshakeResponse(
                status="HANDSHAKE_READY",
                estimated_fuel=fuel,
                estimated_cost_usd=cost_usd,
                invoice=invoice_data.get("invoice", {}),
                macaroon=invoice_data.get("macaroon")
            )
        except httpx.RequestError as e:
            log.error(f"Internal Network Error to {internal_url}: {e}")
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Internal Edge Network Down")

@public_edge.post(
    "/billing/invoice", 
    summary="Issue L402 Invoice for Resource Access"
)
async def public_issue_invoice(
    req: InvoiceIssueRequest,
    internal_url: str = Depends(get_internal_edge_url)
):
    """@desc: 자원 소비 전 지불해야 할 금액과 결제 목적지 정보를 담은 독립된 인보이스 요청"""
    async with httpx.AsyncClient(base_url=internal_url, timeout=10.0) as internal_client:
        try:
            res = await internal_client.post("/v1/eco/exchange/invoice/issue", json=req.model_dump())
            if res.status_code != 200:
                raise HTTPException(res.status_code, detail=f"Invoice Issue Failed: {res.text}")
            return res.json()
        except httpx.RequestError as e:
            log.error(f"Internal Network Error to {internal_url}: {e}")
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Internal Edge Network Down")

@public_edge.get(
    "/billing/balance", 
    summary="Check UTXO Fuel Balance"
)
async def public_get_balance(
    agent_id: str = Query(..., description="조회할 에이전트 주소"),
    asset_type: str = Query("fuel", description="조회할 자산 타입"),
    internal_url: str = Depends(get_internal_edge_url)
):
    """@desc: 에이전트의 현재 인메모리 연료(Fuel) 잔고를 원장 조회 없이 실시간 O(N)으로 확인"""
    async with httpx.AsyncClient(base_url=internal_url, timeout=10.0) as internal_client:
        try:
            res = await internal_client.get(
                "/v1/eco/exchange/balance", 
                params={"agent_id": agent_id, "asset_type": asset_type}
            )
            if res.status_code != 200:
                raise HTTPException(res.status_code, detail=f"Balance Check Failed: {res.text}")
            return res.json()
        except httpx.RequestError as e:
            log.error(f"Internal Network Error to {internal_url}: {e}")
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Internal Edge Network Down")

"""COMPLIANCE SYMMETRY (RECORD ↔ VERIFY)"""
@public_edge.post(
    "/telemetry/logs", 
    tags=["Log Ingress"], 
    summary="Ingest OTLP Telemetry, Verify Integrity & Seal Global Stream",
    status_code=status.HTTP_200_OK
)
async def public_otlp_logs_export(
    payload: ExportLogsServiceRequest = Body(...),
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    bg_tasks: BackgroundTasks = BackgroundTasks(),
    pubsub: DistributedPubSub = Depends(get_pubsub),
    broker: DphiBroker = Depends(get_wasm_broker),
    otlp_engine: StrictOtlpExtractionEngine = Depends(get_otlp_engine)
):
    """
    @desc: Extracts OTLP metrics and generates secure kernel fingerprints before delegating payloads to pub/sub.
    """
    try:
        payload_dict = payload.model_dump(exclude_none=True)
        raw_json_bytes = orjson.dumps(payload_dict)
        content_hash = hashlib.sha256(raw_json_bytes).hexdigest()
        
        try:
            extracted_metrics = otlp_engine.execute(raw_json_bytes)
        except ValueError as e:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))

        kernel_req_dict = KernelOtlpRecord(
            content_hash=content_hash,
            metrics_summary=extracted_metrics,
            receipt_ref=x_x402_receipt
        ).model_dump(exclude_none=True)
        evo_ctx = StateAdapter.build_evolution_context(phase_root={})
        transition_payload = StateAdapter.build_transition_payload(
            intent_action="record_otlp_telemetry",
            intent_payload=kernel_req_dict,
            evolution_ctx=evo_ctx
        )

        canonical_payload = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
        res = await broker.invoke(DphiMethod.COMPUTE_ROOT_FINGERPRINT, canonical_payload)
        if not res.success:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Kernel Seal Rejected")
            
        fingerprint = orjson.loads(res.output).get("fingerprint")
        bg_tasks.add_task(pubsub.publish_batch, topic="otlp_global_stream", events=[payload_dict])
        
        return Response(
            status_code=status.HTTP_200_OK, 
            headers={
                EdgeHeader.STATE: EdgeState.SUCCESS,
                EdgeHeader.CONTENT_HASH: content_hash,
                EdgeHeader.FINGERPRINT: fingerprint
            }, 
            content=b"{}"
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[Public OTLP] Processing failed: {str(e)}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Stream processing error")


@public_edge.post(
    "/audit/event", 
    tags=["Log Ingress"], 
    summary="Secure Audit Event Recording & Conditional Cryptographic Proof Issuance"
)
async def public_audit_log(
    payload: AuditLogRequest,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    secret_auditor: SecretAuditor = Depends(get_secret_auditor),
    broker: DphiBroker = Depends(get_wasm_broker)
) -> AuditLogResponse:
    """
    @desc: Encrypts sensitive audit events for ledger recording, issuing Merkle/ZK proofs upon request.
    """
    request_time = str(time.time())
    event_dict = payload.event.model_dump(exclude_none=True)
    sanitized_event = secret_auditor._encrypt_sensitive_data(event_dict)
    sanitized_event["_billing_ref"] = x_x402_receipt
    
    evo_ctx = StateAdapter.build_evolution_context(phase_root={})
    transition_payload = StateAdapter.build_transition_payload(
        intent_action="record_audit_event",
        intent_payload=sanitized_event,
        evolution_ctx=evo_ctx
    )

    canonical_payload = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
    fp_res = await broker.invoke(DphiMethod.COMPUTE_ROOT_FINGERPRINT, canonical_payload)
    
    if not fp_res.success:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to compute kernel fingerprint")
        
    event_hash = json.loads(fp_res.output)["fingerprint"]
    merkle_proof = None
    
    if payload.verbose:
        proof_res = await broker.invoke(DphiMethod.GENERATE_PROOF, canonical_payload)
        if proof_res.success:
            merkle_proof = json.loads(proof_res.output).get("current_hash")

    envelope = AuditEnvelope(event=payload.event, received_at=datetime.now(timezone.utc).isoformat())
    audit_result = AuditResult(
        envelope=envelope, hash=event_hash, membership_proof=merkle_proof, consistency_proof=[]
    )
    
    return AuditLogResponse(
        request_id=f"req_{uuid.uuid4().hex[:8]}",
        request_time=request_time,
        response_time=str(time.time()),
        status="success",
        result=audit_result
    )

@public_edge.post(
    "/audit/verify", 
    summary="Verify AuditReceipt Authenticity"
)
async def public_audit_verify(
    receipt: Dict[str, Any] = Body(...),
    internal_url: str = Depends(get_internal_edge_url)
):
    """
    @desc: 감사관(Auditor)이 발행받은 AuditReceipt의 수학적 위변조 여부를 커널을 통해 교차 증명(Verify)
    """
    async with httpx.AsyncClient(base_url=internal_url, timeout=10.0) as internal_client:
        try:
            res = await internal_client.post("/v1/core/ledger/verify", json=receipt)
            if res.status_code != 200:
                raise HTTPException(res.status_code, detail=f"Verification Failed: {res.text}")
            return res.json()
        except httpx.RequestError as e:
            log.error(f"Internal Network Error to {internal_url}: {e}")
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Internal Edge Network Down")