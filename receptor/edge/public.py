# receptor.edge.public
import json
import time
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import orjson
import httpx

from fastapi import APIRouter, Body, Header, Response, status, Depends, BackgroundTasks, HTTPException, Request

from phase.anchor.config.client import NotarySwarm
from phase.anchor.config.dphi import dphi_env
from receptor.xe.depend import get_wasm_broker, get_pubsub, get_otlp_engine

from arch.contract.interface import ContractRouter
from arch.contract.model.receptor import EdgeState, EdgeHeader
from arch.topos.tunnel.subs import DistributedPubSub
from arch.xor.parser.otlp import StrictOtlpExtractionEngine

from kernel.dphi.broker import DphiBroker
from kernel.dphi.adapter.state import StateAdapter
from watcher.receptor.contract.model import (
    AgentMandateRequest, 
    CapabilityReceiptResponse,
    CodebotIntent, 
    AuditReceipt,
    ExportLogsServiceRequest, 
    AuditLogRequest, 
    AuditLogResponse, 
    AuditResult, 
    AuditEnvelope
)
from watcher.receptor.audit.secret import SecretAuditor, get_secret_auditor
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.public")

public_edge = ContractRouter(namespace="public", prefix="/v1/public", tags=["Public Gateway"])


# =====================================================================
# [Middleware & Interceptor] 공통 의존성 및 결제 수문장(Tollgate)
# =====================================================================
def get_internal_edge_url(request: Request) -> str:
    """Boot 타임에 주입된 App State에서 동적으로 내부망 URL을 획득합니다."""
    return request.app.state.config.internal_edge_url

async def verify_x402_budget(receipt_hash: Optional[str], estimated_cost: float, internal_url: str):
    """
    [Tollgate Interceptor] 
    헤더에 담긴 X402 영수증을 내부망에 쿼리하여 예산(Allowance)이 충분한지 기계적으로 검증합니다.
    잔고가 부족하거나 영수증이 없으면 즉각 HTTP 402 에러를 반환합니다.
    """
    if not receipt_hash:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail={
                "error": "Payment Required (Capability Token Missing)",
                "payment_endpoint": "/v1/public/billing/mandate",
                "required_usdc": str(estimated_cost)
            }
        )
    
    async with httpx.AsyncClient(base_url=internal_url, timeout=5.0) as client:
        try:
            res = await client.get(f"/v1/eco/budget/verify?receipt={receipt_hash}&cost={estimated_cost}")
            if res.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_402_PAYMENT_REQUIRED,
                    detail={
                        "error": "Insufficient Off-chain Funds or Expired Mandate",
                        "payment_endpoint": "/v1/public/billing/mandate"
                    }
                )
        except httpx.RequestError as e:
            log.error(f"[X402 Interceptor] Failed to reach internal ledger: {e}")
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail="Billing Service Offline")


# =====================================================================
# [Endpoints] M2M API Gateway Routes
# =====================================================================

@public_edge.post(
    "/billing/mandate", 
    summary="[Gateway] 오프체인 서명(Mandate) 제출 및 X402 예치 영수증 발급",
    response_model=CapabilityReceiptResponse
)
async def public_submit_mandate(
    mandate: AgentMandateRequest,
    internal_url: str = Depends(get_internal_edge_url)
):
    request_id = f"mandate_{uuid.uuid4().hex[:8]}"
    with flow_scope(phase="MANDATE_REGISTRATION", bound="edge.public", req_id=request_id):
        log.info(f"Received off-chain mandate from agent: {mandate.agent_id} for {mandate.max_spend_usdc} USDC")
        
        async with httpx.AsyncClient(base_url=internal_url, timeout=10.0) as internal_client:
            # 내부망(EcoAdapter/KernelLedger)에 서명 검증 및 예산 충전(Deposit) 위임
            res = await internal_client.post("/v1/eco/mandate/register", json=mandate.model_dump())
            
            if res.status_code != 200:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Mandate Rejected: {res.text}")
                
            receipt_data = res.json()
            return CapabilityReceiptResponse(
                receipt_id=receipt_data["receipt_id"], # 에이전트가 향후 재사용할 세션 해시
                status="ACTIVE",
                budget_usdc=mandate.max_spend_usdc,
                issued_at=datetime.now(timezone.utc).isoformat()
            )


@public_edge.post(
    "/agent/execute", 
    summary="[Gateway] AI 에이전트 인텐트 단일 실행 (X402 과금 연동)",
    response_model=AuditReceipt
)
async def public_agent_execute(
    intent: CodebotIntent,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt", description="지불 증명 영수증"),
    internal_url: str = Depends(get_internal_edge_url),
    broker: DphiBroker = Depends(get_wasm_broker)
):
    # 1. 톨게이트: 실행 전 X402 예산 검증 (가스비 추산)
    estimated_cost = intent.max_fuel * 0.00001
    await verify_x402_budget(x_x402_receipt, estimated_cost, internal_url)

    request_id = f"cbot_{uuid.uuid4().hex[:8]}"
    with flow_scope(phase="GATEWAY_ORCHESTRATION", bound="edge.public", req_id=request_id):
        async with httpx.AsyncClient(base_url=internal_url, timeout=15.0) as internal_client:
            try:
                # 2. 내부망 Intent 검증
                val_payload = {
                    "requester_id": intent.agent_id,
                    "action": intent.action,
                    "max_fuel_budget": intent.max_fuel,
                    "signature": intent.signature,
                    "payment_receipt": x_x402_receipt # 실행 완료 후 차감을 위해 내부망으로 전달
                }
                val_res = await internal_client.post("/v1/eco/compute/intent/validate", json=val_payload)
                if val_res.status_code != 200:
                    raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"Intent Rejected: {val_res.text}")

                # 3. Trustless Compute (WASM 샌드박스 실행 및 계측)
                exec_payload = {
                    "agent_schema": {
                        "runtime": "python3.11-wasm",
                        "files": {"main.py": intent.source_code}, 
                        "limits": {"max_fuel": intent.max_fuel}
                    },
                    "target_entry": "main.py"
                }
                exec_res = await internal_client.post("/v1/eco/profile/execute/billed", json=exec_payload)
                if exec_res.status_code != 200:
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"Compute Failed: {exec_res.text}")
                
                exec_data = exec_res.json()
                fuel_metered = exec_data.get("fuel_billed", 0)
                cost_usd = exec_data.get("billed_cost_usd", 0.0)

                # 4. Swarm Attestation 및 Kernel 기록
                exec_hash = hashlib.sha256(json.dumps(exec_data).encode()).hexdigest()
                repos = {"vm_trace_hash": exec_hash, "metered_fuel": str(fuel_metered)}

                swarm = NotarySwarm(size=3)
                canonical_hash = StateAdapter.to_canonical_bytes(repos)
                commit_hash_bytes = hashlib.sha256(canonical_hash).digest()
                attested_signatures = swarm.attest_payload(commit_hash_bytes)

                kernel_payload = {
                    "action": "record_agent_execution",
                    "receipt_id": request_id,
                    "repos": repos,
                    "signatures": attested_signatures,
                    "timestamp": float(time.time() * 1000)
                }
                canonical_payload = StateAdapter.to_canonical_bytes(kernel_payload).decode('utf-8')
                fp_res = await broker.invoke("compute_root_fingerprint", canonical_payload)
                
                if not fp_res.success:
                    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Kernel Record Failed: {fp_res.error}")
                
                commit_hash = json.loads(fp_res.output).get("fingerprint", "0x_sealed_root")

                # 5. 영수증 반환
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


@public_edge.post(
    "/telemetry/logs", 
    tags=["Log Ingress"], 
    summary="[Gateway] 외부 에이전트의 OTLP 로그 수집 (고빈도 X402 과금 적용)",
    status_code=status.HTTP_200_OK
)
async def public_otlp_logs_export(
    payload: ExportLogsServiceRequest = Body(...),
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    internal_url: str = Depends(get_internal_edge_url),
    bg_tasks: BackgroundTasks = BackgroundTasks(),
    pubsub: DistributedPubSub = Depends(get_pubsub),
    broker: DphiBroker = Depends(get_wasm_broker),
    otlp_engine: StrictOtlpExtractionEngine = Depends(get_otlp_engine)
):
    # 1. 톨게이트: 고빈도 로그 적재를 위한 초소액 예산 검증
    estimated_cost = 0.001 
    await verify_x402_budget(x_x402_receipt, estimated_cost, internal_url)

    try:
        payload_dict = payload.model_dump(exclude_none=True)
        raw_json_bytes = orjson.dumps(payload_dict)
        content_hash = hashlib.sha256(raw_json_bytes).hexdigest()
        
        try:
            extracted_metrics = otlp_engine.execute(raw_json_bytes)
        except ValueError as e:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))

        kernel_payload = {
            "action": "seal_otlp_transaction",
            "content_hash": content_hash,
            "metrics_summary": extracted_metrics,
            "receipt_ref": x_x402_receipt # 백그라운드 워커가 참조하여 예산 차감
        }
        canonical_payload = StateAdapter.to_canonical_bytes(kernel_payload).decode('utf-8')
        res = await broker.invoke("compute_root_fingerprint", canonical_payload)
        
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
    summary="[Gateway] 단건 Audit Event 마스킹 및 ZK 증명 발급 (X402 과금 적용)"
)
async def public_audit_log(
    payload: AuditLogRequest,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    internal_url: str = Depends(get_internal_edge_url),
    secret_auditor: SecretAuditor = Depends(get_secret_auditor),
    broker: DphiBroker = Depends(get_wasm_broker)
) -> AuditLogResponse:
    # 1. 톨게이트: ZK 증명 생성 여부(verbose)에 따른 차등 과금 검증
    estimated_cost = 0.05 if payload.verbose else 0.01
    await verify_x402_budget(x_x402_receipt, estimated_cost, internal_url)

    request_time = str(time.time())
    event_dict = payload.event.model_dump(exclude_none=True)
    
    sanitized_event = secret_auditor._encrypt_sensitive_data(event_dict)
    
    # 영수증 정보를 포함하여 커널에 씰링 (커널 내부 워커가 차감 처리)
    sanitized_event["_billing_ref"] = x_x402_receipt
    
    canonical_payload = StateAdapter.to_canonical_bytes(sanitized_event).decode('utf-8')
    fp_res = await broker.invoke("compute_root_fingerprint", canonical_payload)
    
    if not fp_res.success:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to compute kernel fingerprint")
        
    event_hash = json.loads(fp_res.output)["fingerprint"]
    merkle_proof = None
    
    if payload.verbose:
        proof_res = await broker.invoke("generate_proof", canonical_payload)
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