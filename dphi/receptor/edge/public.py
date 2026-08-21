# dphi.receptor.edge.public
import os
import json
import time
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import orjson
import httpx

from fastapi import APIRouter, Body, Header, Response, status, Depends, BackgroundTasks, HTTPException, Request

from bound.config.dphi import dphi_env
from dphi.adapter.anchor import NotarySwarm
from dphi.receptor.edge.depend import get_wasm_broker, get_pubsub, get_otlp_engine

from arch.contract.interface import ContractRouter
from arch.contract.model.receptor import EdgeState, EdgeHeader, IntentValidationRequest
from arch.topos.tunnel.subs import DistributedPubSub
from arch.xor.parser.otlp import StrictOtlpExtractionEngine

from kernel.dphi.broker import DphiBroker, DphiMethod
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.sign import NodeSigner
from watcher.receptor.contract.model import (
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
from watcher.receptor.audit.secret import SecretAuditor, get_secret_auditor
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.public")

public_edge = ContractRouter(
    namespace="public", 
    prefix="/v1/public", 
    tags=["Public Gateway"],
    description="Edge Gateway orchestrating WASM kernels and internal nodes for intent validation, metered execution, and immutable cryptographic receipts."
)

def get_internal_edge_url(request: Request) -> str:
    return request.app.state.config.internal_edge_url


@public_edge.get(
    "/keys", 
    summary="Get Trusted Signer Keys (Strictly Pre-Signed)",
    description=(
        "Returns the list of active Edge nodes' public keys. "
        "Strictly serves offline pre-signed signatures by the Master Root Key. Fails securely if misconfigured."
    )
)
async def get_public_keys():
    """
    클라이언트 SDK가 캐싱할 서명자 목록 엔드포인트입니다.
    이 노드는 절대 Root Key를 갖지 않으며, 주입된 환경변수(사전 생성된 서명)만 서빙합니다.
    """
    active_signers_env = os.getenv("DPHI_ACTIVE_SIGNERS")
    root_signature = os.getenv("DPHI_PRE_SIGNED_ROOT_SIG")

    if not active_signers_env or not root_signature:
        log.critical("[Security] DPHI_ACTIVE_SIGNERS or DPHI_PRE_SIGNED_ROOT_SIG not configured. Rejecting request.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Security misconfiguration: Trusted registry is offline."
        )

    # 쉼표로 구분된 퍼블릭 키 목록을 파싱
    payload_dict = {"active_signers": [key.strip() for key in active_signers_env.split(",")]}
    
    return Response(
        content=orjson.dumps(payload_dict),
        media_type="application/json",
        headers={"X-Dphi-Root-Signature": root_signature}
    )


@public_edge.post(
    "/agent/execute", 
    summary="Execute Billed AI Agent Intent & Issue Cryptographic Proof-of-Action",
    description="Orchestrates metered AI intent execution and WASM state transitions to issue an immutable AuditReceipt with a canonical fingerprint.",
    response_model=AuditReceipt
)
async def public_agent_execute(
    intent: CodebotIntent,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt", description="Payment proof receipt (L402/X402)"),
    internal_url: str = Depends(get_internal_edge_url),
    broker: DphiBroker = Depends(get_wasm_broker)
):
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


@public_edge.post(
    "/telemetry/logs", 
    tags=["Log Ingress"], 
    summary="Ingest OTLP Telemetry, Verify Integrity & Seal Global Stream",
    description="Extracts OTLP metrics and generates secure kernel fingerprints before delegating payloads to the distributed pub/sub stream.",
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
    summary="Secure Audit Event Recording & Conditional Cryptographic Proof Issuance",
    description="Encrypts sensitive audit events for ledger recording, conditionally issuing Merkle/ZK proofs upon request."
)
async def public_audit_log(
    payload: AuditLogRequest,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    secret_auditor: SecretAuditor = Depends(get_secret_auditor),
    broker: DphiBroker = Depends(get_wasm_broker)
) -> AuditLogResponse:
    
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