# fiber.gateway.rest.serv.compliance
import os
import json
import time
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import orjson

from fastapi import Body, Header, Response, status, Depends, BackgroundTasks, HTTPException, Query, Request
from pydantic import BaseModel

from fiber.infra.rpc.method import RpcMethod
from fiber.gateway.rest.serv.depend import (
    get_wasm_broker, 
    get_pubsub, 
    get_otlp_engine, 
    get_secret_auditor, 
    get_rpc_client
)
from xphi.arch.bound.client.rpc import InternalRpcClient, RpcException
from fiber.phase.contract.router import ContractRouter
from xphi.arch.model.edge.receptor import EdgeState, EdgeHeader, IntentValidationRequest
from xphi.arch.bound.xor.parser.ruleset.otlp import OtlpExtractionEngine

from xphi.kernel.space.tunnel.subs import DistributedPubSub
from xphi.kernel.wasm.broker import DphiBroker, DphiMethod
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.arch.model.edge.receipt import (
    HandshakeIntent,
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
from xphi.watcher.receptor.warden import SecretAuditor
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.compliance")

compliance_edge = ContractRouter(
    namespace="compliance", 
    prefix="/v1/compliance", 
    tags=["Compliance Gateway"],
    description="Compliance Gateway"
)

"""TRUST ANCHOR"""
@compliance_edge.get("/keys", summary="Get Trusted Signer Keys (Strictly Pre-Signed)")
async def get_trust_keys(request: Request):
    registry = getattr(request.app.state, "origin_registry", None)
    
    if not registry or not registry.is_verified:
        log.critical("[Security] Origin Registry is missing or not verified.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Security misconfiguration: Trusted registry is offline or tampered."
        )

    trusted_state = registry.get_state()
    payload_dict = {"active_signers": trusted_state.active_signers}
    
    return Response(
        content=orjson.dumps(payload_dict),
        media_type="application/json",
        headers={"X-Dphi-Root-Signature": trusted_state.root_signature}
    )

"""COMPLIANCE SYMMETRY (RECORD ↔ VERIFY)"""
@compliance_edge.post(
    "/telemetry/logs", 
    tags=["Log Ingress"], 
    summary="Ingest OTLP Telemetry, Verify Integrity & Seal Global Stream",
    status_code=status.HTTP_200_OK
)
async def otlp_logs_export(
    payload: ExportLogsServiceRequest = Body(...),
    x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    bg_tasks: BackgroundTasks = BackgroundTasks(),
    pubsub: DistributedPubSub = Depends(get_pubsub),
    broker: DphiBroker = Depends(get_wasm_broker),
    otlp_engine: OtlpExtractionEngine = Depends(get_otlp_engine)
):
    try:
        payload_dict = payload.model_dump(exclude_none=True)
        raw_json_bytes = orjson.dumps(payload_dict)
        content_hash = hashlib.sha256(raw_json_bytes).hexdigest()
        
        try:
            extracted_metrics = otlp_engine.execute(raw_json_bytes)
        except ValueError as e:
            error_msg = str(e)
            log.warning(f"[OTLP] Rule extraction rejected payload: {error_msg}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, 
                detail=f"Telemetry ruleset violation: {error_msg}. Please ensure your payload contains all strictly required metrics."
            )

        kernel_req_dict = KernelOtlpRecord(
            content_hash=content_hash,
            metrics_summary=extracted_metrics,
            receipt_ref=x402_receipt
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
        log.error(f"[OTLP] Processing failed: {str(e)}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Stream processing error")

@compliance_edge.post(
    "/audit/event", 
    tags=["Log Ingress"], 
    summary="Secure Audit Event Recording & Conditional Cryptographic Proof Issuance"
)
async def audit_log(
    payload: AuditLogRequest,
    x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    secret_auditor: SecretAuditor = Depends(get_secret_auditor),
    broker: DphiBroker = Depends(get_wasm_broker)
) -> AuditLogResponse:
    request_time = str(time.time())
    try:
        event_dict = payload.event.model_dump(exclude_none=True)
        sanitized_event = secret_auditor._encrypt_sensitive_data(event_dict)
    except ValueError as e:
        log.warning(f"[Audit] Payload failed business validation: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, 
            detail=f"Audit event validation failed: {str(e)}."
        )

    sanitized_event["_billing_ref"] = x402_receipt
    
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

@compliance_edge.post(
    "/audit/verify", 
    summary="Verify AuditReceipt Authenticity"
)
async def audit_verify(
    receipt: AuditReceipt = Body(...),
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    try:
        rpc_payload = {
            "receipt_id": receipt.receipt_id,
            "state_root": receipt.state_root,
            "full_receipt": receipt.model_dump(exclude_none=True)
        }
        return await rpc.call(RpcMethod.PHASE_STORE_RECEIPT_VERIFY, rpc_payload)
    except RpcException as e:
        raise HTTPException(status_code=e.status_code, detail=f"Verification Failed: {{\"detail\":\"{e.detail}\"}}")