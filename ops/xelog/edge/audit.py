# ops.xelog.edge.audit
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Body, Header, Path, Response, status, Depends, BackgroundTasks, HTTPException

from ops.xelog.depend import get_wasm_broker, get_pubsub
from ops.xelog.topos.tenant import TenantEco, get_tenant_eco
from ops.xelog.audit.ledger import AuditLedger, get_audit_ledger

from arch.topos.bound.interface.subs import DistributedPubSub
from arch.contract.audit.model import (
    LogstEvent, LogstEventPayload, 
    AuditLogRequest, AuditLogResponse, AuditResult, AuditEnvelope
)
from watcher.dphi.broker import WasmBroker
from watcher.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("edge.audit")

audit_edge = APIRouter(prefix="/v1")

@audit_edge.post("/audit/log", status_code=status.HTTP_200_OK)
async def pangea_audit_log(
    payload: AuditLogRequest,
    topos_ledger: AuditLedger = Depends(get_audit_ledger),
    broker: WasmBroker = Depends(get_wasm_broker)
) -> AuditLogResponse:
    """Python이 PII 데이터를 암호화(마스킹)하고, WASM 커널이 위변조 방지 증명을 반환"""
    request_time = str(time.time())
    event_dict = payload.event.model_dump(exclude_none=True)
    sanitized_event = topos_ledger._encrypt_sensitive_data(event_dict)
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

    envelope = AuditEnvelope(
        event=payload.event,
        received_at=datetime.now(timezone.utc).isoformat()
    )
    audit_result = AuditResult(
        envelope=envelope,
        hash=event_hash,
        membership_proof=merkle_proof,
        consistency_proof=[]
    )
    return AuditLogResponse(
        request_id=f"req_{uuid.uuid4().hex[:8]}",
        request_time=request_time,
        response_time=str(time.time()),
        status="success",
        result=audit_result
    )