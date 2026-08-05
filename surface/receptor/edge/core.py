# surface.receptor.edge.core
## @lineage: watcher.receptor.edge.core
import json
import time
import uuid
import hashlib
import orjson  # 🌟 고속 직렬화 및 파싱을 위한 모듈 추가
from datetime import datetime, timezone
from typing import Annotated, List, Dict, Any, Optional

from fastapi import Body, Header, Response, status, Depends, BackgroundTasks, HTTPException
from pydantic import BaseModel

from arch.contract.interface import ContractRouter
from arch.topos.tunnel.subs import DistributedPubSub
from arch.contract.model.receptor import (
    EdgeState, EdgeHeader,
    ParityTripletSchema, AnchorProposalRequest, AnchorSealResponse
)
from arch.xor.secret.auditor import SecretAuditor, get_secret_auditor
from arch.xor.parser.otlp import StrictOtlpExtractionEngine  # 🌟 신규 파서 타입 힌팅

from watcher.receptor.contract.model import AuditLogRequest, AuditLogResponse, AuditResult, AuditEnvelope, ExportLogsServiceRequest
from surface.receptor.xe.depend import (
    get_wasm_broker, get_pubsub, get_logstream_store, get_nexus_anchor,
    get_otlp_engine  # 🌟 신규 DI 추가
)
from watcher.plane.emitter import get_emitter, flow_scope

from kernel.dphi.broker import WasmBroker, WasmMethod
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.anchor import NexusAnchor, AnchorProposal
from kernel.phase.stream.store import LogStreamStore

log = get_emitter("edge.core")

core_edge = ContractRouter(namespace="core", prefix="/v1")

@core_edge.post("/logs", tags=["Log Ingress"], status_code=status.HTTP_200_OK)
async def otlp_logs_export(
    payload: ExportLogsServiceRequest = Body(...),
    bg_tasks: BackgroundTasks = BackgroundTasks(),
    pubsub: DistributedPubSub = Depends(get_pubsub),
    broker: WasmBroker = Depends(get_wasm_broker),
    otlp_engine: StrictOtlpExtractionEngine = Depends(get_otlp_engine),  # 🌟 Strict 엔진 주입
    auth_token: Annotated[str | None, Header(alias="Authorization")] = None, 
):
    try:
        # 1. Pydantic 1차 방어를 통과한 데이터를 고속 직렬화 (엔진 입력용)
        payload_dict = payload.model_dump(exclude_none=True)
        raw_json_bytes = orjson.dumps(payload_dict)
        content_hash = hashlib.sha256(raw_json_bytes).hexdigest()
        
        # 2. 🌟 Strict OTLP Parser 적용: 쓰레기 데이터 검증 및 결정론적 추출
        try:
            extracted_metrics = otlp_engine.execute(raw_json_bytes)
        except ValueError as e:
            # Membrane 방어: OTLP 규격 위반 시 WASM 진입을 차단하고 422 에러 반환
            log.warning(f"[OTLP Ingress] Blocked malformed payload: {e}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, 
                detail=str(e),
                headers={EdgeHeader.STATE: EdgeState.ERROR}
            )

        # 3. WASM 합의를 위한 결정론적 Payload 생성 (순수 데이터만 포함)
        kernel_payload = {
            "action": "seal_otlp_transaction",
            "content_hash": content_hash,
            "usage_intent": {
                "tenant_id": extracted_metrics.get("tenant_id", "anonymous"),
                "model_name": extracted_metrics.get("model", "default-model"),
                "usage": {
                    "prompt_tokens": extracted_metrics.get("prompt_tokens", 0),
                    "completion_tokens": extracted_metrics.get("completion_tokens", 0),
                    "reasoning_tokens": extracted_metrics.get("reasoning_tokens", 0)
                }
            },
            "metrics_summary": extracted_metrics
        }
        
        # 4. WASM 브로커 호출 및 검증
        canonical_payload = StateAdapter.to_canonical_bytes(kernel_payload).decode('utf-8')
        res = await broker.invoke("compute_root_fingerprint", canonical_payload)
        
        if not res.success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail=f"Kernel Seal Rejected: {res.error.message}",
                headers={EdgeHeader.STATE: EdgeState.ERROR, EdgeHeader.ERROR_DETAIL: "Kernel Seal Rejected"}
            )
            
        fingerprint = orjson.loads(res.output).get("fingerprint")
        
        with flow_scope(phase="OTLP_INGRESS", bound="edge"):
            log.info(f"[OTLP Anchor] Secured batch. ContentHash: {content_hash[:8]}, Fingerprint: {fingerprint[:16]}")

        # 5. PubSub (비동기 처리용 원본 데이터 전송)
        topic_name = "otlp_global_stream" 
        bg_tasks.add_task(pubsub.publish_batch, topic=topic_name, events=[payload_dict])
        
        response_headers = {
            EdgeHeader.STATE: EdgeState.SUCCESS,
            EdgeHeader.CONTENT_HASH: content_hash,
            EdgeHeader.FINGERPRINT: fingerprint
        }
        
        return Response(status_code=status.HTTP_200_OK, headers=response_headers, content=b"{}")
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[OTLP Anchor] Processing failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Internal stream processing error",
            headers={EdgeHeader.STATE: EdgeState.ERROR}
        )

@core_edge.post("/audit/log", tags=["Log Ingress"], status_code=status.HTTP_200_OK)
async def audit_log(
    payload: AuditLogRequest,
    secret_auditor: SecretAuditor = Depends(get_secret_auditor),
    broker: WasmBroker = Depends(get_wasm_broker)
) -> AuditLogResponse:
    """단건 Audit Event의 PII 마스킹 및 WASM 위변조 방지 증명 반환"""
    request_time = str(time.time())
    event_dict = payload.event.model_dump(exclude_none=True)
    
    # 1. PII 마스킹
    sanitized_event = secret_auditor._encrypt_sensitive_data(event_dict)
    
    # 2. WASM Kernel Seal
    canonical_payload = StateAdapter.to_canonical_bytes(sanitized_event).decode('utf-8')
    fp_res = await broker.invoke("compute_root_fingerprint", canonical_payload)
    
    if not fp_res.success:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to compute kernel fingerprint")
        
    event_hash = json.loads(fp_res.output)["fingerprint"]
    merkle_proof = None
    
    # 3. 추가 증명 (Verbose)
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
    
    with flow_scope(phase="AUDIT_INGRESS", bound="edge"):
        log.info(f"[Audit Anchor] Secured event. Hash: {event_hash[:8]}")
        
    return AuditLogResponse(
        request_id=f"req_{uuid.uuid4().hex[:8]}",
        request_time=request_time,
        response_time=str(time.time()),
        status="success",
        result=audit_result
    )

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

@core_edge.post(
    "/ledger/stream/append", 
    tags=["Ledger (Immutable Stream)"],
    status_code=status.HTTP_200_OK,
    summary="Immutable Ledger Stream Bulk Append",
    response_model=StreamAppendResponse
)
async def append_to_stream(
    req: StreamAppendRequest = Body(...),
    broker: WasmBroker = Depends(get_wasm_broker),
    store: LogStreamStore = Depends(get_logstream_store)
):
    request_id = f"ledg_{uuid.uuid4().hex[:8]}"
    
    with flow_scope(phase="LEDGER_INGRESS", bound="edge", req_id=request_id):
        ## Payload Parsing
        events_dicts = [e.model_dump(exclude_none=True) for e in req.events]
        
        ## Kernel Gateway Authorization & Physical Append
        is_authorized = await store.bulk_append(
            stream_name=req.stream_name, 
            events=events_dicts
        )
        
        if not is_authorized:
            log.warning(f"Kernel rejected append to stream '{req.stream_name}'")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail="Kernel Blocked Stream Append: ToposGateway Authorization Failed or Tension too high"
            )
            
        ## 데이터 정규화(Canonicalization) 및 WASM Broker를 통한 핑거프린트 발급
        payload_to_hash = {
            "stream_name": req.stream_name,
            "timestamp": int(time.time() * 1000),
            "events": events_dicts
        }
        canonical_payload = StateAdapter.to_canonical_bytes(payload_to_hash).decode('utf-8')
        fp_res = await broker.invoke(WasmMethod.COMPUTE_ROOT_FINGERPRINT, canonical_payload)
        
        if not fp_res.success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail=f"WASM Fingerprint Generation Failed: {fp_res.error}"
            )
            
        try:
            event_hash = json.loads(fp_res.output)["fingerprint"]
        except (json.JSONDecodeError, KeyError):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="WASM returned malformed fingerprint format"
            )
        
        ## Verbose 모드일 경우 ZK/Merkle Proof 추가 생성
        merkle_proof = None
        if req.verbose:
            proof_res = await broker.invoke(WasmMethod.GENERATE_PROOF, canonical_payload)
            if proof_res.success:
                try:
                    merkle_proof = json.loads(proof_res.output).get("current_hash")
                except json.JSONDecodeError:
                    log.warning(f"[{request_id}] Merkle proof parsing failed, ignoring.")
                
        log.info(f"Successfully anchored {len(req.events)} events to {req.stream_name}. Hash: {event_hash[:8]}")

        return StreamAppendResponse(
            request_id=request_id,
            status="success",
            result=StreamAppendResult(
                hash=event_hash,
                membership_proof=merkle_proof
            )
        )

@core_edge.post(
    "/anchor/seal", 
    tags=["Anchor (Consensus)"],
    summary="상태 합의 및 영수증 방출 (Seal Epoch)",
    response_model=AnchorSealResponse
)
async def seal_state(
    req: AnchorProposalRequest,
    nexus: NexusAnchor = Depends(get_nexus_anchor)
):
    proposal = AnchorProposal(
        receptor_id=req.receptor_id,
        proposed_parity=req.proposed_parity.model_dump(),
        parent_nexus_id=req.parent_nexus_id,
        self_parent_state=req.self_parent_state,
        repos=req.repos,
        signers=req.signers,
        signatures=req.signatures,
        timestamp=req.timestamp
    )
    result = await nexus.anchor_state(proposal)
    
    if not result.is_sealed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Topology Ruptured or Consensus Failed: {result.rupture_reason}"
        )
        
    return AnchorSealResponse(
        status=EdgeState.SEALED_AND_COMMITTED,
        nexus_id=result.nexus_id,
        commit_hash=result.commit_hash,
        receipt=result.receipt.__dict__ if hasattr(result.receipt, "__dict__") else dict(result.receipt)
    )