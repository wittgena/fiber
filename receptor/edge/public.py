# receptor.edge.public
import json
import time
import uuid
import hashlib
from typing import Annotated, Dict, Any, List, Optional
import orjson
import httpx

from fastapi import APIRouter, Body, Header, Response, status, Depends, BackgroundTasks, HTTPException
from pydantic import BaseModel

from arch.contract.interface import ContractRouter
from arch.contract.model.receptor import EdgeState, EdgeHeader
from arch.xor.parser.otlp import StrictOtlpExtractionEngine
from watcher.receptor.contract.model import ExportLogsServiceRequest, AuditLogRequest, AuditLogResponse, AuditResult, AuditEnvelope
from watcher.receptor.audit.secret import SecretAuditor, get_secret_auditor
from receptor.xe.depend import get_wasm_broker, get_pubsub, get_otlp_engine
from kernel.dphi.broker import DphiBroker
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter, flow_scope
from arch.topos.tunnel.subs import DistributedPubSub

log = get_emitter("edge.public")

public_edge = ContractRouter(namespace="public", prefix="/v1/public", tags=["Public Gateway"])

class CodebotIntent(BaseModel):
    agent_id: str
    action: str
    source_code: str
    max_fuel: int
    signature: str

class AuditReceipt(BaseModel):
    receipt_id: str
    receipt_type: str
    status: str
    fuel_consumed: int
    metered_cost_usd: float
    state_root: str
    audit_trail: List[str]

INTERNAL_EDGE_URL = "http://internal-edge-cluster.local:8080"

@public_edge.post(
    "/agent/execute", 
    summary="[Gateway] AI 에이전트 인텐트 단일 실행 및 영수증 발급",
    response_model=AuditReceipt
)
async def public_agent_execute(intent: CodebotIntent):
    request_id = f"cbot_{uuid.uuid4().hex[:8]}"
    with flow_scope(phase="GATEWAY_ORCHESTRATION", bound="edge.public", req_id=request_id):
        log.info(f"Incoming public request for agent: {intent.agent_id}")
        
        async with httpx.AsyncClient(base_url=INTERNAL_EDGE_URL, timeout=15.0) as internal_client:
            try:
                # 1. [내부 호출] Ingress Guardrail 
                val_payload = {
                    "agent_id": intent.agent_id,
                    "action": intent.action,
                    "payload": {"code_hash": hashlib.sha256(intent.source_code.encode()).hexdigest()},
                    "signature": intent.signature
                }
                val_res = await internal_client.post("/v1/eco/compute/intent/validate", json=val_payload)
                if val_res.status_code != 200:
                    raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"Intent Rejected: {val_res.text}")

                # 2. [내부 호출] Trustless Compute & Metering
                exec_payload = {
                    "agent_schema": {
                        "runtime": "python3.11-wasm",
                        "source": intent.source_code,
                        "limits": {"max_fuel": intent.max_fuel}
                    },
                    "target_entry": "main.py",
                    "context_depth": 1
                }
                exec_res = await internal_client.post("/v1/eco/profile/execute/billed", json=exec_payload)
                if exec_res.status_code != 200:
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"Compute Failed: {exec_res.text}")
                
                exec_data = exec_res.json()
                fuel_metered = exec_data.get("fuel_billed", 0)
                cost_usd = exec_data.get("billed_cost_usd", 0.0)

                # 3. [내부 호출] Anchor & Seal (클라이언트가 해시를 위조할 수 없도록 서버가 직접 해시 생성)
                exec_hash = hashlib.sha256(json.dumps(exec_data).encode()).hexdigest()
                anchor_payload = {
                    "receptor_id": intent.agent_id,
                    "proposed_parity": {"topos_id": "codebot_vm", "nexus_id": 99, "phase_id": 1},
                    "parent_nexus_id": 0,
                    "self_parent_state": "0x00",
                    "repos": {"vm_trace_hash": exec_hash, "metered_fuel": str(fuel_metered)},
                    "signers": ["PublicGateway"],
                    "signatures": ["0x_gateway_signature"],
                    "timestamp": int(time.time() * 1000)
                }
                anchor_res = await internal_client.post("/v1/core/anchor/seal", json=anchor_payload)
                if anchor_res.status_code != 200:
                    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Anchor Failed: {anchor_res.text}")
                
                commit_hash = anchor_res.json().get("commit_hash", "0x_sealed_root")

                # 4. 최종 결과(영수증) 반환
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
                log.error(f"Internal Network Error: {e}")
                raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Internal Edge Network Down")


# =====================================================================
# 2. [기존 API 편입] OTLP Telemetry Ingress (Naturally Public)
# =====================================================================
@public_edge.post(
    "/telemetry/logs", 
    tags=["Log Ingress"], 
    summary="[Gateway] 외부 에이전트의 OTLP 로그 수집 및 핑거프린트 앵커링",
    status_code=status.HTTP_200_OK
)
async def public_otlp_logs_export(
    payload: ExportLogsServiceRequest = Body(...),
    bg_tasks: BackgroundTasks = BackgroundTasks(),
    pubsub: DistributedPubSub = Depends(get_pubsub),
    broker: DphiBroker = Depends(get_wasm_broker),
    otlp_engine: StrictOtlpExtractionEngine = Depends(get_otlp_engine)
):
    """
    기존 core_edge.post("/logs")와 완전히 동일.
    페이로드를 받아 자체적으로 해싱, WASM Broker를 통한 검증, 
    PubSub 전송까지 한 번에 처리하므로 외부 노출에 완벽히 정합함.
    """
    try:
        payload_dict = payload.model_dump(exclude_none=True)
        raw_json_bytes = orjson.dumps(payload_dict)
        content_hash = hashlib.sha256(raw_json_bytes).hexdigest()
        
        # OTLP Strict Parsing
        try:
            extracted_metrics = otlp_engine.execute(raw_json_bytes)
        except ValueError as e:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))

        # WASM Fingerprinting
        kernel_payload = {
            "action": "seal_otlp_transaction",
            "content_hash": content_hash,
            "metrics_summary": extracted_metrics
        }
        canonical_payload = StateAdapter.to_canonical_bytes(kernel_payload).decode('utf-8')
        res = await broker.invoke("compute_root_fingerprint", canonical_payload)
        
        if not res.success:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Kernel Seal Rejected")
            
        fingerprint = orjson.loads(res.output).get("fingerprint")

        # Async PubSub
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


# =====================================================================
# 3. [기존 API 편입] Regulated Audit Event Ingress (Naturally Public)
# =====================================================================
@public_edge.post(
    "/audit/event", 
    tags=["Log Ingress"], 
    summary="[Gateway] 단건 Audit Event의 PII 마스킹 및 ZK 증명 발급"
)
async def public_audit_log(
    payload: AuditLogRequest,
    secret_auditor: SecretAuditor = Depends(get_secret_auditor),
    broker: DphiBroker = Depends(get_wasm_broker)
) -> AuditLogResponse:
    """
    기존 core_edge.post("/audit/log")와 완전히 동일.
    외부에서 민감 데이터를 던지면 PII를 마스킹하고 영수증(Proof)을 
    발급하는 단일 책임(Single Responsibility)을 가지므로 Public API로 적합.
    """
    request_time = str(time.time())
    event_dict = payload.event.model_dump(exclude_none=True)
    
    # PII 마스킹 (Air-gap 전처리)
    sanitized_event = secret_auditor._encrypt_sensitive_data(event_dict)
    
    # WASM Kernel Seal
    canonical_payload = StateAdapter.to_canonical_bytes(sanitized_event).decode('utf-8')
    fp_res = await broker.invoke("compute_root_fingerprint", canonical_payload)
    
    if not fp_res.success:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to compute kernel fingerprint")
        
    event_hash = json.loads(fp_res.output)["fingerprint"]
    merkle_proof = None
    
    # ZK/Merkle Proof (Verbose Mode)
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