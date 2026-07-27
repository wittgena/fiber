# ops.xelog.edge.ledger
import json
import time
import uuid
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, status
from pydantic import BaseModel

from ops.xelog.depend import get_wasm_broker, get_logstream_store
from ops.xelog.topos.log.store import LogStreamStore
from watcher.dphi.broker import WasmBroker, WasmMethod
from watcher.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.ledger")

ledger_edge = APIRouter(prefix="/v1/ledger", tags=["Ledger (Immutable Stream)"])

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

@ledger_edge.post(
    "/stream/append", 
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
        except (json.JSONDecodeError, KeyError) as e:
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

        ## E2E Runner가 파싱할 수 있는 포맷으로 리턴
        return StreamAppendResponse(
            request_id=request_id,
            status="success",
            result=StreamAppendResult(
                hash=event_hash,
                membership_proof=merkle_proof
            )
        )