# topos.xelog.edge.a2a.compute
from fastapi import APIRouter, Depends, HTTPException, status
import json
import time

from topos.xelog.depend import get_wasm_broker
from watcher.dphi.broker import WasmBroker
from watcher.dphi.adapter.state import StateAdapter
from topos.xelog.topos.state.edge import (
    EdgeState,
    IntentValidationRequest, IntentValidationResponse,
    ExecuteComputeRequest, ExecuteComputeResponse,
    ProofGenerationRequest, ProofGenerationResponse
)

compute_edge = APIRouter()

@compute_edge.post("/intent/validate", summary="1. Validate Intent", response_model=IntentValidationResponse)
async def validate_intent(req: IntentValidationRequest, broker: WasmBroker = Depends(get_wasm_broker)):
    raw_payload = {**req.model_dump(), "timestamp": int(time.time() * 1000)}
    canonical_payload = StateAdapter.to_canonical_bytes(raw_payload).decode('utf-8')
    res = await broker.invoke("validate_intent", canonical_payload)
    
    if not res.success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=res.error.message)
        
    return IntentValidationResponse(status=EdgeState.INTENT_VALIDATED, clearance=json.loads(res.output))

@compute_edge.post("/execute", summary="2. Execute Compute", response_model=ExecuteComputeResponse)
async def execute_compute(req: ExecuteComputeRequest, broker: WasmBroker = Depends(get_wasm_broker)):
    res = await broker.execute(code=req.code, variables=req.variables)
    
    if not res.success:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=res.error.message)
        
    return ExecuteComputeResponse(status=EdgeState.EXECUTION_SUCCESS, output=res.output)

@compute_edge.post("/proof/generate", summary="3. Generate Proof", response_model=ProofGenerationResponse)
async def generate_proof(req: ProofGenerationRequest, broker: WasmBroker = Depends(get_wasm_broker)):
    canonical_payload = StateAdapter.to_canonical_bytes(req.model_dump()).decode('utf-8')
    res = await broker.invoke("generate_proof", canonical_payload)
    
    if not res.success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.error.message)
        
    return ProofGenerationResponse(status=EdgeState.PROOF_GENERATED, zk_receipt=json.loads(res.output))