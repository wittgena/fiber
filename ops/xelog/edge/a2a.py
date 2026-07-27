# ops.xelog.edge.a2a
from fastapi import APIRouter, Depends, HTTPException, status
import json
import time

from ops.xelog.depend import get_wasm_broker
from watcher.dphi.broker import WasmBroker
from watcher.dphi.adapter.state import StateAdapter
from ops.xelog.topos.state.edge import (
    EdgeState,
    IntentValidationRequest,
    IntentValidationResponse,
    ExecuteComputeRequest,
    ExecuteComputeResponse,
    ProofGenerationRequest,
    ProofGenerationResponse
)

a2a_edge = APIRouter(prefix="/a2a/v1", tags=["A2A (Trustless Compute)"])

@a2a_edge.post(
    "/intent/validate", 
    summary="1. Validate Intent (요청 검증)",
    response_model=IntentValidationResponse  # 응답 스키마 명시
)
async def validate_intent(
    req: IntentValidationRequest, 
    broker: WasmBroker = Depends(get_wasm_broker)
):
    # 1. 페이로드 구성
    raw_payload = {
        **req.model_dump(), 
        "timestamp": int(time.time() * 1000)
    }
    canonical_payload = StateAdapter.to_canonical_bytes(raw_payload).decode('utf-8')
    res = await broker.invoke("validate_intent", canonical_payload)
    
    if not res.success: 
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail=f"Intent Validation Failed: {res.error.message}"
        )
        
    # 명시된 스키마 객체로 리턴 (또는 dict로 리턴해도 FastAPI가 자동 매핑 및 Enum 검증 수행)
    return IntentValidationResponse(
        status=EdgeState.INTENT_VALIDATED, 
        clearance=json.loads(res.output)
    )

@a2a_edge.post(
    "/compute/execute", 
    summary="2. Execute Compute (신뢰 불필요 실행)",
    response_model=ExecuteComputeResponse
)
async def execute_compute(
    req: ExecuteComputeRequest, 
    broker: WasmBroker = Depends(get_wasm_broker)
):
    res = await broker.execute(code=req.code, variables=req.variables)
    
    if not res.success: 
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, 
            detail=f"WASM Execution Collapsed: {res.error.message}"
        )
        
    return ExecuteComputeResponse(
        status=EdgeState.EXECUTION_SUCCESS, 
        output=res.output
    )

@a2a_edge.post(
    "/compute/proof", 
    summary="3. Generate Proof (연산 증명 생성)",
    response_model=ProofGenerationResponse
)
async def generate_proof(
    req: ProofGenerationRequest, 
    broker: WasmBroker = Depends(get_wasm_broker)
):
    canonical_payload = StateAdapter.to_canonical_bytes(req.model_dump()).decode('utf-8')
    res = await broker.invoke("generate_proof", canonical_payload)
    
    if not res.success: 
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Proof Generation Failed: {res.error.message}"
        )
        
    return ProofGenerationResponse(
        status=EdgeState.PROOF_GENERATED, 
        zk_receipt=json.loads(res.output)
    )