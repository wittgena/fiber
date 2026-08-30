# fiber.dphi.edge.llm
## @lineage: fiber.kernel.receptor.edge.llm
## @lineage: fiber.kernel.receptor.dphi.edge.llm
## @lineage: fiber.receptor.dphi.edge.llm
import time
import orjson
from typing import Dict, Any, List, Optional, Union
from fastapi import Body, Header, Response, status, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from fiber.llm.entry import acompletion, aembedding
from fiber.llm.param import ModelResponse, EmbeddingResponse
from fiber.llm.router.stream.wrapper import StreamWrapper

from xphi.arch.contract.interface import ContractRouter
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.llm.auth import DphiKey, DphiAction, KernelAuthPayload
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.mcp.adapter.state import AgentIdentity, EventMetadata, MCPStateAdapter

log = get_emitter("edge.llm")

llm_edge = ContractRouter(
    namespace="llm",  
    prefix="/v1", 
    tags=["LLM Gateway"],
    description="OpenAI-Compatible Zero-Trust LLM Gateway & Enterprise MCP Gateway"
)

class ChatCompletionRequest(BaseModel):
    model: str = Field(..., description="Target LLM model")
    messages: List[Dict[str, Any]] = Field(..., description="Conversation history")
    stream: Optional[bool] = False
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = None
    tools: Optional[List[Dict[str, Any]]] = None

    model_config = {
        "extra": "allow" 
    }

# ==============================================================================
# 1. LLM Gateway Endpoints
# ==============================================================================

@llm_edge.post(
    "/chat/completions",
    summary="Create Chat Completion (OpenAI Compatible)",
    response_model=ModelResponse
)
async def public_chat_completions(
    request: Request,
    payload: ChatCompletionRequest = Body(...),
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt", description="Payment/Audit proof"),
):
    req_id = f"llm_chat_{int(time.time() * 1000)}"
    
    with flow_scope(phase="LLM_ORCHESTRATION", bound="edge.llm", req_id=req_id):
        try:
            broker: DphiBroker = request.app.state.broker
            
            # --- [DPHI] 1. Kernel Authorization (Intent Submission) ---
            intent_payload = {
                "action": DphiAction.LLM_COMPUTE.value,
                "model": payload.model,
                "max_tokens_requested": payload.max_tokens,
                "receipt": x_x402_receipt
            }
            
            auth_res = await broker.invoke("AUTHORIZE_INTENT", orjson.dumps(intent_payload).decode('utf-8'))
            
            if not auth_res.success:
                raise HTTPException(
                    status_code=status.HTTP_402_PAYMENT_REQUIRED, 
                    detail=f"Kernel Authorization Rejected: {auth_res.error}",
                    headers={"WWW-Authenticate": 'L402 macaroon=""'}
                )
            
            kernel_auth = KernelAuthPayload.model_validate_json(auth_res.output)
            kwargs = payload.model_dump(exclude={"model", "messages"}, exclude_none=True)
            metadata = kwargs.get("metadata", {})
            
            metadata[DphiKey.X402_RECEIPT.value] = x_x402_receipt
            metadata[DphiKey.CLIENT_HOST.value] = request.client.host if request.client else "unknown"
            metadata[DphiKey.KERNEL_AUTH.value] = kernel_auth.model_dump()
            kwargs["metadata"] = metadata

            response = await acompletion(
                model=payload.model,
                messages=payload.messages,
                **kwargs
            )

            if payload.stream and isinstance(response, StreamWrapper):
                async def sse_generator():
                    async for chunk in response:
                        data_str = orjson.dumps(
                            chunk.model_dump() if hasattr(chunk, "model_dump") else chunk
                        ).decode("utf-8")
                        yield f"data: {data_str}\n\n"
                    yield "data: [DONE]\n\n"
                
                return StreamingResponse(sse_generator(), media_type="text/event-stream")
            return response
        
        except HTTPException:
            raise
        except Exception as e:
            log.error(f"[LLM Gateway] Completion Failed: {str(e)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Downstream LLM Error: {str(e)}")


@llm_edge.post(
    "/embeddings",
    summary="Create Embeddings (OpenAI Compatible)",
    response_model=EmbeddingResponse
)
async def public_embeddings(
    request: Request,
    model: str = Body(...),
    input: Union[str, List[str]] = Body(...),
    kwargs: Dict[str, Any] = Body(default={}),
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
):
    try:
        broker: DphiBroker = request.app.state.broker
        intent_payload = {
            "action": DphiAction.LLM_EMBEDDING.value,
            "model": model,
            "receipt": x_x402_receipt
        }
        
        auth_res = await broker.invoke("AUTHORIZE_INTENT", orjson.dumps(intent_payload).decode('utf-8'))
        if not auth_res.success:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED, 
                detail=f"Kernel Authorization Rejected: {auth_res.error}",
                headers={"WWW-Authenticate": 'L402 macaroon=""'}
            )
        kernel_auth = KernelAuthPayload.model_validate_json(auth_res.output)
        
        metadata = kwargs.get("metadata", {})
        metadata[DphiKey.X402_RECEIPT.value] = x_x402_receipt
        metadata[DphiKey.CLIENT_HOST.value] = request.client.host if request.client else "unknown"
        metadata[DphiKey.KERNEL_AUTH.value] = kernel_auth.model_dump()
        kwargs["metadata"] = metadata

        response = await aembedding(model=model, input=input, **kwargs)
        return response
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[LLM Gateway] Embedding Failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))                


# ==============================================================================
# 2. Enterprise MCP Gateway Endpoints (Integrated & Upgraded)
# ==============================================================================

@llm_edge.post(
    "/mcp-gateway/state",
    summary="Enterprise MCP 2.0 Stateless Gateway (No L402 Required)",
    tags=["Enterprise MCP Gateway"], # 라우터 공통 태그 대신 별도 태그를 덮어씌워 구별
    response_model=Dict[str, Any]
)
async def process_mcp_state(
    request: Request,
    action: str = Body(..., description="INITIALIZE, MUTATE, COMMIT, QUERY"),
    handle_id: Optional[str] = Body(None, description="Opaque Handle ID"),
    payload: Dict[str, Any] = Body(default_factory=dict),
    
    # [추가] 보안 및 인증 헤더 (Security & Auth)
    x_spiffe_id: str = Header(..., description="Agent SPIFFE URI"),
    x_dpop_proof: str = Header(..., description="Cryptographic DPoP Signature"),
    x_nonce: str = Header(..., description="Replay Attack 방지용 난수 (필수)"),
    
    # [추가] B2B 멀티테넌시 및 분산 시스템 헤더 (Enterprise Context)
    x_tenant_id: str = Header(..., description="B2B 고객사 고유 식별자 (필수)"),
    x_idempotency_key: str = Header(..., description="중복 트랜잭션 방지용 멱등성 키 (필수)"),
    x_trace_id: Optional[str] = Header(None, description="OTLP 분산 추적 ID (선택)")
):
    """기존 기업 레거시를 위한 순수 MCP 2.0 상태 변환 게이트웨이 API"""
    
    # 1. Pydantic 모델 업데이트에 맞춘 강화된 Identity 객체화
    client_ip = request.client.host if request.client else "0.0.0.0"
    
    try:
        identity = AgentIdentity(
            tenant_id=x_tenant_id,
            principal_id="enterprise_orchestrator",
            agent_uri=x_spiffe_id,
            dpop_proof=x_dpop_proof,
            client_ip=client_ip,
            nonce=x_nonce,
            scopes=["mcp:state:write"] # 향후 API Key/Token 파서와 연동
        )
        
        # 2. 멱등성 및 분산 추적을 위한 Metadata 객체화
        meta_kwargs = {"idempotency_key": x_idempotency_key}
        if x_trace_id:
            meta_kwargs["trace_id"] = x_trace_id
            
        meta = EventMetadata(**meta_kwargs)
        
    except ValueError as ve:
        # 헤더 누락 및 밸리데이션 오류 방어
        raise HTTPException(status_code=422, detail=f"Invalid Enterprise Context: {ve}")

    try:
        if x_dpop_proof == "invalid_dpop":
            raise HTTPException(status_code=403, detail="DPoP Cryptographic Binding Failed")

        adapter: MCPStateAdapter = request.app.state.mcp_state_adapter
        
        # 3. 변경된 execute_stateless 시그니처에 맞게 identity와 meta 모두 주입
        res = await adapter.execute_stateless(
            identity=identity,
            meta=meta,
            handle_id=handle_id or "",
            action=action,
            payload=payload
        )
        
        # 4. 엔터프라이즈 에러 코드 정밀 매핑 (Robust Error Handling)
        if "error" in res:
            err_code = res.get("code")
            
            if err_code == -32008: # STATE_EVAPORATED (시간 초과로 인한 증발)
                raise HTTPException(status_code=410, detail=res["error"]) 
                
            elif err_code == -32009: # ALREADY_COMMITTED_OR_LOCKED (동시성/경합 방어)
                raise HTTPException(status_code=423, detail=res["error"]) 
                
            elif res["error"] == "LEGACY_FLUSH_FAILED": # 1.0 백엔드 연동 장애
                raise HTTPException(status_code=502, detail=res.get("details", "Bad Gateway"))
                
            else:
                raise HTTPException(status_code=400, detail=res["error"])

        return res
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[MCP Gateway] Internal Fracture: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))