# fiber.dphi.edge.serv.llm
import time
import orjson
from typing import Dict, Any, List, Optional, Union
from fastapi import Body, Header, Response, status, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from fiber.dphi.edge.transition.bridge import AgentIdentity, EventMetadata, TransitionBridge, TransitionResult
from fiber.llm.entry import acompletion, aembedding
from fiber.llm.param import ModelResponse, EmbeddingResponse
from fiber.llm.router.stream.wrapper import StreamWrapper

from xphi.arch.contract.interface import ContractRouter
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.llm.auth import DphiKey, DphiAction, KernelAuthPayload
from xphi.watcher.plane.emitter import get_emitter, flow_scope

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

"""LLM Gateway Endpoints (DPHI 코어 바인딩)"""
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

"""Enterprise MCP Gateway Endpoints"""
@llm_edge.post(
    "/mcp-gateway/state",
    summary="Enterprise MCP 1.0 <-> 2.0 Transition Bridge",
    tags=["Enterprise MCP Gateway"],
    response_model=Dict[str, Any]
)
async def process_mcp_state(
    request: Request,
    response: Response, 
    action: str = Body(..., description="INITIALIZE, MUTATE, COMMIT, QUERY"),
    handle_id: Optional[str] = Body(None, description="Opaque Handle ID"),
    payload: Dict[str, Any] = Body(default_factory=dict),
    
    x_spiffe_id: str = Header(..., description="Agent SPIFFE URI"),
    x_dpop_proof: str = Header(..., alias="DPoP", description="Cryptographic DPoP Signature (RFC 9449)"),
    x_nonce: str = Header(..., description="Replay Attack 방지용 난수 (필수)"),
    
    x_tenant_id: str = Header(..., description="B2B 고객사 고유 식별자 (필수)"),
    x_idempotency_key: str = Header(..., description="중복 트랜잭션 방지용 멱등성 키 (필수)"),
    x_trace_id: Optional[str] = Header(None, description="OTLP 분산 추적 ID (선택)")
):
    client_ip = request.client.host if request.client else "0.0.0.0"
    try:
        identity = AgentIdentity(
            tenant_id=x_tenant_id,
            principal_id="enterprise_orchestrator",
            agent_uri=x_spiffe_id,
            proof_of_possession=x_dpop_proof,
            client_ip=client_ip,
            nonce=x_nonce,
            scopes=["mcp:state:write"]
        )
        
        meta_kwargs = {"idempotency_key": x_idempotency_key}
        if x_trace_id:
            meta_kwargs["trace_id"] = x_trace_id
            
        meta = EventMetadata(**meta_kwargs)
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=f"Invalid Enterprise Context: {ve}")

    try:
        adapter: TransitionBridge = request.app.state.mcp_transition_adapter
        res: TransitionResult = await adapter.process_transition_intent(
            identity=identity,
            meta=meta,
            handle_id=handle_id or "",
            action=action,
            payload=payload
        )
        
        if not res.success:
            err_code = res.code
            err_msg = res.error
            
            if err_code == -32001: 
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED, 
                    detail="Invalid or missing DPoP proof.",
                    headers={
                        "WWW-Authenticate": f'Bearer resource_metadata="https://{request.url.hostname}/.well-known/oauth-protected-resource"'
                    }
                )
            elif err_code == -32008: 
                raise HTTPException(status_code=status.HTTP_410_GONE, detail=err_msg) 
            elif err_code == -32009: 
                raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=err_msg) 
            elif err_msg == "LEGACY_FLUSH_FAILED": 
                detail_msg = res.data.get("details", "Bad Gateway") if res.data else "Bad Gateway"
                raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=detail_msg)
                
            else:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=err_msg)

        if res.status == "commit_accepted":
            response.status_code = status.HTTP_202_ACCEPTED

        return res.model_dump(exclude_none=True)
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[MCP Gateway] Internal Fracture: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Edge Failure")