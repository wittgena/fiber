# fiber.dphi.edge.serv.llm
import time
import orjson
from typing import Dict, Any, List, Optional, Union
from fastapi import Body, Header, status, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

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
    description="OpenAI-Compatible Zero-Trust LLM Gateway"
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