# fiber.gateway.rest.serv.llm
import time
import orjson
from typing import Dict, Any, List, Optional, Union
from fastapi import Body, status, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from fiber.llm.response import ModelResponse, EmbeddingResponse
from fiber.gateway.llm.pipeline import PipelineBootstrap 
from fiber.gateway.llm.stream.wrapper import StreamWrapper
from fiber.gateway.rest.security import KernelAuthenticator
from fiber.phase.contract.router import ContractRouter

from xphi.arch.model.auth import BrokerKey, BrokerAction
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("serv.llm")

llm_edge = ContractRouter(
    namespace="llm",  
    prefix="/v1", 
    tags=["LLM Gateway"],
    description="Deterministic LLM Gateway"
)

class ChatCompletionRequest(BaseModel):
    model: str = Field(..., description="Target LLM model")
    messages: List[Dict[str, Any]] = Field(..., description="Conversation history")
    stream: Optional[bool] = False
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = None
    tools: Optional[List[Dict[str, Any]]] = None
    model_config = {"extra": "allow" }

@llm_edge.post(
    "/chat/completions",
    summary="Chat Completion(OpenAI Compatible)",
    response_model=ModelResponse
)
async def public_chat_completions(
    request: Request,
    payload: ChatCompletionRequest = Body(...)
):
    req_id = f"llm_chat_{int(time.time() * 1000)}"
    with flow_scope(phase="LLM_ORCHESTRATION", bound="edge.llm", req_id=req_id):
        try:
            node_auth = await KernelAuthenticator.authorize_intent(
                request=request,
                action=BrokerAction.LLM_COMPUTE.value,
                model=payload.model,
                max_tokens=payload.max_tokens
            )
            
            kwargs = payload.model_dump(exclude={"model", "messages"}, exclude_none=True)
            
            kwargs.pop("base_url", None)
            kwargs.pop("api_base", None)
            kwargs.pop("headers", None)
            
            internal_receipt = getattr(request.state, "fuel_receipt", None)
            
            metadata = kwargs.get("metadata", {})
            metadata[BrokerKey.FUEL_RECEIPT.value] = internal_receipt
            metadata[BrokerKey.CLIENT_HOST.value] = request.client.host if request.client else "unknown"
            metadata[BrokerKey.NODE_AUTH.value] = node_auth.model_dump()
            kwargs["metadata"] = metadata

            response = await PipelineBootstrap.execute_completion(
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
    kwargs: Dict[str, Any] = Body(default={})
):
    # TEMPORARY DISABLED
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="The Embedding API is temporarily unsupported."
    )

    try:
        node_auth = await KernelAuthenticator.authorize_intent(
            request=request,
            action=BrokerAction.LLM_EMBEDDING.value,
            model=model
        )
        
        kwargs.pop("base_url", None)
        kwargs.pop("api_base", None)
        kwargs.pop("headers", None)

        internal_receipt = getattr(request.state, "fuel_receipt", None)

        metadata = kwargs.get("metadata", {})
        metadata[BrokerKey.FUEL_RECEIPT.value] = internal_receipt
        metadata[BrokerKey.CLIENT_HOST.value] = request.client.host if request.client else "unknown"
        metadata[BrokerKey.NODE_AUTH.value] = node_auth.model_dump()
        kwargs["metadata"] = metadata

        response = await PipelineBootstrap.execute_embedding(
            model=model, 
            input_data=input, 
            **kwargs
        )
        return response
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[LLM Gateway] Embedding Failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))