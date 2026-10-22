# fiber.gateway.serv.llm
import time
import orjson
from typing import Dict, Any, List, Optional, Union
from fastapi import Body, status, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from fiber.llm.response import ModelResponse, EmbeddingResponse
from fiber.llm.pipeline.bootstrap import PipelineBootstrap 
from fiber.llm.pipeline.interceptor.registry import InterceptorConfigLoader
from fiber.gateway.llm.stream.wrapper import StreamWrapper
from fiber.gateway.security import KernelAuthenticator, NodeAuthPayload
from fiber.gateway.contract.router import ContractRouter
from fiber.infra.vcr.manager import VCRIdentityRule

from xphi.arch.contract.event.next import next_trace_id
from xphi.arch.model.auth import BrokerKey, BrokerAction
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("serv.llm")

llm_edge = ContractRouter(
    namespace="llm",  
    prefix="/v1", 
    tags=["LLM Gateway"],
    description="Deterministic LLM Gateway"
)

class InterceptorItem(BaseModel):
    class_name: str = Field(alias="class", description="Interceptor class name (e.g. cache.EdgeRedisCache)")
    kwargs: Optional[Dict[str, Any]] = Field(default_factory=dict)

class ChatCompletionRequest(BaseModel):
    model: str = Field(..., description="Target LLM model")
    messages: List[Dict[str, Any]] = Field(..., description="Conversation history")
    stream: Optional[bool] = False
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = None
    tools: Optional[List[Dict[str, Any]]] = None
    interceptors: Optional[List[InterceptorItem]] = None
    
    # Optional top-level VCR convenience fields (also accepted inside `metadata`)
    vcr: Optional[str] = Field(default=None, description="Per-request VCR mode: live | record | replay")
    vcr_scenario: Optional[str] = Field(default=None, description="VCR scenario name for fixture routing")
    vcr_filename: Optional[str] = Field(default=None, description="Explicit VCR fixture filename")
    vcr_invoker: Optional[str] = Field(default=None, description="VCR invoker identifier")
    vcr_speed: Optional[str] = Field(default=None, description="VCR replay speed: max | real")
    vcr_chaos: Optional[float] = Field(default=None, description="VCR artificial chaos latency (ms)")
    vcr_tick: Optional[float] = Field(default=None, description="VCR stream coalescing tick window (ms)")
    
    model_config = {"extra": "allow"}


def _bind_vcr_context(
    request: Request,
    payload: ChatCompletionRequest,
    kwargs: Dict[str, Any],
    metadata: Dict[str, Any],
) -> None:
    """
    Normalizes top-level or metadata VCR parameters and generates a deterministic
    trace_id when operating in 'record' or 'replay' mode.
    """
    # 1. Merge top-level convenience fields into metadata if provided
    vcr_field_map = {
        "vcr": "vcr_mode",
        "vcr_scenario": "vcr_scenario",
        "vcr_filename": "vcr_filename",
        "vcr_invoker": "vcr_invoker",
        "vcr_speed": "vcr_speed",
        "vcr_chaos": "vcr_chaos",
        "vcr_tick": "vcr_tick",
    }
    for attr_name, meta_key in vcr_field_map.items():
        val = getattr(payload, attr_name, None)
        if val is not None and meta_key not in metadata:
            metadata[meta_key] = val

    # 2. Determine effective VCR mode (request metadata -> daemon default -> "live")
    vcr_manager = getattr(request.app.state, "vcr_manager", None)
    daemon_default_mode = vcr_manager.config.mode if vcr_manager else "live"
    effective_mode = str(metadata.get("vcr_mode") or metadata.get("vcr") or daemon_default_mode).lower()

    if effective_mode in ("record", "replay"):
        metadata["vcr_mode"] = effective_mode
        invoker = metadata.setdefault("vcr_invoker", "rest_gateway")
        scenario = metadata.get("vcr_scenario")
        if scenario:
            scenario = str(scenario).replace(" ", "_").lower()
            metadata["vcr_scenario"] = scenario

        # 3. Generate deterministic trace_id using VCRIdentityRule (matches recorder.py behavior)
        if not kwargs.get("trace_id"):
            seed = VCRIdentityRule.generate_seed(
                scenario_name=scenario,
                messages=payload.messages,
                invoker=invoker,
            )
            kwargs["trace_id"] = next_trace_id(seed)


@llm_edge.post(
    "/chat/completions",
    summary="Chat Completion(OpenAI Compatible)",
    response_model=ModelResponse
)
async def chat_completions(
    request: Request,
    payload: ChatCompletionRequest = Body(...)
):
    req_id = f"llm_chat_{int(time.time() * 1000)}"
    with flow_scope(phase="LLM_ORCHESTRATION", bound="edge.llm", req_id=req_id):
        try:
            # node_auth = await KernelAuthenticator.authorize_intent(
            #     request=request,
            #     action=BrokerAction.LLM_COMPUTE.value,
            #     model=payload.model,
            #     max_tokens=payload.max_tokens
            # )
            node_auth = NodeAuthPayload()
            
            # Exclude internal/VCR top-level fields from leaking into downstream LLM SDK kwargs
            exclude_fields = {
                "model", "messages", "interceptors",
                "vcr", "vcr_scenario", "vcr_filename", "vcr_invoker",
                "vcr_speed", "vcr_chaos", "vcr_tick",
            }
            kwargs = payload.model_dump(exclude=exclude_fields, exclude_none=True)
            
            kwargs.pop("base_url", None)
            kwargs.pop("api_base", None)
            kwargs.pop("headers", None)
            
            internal_receipt = getattr(request.state, "fuel_receipt", None)
            
            metadata = kwargs.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}

            # Preserve client-supplied node_auth if passed via metadata (e.g., E2E/tester), ensuring object type
            raw_client_auth = metadata.get(BrokerKey.NODE_AUTH.value)
            if isinstance(raw_client_auth, dict):
                node_auth = NodeAuthPayload.model_validate(raw_client_auth)
            elif isinstance(raw_client_auth, NodeAuthPayload):
                node_auth = raw_client_auth

            metadata[BrokerKey.FUEL_RECEIPT.value] = internal_receipt
            metadata[BrokerKey.CLIENT_HOST.value] = request.client.host if request.client else "unknown"
            metadata[BrokerKey.NODE_AUTH.value] = node_auth

            # Bind per-request VCR configuration and deterministic trace_id
            _bind_vcr_context(request=request, payload=payload, kwargs=kwargs, metadata=metadata)
            kwargs["metadata"] = metadata

            # 1. Boot-time loaded global interceptors (Immutable)
            global_interceptors = getattr(request.app.state, "pipeline_interceptors", [])
            
            # 2. Client-requested interceptors (Additive)
            client_interceptors = InterceptorConfigLoader.build_from_payload(payload.interceptors or [])
            
            # 3. Safe Merge (Deduplication & System Priority)
            global_class_names = {type(i).__name__ for i in global_interceptors}
            final_interceptors = list(global_interceptors)
            
            for ci in client_interceptors:
                if type(ci).__name__ in global_class_names:
                    log.warning(f"Client tried to override global interceptor '{type(ci).__name__}'. Override denied.")
                    continue
                final_interceptors.append(ci)

            # 4. Execute Pipeline
            response = await PipelineBootstrap.execute_completion(
                model=payload.model,
                messages=payload.messages,
                interceptors=final_interceptors,
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
async def embeddings(
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
        metadata[BrokerKey.NODE_AUTH.value] = node_auth
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