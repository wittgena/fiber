# fiber.dphi.edge.mcp.bridge
import json
import asyncio
from typing import Dict, Any, Optional, Union

from fastapi import APIRouter, Body, Header, Request, HTTPException, Depends
from fastapi.responses import JSONResponse

from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from fiber.infra.client.rpc import InternalRpcClient
from fiber.dphi.edge.serv.depend import get_rpc_client
from fiber.infra.agent.bridge.adapter import AgentIdentity, IdempotencyMapper, NonceReplayProtector, DPoPValidator

log = get_emitter("mcp.bridge")

class TransitionBridge:
    def __init__(self, mapper: IdempotencyMapper, nonce_protector: NonceReplayProtector):
        self.mapper = mapper
        self.nonce_protector = nonce_protector
        log.info("[TransitionBridge] Mounted. Pure MCP-to-RPC translation & Zero-Latency Gateway Active.")

    async def invoke_mcp_sync(
        self, identity: AgentIdentity, payload: Dict[str, Any], target_uri: str, target_method: str, rpc: InternalRpcClient
    ) -> Union[Dict[str, Any], JSONResponse]:
        
        # 1. 외곽 망 보안 (Replay Attack 원천 차단)
        if not await self.nonce_protector.validate_and_lock_nonce(identity.nonce):
            raise HTTPException(status_code=423, detail="REPLAY_ATTACK_DETECTED")

        # 2. [클린 아키텍처] 레거시 스키마(arguments) 배려 제거. 오직 MCP 표준 _meta에만 신원 기록.
        # 이 데이터가 특정 레거시에 맞게 변형되는 것은 WorkerConnector 측의 Quarantine 영역이 전담함.
        if "params" not in payload: payload["params"] = {}
        if "_meta" not in payload["params"]: payload["params"]["_meta"] = {}
        
        payload["params"]["_meta"]["user_id"] = str(identity.agent_uri)

        # 3. 멱등성 매핑 (Idempotency Handling)
        handle_id, is_new = await self.mapper.get_or_create_handle(
            identity.target_server_id, identity.idempotency_key
        )

        if not is_new:
            state_res = await rpc.call("mcp.state.query", {"handle_id": handle_id})
            if state_res.get("exists"):
                status = state_res.get("status")
                
                if status == "YIELD":
                    input_responses = payload.get("params", {}).get("_meta", {}).get("inputResponses")
                    
                    if input_responses:
                        log.info(f"[Bridge] TOTP Input detected for {handle_id}. Resuming parked sandbox...")
                        await rpc.publish_intent(
                            channel=f"mcp.intent.queue.{identity.target_server_id}",
                            payload={
                                "handle_id": handle_id, 
                                "action": "RESUME", 
                                "payload": input_responses
                            }
                        )
                        return await self._wait_for_resolution(handle_id)
                        
                    log.debug(f"[Bridge] {handle_id} is YIELDED. Returning existing prompt.")
                    return JSONResponse(status_code=202, content=state_res.get("executable_payload", {}))
                    
                elif status == "FAULTED":
                    raise HTTPException(status_code=502, detail=state_res.get("error_detail", "Execution Fault"))
                elif status == "RESOLVED":
                    return state_res.get("executable_payload", {})
                    
            return JSONResponse(status_code=202, content={"message": "Transaction already in progress."})

        # 4. 보안 및 결제 검증 (DPoP & L402)
        is_authenticated = False

        if identity.proof_of_possession:
            if not DPoPValidator.verify_token(identity.proof_of_possession, identity.nonce, target_uri, target_method):
                raise HTTPException(status_code=401, detail="CRYPTOGRAPHIC_BINDING_FAILED")
            is_authenticated = True

        if identity.receipt:
            try:
                await rpc.call("eco.billing.receipt.validate", {
                    "action": payload.get("name", "unknown_tool"),
                    "payment_receipt": identity.receipt
                })
                is_authenticated = True
            except HTTPException as e:
                raise HTTPException(status_code=e.status_code, detail=f"Payment/Intent Rejected: {e.detail}")

        if not is_authenticated:
            raise HTTPException(status_code=401, detail="Authentication (DPoP) or Payment Receipt (X402) missing.")

        # 5. 상태 씰링 및 실행 지시 (EXECUTE Intent)
        await rpc.call("mcp.state.pending.seal", {
            "handle_id": handle_id,
            "payload": payload,
            "target_server_id": identity.target_server_id
        })

        await rpc.publish_intent(
            channel=f"mcp.intent.queue.{identity.target_server_id}",
            payload={"handle_id": handle_id, "action": "EXECUTE", "payload": payload}
        )
        log.debug(f"[Bridge] Intent {handle_id} published (EXECUTE).")

        return await self._wait_for_resolution(handle_id)

    async def _wait_for_resolution(self, handle_id: str) -> Union[Dict[str, Any], JSONResponse]:
        tunnel = await TunnelFactory.get_default()
        reply_channel = f"mcp.intent.reply.{handle_id}"
        pubsub = tunnel.pubsub()
        await pubsub.subscribe(reply_channel)
        
        log.debug(f"[Bridge] Listening on {reply_channel} for backend resolution...")

        try:
            async with asyncio.timeout(30.0):
                async for msg in pubsub.listen():
                    if msg and msg["type"] == "message":
                        state_data = json.loads(msg["data"])
                        status = state_data.get("status")
                        
                        if status == "YIELD":
                            log.info(f"[Bridge] {handle_id} YIELDED. Prompting Agent.")
                            return JSONResponse(status_code=202, content=state_data.get("executable_payload", {}))
                        elif status == "FAULTED":
                            raise HTTPException(status_code=502, detail=state_data.get("error_detail", "Execution Fault"))
                        elif status == "RESOLVED":
                            log.info(f"[Bridge] {handle_id} RESOLVED successfully.")
                            return state_data.get("executable_payload", {})
                            
        except asyncio.TimeoutError:
            log.warning(f"[Bridge] Timeout waiting for backend resolution of {handle_id}")
            raise HTTPException(status_code=504, detail="Transaction suspended or upstream timeout.")
        finally:
            await pubsub.unsubscribe(reply_channel)
            await pubsub.close()

# ---------------------------------------------------------
# HTTP Ingress Route (MCP 2026-07-28 Spec)
# ---------------------------------------------------------
mcp_bridge = APIRouter(prefix="/v1/mcp-gateway", tags=["Enterprise MCP Bridge"])

@mcp_bridge.post("/{target_server_id}/invoke")
async def invoke_mcp_stateless(
    request: Request,
    target_server_id: str,
    payload: Dict[str, Any] = Body(...),
    x_idempotency_key: str = Header(..., alias="x-idempotency-key"),
    x_nonce: str = Header(..., alias="x-nonce"),
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    x_dpop_proof: Optional[str] = Header(None, alias="DPoP"),
    x_spiffe_id: Optional[str] = Header(None, alias="x-spiffe-id"),
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    try:
        identity = AgentIdentity(
            target_server_id=target_server_id,
            agent_uri=x_spiffe_id or "spiffe://public/agent",
            proof_of_possession=x_dpop_proof,
            receipt=x_x402_receipt,
            client_ip=request.client.host if request.client else "0.0.0.0",
            nonce=x_nonce,
            idempotency_key=x_idempotency_key
        )
        
        adapter: TransitionBridge = request.app.state.mcp_transition_adapter
        target_uri = str(request.url)
        target_method = request.method
        
        result = await adapter.invoke_mcp_sync(identity, payload, target_uri, target_method, rpc)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[MCP Bridge] Internal Facade Fracture: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Edge Communication Failure")