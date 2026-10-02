# fiber.gateway.rest.serv.gateway
import json
import asyncio
import time
import uuid
from typing import Dict, Any, Optional, Union

from fastapi import APIRouter, Body, Header, Request, HTTPException, Depends
from fastapi.responses import JSONResponse

from fiber.infra.rpc.method import RpcMethod
from fiber.gateway.rest.serv.depend import get_rpc_client

from xphi.arch.bound.client.rpc import InternalRpcClient, RpcException
from xphi.arch.bound.adapter.gateway import AgentIdentity, IdempotencyMapper, NonceReplayProtector, DPoPValidator
from xphi.arch.bound.xor.parser.mcp import McpPayloadParser
from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("serv.gateway")

def _extract_error_message(error_detail: Any) -> str:
    if isinstance(error_detail, dict):
        return error_detail.get("message", str(error_detail))
    return str(error_detail) if error_detail else "Unknown Execution Fault"

class TransitionBridge:
    def __init__(self, mapper: IdempotencyMapper, nonce_protector: NonceReplayProtector):
        self.mapper = mapper
        self.nonce_protector = nonce_protector
        log.info("[TransitionBridge] Mounted. Pure MCP-to-RPC translation & Zero-Latency Gateway Active.")

    async def _is_x402_required(self, target_server_id: str) -> bool:
        """개별 서버/워커별 X402 과금 활성화 Flag 여부를 동적으로 확인합니다."""
        try:
            tunnel = await TunnelFactory.get_default()
            policy = await tunnel.get(f"config:x402:{target_server_id}")
            if policy is not None:
                # Redis 클라이언트 반환 타입(bytes or str) 안전 처리
                return policy.decode('utf-8') == "1" if isinstance(policy, bytes) else str(policy) == "1"
        except Exception as e:
            log.warning(f"[Bridge:Config] Failed to fetch X402 flag for {target_server_id}: {e}")
        return False

    async def invoke_mcp_sync(
        self, identity: AgentIdentity, payload: Dict[str, Any], target_uri: str, target_method: str, rpc: InternalRpcClient
    ) -> Union[Dict[str, Any], JSONResponse]:
        
        trace_ctx = {
            "trace_id": str(identity.idempotency_key), 
            "target": str(identity.target_server_id), 
            "agent": str(identity.agent_uri)
        }
        log.info(f"[Bridge:Start] Ingress request received", extra=trace_ctx)

        # Perimeter security: Block replay attacks
        if not await self.nonce_protector.validate_and_lock_nonce(identity.nonce):
            log.warning(f"[Bridge:Security] REPLAY_ATTACK_DETECTED - Nonce lock failed", extra={"nonce": str(identity.nonce), **trace_ctx})
            raise HTTPException(status_code=423, detail="REPLAY_ATTACK_DETECTED")

        # Remove legacy schema fallbacks; enforce identity strictly via MCP _meta
        if "params" not in payload: payload["params"] = {}
        if "_meta" not in payload["params"]: payload["params"]["_meta"] = {}
        payload["params"]["_meta"]["user_id"] = str(identity.agent_uri)

        # Idempotency handling
        handle_id, is_new = await self.mapper.get_or_create_handle(identity.target_server_id, identity.idempotency_key)
        trace_ctx["handle_id"] = str(handle_id)
        log.debug(f"[Bridge:Idempotency] Handle mapped", extra={"is_new": is_new, **trace_ctx})

        # Polling/Status Location header
        location_header = {"Location": f"/v1/mcp-gateway/{identity.target_server_id}/status/{handle_id}"}

        if not is_new:
            state_res = await rpc.call(RpcMethod.MCP_STATE_QUERY, {"handle_id": handle_id})
            if state_res.get("exists"):
                status = state_res.get("status")
                log.info(f"[Bridge:State] Existing state found: {status}", extra=trace_ctx)
                
                if status == "YIELD":
                    input_responses = payload.get("params", {}).get("_meta", {}).get("inputResponses")
                    if input_responses:
                        log.info(f"[Bridge:Resume] TOTP Input detected. Resuming parked sandbox", extra=trace_ctx)
                        await rpc.publish_intent(
                            channel=f"mcp.intent.queue.{identity.target_server_id}",
                            payload={
                                "handle_id": handle_id, 
                                "action": "RESUME", 
                                "payload": input_responses
                            }
                        )
                        return await self._wait_for_resolution(handle_id, identity.target_server_id, rpc, trace_ctx)
                        
                    log.debug(f"[Bridge:Yield] Returning existing prompt to client", extra=trace_ctx)
                    return JSONResponse(status_code=202, content=state_res.get("executable_payload", {}), headers=location_header)
                    
                elif status == "FAULTED":
                    raw_err = state_res.get("error_detail", "Execution Fault")
                    safe_msg = _extract_error_message(raw_err)
                    
                    log.error(f"[Bridge:Fault] Previous execution faulted", extra={"error": raw_err, **trace_ctx})
                    raise HTTPException(status_code=502, detail=safe_msg)
                
                elif status == "RESOLVED":
                    log.debug(f"[Bridge:Resolved] Returning cached resolution", extra=trace_ctx)
                    return state_res.get("executable_payload", {})
                    
            log.warning(f"[Bridge:Conflict] Transaction already in progress but state not queryable", extra=trace_ctx)
            return JSONResponse(status_code=202, content={"message": "Transaction already in progress."}, headers=location_header)

        # Identify MCP discovery and initialization methods
        mcp_method = payload.get("method", "")
        is_discovery_phase = mcp_method in ("initialize", "tools/list", "prompts/list", "resources/list")

        # Authentication (DPoP Mandatory) & Fuel Validation (X402 Conditional)
        if is_discovery_phase:
            # Bypass strict auth and fuel checks for discovery phases
            log.debug(f"[Bridge:Auth] Bypassing strict auth for discovery method: {mcp_method}", extra=trace_ctx)
        else:
            # 1. DPoP 필수 검증 (Mandatory)
            if not identity.proof_of_possession or not DPoPValidator.verify_token(identity.proof_of_possession, identity.nonce, target_uri, target_method):
                log.warning(f"[Bridge:Auth] DPoP Verification Failed or Missing", extra=trace_ctx)
                raise HTTPException(status_code=401, detail="CRYPTOGRAPHIC_BINDING_FAILED")

            # 2. X402 영수증 선택적 검증 (Flag 기반)
            if await self._is_x402_required(identity.target_server_id):
                if not identity.receipt:
                    log.warning(f"[Bridge:Fuel] Missing X402 Receipt for billing-enabled server", extra=trace_ctx)
                    raise HTTPException(status_code=402, detail="Payment Required: X402 Receipt Missing")
                
                try:
                    await rpc.call(RpcMethod.VALIDATE_FUEL_RECEIPT,{
                        "target_server_id": identity.target_server_id,
                        "action": payload.get("params", {}).get("name", "unknown_tool"),
                        "fuel_receipt": identity.receipt
                    })
                except RpcException as e:
                    log.warning(f"[Bridge:Fuel] Fuel validation rejected", extra={"status_code": e.status_code, "detail": e.detail, **trace_ctx})
                    raise HTTPException(status_code=402, detail=f"Fuel/Intent Rejected: {e.detail}")

        await rpc.call(RpcMethod.MCP_STATE_PENDING_SEAL, {
            "handle_id": handle_id,
            "payload": payload,
            "target_server_id": identity.target_server_id
        })

        await rpc.publish_intent(
            channel=f"mcp.intent.queue.{identity.target_server_id}",
            payload={"handle_id": handle_id, "action": "EXECUTE", "payload": payload}
        )
        log.info(f"[Bridge:Intent] EXECUTE Intent published to queue", extra=trace_ctx)
        return await self._wait_for_resolution(handle_id, identity.target_server_id, rpc, trace_ctx)

    async def _wait_for_resolution(self, handle_id: str, target_server_id: str, rpc: InternalRpcClient, trace_ctx: Dict[str, Any]) -> Union[Dict[str, Any], JSONResponse]:
        tunnel = await TunnelFactory.get_default()
        reply_channel = f"mcp.intent.reply.{handle_id}"
        pubsub = tunnel.pubsub()
        await pubsub.subscribe(reply_channel)
        
        log.debug(f"[Bridge:PubSub] Listening for backend resolution", extra={"channel": reply_channel, **trace_ctx})
        start_time = time.time()

        # 202 응답 시 제공될 Location 헤더
        location_header = {"Location": f"/v1/mcp-gateway/{target_server_id}/status/{handle_id}"}

        try:
            async with asyncio.timeout(30.0):
                async for msg in pubsub.listen():
                    if msg and msg["type"] == "message":
                        state_data = json.loads(msg["data"])
                        status = state_data.get("status")
                        duration_ms = round((time.time() - start_time) * 1000, 2)
                        
                        trace_ctx["duration_ms"] = duration_ms
                        
                        if status == "YIELD":
                            log.info(f"[Bridge:PubSub:Yield] Agent prompted for input", extra=trace_ctx)
                            return JSONResponse(status_code=202, content=state_data.get("executable_payload", {}), headers=location_header)
                        
                        elif status == "FAULTED":
                            raw_err = state_data.get("error_detail", "Execution Fault")
                            safe_msg = _extract_error_message(raw_err)
                            
                            log.error(f"[Bridge:PubSub:Fault] Execution failed", extra={"error": str(raw_err), **trace_ctx})
                            raise HTTPException(status_code=502, detail=safe_msg)
                        
                        elif status == "RESOLVED":
                            log.info(f"[Bridge:PubSub:Resolved] Execution completed successfully", extra=trace_ctx)
                            executable_payload = state_data.get("executable_payload", {})
                            
                            telemetry_data = McpPayloadParser.extract_telemetry(executable_payload)
                            if telemetry_data and isinstance(telemetry_data, dict):
                                telemetry_data["target"] = target_server_id
                                try:
                                    await tunnel.publish("eco.telemetry.events", json.dumps(telemetry_data))
                                    log.debug(f"[Bridge:Telemetry] Successfully broadcasted to eco.telemetry.events", extra=trace_ctx)
                                except Exception as emit_err:
                                    log.error(f"[Bridge:Telemetry:Error] Failed to broadcast: {emit_err}", extra=trace_ctx)
                                    
                            return executable_payload
        except asyncio.TimeoutError:
            log.warning(f"[Bridge:PubSub:Timeout] 30s timeout reached. Broadcasting FORCE_ROLLBACK", extra=trace_ctx)
            
            try:
                await rpc.publish_intent(
                    channel=f"mcp.intent.queue.{target_server_id}",
                    payload={"handle_id": handle_id, "action": "FORCE_ROLLBACK"}
                )
                log.info(f"[Bridge:Rollback] FORCE_ROLLBACK published", extra=trace_ctx)
            except Exception as e:
                log.error(f"[Bridge:Rollback:Error] Failed to broadcast FORCE_ROLLBACK", extra={"error": str(e), **trace_ctx}, exc_info=True)
                
            raise HTTPException(status_code=504, detail="Transaction suspended or upstream timeout.")
            
        finally:
            await pubsub.unsubscribe(reply_channel)
            await pubsub.close()

## HTTP Ingress Route (MCP 2026-07-28 Spec)
mcp_bridge = APIRouter(prefix="/v1/mcp-gateway", tags=["Enterprise MCP Bridge"])

@mcp_bridge.post("/{target_server_id}/invoke")
async def invoke_mcp_stateless(
    request: Request,
    target_server_id: str,
    payload: Dict[str, Any] = Body(...),
    x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    x_idempotency_key: str = Header(..., alias="x-idempotency-key"),
    x_nonce: str = Header(..., alias="x-nonce"),
    x_dpop_proof: Optional[str] = Header(None, alias="DPoP"),
    x_spiffe_id: Optional[str] = Header(None, alias="x-spiffe-id"),
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    try:
        identity = AgentIdentity(
            target_server_id=target_server_id,
            agent_uri=x_spiffe_id or "spiffe://public/agent",
            proof_of_possession=x_dpop_proof,
            receipt=x402_receipt,
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
        log.error(f"[Bridge:Ingress:Fatal] Internal Facade Fracture", extra={"error": str(e), "idempotency_key": str(x_idempotency_key)}, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Edge Communication Failure")


@mcp_bridge.get("/{target_server_id}/tools")
async def discover_tools(
    request: Request,
    target_server_id: str,
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    adapter: TransitionBridge = request.app.state.mcp_transition_adapter
    ephemeral_identity = AgentIdentity(
        target_server_id=target_server_id,
        agent_uri="spiffe://gateway/internal_discovery",
        proof_of_possession=None,
        receipt=None,
        client_ip=request.client.host if request.client else "127.0.0.1",
        nonce=uuid.uuid4().hex,
        idempotency_key=uuid.uuid4().hex
    )
    discovery_payload = {
        "jsonrpc": "2.0",
        "id": "discovery_" + ephemeral_identity.idempotency_key[:8],
        "method": "tools/list",
        "params": {}
    }
    
    try:
        result = await adapter.invoke_mcp_sync(
            identity=ephemeral_identity,
            payload=discovery_payload,
            target_uri=str(request.url),
            target_method="POST",
            rpc=rpc
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[Gateway:Discovery] Failed to fetch tools for {target_server_id}: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail="Tool discovery failed due to internal error.")