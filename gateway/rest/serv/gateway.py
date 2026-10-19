# fiber.gateway.rest.serv.gateway
import json
import asyncio
import time
import uuid
from typing import Dict, Any, Optional, Union

from fastapi import APIRouter, Body, Header, Request, HTTPException, Depends
from fastapi.responses import JSONResponse

from xphi.node.rpc.method import RpcMethod
from fiber.gateway.rest.serv.depend import get_rpc_client

from xphi.arch.bound.client.rpc import InternalRpcClient, RpcException
from xphi.arch.bound.adapter.gateway import AgentIdentity, IdempotencyMapper, NonceReplayProtector, DPoPValidator
from xphi.arch.xor.parser.mcp import McpPayloadParser
from xphi.node.space.tunnel.factory import TunnelFactory
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

    async def _is_receipt_required(self, target_server_id: str) -> bool:
        try:
            tunnel = await TunnelFactory.get_default()
            policy = await tunnel.get(f"config:fuel:{target_server_id}")
            if policy is not None:
                return policy.decode('utf-8') == "1" if isinstance(policy, bytes) else str(policy) == "1"
        except Exception as e:
            log.warning(f"[Bridge:Config] Failed to fetch receipt flag for {target_server_id}: {e}")
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

        # Authentication (DPoP Mandatory) & Fuel Validation
        if is_discovery_phase:
            # Bypass strict auth and fuel checks for discovery phases
            log.debug(f"[Bridge:Auth] Bypassing strict auth for discovery method: {mcp_method}", extra=trace_ctx)
        else:
            # DPoP 필수 검증 (Mandatory)
            if not identity.proof_of_possession or not DPoPValidator.verify_token(identity.proof_of_possession, identity.nonce, target_uri, target_method):
                log.warning(f"[Bridge:Auth] DPoP Verification Failed or Missing", extra=trace_ctx)
                raise HTTPException(status_code=401, detail="CRYPTOGRAPHIC_BINDING_FAILED")

            # 영수증 선택적 검증 (Flag 기반)
            if await self._is_receipt_required(identity.target_server_id):
                if not identity.receipt:
                    log.warning(f"[Bridge:Fuel] Missing Receipt for billing-enabled server", extra=trace_ctx)
                    raise HTTPException(status_code=402, detail="Payment Required: Receipt Missing")
                
                try:
                    await rpc.call(RpcMethod.VALIDATE_FUEL_RECEIPT,{
                        "target_server_id": identity.target_server_id,
                        "action": payload.get("params", {}).get("name", "unknown_tool"),
                        "fuel_receipt": identity.receipt
                    })
                except RpcException as e:
                    log.warning(f"[Bridge:Fuel] Fuel validation rejected", extra={"status_code": e.status_code, "detail": e.detail, **trace_ctx})
                    raise HTTPException(status_code=402, detail=f"Fuel/Intent Rejected: {e.detail}")

        # [추가] 기성 IDE(Cursor/Claude)의 Long-Polling 지원을 위해 payload._meta.hold_on_yield 플래그 추출
        hold_on_yield = payload.get("params", {}).get("_meta", {}).get("hold_on_yield", False)
        trace_ctx["hold_on_yield"] = hold_on_yield

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
        
        # 기성 IDE를 위한 Long-Hold 모드일 경우 타임아웃을 300초(5분)로 대폭 연장
        wait_timeout = 300.0 if trace_ctx.get("hold_on_yield") else 30.0

        try:
            async with asyncio.timeout(wait_timeout):
                async for msg in pubsub.listen():
                    if msg and msg["type"] == "message":
                        state_data = json.loads(msg["data"])
                        status = state_data.get("status")
                        duration_ms = round((time.time() - start_time) * 1000, 2)
                        
                        trace_ctx["duration_ms"] = duration_ms
                        
                        if status == "YIELD":
                            log.info(f"[Bridge:PubSub:Yield] Agent prompted for input", extra=trace_ctx)
                            prompt_payload = state_data.get("executable_payload", {})
                            
                            # 외부 Side-Channel Glue(CLI, Telegram, Slack)가 즉시 인지하도록 이벤트 버스 발행
                            yield_event = {
                                "event": "MCP_PHASE_YIELD",
                                "handle_id": handle_id,
                                "target_server_id": target_server_id,
                                "agent": trace_ctx.get("agent"),
                                "prompt": prompt_payload
                            }
                            try:
                                await tunnel.publish("eco.telemetry.events", json.dumps(yield_event))
                                log.debug(f"[Bridge:Telemetry] YIELD event broadcasted to eco.telemetry.events", extra=trace_ctx)
                            except Exception as emit_err:
                                log.error(f"[Bridge:Telemetry:Error] Failed to broadcast YIELD: {emit_err}", extra=trace_ctx)
                            
                            # 기성 IDE(Cursor 등) 호환 모드일 경우 202로 끊지 않고 동일한 루프에서 RESOLVED 대기 연장
                            if trace_ctx.get("hold_on_yield"):
                                log.info(f"[Bridge:Hold] Holding HTTP connection for Out-of-Band RESUME...", extra=trace_ctx)
                                continue
                                
                            return JSONResponse(status_code=202, content=prompt_payload, headers=location_header)
                        
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
            log.warning(f"[Bridge:PubSub:Timeout] {wait_timeout}s timeout reached. Broadcasting FORCE_ROLLBACK", extra=trace_ctx)
            
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
    # fuel_receipt: Optional[str] = Header(None, alias="Fuel-Receipt"),
    x_idempotency_key: str = Header(..., alias="x-idempotency-key"),
    x_nonce: str = Header(..., alias="x-nonce"),
    x_dpop_proof: Optional[str] = Header(None, alias="DPoP"),
    x_spiffe_id: Optional[str] = Header(None, alias="x-spiffe-id"),
    x_fiber_hitl_mode: Optional[str] = Header(None, alias="x-fiber-hitl-mode"),  # hold_on_yield 명시적 요청
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    internal_receipt = getattr(request.state, "fuel_receipt", None)
    try:
        identity = AgentIdentity(
            target_server_id=target_server_id,
            agent_uri=x_spiffe_id or "spiffe://public/agent",
            proof_of_possession=x_dpop_proof,
            receipt=internal_receipt,
            client_ip=request.client.host if request.client else "0.0.0.0",
            nonce=x_nonce,
            idempotency_key=x_idempotency_key
        )
        
        adapter: TransitionBridge = request.app.state.mcp_transition_adapter
        target_uri = str(request.url)
        target_method = request.method
        
        # Unified endpoint 경유 여부 확인
        is_unified = getattr(request.state, "is_unified_mcp", False)
        
        # 클라이언트가 명시적으로 'hold'를 요청했거나, Unified 라우팅을 거친 클라이언트(보통 기성 IDE)의 경우 Hold On Yield 활성화
        if x_fiber_hitl_mode == "hold" or is_unified:
            if "params" not in payload: payload["params"] = {}
            if "_meta" not in payload["params"]: payload["params"]["_meta"] = {}
            payload["params"]["_meta"]["hold_on_yield"] = True

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


ACTIVE_TARGETS_TUNNEL_KEY = "mcp:registry:active_targets"
NAMESPACE_DELIM = "__"


async def _get_active_targets(request: Request) -> list[str]:
    try:
        tunnel = getattr(request.app.state, "tunnel", None) or await TunnelFactory.get_default()
        if tunnel:
            raw = await tunnel.get(ACTIVE_TARGETS_TUNNEL_KEY)
            if raw:
                data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                if isinstance(data, list) and data:
                    return [str(x) for x in data]
    except Exception as e:
        log.warning(f"[Gateway:Unified] Failed to read active targets from tunnel: {e}")

    fallback = getattr(request.app.state, "mcp_active_targets", None)
    return [str(x) for x in fallback] if isinstance(fallback, list) else []


@mcp_bridge.get("/tools")
async def discover_unified_tools(
    request: Request,
    rpc: InternalRpcClient = Depends(get_rpc_client),
):
    targets = await _get_active_targets(request)

    async def _fetch(tid: str) -> list[Dict[str, Any]]:
        try:
            res = await discover_tools(request=request, target_server_id=tid, rpc=rpc)
            tools = res.get("result", {}).get("tools", []) if isinstance(res, dict) else []
            return [
                {**t, "name": f"{tid}{NAMESPACE_DELIM}{t['name']}"}
                for t in tools if isinstance(t, dict) and "name" in t
            ]
        except Exception as e:
            log.warning(f"[Gateway:Unified] Tool discovery skipped for '{tid}': {e}")
            return []

    results = await asyncio.gather(*(_fetch(t) for t in targets)) if targets else []
    merged_tools = [tool for group in results for tool in group]
    return {"jsonrpc": "2.0", "id": "unified_discovery", "result": {"tools": merged_tools}}


@mcp_bridge.post("/invoke")
async def invoke_unified_mcp(
    request: Request,
    payload: Dict[str, Any] = Body(...),
    x_idempotency_key: Optional[str] = Header(None, alias="x-idempotency-key"),
    x_nonce: Optional[str] = Header(None, alias="x-nonce"),
    x_dpop_proof: Optional[str] = Header(None, alias="DPoP"),
    x_spiffe_id: Optional[str] = Header(None, alias="x-spiffe-id"),
    rpc: InternalRpcClient = Depends(get_rpc_client),
):
    req_id = payload.get("id")
    method = payload.get("method", "")

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "fiber-mcp-gateway", "version": "1.0.0"},
            },
        }
    if method.startswith("notifications/"):
        return JSONResponse(status_code=202, content={})

    if method == "tools/list":
        res = await discover_unified_tools(request=request, rpc=rpc)
        res["id"] = req_id
        return res

    params = dict(payload.get("params") or {})
    raw_name = str(params.get("name", ""))
    if NAMESPACE_DELIM not in raw_name:
        raise HTTPException(
            status_code=400,
            detail=f"Tool name must be namespaced as '<target>{NAMESPACE_DELIM}<tool>', got '{raw_name}'",
        )

    target_server_id, actual_tool_name = raw_name.split(NAMESPACE_DELIM, 1)
    params["name"] = actual_tool_name
    routed_payload = {**payload, "params": params}

    # Unified Endpoint 경유 플래그 설정 (invoke_mcp_stateless 내부에서 hold_on_yield 처리에 활용됨)
    request.state.is_unified_mcp = True

    return await invoke_mcp_stateless(
        request=request,
        target_server_id=target_server_id,
        payload=routed_payload,
        x_idempotency_key=x_idempotency_key or uuid.uuid4().hex,
        x_nonce=x_nonce or uuid.uuid4().hex,
        x_dpop_proof=x_dpop_proof,
        x_spiffe_id=x_spiffe_id,
        rpc=rpc,
    )