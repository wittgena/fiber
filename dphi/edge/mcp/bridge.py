# fiber.dphi.edge.mcp.bridge
import json
import asyncio
from typing import Dict, Any, Optional

from fastapi import APIRouter, Body, Header, Request, HTTPException, Depends
from fastapi.responses import JSONResponse

from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from fiber.dphi.rpc.client import InternalRpcClient
from fiber.dphi.edge.serv.depend import get_rpc_client
from fiber.dphi.edge.mcp.adapter import AgentIdentity, IdempotencyMapper, NonceReplayProtector, DPoPValidator

log = get_emitter("mcp.bridge")

# ---------------------------------------------------------
# The Facade: Pure MCP Translator & Event-Driven Orchestrator
# ---------------------------------------------------------
class TransitionBridge:
    def __init__(self, mapper: IdempotencyMapper, nonce_protector: NonceReplayProtector):
        self.mapper = mapper
        self.nonce_protector = nonce_protector
        log.info("[TransitionBridge] Mounted. Pure MCP-to-RPC translation & Zero-Latency Gateway Active.")

    async def invoke_mcp_sync(
        self, identity: AgentIdentity, payload: Dict[str, Any], target_uri: str, target_method: str, rpc: InternalRpcClient
    ) -> Dict[str, Any]:
        
        # 1. 외곽 망 보안 (Replay Attack 원천 차단)
        if not await self.nonce_protector.validate_and_lock_nonce(identity.nonce):
            raise HTTPException(status_code=423, detail="REPLAY_ATTACK_DETECTED")

        handle_id, is_new = await self.mapper.get_or_create_handle(
            identity.target_server_id, identity.idempotency_key
        )

        # ---------------------------------------------------------
        # 2. Idempotency Fast-Path (RPC 기반 원장 디커플링)
        # ---------------------------------------------------------
        if not is_new:
            # 브릿지가 DB를 직접 읽지 않고, 코어망(rpc.handler)에 상태를 질의합니다.
            state_res = await rpc.call("mcp.state.query", {"handle_id": handle_id})
            if state_res.get("exists"):
                status = state_res.get("status")
                if status == "YIELD":
                    return JSONResponse(status_code=202, content=state_res.get("executable_payload", {}))
                elif status == "FAULTED":
                    raise HTTPException(status_code=502, detail=state_res.get("error_detail", "Execution Fault"))
                elif status == "RESOLVED":
                    return state_res.get("executable_payload", {})
                    
            return JSONResponse(status_code=202, content={"message": "Transaction already in progress."})

        # ---------------------------------------------------------
        # 3. Cryptographic Binding & L402 Billing (무임승차 차단)
        # ---------------------------------------------------------
        is_authenticated = False

        if identity.proof_of_possession:
            if not DPoPValidator.verify_token(identity.proof_of_possession, identity.nonce, target_uri, target_method):
                raise HTTPException(status_code=401, detail="CRYPTOGRAPHIC_BINDING_FAILED")
            is_authenticated = True

        if identity.receipt:
            try:
                # [정렬 2] 외부망에서 들어온 결제 영수증을 코어망 정책 엔진에 선제적으로 검증받습니다.
                await rpc.call("eco.billing.receipt.validate", {
                    "action": payload.get("name", "unknown_tool"),
                    "payment_receipt": identity.receipt
                })
                is_authenticated = True
            except HTTPException as e:
                raise HTTPException(status_code=e.status_code, detail=f"Payment/Intent Rejected: {e.detail}")

        if not is_authenticated:
            raise HTTPException(status_code=401, detail="Authentication (DPoP) or Payment Receipt (X402) missing.")

        # ---------------------------------------------------------
        # 4. State Reservation & Zero-Latency Execution
        # ---------------------------------------------------------
        
        # A. 작업 시작 전, 코어망에 Pending 상태 씰링을 지시 (마찬가지로 원장 DB 직접 접근 아님)
        await rpc.call("mcp.state.pending.seal", {
            "handle_id": handle_id,
            "payload": payload,
            "target_server_id": identity.target_server_id
        })

        # B. [정렬 3] 로컬 메모리 대신 분산 Pub/Sub 채널을 이용한 글로벌 콜백 리스닝
        tunnel = await TunnelFactory.get_default()
        reply_channel = f"mcp.intent.reply.{handle_id}"
        pubsub = tunnel.pubsub()
        await pubsub.subscribe(reply_channel)

        try:
            # C. Connector(Sidecar)가 리스닝하는 큐로 작업 지시서 발송 (Fire-and-forget)
            await rpc.publish_intent(
                channel=f"mcp.intent.queue.{identity.target_server_id}",
                payload={"handle_id": handle_id, "action": "EXECUTE", "payload": payload}
            )
            log.debug(f"[Bridge] Intent {handle_id} published. Listening on {reply_channel}...")

            # D. 코어망(rpc.handler)이 원장 기록을 마치고 채널에 브로드캐스트 해줄 때까지 대기
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
                            return state_data.get("executable_payload", {})
                            
        except asyncio.TimeoutError:
            log.warning(f"[Bridge] Timeout waiting for backend resolution of {handle_id}")
            raise HTTPException(status_code=504, detail="Transaction suspended or upstream timeout.")
        finally:
            # 연결 해제: 자원 및 소켓 안전 회수
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
    x_idempotency_key: str = Header(...),
    x_nonce: str = Header(...),
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    x_dpop_proof: Optional[str] = Header(None, alias="DPoP"),
    x_spiffe_id: Optional[str] = Header(None),
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
        
        # FastAPI Application State에 마운트된 브릿지 어댑터 호출
        adapter: TransitionBridge = request.app.state.mcp_transition_adapter
        target_uri = str(request.url)
        target_method = request.method
        
        result = await adapter.invoke_mcp_sync(identity, payload, target_uri, target_method, rpc)
        
        # YIELD 전이의 경우 HTTP 202를 반환하기 위해 JSONResponse 객체가 내려옴
        if isinstance(result, JSONResponse):
            return result
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[MCP Bridge] Internal Facade Fracture: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Edge Communication Failure")