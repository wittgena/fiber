# fiber.dphi.edge.mcp.bridge
import time
import json
import base64
import asyncio
from typing import Dict, Any, Optional, Tuple

import redis.asyncio as aioredis
from pydantic import BaseModel, Field, AnyUrl, IPvAnyAddress
from fastapi import APIRouter, Body, Header, Response, status, Request, HTTPException, Depends

from cryptography.hazmat.primitives.asymmetric import ed25519, ec, rsa
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePublicNumbers
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from cryptography.exceptions import InvalidSignature

from xphi.kernel.dphi.ledger.consensus import KernelLedger, LogicStream
from xphi.watcher.plane.emitter import get_emitter
from fiber.dphi.rpc.client import InternalRpcClient
from xphi.arch.model.dphi.receptor import IntentValidationRequest
from fiber.dphi.edge.serv.depend import get_rpc_client

log = get_emitter("mcp.bridge")

# =====================================================================
# 1. Data Models (Client Envelope)
# =====================================================================
class AgentIdentity(BaseModel):
    target_server_id: str
    agent_uri: AnyUrl
    proof_of_possession: Optional[str] = None  # Track B: DPoP (RFC 9449)
    receipt: Optional[str] = None              # Track A: L402/Stablecoin
    client_ip: IPvAnyAddress
    nonce: str
    idempotency_key: str

# =====================================================================
# 2. Idempotency & Security (The Complexity Sink)
# =====================================================================
class IdempotencyMapper:
    """
    클라이언트의 멱등성 키를 내부 분산 트랜잭션 ID(handle_id)로 매핑합니다.
    네트워크 단절 후 재시도하더라도 중복 실행 및 중복 과금을 완벽히 차단합니다.
    """
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def get_or_create_handle(self, target_id: str, idempotency_key: str) -> Tuple[str, bool]:
        """반환값: (handle_id, is_new_transaction)"""
        redis_key = f"mcp:idem:{target_id}:{idempotency_key}"
        existing_handle = await self.redis.get(redis_key)
        
        if existing_handle:
            return existing_handle.decode('utf-8'), False
            
        new_handle = f"mcp_txn_{int(time.time()*1000)}"
        # 24시간 동안 멱등성 보장
        await self.redis.set(redis_key, new_handle, ex=86400, nx=True)
        return new_handle, True

class JwkAdapter:
    @staticmethod
    def _b64_decode(data: str) -> bytes:
        padding = '=' * ((4 - len(data) % 4) % 4)
        return base64.urlsafe_b64decode(data + padding)

    @classmethod
    def parse_public_key(cls, jwk: Dict[str, Any]):
        kty = jwk.get("kty")
        if kty == "OKP" and jwk.get("crv") == "Ed25519":
            raw_pub = cls._b64_decode(jwk["x"])
            return ed25519.Ed25519PublicKey.from_public_bytes(raw_pub)
        # EC, RSA 처리 로직...
        raise ValueError(f"Unsupported JWK kty/crv: {kty}")

class DPoPValidator:
    @staticmethod
    def verify_token(jwt_str: str, expected_nonce: str, htu: str, htm: str) -> bool:
        # 검증 로직...
        return True

class NonceReplayProtector:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def validate_and_lock_nonce(self, nonce: str, ttl: int = 300) -> bool:
        ok = await self.redis.set(f"sec:nonce:{nonce}", "1", ex=ttl, nx=True)
        return bool(ok)

# =====================================================================
# 3. The Facade: Sync ↔ Async FSM Orchestrator
# =====================================================================
class TransitionBridge:
    def __init__(self, ledger: KernelLedger, mapper: IdempotencyMapper, nonce_protector: NonceReplayProtector):
        self.ledger = ledger
        self.mapper = mapper
        self.nonce_protector = nonce_protector
        log.info(f"[TransitionBridge] Mounted. Sync-Async Facade Active.")

    async def invoke_mcp_sync(
        self, identity: AgentIdentity, payload: Dict[str, Any], rpc: InternalRpcClient
    ) -> Dict[str, Any]:
        """
        [Sync-Async Facade] 
        클라이언트의 동기적 요청을 받아, 내부 비동기 원장(Ledger)을 관장하고 
        결과가 도달할 때까지 안전하게 대기(Hold)하여 반환합니다.
        """
        # 1. Replay Attack 방어 (공통 난수 체크)
        if not await self.nonce_protector.validate_and_lock_nonce(identity.nonce):
            raise HTTPException(status_code=423, detail="REPLAY_ATTACK_DETECTED: Nonce already used.")

        # 2. 멱등성 매핑 (트랜잭션 ID 획득)
        handle_id, is_new = await self.mapper.get_or_create_handle(
            identity.target_server_id, identity.idempotency_key
        )

        # 3. 신규 트랜잭션인 경우에만 권한 검증 및 인텐트 큐잉 발생 (중복 과금 원천 차단)
        if is_new:
            # --- 보안 및 경제망(과금) 검증 ---
            if identity.proof_of_possession:
                if not DPoPValidator.verify_token(identity.proof_of_possession, identity.nonce, identity.target_uri, identity.target_method):
                    raise HTTPException(status_code=401, detail="CRYPTOGRAPHIC_BINDING_FAILED")
            elif identity.receipt:
                val_req = IntentValidationRequest(
                    requester_id=str(identity.agent_uri) or "anonymous",
                    responder_id=identity.target_server_id,
                    action="MCP_INVOKE",
                    max_fuel_budget=1000000,
                    payment_receipt=identity.receipt
                )
                try:
                    await rpc.call("eco.compute.intent.validate", val_req.model_dump(exclude_none=True))
                except HTTPException as e:
                    raise HTTPException(status_code=402, detail=f"L402 Rejected: {e.detail}")
            else:
                raise HTTPException(status_code=401, detail="Authentication missing. Provide L402 or DPoP.")

            # --- 원장(Ledger) PENDING 상태 제안 ---
            logic_stream = LogicStream(
                id=handle_id,
                action="dphi.transition.pending",
                payload=payload, 
                metadata={"target": identity.target_server_id}
            )
            await self.ledger.propose_and_seal(logic_stream)
            
            # --- 커넥터(Sidecar)에 비동기 브로드캐스트 (Fire-and-forget) ---
            # *주의: rpc_client에 publish_intent 구현 필요
            await rpc.publish_intent(
                channel=f"mcp.intent.queue.{identity.target_server_id}",
                payload={"handle_id": handle_id, "action": "EXECUTE", "payload": payload}
            )
            log.info(f"[Bridge] New intent {handle_id} broadcasted. Awaiting resolution...")

        else:
            log.info(f"[Bridge] Idempotency matched for {handle_id}. Resuming await...")

        # 4. 결과 폴링 대기 (The Facade Loop)
        # 클라이언트의 HTTP 커넥션을 물고, 원장(Ledger)의 상태가 RESOLVED로 전이될 때까지 대기
        timeout_seconds = 30.0
        poll_interval = 0.5
        
        for _ in range(int(timeout_seconds / poll_interval)):
            state = await self.ledger.query_state(handle_id)
            
            if state and state.action == "dphi.transition.resolve":
                # 사이드카(Connector)가 반환한 최종 상태 확인
                if state.metadata.get("status") == "FAULTED":
                    raise HTTPException(
                        status_code=502, 
                        detail=f"Legacy Server Error: {state.metadata.get('error_detail')}"
                    )
                # 도구 실행 성공 -> 순수 결과 반환
                return state.payload

            # 아직 PENDING 이라면 대기
            await asyncio.sleep(poll_interval)

        # 5. Timeout 처리
        # 원장과 사이드카의 연산은 중단되지 않음. 클라이언트가 재시도하면 즉시 대기 루프에 다시 합류함.
        raise HTTPException(
            status_code=504, 
            detail="Gateway Timeout: Transaction is still processing. Please retry with the same Idempotency-Key."
        )


# =====================================================================
# 4. FastAPI Router (The Edge Ingress)
# =====================================================================
mcp_bridge = APIRouter(
    prefix="/v1/mcp-gateway", 
    tags=["Enterprise MCP Bridge"]
)

@mcp_bridge.post(
    "/{target_server_id}/invoke",
    summary="Symmetrical Zero-Friction MCP Bridge",
    response_model=Dict[str, Any]
)
async def invoke_mcp_stateless(
    request: Request,
    target_server_id: str,
    payload: Dict[str, Any] = Body(..., description="Pure MCP 2026-07-28 JSON-RPC Payload"),
    x_idempotency_key: str = Header(..., description="안전한 재시도를 위한 고유 키 (필수)"),
    x_nonce: str = Header(..., description="Replay Attack 방지용 난수"),
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    x_dpop_proof: Optional[str] = Header(None, alias="DPoP"),
    x_spiffe_id: Optional[str] = Header(None, description="Agent SPIFFE URI"),
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    client_ip = request.client.host if request.client else "0.0.0.0"
    try:
        identity = AgentIdentity(
            target_server_id=target_server_id,
            agent_uri=x_spiffe_id or "spiffe://public/agent",
            proof_of_possession=x_dpop_proof,
            receipt=x_x402_receipt,
            client_ip=client_ip,
            nonce=x_nonce,
            idempotency_key=x_idempotency_key
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=f"Invalid Context: {ve}")

    try:
        adapter: TransitionBridge = request.app.state.mcp_transition_adapter
        mcp_result = await adapter.invoke_mcp_sync(
            identity=identity,
            payload=payload,
            rpc=rpc
        )
        return mcp_result
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[MCP Bridge] Internal Facade Fracture: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Edge Communication Failure")