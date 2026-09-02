# fiber.dphi.edge.transition.bridge
import time
import json
import base64
from typing import Dict, Any, Optional, List

import redis.asyncio as aioredis
from pydantic import BaseModel, Field, AnyUrl, IPvAnyAddress

from cryptography.hazmat.primitives.asymmetric import ed25519, ec, rsa
from cryptography.exceptions import InvalidSignature

from xphi.kernel.dphi.ledger.consensus import KernelLedger, LogicStream, SealedKernel, LedgerRole
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bridge.transition")

class TransitionResult(BaseModel):
    success: bool
    status: str
    code: Optional[int] = None
    error: Optional[str] = None
    data: Optional[Dict[str, Any]] = None

class AgentIdentity(BaseModel):
    tenant_id: str
    principal_id: str
    agent_uri: AnyUrl
    proof_of_possession: str = Field(..., description="RFC 9449 DPoP JWT")
    scopes: List[str] = Field(default_factory=list)
    client_ip: IPvAnyAddress
    nonce: str
    target_method: str = Field(default="POST")
    target_uri: str = Field(default="/mcp-gateway/state")

class EventMetadata(BaseModel):
    trace_id: str
    span_id: str
    idempotency_key: str

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
        elif kty == "EC" and jwk.get("crv") == "P-256":
            raise NotImplementedError("EC P-256 JWK parsing")
        elif kty == "RSA":
            raise NotImplementedError("RSA JWK parsing")
            
        raise ValueError(f"Unsupported JWK kty/crv: {kty}")

class DPoPValidator:
    @staticmethod
    def verify_token(jwt_str: str, expected_nonce: str, htu: str, htm: str) -> bool:
        try:
            parts = jwt_str.split('.')
            if len(parts) != 3:
                raise ValueError("Invalid JWT format")
                
            header_b64, payload_b64, signature_b64 = parts
            header = json.loads(JwkAdapter._b64_decode(header_b64))
            payload = json.loads(JwkAdapter._b64_decode(payload_b64))
            
            if header.get("typ") != "dpop+jwt":
                raise ValueError("Invalid typ. Must be dpop+jwt")
                
            if payload.get("nonce") != expected_nonce:
                raise ValueError("Nonce mismatch")
            if payload.get("htu") != htu or payload.get("htm") != htm:
                raise ValueError("HTTP Target mismatch")
            
            iat = payload.get("iat", 0)
            if abs(time.time() - iat) > 300:
                raise ValueError("Token expired or issued too far in past")

            jwk = header.get("jwk")
            if not jwk:
                raise ValueError("DPoP JWT missing embedded JWK")
                
            public_key = JwkAdapter.parse_public_key(jwk)
            signature_bytes = JwkAdapter._b64_decode(signature_b64)
            signed_content = f"{header_b64}.{payload_b64}".encode('utf-8')
            
            if isinstance(public_key, ed25519.Ed25519PublicKey):
                public_key.verify(signature_bytes, signed_content)
            else:
                pass 
                
            return True
            
        except InvalidSignature:
            log.warning("[Bridge.JWK] Cryptographic signature verification failed.")
            return False
        except Exception as e:
            log.warning(f"[Bridge.JWK] DPoP Validation Fault: {str(e)}")
            return False

class NonceReplayProtector:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def validate_and_lock_nonce(self, nonce: str, ttl: int = 300) -> bool:
        ok = await self.redis.set(f"sec:nonce:{nonce}", "1", ex=ttl, nx=True)
        return bool(ok)

class TransitionBridge:
    def __init__(self, ledger: KernelLedger, nonce_protector: NonceReplayProtector):
        self.ledger = ledger
        self.nonce_protector = nonce_protector
        log.info(f"[TransitionBridge] Mounted. Operating Ledger Role: {self.ledger.role.value}")

    async def process_transition_intent(
        self, identity: AgentIdentity, meta: EventMetadata, handle_id: str, action: str, payload: Dict[str, Any]
    ) -> TransitionResult:
        
        # ---------------------------------------------------------
        # Phase 1: Zero-Trust Cryptographic Ingress
        # ---------------------------------------------------------
        if not await self.nonce_protector.validate_and_lock_nonce(identity.nonce):
            log.warning(f"[TransitionBridge] Replay attack blocked for nonce: {identity.nonce}")
            return TransitionResult(
                success=False, status="rejected", error="REPLAY_ATTACK_DETECTED", code=-32000
            )
            
        if not DPoPValidator.verify_token(
            jwt_str=identity.proof_of_possession,
            expected_nonce=identity.nonce,
            htu=identity.target_uri,
            htm=identity.target_method
        ):
            log.warning(f"[TransitionBridge] Cryptographic validation failed for URI: {identity.agent_uri}")
            return TransitionResult(
                success=False, status="rejected", error="CRYPTOGRAPHIC_BINDING_FAILED", code=-32001
            )

        # ---------------------------------------------------------
        # Phase 2: Topological Translation (Dict -> LogicStream)
        # ---------------------------------------------------------
        logic_stream = LogicStream(
            id=handle_id,
            action=f"dphi.transition.{action.lower()}", 
            payload=payload,
            metadata={
                "actor_uri": str(identity.agent_uri),
                "trace_id": meta.trace_id,
                "idempotency_key": meta.idempotency_key
            }
        )

        # ---------------------------------------------------------
        # Phase 3: Core Ledger Injection (Sublimation)
        # ---------------------------------------------------------
        try:
            sealed_kernel: Optional[SealedKernel] = await self.ledger.propose_and_seal(logic_stream)
            if self.ledger.role == LedgerRole.FOLLOWER:
                return TransitionResult(
                    success=True,
                    status="commit_accepted",
                    data={"message": "Intent successfully queued to Kernel Ledger mempool.", "handle_id": handle_id}
                )
                
            else:
                if sealed_kernel:
                    return TransitionResult(
                        success=True,
                        status="sealed",
                        data={
                            "kernel_id": sealed_kernel.kernel_id,
                            "tension": sealed_kernel.tension_at_seal,
                            "resolved_state": sealed_kernel.executable_payload
                        }
                    )
                else:
                    return TransitionResult(
                        success=False,
                        status="rejected",
                        error="WASM_EXECUTION_REJECTED_OR_LOCKED",
                        code=-32009
                    )
        except Exception as e:
            log.error(f"[TransitionBridge] Fatal Ledger injection fault: {e}", exc_info=True)
            return TransitionResult(
                success=False, status="fault", error="INTERNAL_KERNEL_FAULT", code=-32003
            )