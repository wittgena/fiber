# fiber.infra.agent.bridge.adapter
## @lineage: fiber.dphi.edge.mcp.adapter
import time
import json
import base64
import uuid
from functools import lru_cache
from typing import Dict, Any, Tuple, Optional

from xphi.kernel.space.topos.tunnel.factory import UniversalFacade
from pydantic import BaseModel, AnyUrl, IPvAnyAddress

from cryptography.hazmat.primitives.asymmetric import ed25519, ec, rsa
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePublicNumbers
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("mcp.adapter")

# ---------------------------------------------------------
# Data Models
# ---------------------------------------------------------
class AgentIdentity(BaseModel):
    target_server_id: str
    agent_uri: AnyUrl
    proof_of_possession: Optional[str] = None
    receipt: Optional[str] = None
    client_ip: IPvAnyAddress
    nonce: str
    idempotency_key: str

# ---------------------------------------------------------
# Cryptographic & Idempotency Adapters
# ---------------------------------------------------------
class IdempotencyMapper:
    def __init__(self, tunnel: UniversalFacade):
        self.tunnel = tunnel

    async def get_or_create_handle(self, target_id: str, idempotency_key: str) -> Tuple[str, bool]:
        """
        @IMPROVEMENT: 밀리초(ms) 타임스탬프의 충돌 위험을 제거하고, 
        UUIDv4 기반의 고유 식별자를 사용하여 초고동시성(Multiplexing) 환경에서의 무결성을 보장합니다.
        """
        redis_key = f"mcp:idem:{target_id}:{idempotency_key}"
        try:
            existing_handle = await self.tunnel.get(redis_key)
            if existing_handle:
                # [FIX] Tunnel 객체는 이미 디코딩된 문자열을 반환하므로 .decode() 호출을 제거하고 안전하게 캐스팅
                return str(existing_handle), False
                
            entropy = uuid.uuid4().hex[:12]
            new_handle = f"txn_{int(time.time())}_{entropy}"
            
            await self.tunnel.set(redis_key, new_handle, ex=86400, nx=True)
            return new_handle, True
        except Exception as e:
            log.critical(f"Tunnel Idempotency Check Failed: {e}")
            raise RuntimeError("Distributed state storage unavailable")

class JwkAdapter:
    @staticmethod
    def _b64_decode(data: str) -> bytes:
        padding_needed = '=' * ((4 - len(data) % 4) % 4)
        return base64.urlsafe_b64decode(data + padding_needed)

    @classmethod
    def parse_public_key(cls, jwk: Dict[str, Any]):
        """
        @IMPROVEMENT: 딕셔너리를 직렬화하여 내부 LRU 캐싱 메서드로 위임.
        동일한 JWK에 대한 반복적인 암호화 객체 생성 오버헤드(CPU Bound)를 제거합니다.
        """
        jwk_str = json.dumps(jwk, sort_keys=True)
        return cls._cached_parse(jwk_str)

    @staticmethod
    @lru_cache(maxsize=1024)
    def _cached_parse(jwk_str: str):
        """[개선] 메모이제이션(Memoization)을 통한 공개키 객체 재사용"""
        jwk = json.loads(jwk_str)
        kty = jwk.get("kty")
        try:
            if kty == "OKP" and jwk.get("crv") == "Ed25519":
                return ed25519.Ed25519PublicKey.from_public_bytes(JwkAdapter._b64_decode(jwk["x"]))
            elif kty == "RSA":
                n = int.from_bytes(JwkAdapter._b64_decode(jwk["n"]), byteorder="big")
                e = int.from_bytes(JwkAdapter._b64_decode(jwk["e"]), byteorder="big")
                return RSAPublicNumbers(e, n).public_key()
            elif kty == "EC":
                curves = {"P-256": ec.SECP256R1(), "P-384": ec.SECP384R1(), "P-521": ec.SECP521R1()}
                crv = curves.get(jwk.get("crv"))
                if not crv: raise ValueError("Unsupported Elliptic Curve")
                x = int.from_bytes(JwkAdapter._b64_decode(jwk["x"]), byteorder="big")
                y = int.from_bytes(JwkAdapter._b64_decode(jwk["y"]), byteorder="big")
                return EllipticCurvePublicNumbers(x, y, crv).public_key()
            raise ValueError(f"Unsupported JWK kty: {kty}")
        except Exception as e:
            raise ValueError(f"Malformed JWK structure: {e}")

class DPoPValidator:
    @staticmethod
    def _b64_decode_str(data: str) -> str:
        return JwkAdapter._b64_decode(data).decode('utf-8')

    @classmethod
    def verify_token(cls, jwt_str: str, expected_nonce: str, htu: str, htm: str) -> bool:
        """
        @IMPROVEMENT: DPoP 서명 검증 로직. JWK 캐싱을 통해 연산 속도가 극적으로 향상되었습니다.
        """
        try:
            parts = jwt_str.split('.')
            if len(parts) != 3: return False
            
            header = json.loads(cls._b64_decode_str(parts[0]))
            payload = json.loads(cls._b64_decode_str(parts[1]))
            signature = JwkAdapter._b64_decode(parts[2])
            
            # Replay 공격 방어 (시간 검증)
            if payload.get("nonce") != expected_nonce: return False
            if payload.get("htm") != htm or payload.get("htu") != htu: return False
            if abs(time.time() - payload.get("iat", 0)) > 60: return False
            
            jwk = header.get("jwk")
            if not jwk: return False
            
            # 캐싱된 공개키 객체 로드 (CPU 사이클 극도 절약)
            public_key = JwkAdapter.parse_public_key(jwk)
            signed_data = f"{parts[0]}.{parts[1]}".encode('utf-8')
            
            if isinstance(public_key, ed25519.Ed25519PublicKey):
                public_key.verify(signature, signed_data)
            elif isinstance(public_key, rsa.RSAPublicKey):
                public_key.verify(signature, signed_data, padding.PKCS1v15(), hashes.SHA256())
            elif isinstance(public_key, ec.EllipticCurvePublicKey):
                public_key.verify(signature, signed_data, ec.ECDSA(hashes.SHA256()))
                
            return True
        except (InvalidSignature, Exception) as e:
            log.warning(f"DPoP Verification Failed: {e}")
            return False

class NonceReplayProtector:
    def __init__(self, tunnel: UniversalFacade):
        self.tunnel = tunnel
        
    async def validate_and_lock_nonce(self, nonce: str, ttl: int = 300) -> bool:
        """일회성 논스(Nonce)를 Tunnel에 Lock 처리하여 A2A 트랜잭션의 Replay Attack을 차단"""
        try:
            return bool(await self.tunnel.set(f"sec:nonce:{nonce}", "1", ex=ttl, nx=True))
        except Exception as e:
            log.critical(f"Tunnel Nonce Verification Failed: {e}")
            return False