# fiber.gateway.contract.token.gen
import time
import jwt
from typing import Literal, Optional
from cryptography.hazmat.primitives.asymmetric import ed25519
from xphi.arch.bound.adapter.pta import NodeSigner

FinOpsOpType = Literal["PROVISION", "SYNC_QUOTA", "TOP_UP", "REVOKE"]


class FinOpsTokenGenerator:
    def __init__(self, private_key_hex: Optional[str] = None, issuer: str = "fiber_local_auth"):
        if private_key_hex:
            seed_bytes = bytes.fromhex(private_key_hex.strip())
        else:
            seed_bytes = bytes(NodeSigner.get_instance().signing_key)

        self.private_key = ed25519.Ed25519PrivateKey.from_private_bytes(seed_bytes)
        self.issuer = issuer
        self.algorithm = "EdDSA"

    def generate_token(
        self,
        tenant_id: str,
        budget_usd: float,
        op: FinOpsOpType = "SYNC_QUOTA",
        policy_ver: int = 1,
        allocation_id: Optional[str] = None,
        ttl_seconds: int = 3600 * 24 * 30
    ) -> str:
        now = int(time.time())
        jti = allocation_id or f"alloc_{tenant_id}_v{policy_ver}"
        effective_budget = 0.0 if op == "REVOKE" else float(budget_usd)

        payload = {
            "iss": self.issuer,
            "sub": tenant_id,
            "jti": jti,
            "iat": now,
            "exp": now + ttl_seconds,
            "budget_limit_usd": effective_budget,
            "finops": {
                "op": op,
                "budget_usd": effective_budget,
                "exchange_rate": 1000,
                "policy_ver": int(policy_ver)
            }
        }
        return jwt.encode(payload, self.private_key, algorithm=self.algorithm)