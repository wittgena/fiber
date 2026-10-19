# fiber.gateway.security
## @lineage: fiber.gateway.rest.security
import os
import time
import json
import secrets
import orjson
import jwt
from pathlib import Path
from typing import Any, Optional, Tuple

from cryptography.hazmat.primitives.asymmetric import ed25519
from fastapi import Request, HTTPException, status

from xphi.arch.xor.secret.cipher import Cipher
from xphi.arch.xor.secret.client import get_secret_from_vendor, KMSVendor
from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.receptor.warden import SecretAuditor
from xphi.watcher.plane.emitter import get_emitter

from xphi.node.runtime.wasm.broker import DphiBroker
from xphi.arch.model.auth import NodeAuthPayload
from xphi.arch.bound.adapter.pta import SignStateAdapter
from xphi.arch.contract.config.env import FINOPS_JWT_PUBLIC_KEY

from xphi.arch.bound.adapter.settlement import (
    Ap2MandateConstraints,
    Ap2MandatePayload,
    Ap2Authorization,
    Ap2MandateResult
)
from fiber.infra.watcher.finops import default_finops_watcher

log = get_emitter(__name__)

SIGN_ROOT = resolve_path("sign")

class SecurityProvisioner:
    LOCAL_KEY_PATH = Path(SIGN_ROOT) / "local_cipher.key"

    @classmethod
    def _get_or_create_local_key(cls) -> str:
        if cls.LOCAL_KEY_PATH.exists():
            try:
                secret_key = cls.LOCAL_KEY_PATH.read_text(encoding="utf-8").strip()
                log.info(f"Loaded persistent cipher key from local disk ({cls.LOCAL_KEY_PATH}).")
                return secret_key
            except Exception as e:
                log.error(f"Failed to read local cipher key: {e}")
                raise RuntimeError("Local key file exists but could not be read. Check file permissions.") from e
        
        # Generate a 32-byte random key if not exists
        new_secret_key = secrets.token_hex(32)
        try:
            cls.LOCAL_KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
            cls.LOCAL_KEY_PATH.write_text(new_secret_key, encoding="utf-8")
            if os.name != 'nt':
                cls.LOCAL_KEY_PATH.chmod(0o600)
            log.warning(f"🚨 [ZERO-CONFIG] Generated and securely persisted a NEW cipher key to {cls.LOCAL_KEY_PATH}")
            log.warning("Please backup this file! If lost, previously encrypted audit logs cannot be decrypted.")
        except Exception as e:
            log.error(f"Failed to persist new cipher key to disk: {e}")
            raise RuntimeError("Cannot write to SIGN_ROOT. Check filesystem permissions.") from e
            
        return new_secret_key

    @classmethod
    def provision_secret_auditor(cls, secret_name: str = "DPHI_CIPHER_KEY", key_manager: KMSVendor = KMSVendor.LOCAL) -> SecretAuditor:
        manager_name = getattr(key_manager, 'name', str(key_manager))
        log.info(f"Provisioning Cryptographic Secret Auditor (Manager: {manager_name})...")
        
        secret_key = None
        if key_manager != KMSVendor.LOCAL:
            try:
                secret_key = get_secret_from_vendor(
                    client=None,
                    key_manager=key_manager,
                    secret_name=secret_name
                )
            except Exception as e:
                log.debug(f"KMS retrieval bypassed or failed: {e}")

        if not secret_key:
            log.debug("Engaging Local Persistence Flow (Zero-Config).")
            secret_key = cls._get_or_create_local_key()
                
        return SecretAuditor(cipher=Cipher(secret_key=secret_key))


class TokenTranslator:
    """Validates stateless JWTs issued by external partners (FinOps/IdP) and mints real-time AP2 Mandates required by the internal backend"""
    
    @classmethod
    def _resolve_public_key(cls, raw_pub_key: str) -> Any:
        """Resolves either a 64-char Ed25519 Hex string or a standard PEM/SSH public key string."""
        cleaned = raw_pub_key.strip()
        if len(cleaned) == 64:
            try:
                return ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(cleaned))
            except ValueError:
                pass
        return cleaned

    @classmethod
    def _decode_jwt(cls, raw_token: str) -> dict[str, Any]:
        pub_key_str = FINOPS_JWT_PUBLIC_KEY or os.environ.get("FINOPS_JWT_PUBLIC_KEY", "")
        if pub_key_str:
            resolved_key = cls._resolve_public_key(pub_key_str)
            return jwt.decode(raw_token, resolved_key, algorithms=["EdDSA", "RS256", "ES256"])
        
        return jwt.decode(raw_token, options={"verify_signature": False})

    @classmethod
    def _extract_and_sync_finops_state(cls, claims: dict[str, Any]) -> Tuple[bool, str, float]:
        """
        Extracts FinOps claims, enforces version validity & JTI idempotency via FinOpsWatcher,
        and determines whether an AP2 Mandate should be issued.
        Returns: (is_allowed, tenant_id, granted_budget)
        """
        tenant_id = str(claims.get("sub") or "anonymous_tenant")
        jti = claims.get("jti")
        finops_block = claims.get("finops") or {}

        if "budget_usd" in finops_block:
            granted_budget = float(finops_block["budget_usd"])
        else:
            granted_budget = float(claims.get("budget_limit_usd", 1.0))

        op = str(finops_block.get("op") or claims.get("op") or "SYNC_QUOTA").upper()
        exchange_rate = int(finops_block.get("exchange_rate") or 1000)
        
        # Default to version 1 if omitted so legacy/unversioned tokens cannot bypass v2+ upgrades
        raw_ver = finops_block.get("policy_ver")
        if raw_ver is None:
            raw_ver = claims.get("policy_ver", 1)
        policy_ver = int(raw_ver)

        # 1. Version Check: Reject stale tokens immediately
        if not default_finops_watcher.is_version_valid(tenant_id, policy_ver):
            log.warning(f"[TokenTranslator] Rejected outdated token version (v{policy_ver}) for tenant '{tenant_id}'.")
            return False, tenant_id, 0.0

        # 2. State Synchronization (JTI idempotency & PROVISION / SYNC_QUOTA / TOP_UP / REVOKE)
        default_finops_watcher.apply_finops_spec(
            tenant_id=tenant_id,
            op=op,
            budget_usd=granted_budget,
            exchange_rate=exchange_rate,
            policy_ver=policy_ver,
            jti=jti
        )

        # 3. Revocation / Zero-Budget Gate
        if op == "REVOKE" or granted_budget <= 0.0:
            log.warning(f"[TokenTranslator] Access revoked or zero budget for tenant '{tenant_id}' (op={op}).")
            return False, tenant_id, 0.0

        return True, tenant_id, granted_budget

    @classmethod
    def _mint_ap2_mandate(cls, tenant_id: str, granted_budget: float, ttl_ms: int = 3600 * 1000) -> str:
        """Constructs and cryptographically signs the internal AP2 Mandate receipt."""
        current_ts = int(time.time() * 1000)
        expire_ts = current_ts + ttl_ms

        constraints = Ap2MandateConstraints(max_spend_usdc=str(granted_budget), expiration_ts=expire_ts)
        mandate_payload = Ap2MandatePayload(
            protocol="AP2-v1.0",
            requester_id=tenant_id,
            target_action="llm_compute_gateway",
            constraints=constraints,
            issued_at=current_ts
        )

        mandate_dict = mandate_payload.model_dump(exclude_none=True)
        canonical_mandate = json.dumps(mandate_dict, sort_keys=True, separators=(',', ':'))

        real_signature = SignStateAdapter.sign_state_payload({"payload": canonical_mandate})
        node_pubkey = SignStateAdapter.get_signer_pubkey()

        auth = Ap2Authorization(signer_pub=node_pubkey, signature=real_signature)
        ap2_result = Ap2MandateResult(mandate=mandate_payload, authorization=auth)
        return json.dumps(ap2_result.model_dump(exclude_none=True))

    @classmethod
    async def exchange(cls, raw_token: Optional[str], request: Optional[Request] = None) -> Optional[str]:
        if not raw_token:
            return None
            
        # Backward Compatibility: Bypass native AP2/fuel receipts or E2E mock strings
        if not raw_token.startswith("ey"):
            log.debug("[TokenTranslator] Token bypassed translation (native AP2/receipt detected).")
            return raw_token

        log.info("[TokenTranslator] Enterprise JWT detected. Initiating internal AP2 Mandate minting.")
        
        try:
            claims = cls._decode_jwt(raw_token)
            is_allowed, tenant_id, granted_budget = cls._extract_and_sync_finops_state(claims)
            if not is_allowed:
                return None

            translated_receipt = cls._mint_ap2_mandate(tenant_id=tenant_id, granted_budget=granted_budget)
            log.info(f"[TokenTranslator] JWT exchanged to AP2 Mandate for '{tenant_id}' (Budget: ${granted_budget})")
            return translated_receipt
        except Exception as e:
            log.error(f"[TokenTranslator] AP2 Mandate minting crashed: {e}", exc_info=True)
            return None


class KernelAuthenticator:
    """Extracts the injected hidden receipt and transparently delegates kernel Wasm broker authorization without business logic intervention"""
    @classmethod
    async def authorize_intent(
        cls, 
        request: Request, 
        action: str, 
        model: str, 
        max_tokens: Optional[int] = None
    ) -> NodeAuthPayload:
        receipt = getattr(request.state, "fuel_receipt", None)
        intent_payload = {
            "action": action,
            "model": model,
            "max_tokens_requested": max_tokens,
            "receipt": receipt
        }
        
        broker: DphiBroker = getattr(request.app.state, "broker", None)
        if not broker:
            log.critical("[KernelAuthenticator] Wasm Broker is offline or not injected.")
            raise HTTPException(status_code=500, detail="Internal Security Engine Offline")
            
        try:
            auth_res = await broker.invoke("AUTHORIZE_INTENT", orjson.dumps(intent_payload).decode('utf-8'))
        except Exception as e:
            log.error(f"[KernelAuthenticator] Kernel intent verification crashed: {e}")
            raise HTTPException(status_code=500, detail="Internal Intent Verification Failure")

        if not auth_res.success:
            log.warning(f"[KernelAuthenticator] Intent Rejected by Kernel: {auth_res.error}")
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED, 
                detail="Zero-Trust Enforced: Token Quota Exceeded or Invalid Access Credential.",
                headers={"WWW-Authenticate": 'Bearer realm="api"'}
            )
        return NodeAuthPayload.model_validate_json(auth_res.output)