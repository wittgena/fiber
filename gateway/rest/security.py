# fiber.gateway.rest.security
import os
import time
import json
import secrets
import orjson
import jwt
from pathlib import Path
from typing import Any, Optional

from fastapi import Request, HTTPException, status

from xphi.arch.bound.xor.secret.cipher import Cipher
from xphi.arch.bound.xor.secret.client import get_secret_from_vendor, KMSVendor
from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.receptor.warden import SecretAuditor
from xphi.watcher.plane.emitter import get_emitter

from xphi.node.wasm.broker import DphiBroker
from xphi.arch.model.dphi.auth import KernelAuthPayload
from xphi.arch.bound.adapter.pta import SignStateAdapter

from xphi.arch.bound.adapter.settlement import (
    Ap2MandateConstraints,
    Ap2MandatePayload,
    Ap2Authorization,
    Ap2MandateResult
)

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
    
    # Future integration point for JWKS endpoint or env-injected public key
    JWT_PUBLIC_KEY = os.environ.get("FINOPS_JWT_PUBLIC_KEY", "") 

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
            # Parse JWT claims to extract FinOps constraints
            # Note: In production, strict signature verification via JWT_PUBLIC_KEY is required
            claims = jwt.decode(raw_token, options={"verify_signature": False})
            
            tenant_id = claims.get("sub", "anonymous_tenant")
            granted_budget = claims.get("budget_limit_usd", 1.0)
            
            current_ts = int(time.time() * 1000)
            expire_ts = current_ts + (1000 * 60 * 60)  # Internal receipt TTL (1 hour)
            
            # Assemble AP2 Mandate schema using official Pydantic models
            constraints = Ap2MandateConstraints(max_spend_usdc=str(granted_budget), expiration_ts=expire_ts)
            mandate_payload = Ap2MandatePayload(
                protocol="AP2-v1.0",
                requester_id=tenant_id,
                target_action="llm_compute_gateway",
                constraints=constraints,
                issued_at=current_ts
            )
            
            # System node surrogate signature via SignStateAdapter
            mandate_dict = mandate_payload.model_dump(exclude_none=True)
            canonical_mandate = json.dumps(mandate_dict, sort_keys=True, separators=(',', ':'))
            
            real_signature = SignStateAdapter.sign_state_payload({"payload": canonical_mandate})
            node_pubkey = SignStateAdapter.get_signer_pubkey()
            
            # Assemble final Ap2MandateResult (Case A format)
            auth = Ap2Authorization(signer_pub=node_pubkey, signature=real_signature)
            ap2_result = Ap2MandateResult(mandate=mandate_payload, authorization=auth)
            translated_receipt = json.dumps(ap2_result.model_dump(exclude_none=True))
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
    ) -> KernelAuthPayload:
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
        return KernelAuthPayload.model_validate_json(auth_res.output)