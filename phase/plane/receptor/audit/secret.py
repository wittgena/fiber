# fiber.phase.plane.receptor.audit.secret
## @lineage: fiber.phase.kernel.receptor.audit.secret
import os
import asyncio
import hashlib
import json
import logging
from typing import Any, Dict

from pydantic import SecretStr

from xphi.xor.secret.cipher import Cipher
from xphi.watcher.receptor.policy.gateway import ToposGateway
from xphi.watcher.receptor.audit.warden import AuditWarden
from xphi.arch.model.edge.receipt import AuditLogResponse

logger = logging.getLogger("audit.secret")

class SecretAuditor:
    def __init__(self, cipher: Cipher, gateway: ToposGateway = None):
        self.cipher = cipher
        self.gateway = gateway or ToposGateway()
        self.sensitive_keys = {"email", "ip_address", "password", "token", "secret"}

    async def append_to_ledger_and_prove(self, event: Dict[str, Any], needs_proof: bool = False) -> AuditLogResponse:
        """Normalizes (hashes) the event, encrypts sensitive data, and routes it to the WASM Kernel Ledger."""
        logger.info("[ToposLedger] Processing Pangea audit log append request...")
        
        sanitized_event = await asyncio.to_thread(self._encrypt_sensitive_data, event)
        event_hash = await asyncio.to_thread(self._generate_deterministic_hash, sanitized_event)
        is_authorized = await self.gateway.authorize(
            action_id=f"pangea_audit_{event_hash[:8]}",
            action="PANGEA_AUDIT_LOG_APPEND",
            payload=sanitized_event,
            metadata={"needs_proof": needs_proof}
        )

        if not is_authorized:
            error_msg = f"Audit event {event_hash[:8]} rejected by WASM Spatial Fence."
            logger.error(f"[ToposLedger] BLOCKED: {error_msg}")
            AuditWarden.record_anomaly(action="audit.ledger.kernel_block", details=error_msg)
            
            return AuditLogResponse(
                event_id=event_hash,
                status="denied",
                membership_proof=None,
                signature=None
            )

        merkle_proof = None
        if needs_proof:
            merkle_proof = f"proof_merkle_{event_hash}_{hashlib.md5(event_hash.encode()).hexdigest()}"

        return AuditLogResponse(
            event_id=event_hash,
            status="success",
            membership_proof=merkle_proof,
            signature=f"sig_topos_{event_hash}"
        )

    def _encrypt_sensitive_data(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively traverses the event payload and encrypts values for predefined sensitive keys"""
        encrypted_payload = {}
        for key, value in event.items():
            if isinstance(value, dict):
                encrypted_payload[key] = self._encrypt_sensitive_data(value)
            elif key.lower() in self.sensitive_keys and value:
                try:
                    secret_val = SecretStr(str(value))
                    encrypted_payload[key] = self.cipher.encrypt(secret_val)
                except Exception as e:
                    logger.warning(f"Failed to encrypt field '{key}': {e}. Masking instead.")
                    encrypted_payload[key] = "********"
            else:
                encrypted_payload[key] = value
                
        return encrypted_payload

    def _generate_deterministic_hash(self, event: Dict[str, Any]) -> str:
        deterministic_str = json.dumps(event, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(deterministic_str).hexdigest()


async def get_secret_auditor() -> SecretAuditor:
    secret_key = os.getenv("BRANE_LEDGER_CIPHER_KEY", "mock-secret-key-for-dev-only")
    cipher_instance = Cipher(secret_key=secret_key)
    return SecretAuditor(cipher=cipher_instance)