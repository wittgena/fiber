# ops.xelog.audit.ledger
## @lineage: xe.extime.logst.audit.ledger.logst
import os
import asyncio
import hashlib
import json
import logging
from typing import Any, Dict

from pydantic import SecretStr

from atoa.secure.secret.validator import Cipher

from arch.contract.audit.model import AuditLogResponse
from watcher.kernel.bridge.gateway import ToposGateway
from watcher.kernel.audit.warden import AuditWarden

logger = logging.getLogger("audit.ledger")

class AuditLedger:
    def __init__(self, cipher: Cipher, gateway: ToposGateway = None):
        self.cipher = cipher
        self.gateway = gateway or ToposGateway()
        
        # List of keys considered Personally Identifiable Information (PII)
        self.sensitive_keys = {"email", "ip_address", "password", "token", "secret"}

    async def append_to_ledger_and_prove(self, event: Dict[str, Any], needs_proof: bool = False) -> AuditLogResponse:
        """Normalizes (hashes) the event, encrypts sensitive data, and routes it to the WASM Kernel Ledger."""
        logger.info("[ToposLedger] Processing Pangea audit log append request...")
        
        sanitized_event = await asyncio.to_thread(self._encrypt_sensitive_data, event)
        event_hash = await asyncio.to_thread(self._generate_deterministic_hash, sanitized_event)
        
        # [EVOLUTION] Warden 억지 호출 제거. Gateway를 통해 WASM 커널에 합의(Seal) 제안.
        is_authorized = await self.gateway.authorize(
            action_id=f"pangea_audit_{event_hash[:8]}",
            action="PANGEA_AUDIT_LOG_APPEND",
            payload=sanitized_event,
            metadata={"needs_proof": needs_proof}
        )

        if not is_authorized:
            error_msg = f"Audit event {event_hash[:8]} rejected by WASM Spatial Fence."
            logger.error(f"[ToposLedger] BLOCKED: {error_msg}")
            # 커널이 비즈니스 로그를 차단했다면, 이는 시스템 무결성 위협일 수 있으므로 이때만 Warden에 이상징후 보고
            AuditWarden.record_anomaly(action="audit.ledger.kernel_block", details=error_msg)
            
            return AuditLogResponse(
                event_id=event_hash,
                status="denied",
                membership_proof=None,
                signature=None
            )

        ## Generate mock Merkle Proof if requested (To be replaced with actual Kernel Ledger root trace)
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
        """Generates a deterministic hash for the event payload"""
        deterministic_str = json.dumps(event, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(deterministic_str).hexdigest()


async def get_audit_ledger() -> AuditLedger:
    secret_key = os.getenv("BRANE_LEDGER_CIPHER_KEY", "mock-secret-key-for-dev-only")
    cipher_instance = Cipher(secret_key=secret_key)
    return AuditLedger(cipher=cipher_instance)