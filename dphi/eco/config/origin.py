# fiber.dphi.eco.config.origin
import os
import json
import hashlib
import nacl.signing
import nacl.encoding
import nacl.exceptions
from typing import List, Optional
from pydantic import BaseModel

from xphi.bound.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

ORIGIN_ROOT = resolve_path("origin")
log = get_emitter("dphi.infra.origin")

class TrustedOriginState(BaseModel):
    """검증이 완료된 인메모리 상태를 보장하는 데이터 모델"""
    network: str
    active_signers: List[str]
    root_signature: str
    root_pubkey: str

class OriginRegistry:
    """
    Origin Config JSON 파일의 변조 여부를 자가 검증(Self-Verification)하고
    안전한 읽기 전용 상태를 Edge 애플리케이션에 제공하는 레지스트리
    """
    def __init__(self, config_path: Optional[str] = None):
        # [개선] 하드코딩된 OS 절대경로 제거, 프레임워크 표준 ORIGIN_ROOT 활용
        if not config_path:
            self.config_path = os.path.join(str(ORIGIN_ROOT), "config.json")
        else:
            self.config_path = config_path
            
        self._state: Optional[TrustedOriginState] = None

    def load_and_verify(self) -> TrustedOriginState:
        """
        JSON 파일을 읽고, Ed25519 서명을 검증한 뒤 메모리에 캐싱
        (Fail-Fast: 서명 불일치 시 즉각 예외 발생)
        """
        log.info(f"Loading and verifying Origin Config from: {self.config_path}")
        
        if not os.path.exists(self.config_path):
            log.critical(f"[Security] Origin config file not found: {self.config_path}")
            raise FileNotFoundError(f"Missing mandatory origin config: {self.config_path}")

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                origin_config = json.load(f)
                
            network = origin_config.get("network", "unknown")
            validators = origin_config.get("validators", [])
            attestation = origin_config.get("attestation", {})
            
            root_pubkey_hex = attestation.get("root_pubkey")
            root_signature_hex = attestation.get("pre_signed_root_sig")
            
            if not root_pubkey_hex or not root_signature_hex:
                raise ValueError("Attestation payload (root_pubkey, pre_signed_root_sig) is missing.")

            # 1. 서명 원본 데이터 복원 (CLI와 동일하게 Canonicalization)
            val_pubs = [v["pubkey"] for v in validators]
            payload_dict = {"active_signers": val_pubs}
            
            canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
            payload_hash_str = hashlib.sha256(canonical_bytes).hexdigest()

            # 2. PyNaCl (Ed25519) 서명 자가 검증
            verify_key = nacl.signing.VerifyKey(root_pubkey_hex, encoder=nacl.encoding.HexEncoder)
            
            try:
                verify_key.verify(
                    payload_hash_str.encode('utf-8'),
                    bytes.fromhex(root_signature_hex)
                )
            except nacl.exceptions.BadSignatureError:
                log.critical("[SECURITY_ALERT] Origin config tampering detected! Signature mismatch.")
                raise ValueError("Cryptographic verification failed. File has been tampered with.")

            # 3. 검증 통과 후 상태 락(Lock)
            self._state = TrustedOriginState(
                network=network,
                active_signers=val_pubs,
                root_signature=root_signature_hex,
                root_pubkey=root_pubkey_hex
            )
            
            log.info(f"✅ Origin Config cryptographically verified. (Network: {network}, Signers: {len(val_pubs)})")
            return self._state

        except Exception as e:
            log.error(f"Failed to initialize OriginRegistry: {e}")
            raise RuntimeError(f"Zero-Trust Policy Enforced: {e}")

    @property
    def is_verified(self) -> bool:
        return self._state is not None

    def get_state(self) -> TrustedOriginState:
        """라우터(Endpoint)에서 접근하는 안전한 Getter 메서드"""
        if not self._state:
            raise RuntimeError("OriginRegistry is not initialized. Must call load_and_verify() first.")
        return self._state