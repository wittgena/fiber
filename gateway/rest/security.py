# fiber.gateway.rest.security
import os
import secrets
from pathlib import Path
from typing import Any

from xphi.arch.bound.xor.secret.cipher import Cipher
from xphi.arch.bound.xor.secret.client import get_secret_from_vendor, KMSVendor
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.receptor.warden import SecretAuditor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

SIGN_ROOT = resolve_path("sign")

class SecurityProvisioner:
    """@policy: KMS 주입을 최우선으로 하며, KMS가 없을 시 디스크 영속성(Local Key Persistence) 기반의 Zero-Config 플로우를 기본값"""
    LOCAL_KEY_PATH = Path(SIGN_ROOT) / "local_cipher.key"

    @classmethod
    def _get_or_create_local_key(cls) -> str:
        """랜덤 키를 생성하고 영속화(파일 저장)하여 멱등성을 보장하는 기본 플로우"""
        if cls.LOCAL_KEY_PATH.exists():
            try:
                secret_key = cls.LOCAL_KEY_PATH.read_text(encoding="utf-8").strip()
                log.info(f"Loaded persistent cipher key from local disk ({cls.LOCAL_KEY_PATH}).")
                return secret_key
            except Exception as e:
                log.error(f"Failed to read local cipher key: {e}")
                raise RuntimeError("Local key file exists but could not be read. Check file permissions.") from e
        
        ## 파일이 없을 시, 32바이트 랜덤 키 생성 (hex 포맷 64자)
        new_secret_key = secrets.token_hex(32)
        try:
            cls.LOCAL_KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
            cls.LOCAL_KEY_PATH.write_text(new_secret_key, encoding="utf-8")
            
            ## 유닉스 계열이면 권한 엄격 설정 (600: rw-------)
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
        
        ## step.1: 지정된 KMS 벤더로부터 Secret Key 획득 시도
        if key_manager != KMSVendor.LOCAL:
            try:
                secret_key = get_secret_from_vendor(
                    client=None,
                    key_manager=key_manager,
                    secret_name=secret_name
                )
            except Exception as e:
                log.debug(f"KMS retrieval bypassed or failed: {e}")

        ## step.2: KMS 획득 실패 시, Local 파일 기반 생성
        if not secret_key:
            log.debug("Engaging Local Persistence Flow (Zero-Config).")
            secret_key = cls._get_or_create_local_key()
                
        return SecretAuditor(cipher=Cipher(secret_key=secret_key))