# fiber.dphi.rpc.legacy.validator
## @lineage: fiber.dphi.rpc.validator
import os
import sqlite3
import logging
import time
import hmac
import hashlib
import base64
import struct

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.asymmetric import ed25519
from xphi.kernel.space.bind.resolver import resolve_path

log = logging.getLogger("rpc.validator")

class AdminSecretVault:
    def __init__(self, passphrase: str):
        self.passphrase = passphrase.encode('utf-8')

    def decrypt(self, salt_hex: str, nonce_hex: str, ciphertext_hex: str) -> str:
        salt = bytes.fromhex(salt_hex)
        nonce = bytes.fromhex(nonce_hex)
        ciphertext = bytes.fromhex(ciphertext_hex)
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=600_000)
        aesgcm = AESGCM(kdf.derive(self.passphrase))
        return aesgcm.decrypt(nonce, ciphertext, None).decode('utf-8')

class TotpValidator:
    def __init__(self, base32_secret: str):
        self.secret = base32_secret

    def verify(self, token: str, window: int = 1) -> bool:
        def _get_totp(intervals_no):
            key = base64.b32decode(self.secret, True)
            msg = struct.pack(">Q", intervals_no)
            h = hmac.new(key, msg, hashlib.sha1).digest()
            o = h[19] & 15
            h = (struct.unpack(">I", h[o:o+4])[0] & 0x7fffffff) % 1000000
            return f"{h:06d}"
            
        intervals = int(time.time()) // 30
        return any(_get_totp(intervals + i) == str(token).zfill(6) for i in range(-window, window + 1))

class AuthValidatorService:
    """
    [PDP: Policy Decision Point] 
    MCP Agent 샌드박스가 아닌, 외부망과 철저히 단절된 데몬 내부의 순수 백엔드 RPC 핸들러로 동작합니다.
    """
    def __init__(self):
        self.master_passphrase = os.environ.get("DPHI_MASTER_PASSPHRASE")
        if not self.master_passphrase:
            log.critical("⚠️ DPHI_MASTER_PASSPHRASE missing. Validator cannot operate.")
            
        vault_key_hex = os.environ.get("DPHI_VALIDATOR_PRIVATE_KEY")
        if vault_key_hex:
            self.signing_key = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(vault_key_hex))
        else:
            log.warning("Generating Ephemeral Signing Key for Validator.")
            self.signing_key = ed25519.Ed25519PrivateKey.generate()

        db_path = os.environ.get("DPHI_AUDIT_DB_PATH", os.path.join(str(resolve_path("sign")), "deploy_audit.sqlite"))
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_users (
                    user_id TEXT PRIMARY KEY, salt TEXT, nonce TEXT, ciphertext TEXT
                )
            """)
            self.conn.commit()
        except Exception as e:
            log.error(f"DB Initialization failed: {e}")

    async def handle_attestation(self, params: dict, ctx=None) -> dict:
        """Internal RPC Bus를 통해 Deploy 에이전트의 서명 요청을 처리합니다."""
        user_id = params.get("user_id")
        otp_code = params.get("otp_code")
        payload_hash_hex = params.get("payload_hash", "")

        log.info(f"Verification requested by {user_id} for hash [{payload_hash_hex[:8]}...]")

        try:
            row = self.conn.execute("SELECT salt, nonce, ciphertext FROM admin_users WHERE user_id = ?", (user_id,)).fetchone()
            if not row:
                log.warning(f"Validation failed: Unregistered Admin ({user_id})")
                return {"error": True, "code": -32000, "message": f"Unregistered Admin: {user_id}"}

            vault = AdminSecretVault(self.master_passphrase)
            secret = vault.decrypt(*row)
            
            if not TotpValidator(secret).verify(otp_code):
                log.warning(f"Validation failed: Invalid TOTP for {user_id}")
                return {"error": True, "code": -32000, "message": "Invalid or Expired TOTP."}
                
        except Exception as e:
            log.error(f"Cryptography/DB error: {e}")
            return {"error": True, "code": -500, "message": "Internal Crypto Error."}

        signature = self.signing_key.sign(bytes.fromhex(payload_hash_hex))
        log.info(f"✅ OTP Validated. Attestation Signature issued for {user_id}.")
        return {"success": True, "signature": signature.hex()}