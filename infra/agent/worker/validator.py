# fiber.infra.agent.worker.validator
## @lineage: fiber.a2a.worker.validator
## @lineage: fiber.infra.worker.agent.validator
import os
import sqlite3
import logging
import time
import hmac
import hashlib
import base64
import struct
from typing import Dict, Any

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from fiber.infra.agent.bridge.protocol import AgentProtocol
from xphi.kernel.space.bind.resolver import resolve_path

log = logging.getLogger("agent.validator")

SIGN_ROOT = resolve_path("sign")
DEFAULT_DB_PATH = os.path.join(str(SIGN_ROOT), "deploy_audit.sqlite")

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

class AuthValidatorAgent(AgentProtocol):
    """
    [PDP: Policy Decision Point] 
    마스터 키를 보유하고 OTP를 검증하며, 유효한 요청에 대해 서명(Attestation)을 발급합니다.
    """
    def __init__(self):
        super().__init__(agent_name="agent.validator")
        
        self.master_passphrase = os.environ.get("DPHI_MASTER_PASSPHRASE")
        if not self.master_passphrase:
            self.log.critical("⚠️ DPHI_MASTER_PASSPHRASE missing. Validator cannot operate.")
            
        # Validator 자신의 고유 서명 키 (CryptoVault 역할)
        vault_key_hex = os.environ.get("DPHI_VALIDATOR_PRIVATE_KEY")
        if vault_key_hex:
            self.signing_key = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(vault_key_hex))
        else:
            self.log.warning("Generating Ephemeral Signing Key for Validator.")
            self.signing_key = ed25519.Ed25519PrivateKey.generate()

        self.conn = sqlite3.connect(DEFAULT_DB_PATH, check_same_thread=False)

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        if tool_name == "request_attestation":
            self._handle_attestation(req_id, arguments)
        else:
            self.send_error(req_id, -32601, f"Unknown method: {tool_name}")

    def _handle_attestation(self, req_id: Any, args: Dict[str, Any]):
        user_id = args.get("user_id")
        otp_code = args.get("otp_code")
        payload_hash_hex = args.get("payload_hash")

        self.log.info(f"Verification requested by {user_id} for hash [{payload_hash_hex[:8]}...]")

        # 1. DB에서 사용자 정보 조회
        row = self.conn.execute("SELECT salt, nonce, ciphertext FROM admin_users WHERE user_id = ?", (user_id,)).fetchone()
        if not row:
            return self.send_error(req_id, -32000, "Unregistered Admin.")

        # 2. 복호화 및 OTP 검증
        try:
            vault = AdminSecretVault(self.master_passphrase)
            secret = vault.decrypt(*row)
            if not TotpValidator(secret).verify(otp_code):
                return self.send_error(req_id, -32000, "Invalid or Expired TOTP.")
        except Exception as e:
            return self.send_error(req_id, -500, "Cryptography Error.")

        # 3. 서명(Attestation) 생성
        signature = self.signing_key.sign(bytes.fromhex(payload_hash_hex))
        
        self.log.info(f"✅ OTP Validated. Attestation Signature issued for {user_id}.")
        self.send_response(req_id, {"signature": signature.hex()})

def main():
    server = AuthValidatorAgent()
    try:
        server.serve_forever()
    finally:
        if hasattr(server, 'conn') and server.conn:
            server.conn.close()

if __name__ == "__main__":
    main()