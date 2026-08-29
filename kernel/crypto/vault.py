# fiber.kernel.crypto.vault
import os
import json
import hashlib
from typing import List, Optional
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("crypto.vault")

class CryptoVault:
    """
    암호학적 서명을 수행하는 절대적인 로컬 격리 구역.
    어떤 외부 네트워크 호출도 존재하지 않으며, 인메모리 연산으로만 귀결됩니다.
    """
    def attest_payload(self, canonical_hash: bytes) -> List[str]:
        raise NotImplementedError

    @property
    def public_keys(self) -> List[str]:
        raise NotImplementedError


class SovereignVault(CryptoVault):
    """
    [Production] 클라우드 의존성 제로(0). 
    노드의 로컬 디스크에 암호화되어 저장된 키를 메모리에 복호화하여 유지하는 순수 독립 볼트.
    """
    def __init__(self, keystore_path: str = ".dphi/keystore.json"):
        self.keystore_path = keystore_path
        self._unlocked_keys: List[ed25519.Ed25519PrivateKey] = []
        self._is_unlocked = False

    def _derive_aes_key(self, passphrase: str, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=600_000,
        )
        return kdf.derive(passphrase.encode())

    def initialize_genesis_keys(self, passphrase: str, size: int = 3):
        """초기 노드 구동 시 로컬에 암호화된 Keystore 생성"""
        if os.path.exists(self.keystore_path):
            raise FileExistsError("Keystore already exists. Cannot overwrite genesis keys.")

        salt = os.urandom(16)
        aes_key = self._derive_aes_key(passphrase, salt)
        aesgcm = AESGCM(aes_key)

        encrypted_records = []
        for _ in range(size):
            priv_key = ed25519.Ed25519PrivateKey.generate()
            raw_bytes = priv_key.private_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PrivateFormat.Raw,
                encryption_algorithm=serialization.NoEncryption()
            )
            nonce = os.urandom(12)
            ciphertext = aesgcm.encrypt(nonce, raw_bytes, None)
            
            encrypted_records.append({
                "nonce": nonce.hex(),
                "ciphertext": ciphertext.hex()
            })

        keystore_data = {
            "version": 1,
            "kdf": "pbkdf2",
            "salt": salt.hex(),
            "keys": encrypted_records
        }

        os.makedirs(os.path.dirname(self.keystore_path), exist_ok=True)
        with open(self.keystore_path, "w") as f:
            json.dump(keystore_data, f)
            
        log.info(f"[Vault] Genesis keys generated and locally encrypted at {self.keystore_path}")

    def unlock(self, passphrase: str):
        """메모리에만 일시적으로 복호화된 키를 적재"""
        if not os.path.exists(self.keystore_path):
            raise FileNotFoundError("Keystore not found.")

        with open(self.keystore_path, "r") as f:
            data = json.load(f)

        salt = bytes.fromhex(data["salt"])
        aes_key = self._derive_aes_key(passphrase, salt)
        aesgcm = AESGCM(aes_key)

        unlocked = []
        for record in data["keys"]:
            nonce = bytes.fromhex(record["nonce"])
            ciphertext = bytes.fromhex(record["ciphertext"])
            try:
                raw_bytes = aesgcm.decrypt(nonce, ciphertext, None)
                unlocked.append(ed25519.Ed25519PrivateKey.from_private_bytes(raw_bytes))
            except Exception:
                raise ValueError("Vault Unlock Failed: Invalid passphrase or corrupted keystore.")

        self._unlocked_keys = unlocked
        self._is_unlocked = True
        log.info("[Vault] Sovereign Keystore unlocked successfully into secure memory.")

    def attest_payload(self, canonical_hash: bytes) -> List[str]:
        if not self._is_unlocked:
            raise RuntimeError("Vault is locked. Cannot attest payload.")
        return [key.sign(canonical_hash).hex() for key in self._unlocked_keys]

    @property
    def public_keys(self) -> List[str]:
        if not self._is_unlocked:
            return []
        return [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, 
                format=serialization.PublicFormat.Raw
            ).hex() 
            for k in self._unlocked_keys
        ]


class EphemeralVault(CryptoVault):
    """
    [E2E / Testing] 파일 I/O 없이 인메모리에서만 일회성으로 유지되는 볼트.
    기존 NotarySwarm을 완벽히 대체하며, 프로덕션 코드를 우회/오염시키지 않습니다.
    """
    def __init__(self, size: int = 3):
        self._keys = []
        for i in range(size):
            seed = hashlib.sha256(f"ephemeral_node_seed_{i}".encode()).digest()
            self._keys.append(ed25519.Ed25519PrivateKey.from_private_bytes(seed))

    def attest_payload(self, canonical_hash: bytes) -> List[str]:
        return [key.sign(canonical_hash).hex() for key in self._keys]

    @property
    def public_keys(self) -> List[str]:
        return [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, 
                format=serialization.PublicFormat.Raw
            ).hex() 
            for k in self._keys
        ]