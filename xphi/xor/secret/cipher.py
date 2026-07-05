# xphi.xor.secret.cipher
## @lineage: xphi.xor.auth.secret.cipher
## @lineage: bound.xor.secret.cipher
## @lineage: xor.secret.cipher
## @lineage: bound.channel.secret.cipher
## @lineage: channel.secret.cipher
## @lineage: meta.ops.observer.security.secret.cipher
import hashlib
from base64 import b64encode
from cryptography.fernet import Fernet
from pydantic import SecretStr

class Cipher:
    def __init__(self, secret_key: str):
        self.secret_key = secret_key
        self._fernet: Fernet | None = None

    def encrypt(self, secret: SecretStr | None) -> str | None:
        if secret is None:
            return None
        secret_value = secret.get_secret_value().encode()
        fernet = self._get_fernet()
        result = fernet.encrypt(secret_value).decode()
        return result

    def decrypt(self, secret: str | None) -> SecretStr | None:
        if secret is None:
            return None
        try:
            fernet = self._get_fernet()
            decrypted = fernet.decrypt(secret.encode()).decode()
            return SecretStr(decrypted)
        except Exception as e:
            # Import here to avoid circular imports
            from watcher.plane.emitter import get_logger

            logger = get_logger(__name__)
            logger.warning(
                f"Failed to decrypt secret value (setting to None): {e}. "
                "This may occur when loading conversations encrypted with a different "
                "key or when upgrading from older versions."
            )
            return None

    def _get_fernet(self):
        fernet = self._fernet
        if fernet is None:
            secret_key = self.secret_key.encode()
            # Hash the key to make sure we have a 256 bit value
            fernet_key = b64encode(hashlib.sha256(secret_key).digest())
            fernet = Fernet(fernet_key)
            object.__setattr__(self, "_fernet", fernet)
        return fernet
