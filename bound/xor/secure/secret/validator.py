# bound.xor.secure.secret.validator
## @lineage: xor.secure.secret.validator
## @lineage: xphi.xor.secure.secret.validator
## @lineage: xphi.xor.secure.secret.validator.manager
from pydantic import SecretStr
from typing import List
import hashlib
from base64 import b64encode
from cryptography.fernet import Fernet

from bound.resolver.model.config.resolver import config
from bound.legacy.types import CredentialItem

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
            fernet_key = b64encode(hashlib.sha256(secret_key).digest())
            fernet = Fernet(fernet_key)
            object.__setattr__(self, "_fernet", fernet)
        return fernet

class CredentialAccessor:
    @staticmethod
    def get_credential_values(credential_name: str) -> dict:
        """Safe accessor for credentials."""

        if not config.credential_list:
            return {}
        for credential in config.credential_list:
            if credential.credential_name == credential_name:
                return credential.credential_values.copy()
        return {}

    @staticmethod
    def upsert_credentials(credentials: List[CredentialItem]):
        """Add a credential to the list of credentials."""
        credential_names = [cred.credential_name for cred in config.credential_list]
        for credential in credentials:
            if credential.credential_name in credential_names:
                # Find and replace the existing credential in the list
                for i, existing_cred in enumerate(config.credential_list):
                    if existing_cred.credential_name == credential.credential_name:
                        config.credential_list[i] = credential
                        break
            else:
                config.credential_list.append(credential)


def serialize_secret(v: SecretStr | None, info):
    if v is None:
        return None

    if info.context and info.context.get("cipher"):
        cipher: Cipher = info.context.get("cipher")
        return cipher.encrypt(v)

    if info.context and info.context.get("expose_secrets"):
        return v.get_secret_value()

    return v


def validate_secret(v: str | SecretStr | None, info) -> SecretStr | None:
    if v is None:
        return None

    if isinstance(v, SecretStr):
        secret_value = v.get_secret_value()
    else:
        secret_value = v

    if not secret_value or not secret_value.strip() or secret_value == "**********":
        return None

    if info.context and info.context.get("cipher"):
        cipher: Cipher = info.context.get("cipher")
        return cipher.decrypt(secret_value)

    if isinstance(v, SecretStr):
        return v
    else:
        return SecretStr(secret_value)
