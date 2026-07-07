# xphi.xor.secret.credential
## @lineage: xphi.xor.auth.secret.credential
from typing import List
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.types import CredentialItem

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
