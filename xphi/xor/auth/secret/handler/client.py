# xphi.xor.auth.secret.handler.client
## @lineage: xphi.xor.secret.handler.client
## @lineage: bound.xor.secret.handler.client
## @lineage: xor.secret.handler.client
## @lineage: bound.channel.secret.handler.client
## @lineage: channel.secret.handler.client
## @lineage: anchor.secret.handler
import base64
import os
from typing import Any, Optional
from xphi.xor.auth.secret.handler.base import BaseSecretManager
from xphi.xor.auth.secret.kms import KeyManagementSystem
from bound.channel.config.resolver import config
from watcher.plane.emitter import get_emitter

log = get_emitter("secret.handler.client")

def _is_base64(s):
    """Check if a string is valid base64."""
    import binascii
    try:
        return base64.b64encode(base64.b64decode(s)).decode() == s
    except binascii.Error:
        return False

def get_secret_from_vendor(
    client: Any,
    key_manager: str,
    secret_name: str,
    key_management_settings: Optional[Any] = None,
) -> Optional[str]:
    secret = None
    
    if key_manager == KeyManagementSystem.AZURE_KEY_VAULT.value or type(client).__module__ + "." + type(client).__name__ == "azure.keyvault.secrets._client.SecretClient":
        secret = client.get_secret(secret_name).value

    elif key_manager == KeyManagementSystem.GOOGLE_KMS.value or client.__class__.__name__ == "KeyManagementServiceClient":
        encrypted_secret: Any = os.getenv(secret_name)
        if encrypted_secret is None:
            raise ValueError("Google KMS requires the encrypted secret to be in the environment!")
        b64_flag = _is_base64(encrypted_secret)
        if b64_flag is True:
            ciphertext = base64.b64decode(encrypted_secret)
        else:
            raise ValueError("Google KMS requires the encrypted secret to be encoded in base64")
        response = client.decrypt(
            request={
                "name": config._google_kms_resource_name,
                "ciphertext": ciphertext,
            }
        )
        secret = response.plaintext.decode("utf-8")

    elif key_manager == KeyManagementSystem.AWS_KMS.value:
        encrypted_value = os.getenv(secret_name, None)
        if encrypted_value is None:
            raise Exception("AWS KMS - Encrypted Value of Key={} is None".format(secret_name))
        ciphertext_blob = base64.b64decode(encrypted_value)
        response = client.decrypt(CiphertextBlob=ciphertext_blob)
        secret = response["Plaintext"].decode("utf-8")
        if isinstance(secret, str):
            secret = secret.strip()

    elif key_manager == KeyManagementSystem.AWS_SECRET_MANAGER.value:
        # 이 부분은 프로젝트 내부의 파일 경로를 참조하는 것으로 보이므로 그대로 둡니다.
        from config.secret_managers.aws_secret_manager_v2 import AWSSecretsManagerV2
        if isinstance(client, AWSSecretsManagerV2):
            primary_secret_name = key_management_settings.primary_secret_name if key_management_settings else None
            secret = client.sync_read_secret(
                secret_name=secret_name,
                primary_secret_name=primary_secret_name,
            )

    elif key_manager == KeyManagementSystem.GOOGLE_SECRET_MANAGER.value:
        secret = client.get_secret_from_google_secret_manager(secret_name)
        if secret is None:
            raise ValueError(f"No secret found in Google Secret Manager for {secret_name}")

    elif key_manager in (KeyManagementSystem.HASHICORP_VAULT.value, KeyManagementSystem.CYBERARK.value):
        secret = client.sync_read_secret(secret_name=secret_name)
        if secret is None:
            raise ValueError(f"No secret found in {key_manager} for {secret_name}")

    elif key_manager == KeyManagementSystem.CUSTOM.value:
        # 핵심 변경점: litellm.CustomSecretManager -> gate.secret.base.BaseSecretManager
        if isinstance(client, BaseSecretManager):
            secret = client.sync_read_secret(
                secret_name=secret_name,
                optional_params=(
                    key_management_settings.model_dump()
                    if key_management_settings
                    else None
                ),
            )
            if secret is None:
                raise ValueError(f"No secret found in Custom Secret Manager for {secret_name}")
        else:
            raise ValueError(
                f"Custom secret manager client must be an instance of BaseSecretManager, got {type(client).__name__}"
            )

    elif key_manager == "local":
        secret = os.getenv(secret_name)

    else:
        secret = client.get_secret(secret_name).secret_value
    return secret