# xphi.xor.secure.secret.client
## @lineage: xphi.xor.secret.handler.client
import base64
import os
import httpx
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union

from anchor.registry.model.config.resolver import config
from xphi.xor.secure.secret.access.kms import KeyManagementSystem

from watcher.plane.emitter import get_emitter

log = get_emitter("secret.client")

class BaseSecretManager(ABC):
    @abstractmethod
    async def async_read_secret(
        self,
        secret_name: str,
        optional_params: Optional[dict] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
    ) -> Optional[str]:
        pass

    @abstractmethod
    def sync_read_secret(
        self,
        secret_name: str,
        optional_params: Optional[dict] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
    ) -> Optional[str]:
        pass

    @abstractmethod
    async def async_write_secret(
        self,
        secret_name: str,
        secret_value: str,
        description: Optional[str] = None,
        optional_params: Optional[dict] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        tags: Optional[Union[dict, list]] = None,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def async_delete_secret(
        self,
        secret_name: str,
        recovery_window_in_days: Optional[int] = 7,
        optional_params: Optional[dict] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
    ) -> dict:
        pass

    async def async_rotate_secret(
        self,
        current_secret_name: str,
        new_secret_name: str,
        new_secret_value: str,
        optional_params: Optional[dict] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
    ) -> dict:
        try:
            # First verify the old secret exists
            old_secret = await self.async_read_secret(
                secret_name=current_secret_name,
                optional_params=optional_params,
                timeout=timeout,
            )

            if old_secret is None:
                raise ValueError(f"Current secret {current_secret_name} not found")

            # Create new secret with new name and value
            create_response = await self.async_write_secret(
                secret_name=new_secret_name,
                secret_value=new_secret_value,
                description=f"Rotated from {current_secret_name}",
                optional_params=optional_params,
                timeout=timeout,
            )

            # Verify new secret was created successfully
            new_secret = await self.async_read_secret(
                secret_name=new_secret_name,
                optional_params=optional_params,
                timeout=timeout,
            )

            if new_secret is None:
                raise ValueError(f"Failed to verify new secret {new_secret_name}")

            # If everything is successful, delete the old secret
            await self.async_delete_secret(
                secret_name=current_secret_name,
                recovery_window_in_days=7,  # Keep for recovery if needed
                optional_params=optional_params,
                timeout=timeout,
            )

            return create_response

        except httpx.HTTPStatusError as err:
            log.exception(
                "Error rotating secret in AWS Secrets Manager: %s",
                str(err.response.text),
            )
            raise ValueError(f"HTTP error occurred: {err.response.text}")
        except httpx.TimeoutException:
            raise ValueError("Timeout error occurred")
        except Exception as e:
            log.exception(
                "Error rotating secret in AWS Secrets Manager: %s", str(e)
            )
            raise


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