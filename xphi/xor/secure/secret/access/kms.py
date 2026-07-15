# xphi.xor.secure.secret.access.kms
## @lineage: xphi.xor.secret.kms
import enum
from typing import Dict, List, Literal, Optional
from bound.surface.legacy.base import PydanticObjectBase

class KeyManagementSystem(enum.Enum):
    GOOGLE_KMS = "google_kms"
    AZURE_KEY_VAULT = "azure_key_vault"
    AWS_SECRET_MANAGER = "aws_secret_manager"
    GOOGLE_SECRET_MANAGER = "google_secret_manager"
    HASHICORP_VAULT = "hashicorp_vault"
    CYBERARK = "cyberark"
    LOCAL = "local"
    AWS_KMS = "aws_kms"
    CUSTOM = "custom"

class KeyManagementSettings(PydanticObjectBase):
    hosted_keys: Optional[List] = None
    store_virtual_keys: Optional[bool] = False
    prefix_for_stored_virtual_keys: str = "litellm/"
    access_mode: Literal["read_only", "write_only", "read_and_write"] = "read_only"
    primary_secret_name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    custom_secret_manager: Optional[str] = None
    aws_region_name: Optional[str] = None
    aws_role_name: Optional[str] = None
    aws_session_name: Optional[str] = None
    aws_external_id: Optional[str] = None
    aws_profile_name: Optional[str] = None
    aws_web_identity_token: Optional[str] = None
    aws_sts_endpoint: Optional[str] = None
