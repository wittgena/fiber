# ator.driver.config.vendor
## @lineage: engine.config.driver.aws
from pydantic import BaseModel, Field, SecretStr, field_validator, field_serializer
from typing import Any
import os
from arch.xor.secret.validator import serialize_secret, validate_secret

class VendorConfig(BaseModel):
    """@desc: 특정 벤더(AWS, OpenRouter 등)에 종속된 환경 변수 및 인증 정보를 격리하는 설정 객체"""
    aws_access_key_id: str | SecretStr | None = Field(default=None)
    aws_secret_access_key: str | SecretStr | None = Field(default=None)
    aws_session_token: str | SecretStr | None = Field(default=None)
    aws_region_name: str | None = Field(default=None)
    aws_profile_name: str | None = Field(default=None)
    aws_role_name: str | None = Field(default=None)
    aws_session_name: str | None = Field(default=None)
    aws_bedrock_runtime_endpoint: str | None = Field(default=None)

    openrouter_site_url: str = Field(default="https://localhost/")
    openrouter_app_name: str = Field(default="surgent")

    @field_validator("aws_access_key_id", "aws_secret_access_key", "aws_session_token", mode="before")
    @classmethod
    def _validate_vendor_secrets(cls, v: str | SecretStr | None, info) -> SecretStr | None:
        return validate_secret(v, info)

    @field_serializer("aws_access_key_id", "aws_secret_access_key", "aws_session_token", when_used="always")
    def _serialize_vendor_secrets(self, v: SecretStr | None, info):
        return serialize_secret(v, info)

    def inject_vendor_environment(self) -> None:
        if self.openrouter_site_url:
            os.environ["OR_SITE_URL"] = self.openrouter_site_url
        if self.openrouter_app_name:
            os.environ["OR_APP_NAME"] = self.openrouter_app_name
            
        if self.aws_access_key_id:
            assert isinstance(self.aws_access_key_id, SecretStr)
            os.environ["AWS_ACCESS_KEY_ID"] = self.aws_access_key_id.get_secret_value()
        if self.aws_secret_access_key:
            assert isinstance(self.aws_secret_access_key, SecretStr)
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.aws_secret_access_key.get_secret_value()
        if self.aws_session_token:
            assert isinstance(self.aws_session_token, SecretStr)
            os.environ["AWS_SESSION_TOKEN"] = self.aws_session_token.get_secret_value()
        if self.aws_region_name:
            os.environ["AWS_REGION_NAME"] = self.aws_region_name

    def get_vendor_transport_kwargs(self) -> dict[str, Any]:
        kw: dict[str, Any] = {}
        if self.aws_access_key_id:
            assert isinstance(self.aws_access_key_id, SecretStr)
            kw["aws_access_key_id"] = self.aws_access_key_id.get_secret_value()
        if self.aws_secret_access_key:
            assert isinstance(self.aws_secret_access_key, SecretStr)
            kw["aws_secret_access_key"] = self.aws_secret_access_key.get_secret_value()
        if self.aws_session_token:
            assert isinstance(self.aws_session_token, SecretStr)
            kw["aws_session_token"] = self.aws_session_token.get_secret_value()
        if self.aws_region_name:
            kw["aws_region_name"] = self.aws_region_name
        if self.aws_profile_name:
            kw["aws_profile_name"] = self.aws_profile_name
        if self.aws_role_name:
            kw["aws_role_name"] = self.aws_role_name
        if self.aws_session_name:
            kw["aws_session_name"] = self.aws_session_name
        if self.aws_bedrock_runtime_endpoint:
            kw["aws_bedrock_runtime_endpoint"] = self.aws_bedrock_runtime_endpoint
        return kw