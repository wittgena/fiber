# xphi.xor.secret.source
## @lineage: xphi.xor.auth.secret.source
## @lineage: bound.xor.secret.source
## @lineage: xor.secret.source
## @lineage: bound.channel.secret.source
## @lineage: channel.secret.source
## @lineage: meta.ops.observer.security.secret.source
## @lineage: meta.observer.security.secret.source
## @lineage: meta.watcher.security.secret.source
## @lineage: gov.watcher.security.secret.source
## @lineage: bound.security.secret.source
## @lineage: meta.sphere.security.secret.source
## @lineage: bound.sphere.security.secret.source
## @lineage: gov.secret.source
## @lineage: iso.gov.secret.source
from abc import ABC, abstractmethod
import httpx
from pydantic import Field, SecretStr, field_serializer, field_validator

from xphi.xor.secret.validator.manager import serialize_secret, validate_secret
from xphi.xor.secret.redact import is_secret_key

from arch.topos.state.disc import DiscMixin
from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

class SecretSource(DiscMixin, ABC):
    """Source for a named secret which may be obtained dynamically"""

    description: str | None = Field(
        default=None,
        description="Optional description for this secret",
    )

    @abstractmethod
    def get_value(self) -> str | None:
        """Get the value of a secret in plain text"""


class StaticSecret(SecretSource):
    """A secret stored locally"""

    value: SecretStr | None = None

    def get_value(self) -> str | None:
        if self.value is None:
            return None
        return self.value.get_secret_value()

    @field_validator("value")
    @classmethod
    def _validate_secrets(cls, v: SecretStr | None, info):
        return validate_secret(v, info)

    @field_serializer("value", when_used="always")
    def _serialize_secrets(self, v: SecretStr | None, info):
        return serialize_secret(v, info)


class LookupSecret(SecretSource):
    """A secret looked up from some external url"""

    url: str
    headers: dict[str, str] = Field(default_factory=dict)

    def get_value(self) -> str:
        response = httpx.get(self.url, headers=self.headers, timeout=30.0)
        response.raise_for_status()
        return response.text

    @field_validator("headers")
    @classmethod
    def _validate_secrets(cls, headers: dict[str, str], info):
        result = {}
        for key, value in headers.items():
            if is_secret_key(key):
                secret_value = validate_secret(SecretStr(value), info)
                # Skip headers with redacted/empty secret values
                if secret_value is None:
                    logger.debug(
                        f"Skipping redacted header '{key}' during deserialization"
                    )
                    continue
                result[key] = secret_value.get_secret_value()
            else:
                result[key] = value
        return result

    @field_serializer("headers", when_used="always")
    def _serialize_secrets(self, headers: dict[str, str], info):
        result = {}
        for key, value in headers.items():
            if is_secret_key(key):
                secret_value = serialize_secret(SecretStr(value), info)
                if secret_value is None:
                    logger.debug(
                        f"Skipping redacted header '{key}' during serialization"
                    )
                    continue
                result[key] = secret_value
            else:
                result[key] = value
        return result


# Type alias for secret values - can be a plain string or a SecretSource
SecretValue = str | SecretSource
