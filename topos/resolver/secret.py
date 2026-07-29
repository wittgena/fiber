# topos.resolver.secret
## @lineage: bound.resolver.secret
from abc import ABC, abstractmethod
from collections.abc import Mapping
from pydantic import Field, PrivateAttr, SecretStr

from arch.topos.bound.surge.disc import DiscMixin, SurgeBaseModel
from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

class SecretSource(DiscMixin, ABC):
    description: str | None = Field(default=None, description="Optional description for this secret")

    @abstractmethod
    def get_value(self) -> str | None:
        """Get the value of a secret in plain text"""

class SecretItem(SurgeBaseModel):
    value: SecretStr
    description: str | None = Field(default=None,  description="Optional description for this secret")
    def get_value(self) -> str:
        return self.value.get_secret_value()

SecretValue = str | SecretItem

class SecretRegistry(SurgeBaseModel):
    """Manages secrets and injects them into bash commands when needed"""
    secret_sources: dict[str, SecretItem] = Field(default_factory=dict)
    _exported_values: dict[str, str] = PrivateAttr(default_factory=dict)

    def update_secrets(self, secrets: Mapping[str, SecretValue]) -> None:
        """문자열이 들어오면 SecretItem으로 일괄 캐스팅하여 저장"""
        for name, val in secrets.items():
            if isinstance(val, SecretItem):
                self.secret_sources[name] = val
            else:
                self.secret_sources[name] = SecretItem(value=SecretStr(val))

    def find_secrets_in_text(self, text: str) -> set[str]:
        found_keys = set()
        for key in self.secret_sources.keys():
            if key.lower() in text.lower():
                found_keys.add(key)
        return found_keys

    def get_secrets_as_env_vars(self, command: str) -> dict[str, str]:
        found_secrets = self.find_secrets_in_text(command)
        if not found_secrets:
            return {}

        logger.debug(f"Found secrets in command: {found_secrets}")
        env_vars = {}
        for key in found_secrets:
            try:
                value = self.secret_sources[key].get_value()
                if value:
                    env_vars[key] = value
                    self._exported_values[key] = value
            except Exception as e:
                logger.error(f"Failed to retrieve secret for key '{key}': {e}")
                continue

        logger.debug(f"Prepared {len(env_vars)} secrets as environment variables")
        return env_vars

    def mask_secrets_in_output(self, text: str) -> str:
        if not text:
            return text

        masked_text = text
        sorted_values = sorted(self._exported_values.values(), key=len, reverse=True)
        for value in sorted_values:
            masked_text = masked_text.replace(value, "<secret-hidden>")

        return masked_text

    def get_secret_infos(self) -> list[dict[str, str | None]]:
        if not self.secret_sources:
            return []
        
        return [
            {"name": name, "description": source.description}
            for name, source in self.secret_sources.items()
        ]