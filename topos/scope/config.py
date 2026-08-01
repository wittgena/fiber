# topos.scope.config
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, get_args, get_origin
from agent.atoa.mcp.config import MCPConfig
from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
)
from pydantic.fields import FieldInfo

from agent.atoa.schema.ator.context import AtorContext

from agent.atoa.schema.disc.tool import Tool
from agent.llm.driver.tensor import Driver
from agent.activator import Activator

from topos.scope.metadata import SettingsSchema
from topos.scope.setting import VerificationSettings, export_settings_schema

from arch.model.surge.disc import SurgeBaseModel
from arch.xor.bridge.mark.convset import (
    SETTINGS_METADATA_KEY,
    SETTINGS_SECTION_METADATA_KEY,
    SettingProminence,
    SettingsFieldMetadata,
    SettingsSectionMetadata,
)

AGENT_SETTINGS_SCHEMA_VERSION = 1

def _default_llm_settings() -> Driver:
    model = Driver.model_fields["model"].get_default()
    assert isinstance(model, str)
    return Driver(model=model)

class AgentConfig(SurgeBaseModel):
    schema_version: int = Field(default=AGENT_SETTINGS_SCHEMA_VERSION, ge=1)
    agent: str = Field(
        default="CodeActAgent",
        description="Agent class to use.",
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Agent",
                prominence=SettingProminence.MAJOR,
            ).model_dump()
        },
    )
    llm: Driver = Field(
        default_factory=_default_llm_settings,
        description="LLM settings for the agent.",
        json_schema_extra={
            SETTINGS_SECTION_METADATA_KEY: SettingsSectionMetadata(
                key="llm",
                label="LLM",
            ).model_dump()
        },
    )
    tools: list[Tool] = Field(
        default_factory=list,
        description="Tools available to the agent.",
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Tools",
                prominence=SettingProminence.MAJOR,
            ).model_dump()
        },
    )
    mcp_config: MCPConfig | None = Field(
        default=None,
        description="MCP server configuration for the agent.",
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="MCP configuration",
                prominence=SettingProminence.MINOR,
            ).model_dump()
        },
    )
    ator_context: AtorContext = Field(
        default_factory=AtorContext,
        description="Context for the agent (skills, secrets, message suffixes).",
    )
    verification: VerificationSettings = Field(
        default_factory=VerificationSettings,
        description="Verification settings for the agent reflector.",
        json_schema_extra={
            SETTINGS_SECTION_METADATA_KEY: SettingsSectionMetadata(
                key="verification",
                label="Verification",
            ).model_dump()
        },
    )

    @field_validator("mcp_config", mode="before")
    @classmethod
    def _normalize_empty_mcp_config(cls, value: Any) -> Any:
        if value in (None, {}):
            return None
        return value

    @field_serializer("mcp_config")
    def _serialize_mcp_config(self, value: MCPConfig | None) -> dict[str, Any]:
        if value is None:
            return {}
        return value.model_dump(exclude_none=True, exclude_defaults=True)

    @classmethod
    def export_schema(cls) -> SettingsSchema:
        """Export a structured schema describing configurable agent settings."""
        return export_settings_schema(cls)

    def create_activator(self) -> Activator:
        return Activator(
            llm=self.llm,
            tools=self.tools,
            mcp_config=self._serialize_mcp_config(self.mcp_config),
            agent_context=self.ator_context
        )