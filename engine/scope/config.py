# engine.scope.config
## @lineage: dphi.topos.scope.config
## @lineage: agent.runtime.scope.config
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, get_args, get_origin
from engine.atoa.mcp.config import MCPConfig
from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
)
from pydantic.fields import FieldInfo

from agent.resolver.context import PromptContext

from engine.driver.disc.tool import Tool
from engine.driver.llm.model import LLMModel
from agent.runtime.activator import Activator

from engine.scope.metadata import SettingsSchema
from engine.scope.setting import VerificationSettings, export_settings_schema

from arch.model.surge.disc import SurgeBaseModel
from arch.xor.bridge.mark.convset import (
    SETTINGS_METADATA_KEY,
    SETTINGS_SECTION_METADATA_KEY,
    SettingProminence,
    SettingsFieldMetadata,
    SettingsSectionMetadata,
)

AGENT_SETTINGS_SCHEMA_VERSION = 1

def _default_llm_settings() -> LLMModel:
    model = LLMModel.model_fields["model"].get_default()
    assert isinstance(model, str)
    return LLMModel(model=model)

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
    llm: LLMModel = Field(
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
    prompt_context: PromptContext = Field(
        default_factory=PromptContext,
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
            prompt_context=self.prompt_context
        )