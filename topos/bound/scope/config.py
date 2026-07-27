# topos.bound.scope.config
## @lineage: ops.scope.topos.config
## @lineage: void.extime.logst.topos.config
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, get_args, get_origin
from mesh.mcp.config import MCPConfig
from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
)
from pydantic.fields import FieldInfo

from atoa.schema.reflect import ReflectorBase
from atoa.context import AtorContext

from agent.disc.tool import Tool
from agent.driver.tensor import Driver
from agent.activator import Activator

from topos.bound.scope.metadata import SettingsSchema
from topos.bound.scope.setting import VerificationSettings, export_settings_schema

from arch.topos.bound.surge.disc import SurgeBaseModel
from arch.xor.xe.convset import (
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
            agent_context=self.ator_context,
            reflector=self.build_reflector(),
        )

    def build_reflector(self) -> ReflectorBase | None:
        if not self.verification.reflector_enabled:
            return None

        api_key = self.llm.api_key
        if api_key is None:
            return None

        from atoa.schema.reflect import IterativeRefinementConfig
        from agent.config.reflect import Reflector

        iterative_refinement = None
        if self.verification.enable_iterative_refinement:
            iterative_refinement = IterativeRefinementConfig(
                success_threshold=self.verification.reflector_threshold,
                max_iterations=self.verification.max_refinement_iterations,
            )

        overrides: dict[str, Any] = {}
        if self.verification.reflector_server_url is not None:
            overrides["server_url"] = self.verification.reflector_server_url
        if self.verification.reflector_model_name is not None:
            overrides["model_name"] = self.verification.reflector_model_name

        return Reflector(
            api_key=api_key,
            mode=self.verification.reflector_mode,
            iterative_refinement=iterative_refinement,
            **overrides,
        )