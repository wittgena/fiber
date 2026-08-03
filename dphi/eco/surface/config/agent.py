# dphi.eco.surface.config.agent
## @lineage: dphi.eco.config.agent
from typing import Any, Literal

from pydantic import Field, field_serializer, field_validator

from ator.context.prompt import PromptContext
from ator.topos.activator import Activator

from engine.protocol.mcp.config import MCPConfig
from engine.driver.disc.tool import Tool
from engine.driver.llm.model import LLMModel

from arch.model.surge.disc import SurgeBaseModel
from arch.xor.bridge.mark.depre import warn_deprecated

AGENT_SETTINGS_SCHEMA_VERSION = 1
SecurityAnalyzerType = Literal["llm", "none"]

class VerificationSettings(SurgeBaseModel):
    reflector_enabled: bool = Field(
        default=False,
        description="Enable evaluation for the agent.",
    )
    
    enable_iterative_refinement: bool = Field(
        default=False,
        description="Automatically retry tasks when reflector scores fall below the threshold.",
    )
    
    reflector_threshold: float = Field(
        default=0.6,
        ge=0.0,
        le=1.0,
        description="Reflector success threshold used for iterative refinement.",
    )
    
    max_refinement_iterations: int = Field(
        default=3,
        ge=1,
        description="Maximum number of refinement attempts after reflector feedback.",
    )
    
    reflector_server_url: str | None = Field(
        default=None,
        description="Override the reflector service URL. When None, the Reflector default is used.",
    )
    
    reflector_model_name: str | None = Field(
        default=None,
        description="Override the reflector model name. When None, the Reflector default is used.",
    )

    confirmation_mode: bool = Field(
        default=False,
        description="Require user confirmation before executing risky actions.",
        deprecated=(
            "Deprecated in 1.17.0; use ConversationSettings.confirmation_mode "
            "instead. Will be removed in 1.22.0."
        ),
    )
    
    security_analyzer: SecurityAnalyzerType | None = Field(
        default=None,
        description="Security analyzer that evaluates actions before execution.",
        deprecated=(
            "Deprecated in 1.17.0; use ConversationSettings.security_analyzer "
            "instead. Will be removed in 1.22.0."
        ),
    )

    @field_validator("confirmation_mode", mode="before")
    @classmethod
    def _warn_confirmation_mode(cls, v: Any) -> Any:
        if v:
            warn_deprecated(
                "VerificationSettings.confirmation_mode",
                deprecated_in="1.17.0",
                removed_in="1.22.0",
                details="Use ConversationSettings.confirmation_mode instead.",
            )
        return v

    @field_validator("security_analyzer", mode="before")
    @classmethod
    def _warn_security_analyzer(cls, v: Any) -> Any:
        if v is not None:
            warn_deprecated(
                "VerificationSettings.security_analyzer",
                deprecated_in="1.17.0",
                removed_in="1.22.0",
                details="Use ConversationSettings.security_analyzer instead.",
            )
        return v


def _default_llm_settings() -> LLMModel:
    model = LLMModel.model_fields["model"].get_default()
    assert isinstance(model, str)
    return LLMModel(model=model)

class AgentConfig(SurgeBaseModel):
    schema_version: int = Field(default=AGENT_SETTINGS_SCHEMA_VERSION, ge=1)
    agent: str = Field(
        default="CodeActAgent",
        description="Agent class to use.",
    )
    
    llm: LLMModel = Field(
        default_factory=_default_llm_settings,
        description="LLM settings for the agent.",
    )
    
    tools: list[Tool] = Field(
        default_factory=list,
        description="Tools available to the agent.",
    )
    
    mcp_config: MCPConfig | None = Field(
        default=None,
        description="MCP server configuration for the agent.",
    )
    
    prompt_context: PromptContext = Field(
        default_factory=PromptContext,
        description="Context for the agent (skills, secrets, message suffixes).",
    )
    
    verification: VerificationSettings = Field(
        default_factory=VerificationSettings,
        description="Verification settings for the agent reflector.",
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

    def create_activator(self) -> Activator:
        return Activator(
            llm=self.llm,
            tools=self.tools,
            mcp_config=self._serialize_mcp_config(self.mcp_config),
            prompt_context=self.prompt_context
        )