# fiber.llm.driver.config.agent
from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import Any, Literal, Dict, Optional, Tuple, Union, List, Callable

from pydantic import BaseModel, Field, field_serializer, field_validator

from fiber.llm.model.message import Message, TextContent
from fiber.llm.driver.model import LLMModel
from fiber.llm.driver.config.mcp import MCPConfig

from xphi.arch.model.dphi.graph import EntryNode
from fiber.phase.plane.resolver.secret import SecretSource, SecretValue
from xphi.arch.model.conv.tool import Tool
from xphi.arch.model.surge.blueprint import SurgeBlueprint, SurgeNode
from xphi.arch.model.surge.disc import SurgeBaseModel
from xphi.xor.parser.mark import warn_deprecated
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("config.agent")

SYSTEM_PROMPTS = {
    "role": "You are a precise, autonomous, and cost-aware execution agent. Your primary purpose is to perfectly execute structural blueprints while minimizing computational overhead.",
    "economics": "Resource Constraints: You operate inside a deterministic WASM Sandbox. Every execution consumes 'Fuel' from a hard cap dictated by your current Tier. Exhausting fuel causes an immediate Trap (termination). Always optimize algorithms and CLI commands (e.g., limit find depths, avoid large data pipes) to minimize overhead.",
    "execution": "Execution Protocol: Process the Blueprint sequentially. Use the 'terminal' tool to execute the exact commands provided. If a 'file not found' error occurs, use `find $(pwd) -maxdepth 3 -name <filename>` with targeted depth to locate it. NEVER repeat the exact same failed command.",
    "focus": "Task Focus: Do not invent steps outside the blueprint. Focus entirely on the current node. If a command fails, you MUST analyze the error and attempt ONE logical alternative (Self-Heal) before triggering an escalation via the 'bridge' tool. Once validated, use 'finish'."
}

class PromptContext(BaseModel):
    system_message_suffix: str | None = Field(default=None)
    user_message_suffix: str | None = Field(default=None)
    secrets: Mapping[str, SecretValue] | None = Field(default=None)
    current_datetime: datetime | str | None = Field(default_factory=datetime.now)

    def get_secret_infos(self) -> list[dict[str, str | None]]:
        if not self.secrets:
            return []
        return [
            {
                "name": name, 
                "description": val.description if isinstance(val, SecretSource) else None
            }
            for name, val in self.secrets.items()
        ]

    def get_formatted_datetime(self) -> str | None:
        if not self.current_datetime:
            return None
        return self.current_datetime.isoformat() if isinstance(self.current_datetime, datetime) else str(self.current_datetime)

    def get_static_system_message(
        self, 
        llm_model: str, 
        llm_model_canonical: str | None, 
        has_browser_tool: bool
    ) -> str:
        base_prompt = "\n\n".join(SYSTEM_PROMPTS.values())
        if has_browser_tool:
            base_prompt += "\n\nBrowser: Tool is enabled for web research."
            
        return base_prompt

    def get_system_message_suffix(
        self,
        llm_model: str | None = None,
        llm_model_canonical: str | None = None,
        additional_secret_infos: list[dict[str, str | None]] | None = None,
    ) -> str | None:
        parts = []
        if dt := self.get_formatted_datetime():
            parts.append(f"[Current Time Context: {dt}]")

        if self.system_message_suffix and self.system_message_suffix.strip():
            parts.append(self.system_message_suffix.strip())

        secret_infos = self.get_secret_infos()
        if additional_secret_infos:
            secret_dict = {s["name"]: s for s in secret_infos}
            for add in additional_secret_infos:
                secret_dict[add["name"]] = add
            secret_infos = list(secret_dict.values())
            
        if secret_infos:
            secret_lines = ["<SECRETS> (Auto-exported as ENV vars)"]
            for s in secret_infos:
                desc = f" - {s.get('description', '')}" if s.get('description') else ""
                secret_lines.append(f"* ${{{s['name']}}}{desc}")
            secret_lines.append("</SECRETS>")
            parts.append("\n".join(secret_lines))

        return "\n\n".join(parts) if parts else None

    def get_user_message_suffix(self, user_message: Message, skip_skill_names: list[str]) -> tuple[TextContent, list[str]] | None:
        if self.user_message_suffix and self.user_message_suffix.strip():
            return TextContent(text=self.user_message_suffix.strip()), []
        return None

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
