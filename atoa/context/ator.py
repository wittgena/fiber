# atoa.context.ator
## @lineage: agent.context.ator
## @lineage: meta.agent.context
## @lineage: meta.ops.agent.context
## @lineage: meta.ator.context
from __future__ import annotations
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field

from bound.resolver.model.info import get_model_prompt_spec
from bound.resolver.secret import SecretSource, SecretValue

from atoa.context.parser import render_template
from eco.call.action.message import Message, TextContent

from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)
PROMPT_ROOT = resolve_path("prompt")

class AtorContext(BaseModel):
    """@desc: Encapsulates prompt templates, secrets, and dynamic context."""

    system_message_suffix: str | None = Field(default=None, description="System prompt suffix.")
    user_message_suffix: str | None = Field(default=None, description="User message suffix.")
    
    secrets: Mapping[str, SecretValue] | None = Field(
        default=None,
        description="Auth and sensitive data mapping.",
    )
    
    current_datetime: datetime | str | None = Field(
        default_factory=datetime.now,
        description="Current time context.",
    )

    system_prompt_filename: str = Field(
        default="system_prompt.j2",
        description="Base system prompt template.",
    )
    
    security_policy_filename: str = Field(
        default="security_policy.j2",
        description="Security policy template.",
    )
    
    system_prompt_kwargs: dict[str, Any] = Field(
        default_factory=dict,
        description="Jinja2 template kwargs.",
    )

    def get_secret_infos(self) -> list[dict[str, str | None]]:
        """@desc: Extract secret metadata."""
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
        """@desc: Format datetime to ISO 8601."""
        if not self.current_datetime:
            return None
        if isinstance(self.current_datetime, datetime):
            return self.current_datetime.isoformat()
        return self.current_datetime

    def get_static_system_message(
        self, 
        llm_model: str, 
        llm_model_canonical: str | None, 
        has_browser_tool: bool
    ) -> str:
        """@desc: Render the core static system prompt."""
        kwargs = dict(self.system_prompt_kwargs)
        kwargs.setdefault("enable_browser", has_browser_tool)
        kwargs["security_policy_filename"] = self.security_policy_filename
        kwargs.setdefault("model_name", llm_model)

        if "model_family" not in kwargs or "model_variant" not in kwargs:
            spec = get_model_prompt_spec(llm_model, llm_model_canonical)
            if spec.family:
                kwargs.setdefault("model_family", spec.family)
            if spec.variant:
                kwargs.setdefault("model_variant", spec.variant)

        return render_template(
            prompt_dir=str(PROMPT_ROOT),
            template_name=self.system_prompt_filename,
            **kwargs,
        )

    def get_system_message_suffix(
        self,
        llm_model: str | None = None,
        llm_model_canonical: str | None = None,
        additional_secret_infos: list[dict[str, str | None]] | None = None,
    ) -> str | None:
        """@desc: Render dynamic context (secrets, time, suffix)."""
        secret_infos = self.get_secret_infos()
        if additional_secret_infos:
            secret_dict = {s["name"]: s for s in secret_infos}
            for add in additional_secret_infos:
                secret_dict[add["name"]] = add
            secret_infos = list(secret_dict.values())
            
        formatted_dt = self.get_formatted_datetime()
        has_content = self.system_message_suffix or secret_infos or formatted_dt
        
        if has_content:
            return render_template(
                prompt_dir=str(PROMPT_ROOT),
                template_name="system_message_suffix.j2",
                repo_skills=[],
                system_message_suffix=self.system_message_suffix or "",
                secret_infos=secret_infos,
                available_skills_prompt="",
                current_datetime=formatted_dt,
            ).strip()
            
        if self.system_message_suffix and self.system_message_suffix.strip():
            return self.system_message_suffix.strip()
            
        return None

    def get_user_message_suffix(self, user_message: Message, skip_skill_names: list[str]) -> tuple[TextContent, list[str]] | None:
        """@desc: Retrieve user message suffix."""
        if self.user_message_suffix and self.user_message_suffix.strip():
            return TextContent(text=self.user_message_suffix.strip()), []
        return None