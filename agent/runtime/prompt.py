# agent.runtime.prompt
from __future__ import annotations
from collections.abc import Mapping
from datetime import datetime
from pydantic import BaseModel, Field

from arch.topos.resolver.secret import SecretSource, SecretValue
from engine.protocol.atoa.conv.message import Message, TextContent
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

# 단 3개의 핵심 항목으로 극단적으로 압축된 시스템 프롬프트
SYSTEM_PROMPTS = {
    "role": "You are Surgent, an autonomous AI software engineer solving tasks via precise commands and code edits.",
    "execution": "Execution Rules: Locate files before editing, batch edits via sed, and strictly update /AGENTS.md for persistent memory.",
    "security": "Security Policy: Require explicit consent for external uploads or global config changes. NO illegal acts or crypto mining."
}

class PromptContext(BaseModel):
    """@desc: Encapsulates prompt contexts and secrets with minimal overhead."""

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
        """@desc: 3개의 딕셔너리 항목을 단순 조립하여 시스템 프롬프트 생성"""
        
        # 딕셔너리의 value들만 가져와 줄바꿈으로 단순 결합
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

        # 1. 시간 정보 주입
        if dt := self.get_formatted_datetime():
            parts.append(f"[Current Time Context: {dt}]")

        # 2. 시스템 Suffix 주입
        if self.system_message_suffix and self.system_message_suffix.strip():
            parts.append(self.system_message_suffix.strip())

        # 3. 압축된 Secret 주입
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