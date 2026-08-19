# ator.conv.context.prompt
## @lineage: ator.context.prompt
from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Tuple, Union, List, Callable, Any

from pydantic import BaseModel, Field

from ator.runtime.action.factory import CoreAction
from ator.conv.context.blueprint import BlueprintType, build_blueprint, TaskResolver, BLUEPRINT_REGISTRY
from ator.conv.schema.message import Message, TextContent

from arch.contract.model.graph import EntryNode
from arch.xor.surge.blueprint import SurgeBlueprint, SurgeNode
from arch.contract.resolver.secret import SecretSource, SecretValue

from kernel.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.context")

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


BLUEPRINT_TEMPLATE = """\
{context_block}
{directives_block}
## Execution Blueprint (Execute the following strictly in sequence):
{steps_block}

*Instructions: Process all above nodes step-by-step using the designated tools. Maintain context integrity and report final completion using the 'finish' tool.*
"""

class BlueprintCompiler:
    @staticmethod
    def compile(context_node: Optional[EntryNode], nodes: List[Any], system_instructions: str = "") -> str:
        context_block = ""
        if context_node:
            relations = ", ".join(context_node.relations) if getattr(context_node, 'relations', None) else "None"
            context_block = (
                f"## System Context: {context_node.entry}\n"
                f"- **Focus**: {context_node.focus}\n"
                f"- **Depth Limit**: {context_node.depth}\n"
                f"- **Relations Constraint**: {relations}\n---"
            )

        directives_block = f"## Core Directives:\n{system_instructions.strip()}\n---" if system_instructions else ""
        steps = []
        for idx, node in enumerate(nodes, 1):
            action_name = getattr(node, 'action', 'terminal').upper()
            intent = getattr(node, 'intent', '')
            desc = getattr(node, 'description', '')
            step_line = f"{idx}. [{action_name}] {f'({intent.upper()}) ' if intent else ''}{desc}"
            steps.append(step_line)
            
            if params := getattr(node, 'params_template', None):
                steps.append(f"   > Required Tool Params: {params}")

        return BLUEPRINT_TEMPLATE.format(
            context_block=context_block,
            directives_block=directives_block,
            steps_block="\n".join(steps)
        ).strip()