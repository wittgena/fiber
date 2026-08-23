# phase.agent.runtime.prompt
## @lineage: agent.nexus.runtime.prompt
## @lineage: nexus.agent.runtime.prompt
## @lineage: meta.agent.runtime.prompt
## @lineage: agent.runtime.prompt
from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Tuple, Union, List, Callable, Any

from pydantic import BaseModel, Field

from phase.agent.runtime.blueprint import BlueprintType, build_blueprint, TaskResolver, BLUEPRINT_REGISTRY

from agent.loop.conv.action.factory import CoreAction
from agent.space.action.message import Message, TextContent

from arch.contract.model.graph import EntryNode
from arch.model.surge.blueprint import SurgeBlueprint, SurgeNode
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