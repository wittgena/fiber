# ops.scope.execution.compiler
## @lineage: meta.scope.execution.compiler
## @lineage: topos.scope.execution.compiler
from typing import Optional, Dict, Any, List, Callable
from arch.contract.schema.graph import EntryNode
from watcher.plane.emitter import get_emitter

log = get_emitter("execution.compiler")

BLUEPRINT_TEMPLATE = """\
{context_block}
{directives_block}
## Execution Blueprint (Execute the following strictly in sequence):
{steps_block}

*Instructions: Process all above nodes step-by-step using the designated tools. Maintain context integrity and report final completion using the 'finish' tool.*
"""

class BlueprintCompiler:
    """@desc: Translates structural DAG nodes into a unified LLM prompt."""
    
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

        directives_block = ""
        if system_instructions:
            directives_block = f"## Core Directives:\n{system_instructions.strip()}\n---"

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