# agent.resolver.context
from __future__ import annotations

import asyncio
import json
from typing import Optional, Dict, Any, List, Callable

import os
from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Tuple, Union

from pydantic import BaseModel, Field

from engine.atoa.action.factory import CoreAction
from engine.atoa.conv.message import Message, TextContent

from arch.model.contract.graph import EntryNode
from arch.model.surge.blueprint import SurgeBlueprint, SurgeNode
from arch.topos.resolver.secret import SecretSource, SecretValue

from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.context")

SYSTEM_PROMPTS = {
    "role": "You are a precise, autonomous execution agent. Your primary purpose is to perfectly execute structural blueprints and complex operational tasks.",
    "execution": "Execution Protocol: Process the Blueprint sequentially. Use the 'terminal' tool to execute the exact commands provided. If a 'file not found' error occurs, use `find $(pwd) -name <filename>` to locate it. NEVER repeat the exact same failed command.",
    "focus": "Task Focus: Do not invent steps. Focus entirely on the current node. If blocked after 1 retry, use the 'bridge' tool to escalate immediately. Once validated, use 'finish'."
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

class BlueprintType(Enum):
    SCHEME = "scheme"
    TRANSACTION = "transaction"
    TRACER = "tracer"
    RESOLUTION = "resolution"

class SchemeCategory(Enum):
    AGENT = "agent"  
    GOV = "gov"      
    META = "meta"    
    AUTOPOIESIS = "autopoiesis"

class TransactionDomain(Enum):
    CODE_AUDITOR = "code_auditor"
    DATA_FOLDER = "data_folder"
    INFRA_SEALER = "infra_sealer"

class TraceDomain(Enum):
    DIVERGENCE = "divergence"
    OOM = "oom"
    REPRO = "repro"

def build_blueprint(
    topology_name: str, 
    focus: str, 
    steps: list[dict], 
    depth: int = 4, 
    relations: str = "sequential",
    system_instructions: str = "",
    min_cognitive_score: int = 1  
) -> Tuple[SurgeBlueprint, int]:
    
    nodes = []
    for i, step in enumerate(steps):
        action_name = step.get("action", "terminal")
        content = step.get("content", "")
        
        node = SurgeNode(
            id=f"step_{i+1}_{action_name}",
            intent=step.get("intent", "execute"),
            action=action_name,
            description=f"[{action_name.upper()}] {content}",
            expected_outcome=step.get("expected_outcome", f"Successfully completed {action_name} phase.")
        )
        
        if "params_template" in step:
            node.params_template = step["params_template"]
            
        nodes.append(node)

    if not system_instructions:
        system_instructions = (
            f"You are executing the '{focus}' scheme. "
            f"Navigate through the specified {len(steps)} events sequentially. "
            f"If an anomaly is detected, trigger the `signal` tool to broadcast architectural telemetry."
        )

    blueprint = SurgeBlueprint(
        topology_name=topology_name,
        focus=focus,
        depth_limit=depth,
        relations_constraint=relations,
        system_instructions=system_instructions.strip(),
        nodes=nodes
    )
    return blueprint, min_cognitive_score

RESOLUTION_SPEC = {
    "resolution_hacking": build_blueprint(
        topology_name="Semantic Resolution Funnel", focus="Resolution Hacking & Architectural Signaling", depth=4, min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "explore", "content": "Run `find $(pwd)/logs -type f -exec grep -rn 'Exception' {} +` to locate the exact file and line causing the structural rupture.", "expected_outcome": "Stack trace isolated."},
            {"action": "terminal", "intent": "modify", "content": "Use `sed -i` or edit the target file to implement the dynamic factory pattern fix.", "expected_outcome": "Code modification applied."},
            {"action": CoreAction.SIGNAL.value, "intent": "evangelize", "content": "Emit JSON payload summarizing the fix: `{\"file\": \"<path>\", \"fix\": \"factory_pattern_applied\", \"status\": \"ready_for_review\"}`.", "params_template": {"channel": "slack_#architecture", "requires_consensus": True}},
            {"action": CoreAction.FINISH.value, "intent": "commit", "content": "Run `git commit -am 'fix: structural decouple'` and finish the execution.", "expected_outcome": "ConverStatus.FINISHED"}
        ]
    )
}

SCHEME_SPEC: Dict[SchemeCategory, Tuple[SurgeBlueprint, int]] = {
    SchemeCategory.AGENT: build_blueprint(
        topology_name="agent.cognitive", focus="Cognitive State Validation", depth=2, min_cognitive_score=3,
        steps=[
            {"action": "terminal", "intent": "phase.cognitive", "content": "Execute `find $(pwd) -name dphi_node.log -exec grep 'WASM Metrics' {} +` to extract fuel and memory consumption data."},
            {"action": "terminal", "intent": "phase.cognitive", "content": "Run `PYTHONPATH=$(pwd) find $(pwd) -name validate_parity.py -exec python3 {} \\;` to assert that all agent nodes generated identical state hashes."},
            {"action": CoreAction.SIGNAL.value, "intent": "phase.cognitive", "content": "Emit telemetry signal: `{\"parity_valid\": true, \"fuel_usage_avg\": <value>}`."},
            {"action": CoreAction.FINISH.value, "intent": "phase.cognitive", "content": "Append validation result to `collapse_log.md` and complete task."}
        ]
    ),
    SchemeCategory.GOV: build_blueprint(
        topology_name="gov.sandbox", focus="WASM & Cgroup Physical Isolation Check", depth=3, relations="coupled,isolated", min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "phase.sandbox", "content": "Run `PYTHONPATH=$(pwd) python3 -m tester.dphi --suite sandbox` to trigger the WASM Cgroup isolation test suite."},
            {"action": "terminal", "intent": "phase.sandbox", "content": "Verify fuel exhaustion: `find $(pwd) -name test_output.log -exec grep 'wasm trap: all fuel consumed' {} +`. Ensure the STANDARD tier successfully blocked the payload."},
            {"action": "terminal", "intent": "phase.sandbox", "content": "Check process leaks: Run `ps aux | grep wasmtasker` to ensure supervisor cleanly terminated child processes."},
            {"action": CoreAction.FINISH.value, "intent": "phase.sandbox", "content": "Confirm isolation boundary is secure and finalize."}
        ]
    ),
    SchemeCategory.META: build_blueprint(
        topology_name="meta.telemetry", focus="Protocol Mutation & Survival Simulation", depth=4, relations="mutated,survived", min_cognitive_score=5,
        steps=[
            {"action": "terminal", "intent": "phase.meta", "content": "Execute `find $(pwd) -name broker.yaml -exec sed -i 's/latency:.*/latency: strict/g' {} +` to inject strict latency rules."},
            {"action": "terminal", "intent": "phase.meta", "content": "Run `systemctl restart dphi-broker` or equivalent to hot-reload the MCP broker."},
            {"action": "terminal", "intent": "phase.meta", "content": "Execute `PYTHONPATH=$(pwd) python3 -m tester.dphi --suite a2a` to verify that Agents can successfully recover Parity IDs."},
            {"action": CoreAction.FINISH.value, "intent": "phase.meta", "content": "Verify 'Epoch Sealed Successfully' in logs, proving structural survival, then exit."}
        ]
    ),
    SchemeCategory.AUTOPOIESIS: build_blueprint(
        topology_name="agent.autopoiesis", focus="Self-Healing Background Orchestration", depth=4, relations="decoupled_io,self_corrected", min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "phase.autopoiesis", "content": "Write a lightweight `health_api.py` using FastAPI that returns `{\"status\": \"alive\"}` on port 8080."},
            {"action": "terminal", "intent": "phase.autopoiesis", "content": "Launch the server in background: `nohup python3 health_api.py > api.log 2>&1 &`."},
            {"action": "terminal", "intent": "phase.autopoiesis", "content": "Poll the endpoint: `curl -s http://localhost:8080`. If it fails, `cat api.log`, install missing dependencies (`pip install fastapi uvicorn`), and retry."},
            {"action": CoreAction.FINISH.value, "intent": "phase.autopoiesis", "content": "Kill the background PID (`pkill -f health_api.py`), report success, and exit."}
        ]
    )
}

TRANSACTION_SPEC: Dict[TransactionDomain, Tuple[SurgeBlueprint, int]] = {
    TransactionDomain.CODE_AUDITOR: build_blueprint(
        topology_name="nexus.fiber.scan", focus="Dependency Graph Generation", depth=3, relations="scanned,isolated", min_cognitive_score=3,
        steps=[
            {"action": "terminal", "intent": "phase.auditor", "content": "Run `find $(pwd)/src -name '*.py' -type f | xargs grep -l 'import'` to list all files with dependencies."},
            {"action": "terminal", "intent": "phase.auditor", "content": "Execute `find $(pwd) -name build_ast_graph.py -exec python3 {} $(pwd)/src \\; > graph.json`."},
            {"action": CoreAction.SIGNAL.value, "intent": "phase.auditor", "content": "Read `graph.json` and emit the structural coupling report via JSON signal."},
            {"action": CoreAction.FINISH.value, "intent": "phase.auditor", "content": "Run `rm -f graph.json` and complete transaction."}
        ]
    ),
    TransactionDomain.DATA_FOLDER: build_blueprint(
        topology_name="theoria.compiler.fold", focus="Deterministic Schema Enforcement", depth=2, relations="transformed,sealed", min_cognitive_score=1,
        steps=[
            {"action": "terminal", "intent": "phase.folder", "content": "Run `find $(pwd) -name raw_input.txt -exec cat {} +` to inspect unstructured noisy data."},
            {"action": "terminal", "intent": "phase.folder", "content": "Execute `find $(pwd) -name topos_compiler.py -exec python3 {} $(pwd)/data/raw_input.txt --schema schema.json \\; > compiler_output.json`."},
            {"action": "terminal", "intent": "phase.folder", "content": "Verify JSON structure: `cat compiler_output.json | jq .` to ensure schema enforcement."},
            {"action": CoreAction.FINISH.value, "intent": "phase.folder", "content": "Seal validated data into KernelCommit (KNOTTED) and exit."}
        ]
    ),
    TransactionDomain.INFRA_SEALER: build_blueprint(
        topology_name="nexus.sphere.deploy", focus="IaC Resonance Simulation", depth=3, relations="projected,validated", min_cognitive_score=3,
        steps=[
            {"action": "terminal", "intent": "phase.sealer", "content": "Read raw definitions: `find $(pwd) -name deployment.yaml -exec cat {} +`."},
            {"action": "terminal", "intent": "phase.sealer", "content": "Run dry-run simulation: `find $(pwd) -name simulate_iac.py -exec python3 {} --manifest $(pwd)/k8s/deployment.yaml \\; > sim_results.json`."},
            {"action": CoreAction.SIGNAL.value, "intent": "phase.sealer", "content": "Extract metrics from `sim_results.json` and emit `{\"status\": \"simulated\", \"livelocks_detected\": 0}`."},
            {"action": CoreAction.FINISH.value, "intent": "phase.sealer", "content": "Seal validated manifest, append to pipeline logs, and drop simulation context."}
        ]
    )
}

TRACER_SPEC: Dict[TraceDomain, Tuple[SurgeBlueprint, int]] = {
    TraceDomain.DIVERGENCE: build_blueprint(
        topology_name="tracer.kube.divergence", focus="Control Plane Oscillation Audit", depth=4, relations="observed,collapsed", min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "phase.genesis", "content": "Apply base topology: `kubectl apply -f $(pwd)/k8s/base/`."},
            {"action": "terminal", "intent": "phase.stimulus", "content": "Inject tainted ConfigMap: `kubectl apply -f $(pwd)/k8s/taint_config.yaml` to trigger allostatic overload."},
            {"action": "terminal", "intent": "phase.resonance", "content": "Run `kubectl get pods -w` in background, sleep 60s, then pipe output to `oscillation_log.txt`."},
            {"action": CoreAction.SIGNAL.value, "intent": "phase.judgment", "content": "Parse `oscillation_log.txt`. Emit `{\"rupture_detected\": true, \"replicas_spiked\": <count>}`."},
            {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Run `kubectl delete -f $(pwd)/k8s/base/` to leave cluster in collapsed state and finish."}
        ]
    ),
    TraceDomain.OOM: build_blueprint(
        topology_name="tracer.docker.oom", focus="Absolute Resource Collapse (Cgroup OOM)", depth=4, relations="isolated,crushed", min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "phase.genesis", "content": "Build image: `docker build -t test-oom -f $(pwd)/Dockerfile.oom $(pwd)`."},
            {"action": "terminal", "intent": "phase.stimulus", "content": "Run constrained container: `docker run -d --name isolate_oom --memory=64m test-oom`."},
            {"action": "terminal", "intent": "phase.resonance", "content": "Wait for exit: `docker wait isolate_oom` and capture the Exit Code."},
            {"action": CoreAction.SIGNAL.value, "intent": "phase.judgment", "content": "If Exit Code is 137, emit `{\"status\": \"OOM_KILLED\", \"cgroup_enforced\": true}`."},
            {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Run `docker rm -f isolate_oom` and complete task."}
        ]
    ),
    TraceDomain.REPRO: build_blueprint(
        topology_name="tracer.compose.repro", focus="Queue Synchronization Resonance", depth=4, relations="synchronized,restored", min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "phase.genesis", "content": "Run `docker-compose -f $(pwd)/docker-compose.yml down -v && docker-compose -f $(pwd)/docker-compose.yml up -d` to guarantee clean boot."},
            {"action": "terminal", "intent": "phase.stimulus", "content": "Run `find $(pwd) -name inject_delayed_message.py -exec python3 {} \\;` to push stimulus to the queue."},
            {"action": "terminal", "intent": "phase.resonance", "content": "Monitor logs: `docker-compose -f $(pwd)/docker-compose.yml logs worker | grep 'Resonance Caught'` for 35 seconds."},
            {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Run `docker-compose -f $(pwd)/docker-compose.yml down` to restore field to absolute zero and conclude."}
        ]
    )
}

class TaskResolver:
    def __init__(self):
        self._schemes: Dict[SchemeCategory, Tuple[SurgeBlueprint, int]] = SCHEME_SPEC
        self._transactions: Dict[TransactionDomain, Tuple[SurgeBlueprint, int]] = TRANSACTION_SPEC
        self._tracers: Dict[TraceDomain, Tuple[SurgeBlueprint, int]] = TRACER_SPEC
        self._resolutions: Dict[str, Tuple[SurgeBlueprint, int]] = RESOLUTION_SPEC
        log.debug("[TaskResolver] Universal Executable Blueprints loaded into memory.")

    def resolve(self, category: Union[Enum, str], b_type: BlueprintType) -> Tuple[Optional[SurgeBlueprint], int]:
        item = None
        if b_type == BlueprintType.SCHEME:
            item = self._schemes.get(category)
        elif b_type == BlueprintType.TRANSACTION:
            item = self._transactions.get(category)
        elif b_type == BlueprintType.TRACER:
            item = self._tracers.get(category)
        elif b_type == BlueprintType.RESOLUTION:
            item = self._resolutions.get(category)

        if item:
            return item[0], item[1]
        return None, 1

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