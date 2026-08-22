# agent.nexus.runtime.blueprint
## @lineage: nexus.agent.runtime.blueprint
## @lineage: meta.agent.runtime.blueprint
## @lineage: agent.runtime.blueprint
from __future__ import annotations

import os
import json
from typing import Optional, Dict, Any, List, Tuple, Union
from enum import Enum

from agent.runtime.conv.action.factory import CoreAction
from arch.model.surge.blueprint import SurgeBlueprint, SurgeNode
from watcher.plane.emitter import get_emitter

log = get_emitter("context.blueprint")

class BlueprintType(Enum):
    SCHEME = "scheme"
    TRANSACTION = "transaction"
    TRACER = "tracer"
    RESOLUTION = "resolution"

def build_blueprint(
    topology_name: str, 
    focus: str, 
    steps: list[dict], 
    relations: str = "sequential",
    min_cognitive_score: int = 1,
    target_tier: str = "SYSTEM",
    fuel_limit: int = 2_000_000_000
) -> Tuple[SurgeBlueprint, int]:
    nodes = []
    for i, step in enumerate(steps):
        action_name = step.get("action", "terminal")
        content = step.get("content", "")
        
        node = SurgeNode(
            id=f"step_{i+1}_{action_name}",
            intent=step.get("intent", "execute"),
            action=action_name,
            description=f"Use the '{action_name.lower()}' tool: {content}",
            expected_outcome=step.get("expected_outcome", f"Successfully completed {action_name} phase.")
        )
        
        if "params_template" in step:
            node.params_template = step["params_template"]
            
        nodes.append(node)

    # =========================================================================
    # [수정됨] CoreAction.TERMINAL.value 를 일반 문자열 'terminal'로 교체
    # =========================================================================
    system_instructions = (
        f"You are executing the '{focus}' topology under {target_tier} constraints (Fuel Limit: {fuel_limit}).\n"
        f"Navigate through the specified {len(steps)} events sequentially.\n"
        f"Determine the most efficient commands dynamically based on your environment.\n"
        f"If an anomaly is detected, trigger the 'signal' tool to broadcast architectural telemetry.\n\n"
        f"CRITICAL RULES FOR FUNCTION CALLING:\n"
        f"1. EXACT TOOL NAMES: You must strictly use the exact lowercase tool names as registered (e.g., 'terminal', '{CoreAction.SIGNAL.value}', '{CoreAction.FINISH.value}').\n"
        f"2. REQUIRED PARAMETERS: Never omit required parameters. For example, when using the 'terminal' tool, you must provide BOTH 'command' and 'security_risk'."
    )
    
    blueprint = SurgeBlueprint(
        topology_name=topology_name,
        focus=focus,
        depth_limit=len(steps),
        relations_constraint=relations,
        system_instructions=system_instructions.strip(),
        nodes=nodes
    )
    return blueprint, min_cognitive_score

BLUEPRINT_REGISTRY: Dict[BlueprintType, Dict[str, Tuple[SurgeBlueprint, int]]] = {
    BlueprintType.RESOLUTION: {
        "resolution_hacking": build_blueprint(
            topology_name="Semantic Resolution Funnel", 
            focus="Resolution Hacking & Architectural Signaling", 
            min_cognitive_score=4,
            steps=[
                {"action": "terminal", "intent": "explore", "content": "Search the workspace logs to locate the exact file and line causing the structural Exception rupture.", "expected_outcome": "Stack trace isolated."},
                {"action": "terminal", "intent": "modify", "content": "Modify the target file to implement the dynamic factory pattern fix.", "expected_outcome": "Code modification applied."},
                {"action": CoreAction.SIGNAL.value, "intent": "evangelize", "content": "Emit a JSON payload summarizing the applied fix (file, fix_type, status).", "params_template": {"channel": "slack_#architecture", "requires_consensus": True}},
                {"action": CoreAction.FINISH.value, "intent": "commit", "content": "Commit the changes to the repository with a descriptive message and finish the execution.", "expected_outcome": "ConverStatus.FINISHED"}
            ]
        )
    },

    BlueprintType.SCHEME: {
        "agent": build_blueprint(
            topology_name="agent.cognitive", 
            focus="Cognitive State Validation", 
            min_cognitive_score=3,
            steps=[
                {"action": "terminal", "intent": "phase.cognitive", "content": "Scan the workspace for 'dphi_node.log' and extract fuel and memory consumption metrics."},
                {"action": "terminal", "intent": "phase.cognitive", "content": "Execute the 'validate_parity.py' script within the workspace context to assert state hash identity across agent nodes."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.cognitive", "content": "Emit telemetry signal containing parity validation status and average fuel usage."},
                {"action": CoreAction.FINISH.value, "intent": "phase.cognitive", "content": "Append the validation result to 'collapse_log.md' and complete the task."}
            ]
        ),
        "gov": build_blueprint(
            topology_name="gov.sandbox", 
            focus="WASM & Cgroup Physical Isolation Check", 
            relations="coupled,isolated", 
            min_cognitive_score=4,
            steps=[
                {"action": "terminal", "intent": "phase.sandbox", "content": "Trigger the WASM Cgroup isolation test suite via the dphi tester module (sandbox suite)."},
                {"action": "terminal", "intent": "phase.sandbox", "content": "Analyze 'test_output.log' to verify fuel exhaustion events. Ensure the STANDARD tier successfully blocked the payload."},
                {"action": "terminal", "intent": "phase.sandbox", "content": "Verify process isolation by checking for active 'wasmtasker' processes to ensure clean termination."},
                {"action": CoreAction.FINISH.value, "intent": "phase.sandbox", "content": "Confirm the isolation boundary is secure and finalize."}
            ]
        ),
        "meta": build_blueprint(
            topology_name="meta.telemetry", 
            focus="Protocol Mutation & Survival Simulation", 
            relations="mutated,survived", 
            min_cognitive_score=5,
            steps=[
                {"action": "terminal", "intent": "phase.meta", "content": "Locate the 'broker.yaml' configuration and inject strict latency rules into its parameters."},
                {"action": "terminal", "intent": "phase.meta", "content": "Hot-reload or restart the MCP broker service to apply the new latency rules."},
                {"action": "terminal", "intent": "phase.meta", "content": "Execute the 'a2a' test suite to verify that Agents can successfully recover Parity IDs under strict latency."},
                {"action": CoreAction.FINISH.value, "intent": "phase.meta", "content": "Verify 'Epoch Sealed Successfully' in the execution logs, proving structural survival, then exit."}
            ]
        ),
        "autopoiesis": build_blueprint(
            topology_name="agent.autopoiesis", 
            focus="Self-Healing Background Orchestration", 
            relations="decoupled_io,self_corrected", 
            min_cognitive_score=4,
            steps=[
                {"action": "terminal", "intent": "phase.autopoiesis", "content": "Author a lightweight 'health_api.py' (using FastAPI) that returns {'status': 'alive'} on port 8080."},
                {"action": "terminal", "intent": "phase.autopoiesis", "content": "Deploy the API server as a background process, ensuring logs are captured to 'api.log'."},
                {"action": "terminal", "intent": "phase.autopoiesis", "content": "Poll the local health endpoint. If it fails, analyze 'api.log', install any missing dependencies, and retry."},
                {"action": CoreAction.FINISH.value, "intent": "phase.autopoiesis", "content": "Terminate the background API process, report success, and exit."}
            ]
        )
    },

    BlueprintType.TRANSACTION: {
        "code_auditor": build_blueprint(
            topology_name="nexus.fiber.scan", 
            focus="Dependency Graph Generation", 
            relations="scanned,isolated", 
            min_cognitive_score=3,
            steps=[
                {"action": "terminal", "intent": "phase.auditor", "content": "Scan the 'src' directory to identify and list all Python files containing import dependencies."},
                {"action": "terminal", "intent": "phase.auditor", "content": "Execute 'build_ast_graph.py' against the 'src' directory to generate 'graph.json'."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.auditor", "content": "Read 'graph.json' and emit the structural coupling report via JSON signal."},
                {"action": CoreAction.FINISH.value, "intent": "phase.auditor", "content": "Remove the temporary 'graph.json' file and complete the transaction."}
            ]
        ),
        "data_folder": build_blueprint(
            topology_name="theoria.compiler.fold", 
            focus="Deterministic Schema Enforcement", 
            relations="transformed,sealed", 
            min_cognitive_score=1,
            steps=[
                {"action": "terminal", "intent": "phase.folder", "content": "Locate and read 'raw_input.txt' to inspect the unstructured noisy data."},
                {"action": "terminal", "intent": "phase.folder", "content": "Execute 'topos_compiler.py' on 'raw_input.txt' using 'schema.json' to produce 'compiler_output.json'."},
                {"action": "terminal", "intent": "phase.folder", "content": "Verify the structural integrity of 'compiler_output.json' against the schema requirements."},
                {"action": CoreAction.FINISH.value, "intent": "phase.folder", "content": "Seal the validated data into a KernelCommit (KNOTTED) and exit."}
            ]
        ),
        "infra_sealer": build_blueprint(
            topology_name="nexus.sphere.deploy", 
            focus="IaC Resonance Simulation", 
            relations="projected,validated", 
            min_cognitive_score=3,
            steps=[
                {"action": "terminal", "intent": "phase.sealer", "content": "Locate and inspect the 'deployment.yaml' manifest within the workspace."},
                {"action": "terminal", "intent": "phase.sealer", "content": "Run a dry-run simulation using 'simulate_iac.py' against the Kubernetes deployment manifest to generate 'sim_results.json'."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.sealer", "content": "Extract metrics from 'sim_results.json' and emit a signal indicating simulation status and livelock count."},
                {"action": CoreAction.FINISH.value, "intent": "phase.sealer", "content": "Seal the validated manifest, append to pipeline logs, and drop the simulation context."}
            ]
        )
    },

    BlueprintType.TRACER: {
        "divergence": build_blueprint(
            topology_name="tracer.kube.divergence", 
            focus="Control Plane Oscillation Audit", 
            relations="observed,collapsed", 
            min_cognitive_score=4,
            steps=[
                {"action": "terminal", "intent": "phase.genesis", "content": "Apply the base Kubernetes topology from the 'k8s/base' directory."},
                {"action": "terminal", "intent": "phase.stimulus", "content": "Inject a tainted ConfigMap to trigger allostatic overload in the cluster."},
                {"action": "terminal", "intent": "phase.resonance", "content": "Monitor pod states for 60 seconds and pipe the oscillation observations to a log file."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.judgment", "content": "Parse the oscillation log and emit a payload detailing whether a rupture was detected and the replica spike count."},
                {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Delete the applied topology to leave the cluster in a collapsed state and finish."}
            ]
        ),
        "oom": build_blueprint(
            topology_name="tracer.docker.oom", 
            focus="Absolute Resource Collapse (Cgroup OOM)", 
            relations="isolated,crushed", 
            min_cognitive_score=4,
            steps=[
                {"action": "terminal", "intent": "phase.genesis", "content": "Build the 'test-oom' Docker image using 'Dockerfile.oom' located in the workspace."},
                {"action": "terminal", "intent": "phase.stimulus", "content": "Run the image as a background container named 'isolate_oom' with a strict memory limit (e.g., 64m)."},
                {"action": "terminal", "intent": "phase.resonance", "content": "Wait for the 'isolate_oom' container to exit and capture its Exit Code."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.judgment", "content": "If the Exit Code is 137, emit a signal confirming OOM_KILLED and cgroup enforcement."},
                {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Remove the isolated container and complete the task."}
            ]
        ),
        "repro": build_blueprint(
            topology_name="tracer.compose.repro", 
            focus="Queue Synchronization Resonance", 
            relations="synchronized,restored", 
            min_cognitive_score=4,
            steps=[
                {"action": "terminal", "intent": "phase.genesis", "content": "Tear down and rebuild the docker-compose environment to guarantee a clean boot state."},
                {"action": "terminal", "intent": "phase.stimulus", "content": "Execute 'inject_delayed_message.py' to push a stimulus into the queue."},
                {"action": "terminal", "intent": "phase.resonance", "content": "Monitor the worker logs for 'Resonance Caught' events for a duration of 35 seconds."},
                {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Tear down the docker-compose environment to restore the field to absolute zero and conclude."}
            ]
        )
    }
}


class TaskResolver:
    def __init__(self):
        log.debug("[TaskResolver] Unified Blueprint Registry loaded into memory.")

    def resolve(self, category: Union[Enum, str], b_type: BlueprintType) -> Tuple[Optional[SurgeBlueprint], int]:
        cat_key = category.value if isinstance(category, Enum) else str(category)
        
        # 1. 딕셔너리에서 동적 매칭
        item = BLUEPRINT_REGISTRY.get(b_type, {}).get(cat_key)
        if item:
            return item[0], item[1]
            
        # 2. 자율 진화를 위한 Fallback (파일 시스템 JSON 동적 로드)
        fallback_path = f"./blueprints/{b_type.value}/{cat_key}.json"
        if os.path.exists(fallback_path):
            try:
                log.info(f"[TaskResolver] Dynamically loading blueprint from {fallback_path}")
                # 향후 에이전트가 생성한 추상 Blueprint 로딩을 위한 준비
                # with open(fallback_path, 'r') as f:
                #     data = json.load(f)
                #     return build_blueprint(**data)
            except Exception as e:
                log.error(f"[TaskResolver] Failed to parse dynamic blueprint {cat_key}: {e}")
                
        return None, 1