# actor.topos.resolver.task
## @lineage: topos.resolver.task
from enum import Enum
from typing import Dict, Optional, Union, Tuple

from engine.protocol.action.factory import CoreAction
from arch.model.surge.blueprint import SurgeBlueprint, SurgeNode
from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.task")

# =========================================================================
# 1. Enums (Domains & Categories)
# =========================================================================

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


# =========================================================================
# 2. Spec Definition Helper (Tuple 형태 반환으로 Pydantic 에러 원천 차단)
# =========================================================================

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


# =========================================================================
# 3. Action-Focused Blueprint Specifications
# =========================================================================

RESOLUTION_SPEC = {
    "resolution_hacking": build_blueprint(
        topology_name="Semantic Resolution Funnel", focus="Resolution Hacking & Architectural Signaling", depth=4, min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "explore", "content": "Scan system logs and stack traces to identify the structural rupture.", "expected_outcome": "Identify root cause."},
            {"action": "terminal", "intent": "modify", "content": "Use sed/file operations to decouple legacy static imports and apply dynamic factory patterns.", "expected_outcome": "Codebase aligned."},
            {"action": CoreAction.SIGNAL.value, "intent": "evangelize", "content": "Translate structural fix into architectural win and request consensus.", "params_template": {"channel": "slack_#architecture", "audience": "architect", "semantic_translation": "🚀 Ready for review.", "requires_consensus": True}},
            {"action": CoreAction.FINISH.value, "intent": "commit", "content": "Upon human consensus, merge state into 'collapse_log.md' and finalize.", "expected_outcome": "ConverStatus.FINISHED"}
        ]
    )
}

SCHEME_SPEC: Dict[SchemeCategory, Tuple[SurgeBlueprint, int]] = {
    SchemeCategory.AGENT: build_blueprint(
        topology_name="agent.cognitive", focus="Cognitive State Validation", depth=2, min_cognitive_score=3,
        steps=[
            {"action": "terminal", "intent": "phase.cognitive", "content": "Scan fragmented system logs via file tools to extract structural rupture signals."},
            {"action": "terminal", "intent": "phase.cognitive", "content": "Execute 'multi_chain_comparison.py' to cross-validate logical consistency."},
            {"action": "terminal", "intent": "phase.cognitive", "content": "Project validated state vectors into 'qdrant_semantic_cache.py'."},
            {"action": "terminal", "intent": "phase.cognitive", "content": "Measure cache hit rates and evaluate topology performance."},
            {"action": CoreAction.FINISH.value, "intent": "phase.cognitive", "content": "Merge the synthesized state into 'collapse_log.md' and authorize transition."}
        ]
    ),
    SchemeCategory.GOV: build_blueprint(
        topology_name="gov.sandbox", focus="Physical Membrane Isolation", depth=3, relations="coupled,isolated", min_cognitive_score=4,
        steps=[
            {"action": CoreAction.THINK.value, "intent": "phase.sandbox", "content": "Call MCP POSIX utilities to verify host resource mounts."},
            {"action": "terminal", "intent": "phase.sandbox", "content": "Spin up 'DockerWorkspaceNode' to provision absolute isolated container."},
            {"action": "terminal", "intent": "phase.sandbox", "content": "Inject a 'pytest' suite inside the sandbox to test 'store.fifo' concurrency locks."},
            {"action": CoreAction.THINK.value, "intent": "phase.sandbox", "content": "Trace all file system side-effects to audit spatial isolation leaks."},
            {"action": "terminal", "intent": "phase.sandbox", "content": "Teardown container and restore boundary to absolute zero state."}
        ]
    ),
    SchemeCategory.META: build_blueprint(
        topology_name="meta.telemetry", focus="Recursive Protocol Mutation & Survival", depth=5, relations="mutated,survived", min_cognitive_score=5,
        steps=[
            {"action": "terminal", "intent": "phase.meta", "content": "Locate and parse the source code of the currently active MCP server handling your tool calls."},
            {"action": "terminal", "intent": "phase.meta", "content": "Inject a breaking schema mutation into the server code."},
            {"action": "terminal", "intent": "phase.meta", "content": "Trigger a hot-reload of the MCP server."},
            {"action": CoreAction.THINK.value, "intent": "phase.meta", "content": "Catch the BrokenPipe error, re-initialize the connection using the newly mutated schema."},
            {"action": "terminal", "intent": "phase.meta", "content": "Execute a test query through the mutated protocol to prove structural survival."}
        ]
    ),
    SchemeCategory.AUTOPOIESIS: build_blueprint(
        topology_name="agent.autopoiesis", focus="Self-Healing Background Orchestration", depth=4, relations="decoupled_io,self_corrected", min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "phase.autopoiesis", "content": "Generate 'health_api.py' (FastAPI) configured for port 8080 to return system time."},
            {"action": "terminal", "intent": "phase.autopoiesis", "content": "Spawn server as a detached background process within the terminal pool."},
            {"action": "terminal", "intent": "phase.autopoiesis", "content": "Execute 'curl http://localhost:8080' to validate state."},
            {"action": "terminal", "intent": "phase.autopoiesis", "content": "Analyze stdout/stderr anomalies, install missing dependencies (fastapi, uvicorn), and restart server."},
            {"action": CoreAction.FINISH.value, "intent": "phase.autopoiesis", "content": "Confirm successful HTTP 200 response and authorize state transition."}
        ]
    )
}

TRANSACTION_SPEC: Dict[TransactionDomain, Tuple[SurgeBlueprint, int]] = {
    TransactionDomain.CODE_AUDITOR: build_blueprint(
        topology_name="nexus.fiber.scan", focus="Topological Legacy Decoupling", depth=3, relations="scanned,isolated", min_cognitive_score=3,
        steps=[
            {"action": "terminal", "intent": "phase.auditor", "content": "Ingest external GitHub repository as static 1D text payload (No execution)."},
            {"action": CoreAction.THINK.value, "intent": "phase.auditor", "content": "Execute 'fiber.scan.fragment' to scavenge AST and build reference graphs."},
            {"action": CoreAction.THINK.value, "intent": "phase.auditor", "content": "Calculate structural coupling and isolate circular dependencies."},
            {"action": CoreAction.SIGNAL.value, "intent": "phase.auditor", "content": "Synthesize architecture decouple blueprint and refactoring JSON."},
            {"action": CoreAction.FINISH.value, "intent": "phase.auditor", "content": "Seal output, return to client, and wipe local memory (Zero-Ops)."}
        ]
    ),
    TransactionDomain.DATA_FOLDER: build_blueprint(
        topology_name="theoria.compiler.fold", focus="Deterministic Schema Enforcement", depth=2, relations="transformed,sealed", min_cognitive_score=1,
        steps=[
            {"action": "terminal", "intent": "phase.folder", "content": "Receive unstructured noisy data and target JSON schema."},
            {"action": CoreAction.THINK.value, "intent": "phase.folder", "content": "Map input to LogicStream and initiate ToposCompiler evaluation."},
            {"action": "terminal", "intent": "phase.folder", "content": "Iterate LLM extraction loop until Tension breaches critical threshold."},
            {"action": CoreAction.FINISH.value, "intent": "phase.folder", "content": "Seal validated data into KernelCommit (KNOTTED)."}
        ]
    ),
    TransactionDomain.INFRA_SEALER: build_blueprint(
        topology_name="nexus.sphere.deploy", focus="IaC Resonance Simulation", depth=3, relations="projected,validated", min_cognitive_score=3,
        steps=[
            {"action": "terminal", "intent": "phase.sealer", "content": "Parse raw Kubernetes YAML or Terraform HCL definitions."},
            {"action": "terminal", "intent": "phase.sealer", "content": "Project IaC into logical graph topology without physical provisioning."},
            {"action": CoreAction.THINK.value, "intent": "phase.sealer", "content": "Utilize 'fiber.ator' to probe for potential livelocks or oscillation."},
            {"action": "terminal", "intent": "phase.sealer", "content": "Align detected ruptures and generate optimized IaC manifest."},
            {"action": CoreAction.FINISH.value, "intent": "phase.sealer", "content": "Seal validated manifest, return to CI/CD pipeline, and drop simulation context."}
        ]
    )
}

TRACER_SPEC: Dict[TraceDomain, Tuple[SurgeBlueprint, int]] = {
    TraceDomain.DIVERGENCE: build_blueprint(
        topology_name="tracer.kube.divergence", focus="K8s Control Plane Oscillation vs VM Livelock", depth=6, relations="observed,collapsed", min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "phase.genesis", "content": "Assume Genesis or setup base K8s topology."},
            {"action": "terminal", "intent": "phase.audit", "content": "Attach X-Y Auditor (K8s Replicas) and Z Auditor (VM Logs)."},
            {"action": "terminal", "intent": "phase.stimulus", "content": "Inject tainted ConfigMap to trigger allostatic overload."},
            {"action": "terminal", "intent": "phase.resonance", "content": "Wait for 60s to observe control plane resonance and replication spikes."},
            {"action": CoreAction.SIGNAL.value, "intent": "phase.judgment", "content": "Evaluate if infinite expansion (Rupture) occurred. Emit proof signal."},
            {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Leave cluster in collapsed state for autopsy and terminate background auditors."}
        ]
    ),
    TraceDomain.OOM: build_blueprint(
        topology_name="tracer.docker.oom", focus="Absolute Resource Collapse", depth=6, relations="isolated,crushed", min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "phase.genesis", "content": "Build Docker image targeting specific bug (Rustc/Cranelift)."},
            {"action": "terminal", "intent": "phase.audit", "content": "Attach CPU/Mem Entropy observer and Log streamer."},
            {"action": "terminal", "intent": "phase.stimulus", "content": "Run isolated container with strict resource limits and inject payload."},
            {"action": "terminal", "intent": "phase.resonance", "content": "Poll container state dynamically until ExitCode is detected."},
            {"action": CoreAction.SIGNAL.value, "intent": "phase.judgment", "content": "Judge if OOM (137) or Semantic Hang (>95% CPU) occurred. Emit structural proof."},
            {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Force remove container and reclaim host resources."}
        ]
    ),
    TraceDomain.REPRO: build_blueprint(
        topology_name="tracer.compose.repro", focus="Dramatiq Queue Synchronization Resonance", depth=6, relations="synchronized,restored", min_cognitive_score=4,
        steps=[
            {"action": "terminal", "intent": "phase.genesis", "content": "Annihilate remnants and perform clean boot of Docker Compose infrastructure."},
            {"action": "terminal", "intent": "phase.audit", "content": "Deploy LeakDetector script as an external background observer."},
            {"action": "terminal", "intent": "phase.stimulus", "content": "Execute internal worker script to inject delayed message stimulus."},
            {"action": "terminal", "intent": "phase.resonance", "content": "Observe bridging logs for 35s countdown until resonance is caught."},
            {"action": CoreAction.SIGNAL.value, "intent": "phase.judgment", "content": "Evaluate parsed signals to confirm if synchronization rupture occurred."},
            {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Down docker-compose network and restore field to absolute zero."}
        ]
    )
}

# =========================================================================
# 4. Universal Task Resolver
# =========================================================================

class TaskResolver:
    """
    @desc: 요청된 Category와 Type을 기반으로 (SurgeBlueprint, min_cognitive_score) 튜플을 반환합니다.
    """
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