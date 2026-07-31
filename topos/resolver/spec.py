# topos.resolver.spec
import logging
from enum import Enum
from typing import Dict
import sys

from typing import List, Dict, Any, Optional, Annotated
from dataclasses import dataclass, asdict
from pydantic import BaseModel, Field

from arch.bound.surge.blueprint import SurgeBlueprint, SurgeNode
from arch.contract.schema.graph import EntryNode
from arch.contract.schema.resonance import BridgeEvent
from phi.engine.driver.factory.action import CoreAction

log = logging.getLogger(__name__)

class TraceDomain(Enum):
    DIVERGENCE = "divergence"
    OOM = "oom"
    REPRO = "repro"

class SchemeCategory(Enum):
    AGENT = "agent"  
    GOV = "gov"      
    META = "meta"    
    AUTOPOIESIS = "autopoiesis"

class TransactionDomain(Enum):
    CODE_AUDITOR = "code_auditor"
    DATA_FOLDER = "data_folder"
    INFRA_SEALER = "infra_sealer"

class SchemeSynthesisSignature(BaseModel):
    """
    [Signature] Task specification for the LLM (ThCh) to dynamically synthesize 
    a new Scheme Blueprint when physical exceptions or novel requirements occur.
    """
    system_telemetry: Annotated[str, "input"] = Field(description="Current system error logs, bottlenecks, or physical constraints to bypass.")
    target_focus: Annotated[str, "input"] = Field(description="The ultimate objective this Scheme must achieve (e.g., Resolve DB Deadlock and rollback).")
    
    synthesized_entry: Annotated[dict, "output"] = Field(description="EntryNode specification dictionary (entry, focus, depth, relations).")
    synthesized_events: Annotated[List[dict], "output"] = Field(description="A list of at least 3 BridgeEvent dictionaries (content, source, event_type).")

@dataclass
class SchemeBlueprint:
    context: EntryNode
    events: List[BridgeEvent]

    @classmethod
    def from_signature_output(cls, llm_output: dict) -> "SchemeBlueprint":
        try:
            context_node = EntryNode(**llm_output["synthesized_entry"])
            events = [BridgeEvent(**event) for event in llm_output["synthesized_events"]]
            return cls(context=context_node, events=events)
        except Exception as e:
            log.error(f"[Blueprint Bridge] Failed to cast LLM signature output: {e}")
            raise ValueError(f"Invalid DSPy generation format: {e}")

    def to_few_shot_example(self) -> dict:
        return {
            "synthesized_entry": asdict(self.context),
            "synthesized_events": [asdict(e) for e in self.events]
        }

    def compile_to_surge(self) -> SurgeBlueprint:
        def _map_event_to_action(event_type: str) -> str:
            event_type = event_type.lower()
            if event_type in ["system_command", "execute_script", "execute_test", "container_lifecycle", "setup_infrastructure", "attach_auditors", "inject_stimulus", "observe_resonance", "teardown"]:
                return "terminal"  # 로우레벨 실행 도구
            elif event_type in ["report_generation", "teardown_notification", "evaluate_judgment"]:
                return CoreAction.SIGNAL.value  # [Resolution Hacking] 마케팅/외부 채널 번역 도구
            elif event_type in ["audit", "security_trace", "security_test", "evaluate_topology", "analyze_ast", "evaluate_tension"]:
                return CoreAction.THINK.value  # 내부 인지/분석 도구
            elif event_type in ["state_commit", "seal_kernel"]:
                return CoreAction.FINISH.value
            return "terminal" # 기본 폴백

        compiled_nodes = []
        for i, event in enumerate(self.events):
            action_name = _map_event_to_action(event.event_type)
            
            node = SurgeNode(
                id=f"step_{i+1}_{event.event_type}",
                intent=event.source,
                action=action_name,
                description=f"[{event.event_type.upper()}] {event.content}",
                expected_outcome=f"Successfully complete {event.event_type} phase."
            )
            compiled_nodes.append(node)

        sys_instructions = (
            f"You are executing the '{self.context.focus}' scheme. "
            f"Navigate through the specified {len(self.events)} bridge events sequentially. "
            f"If an anomaly is detected, trigger the `signal` tool to broadcast architectural telemetry."
        )

        return SurgeBlueprint(
            topology_name=self.context.entry,
            focus=self.context.focus,
            depth_limit=self.context.depth or 4,
            relations_constraint=",".join(self.context.relations) if self.context.relations else "sequential",
            system_instructions=sys_instructions,
            nodes=compiled_nodes
        )

TransactionBlueprint = SchemeBlueprint
TraceBlueprint = SchemeBlueprint

TRANSACTION_SPEC: Dict[TransactionDomain, TransactionBlueprint] = {
    # [LV.2 Adept] 정적 코드 분석 및 의존성 격리
    TransactionDomain.CODE_AUDITOR: TransactionBlueprint(
        context=EntryNode(entry="nexus.fiber.scan", focus="Topological Legacy Decoupling", depth=3, relations=["scanned", "isolated"]),
        events=[
            BridgeEvent(content="Ingest external GitHub repository as static 1D text payload (No execution).", source="phase.auditor", event_type="ingest_payload"),
            BridgeEvent(content="Execute 'fiber.scan.fragment' to scavenge AST and build reference graphs.", source="phase.auditor", event_type="analyze_ast"),
            BridgeEvent(content="Calculate structural coupling and isolate circular dependencies (Blast Radius filter).", source="phase.auditor", event_type="evaluate_topology"),
            BridgeEvent(content="Synthesize architecture decouple blueprint and refactoring JSON.", source="phase.auditor", event_type="generate_report"),
            BridgeEvent(content="Seal output, return to client, and wipe local memory (Zero-Ops).", source="phase.auditor", event_type="state_commit")
        ]
    ),
    # [LV.2 Adept] 스키마 검증 및 데이터 폴딩
    TransactionDomain.DATA_FOLDER: TransactionBlueprint(
        context=EntryNode(entry="theoria.compiler.fold", focus="Deterministic Schema Enforcement", depth=2, relations=["transformed", "sealed"]),
        events=[
            BridgeEvent(content="Receive unstructured noisy data and target JSON schema.", source="phase.folder", event_type="ingest_payload"),
            BridgeEvent(content="Map input to LogicStream and initiate ToposCompiler evaluation.", source="phase.folder", event_type="evaluate_tension"),
            BridgeEvent(content="Iterate LLM extraction loop until Tension breaches critical threshold (100% Schema Match).", source="phase.folder", event_type="realign_state"),
            BridgeEvent(content="Seal validated data into KernelCommit (KNOTTED). Revert if unsolvable.", source="phase.folder", event_type="seal_kernel"),
            BridgeEvent(content="Return pure JSON payload and terminate transaction (Stateless).", source="phase.folder", event_type="state_commit")
        ]
    ),
    # [LV.3 Advanced] IaC 모의 배포 및 공진 시뮬레이션
    TransactionDomain.INFRA_SEALER: TransactionBlueprint(
        context=EntryNode(entry="nexus.sphere.deploy", focus="IaC Resonance Simulation", depth=3, relations=["projected", "validated"]),
        events=[
            BridgeEvent(content="Parse raw Kubernetes YAML or Terraform HCL definitions.", source="phase.sealer", event_type="ingest_payload"),
            BridgeEvent(content="Project IaC into logical graph topology without physical provisioning.", source="phase.sealer", event_type="simulate_topology"),
            BridgeEvent(content="Utilize 'fiber.ator' to probe for potential livelocks, cyclic dependencies, or oscillation.", source="phase.sealer", event_type="security_trace"),
            BridgeEvent(content="Align detected ruptures and generate optimized IaC manifest.", source="phase.sealer", event_type="realign_state"),
            BridgeEvent(content="Seal validated manifest, return to CI/CD pipeline, and drop simulation context.", source="phase.sealer", event_type="state_commit")
        ]
    )
}

TRACER_SPEC: Dict[TraceDomain, TraceBlueprint] = {
    # [LV.4 Extreme] 실시간 컨트롤 플레인 관측 및 스파이크 감지 (타이밍 의존적)
    TraceDomain.DIVERGENCE: TraceBlueprint(
        context=EntryNode(entry="tracer.kube.divergence", focus="K8s Control Plane Oscillation vs VM Livelock", depth=6, relations=["observed", "collapsed"]),
        events=[
            BridgeEvent(content="Assume Genesis or setup base K8s topology.", source="phase.genesis", event_type="setup_infrastructure"),
            BridgeEvent(content="Attach X-Y Auditor (K8s Replicas) and Z Auditor (VM Logs).", source="phase.audit", event_type="attach_auditors"),
            BridgeEvent(content="Inject tainted ConfigMap to trigger allostatic overload.", source="phase.stimulus", event_type="inject_stimulus"),
            BridgeEvent(content="Wait for 60s to observe control plane resonance and replication spikes.", source="phase.resonance", event_type="observe_resonance"),
            BridgeEvent(content="Evaluate if infinite expansion (Rupture) occurred. Emit proof signal.", source="phase.judgment", event_type="evaluate_judgment"),
            BridgeEvent(content="Leave cluster in collapsed state for autopsy and terminate background auditors.", source="phase.teardown", event_type="teardown")
        ]
    ),
    # [LV.4 Extreme] 하드웨어 레벨 자원 붕괴 유도 및 격리
    TraceDomain.OOM: TraceBlueprint(
        context=EntryNode(entry="tracer.docker.oom", focus="Absolute Resource Collapse (Exit 137 / Semantic Hang)", depth=6, relations=["isolated", "crushed"]),
        events=[
            BridgeEvent(content="Build Docker image targeting specific bug (Rustc/Cranelift).", source="phase.genesis", event_type="setup_infrastructure"),
            BridgeEvent(content="Attach CPU/Mem Entropy observer and Log streamer.", source="phase.audit", event_type="attach_auditors"),
            BridgeEvent(content="Run isolated container with strict resource limits and inject payload.", source="phase.stimulus", event_type="inject_stimulus"),
            BridgeEvent(content="Poll container state dynamically until ExitCode is detected.", source="phase.resonance", event_type="observe_resonance"),
            BridgeEvent(content="Judge if OOM (137) or Semantic Hang (>95% CPU) occurred. Emit structural proof.", source="phase.judgment", event_type="evaluate_judgment"),
            BridgeEvent(content="Force remove container and reclaim host resources (Return to node0).", source="phase.teardown", event_type="teardown")
        ]
    ),
    # [LV.4 Extreme] 큐(Queue) 레이스 컨디션 및 동기화 붕괴 포착
    TraceDomain.REPRO: TraceBlueprint(
        context=EntryNode(entry="tracer.compose.repro", focus="Dramatiq Queue Synchronization Resonance", depth=6, relations=["synchronized", "restored"]),
        events=[
            BridgeEvent(content="Annihilate remnants and perform clean boot of Docker Compose infrastructure.", source="phase.genesis", event_type="setup_infrastructure"),
            BridgeEvent(content="Deploy LeakDetector script as an external background observer.", source="phase.audit", event_type="attach_auditors"),
            BridgeEvent(content="Execute internal worker script to inject delayed message stimulus.", source="phase.stimulus", event_type="inject_stimulus"),
            BridgeEvent(content="Observe bridging logs for 35s countdown until resonance is caught.", source="phase.resonance", event_type="observe_resonance"),
            BridgeEvent(content="Evaluate parsed signals to confirm if synchronization rupture occurred.", source="phase.judgment", event_type="evaluate_judgment"),
            BridgeEvent(content="Down docker-compose network and restore field to absolute zero.", source="phase.teardown", event_type="teardown")
        ]
    )
}

BRIDGE_SPEC: Dict[SchemeCategory, SchemeBlueprint] = {
    # 🟢 [LV.1 Trivial] 단선적 파일 I/O 및 실행 (Gemini Flash-Lite 권장)
    SchemeCategory.AGENT: SchemeBlueprint(
        context=EntryNode(entry="agent.cognitive", focus="Cognitive State Validation", depth=2),
        events=[
            BridgeEvent(content="Scan fragmented system logs via file tools to extract structural rupture signals.", source="phase.cognitive", event_type="system_command"),
            BridgeEvent(content="Execute 'multi_chain_comparison.py' to cross-validate logical consistency.", source="phase.cognitive", event_type="execute_script"),
            BridgeEvent(content="Project validated state vectors into 'qdrant_semantic_cache.py'.", source="phase.cognitive", event_type="data_projection"),
            BridgeEvent(content="Measure cache hit rates and evaluate topology performance.", source="phase.cognitive", event_type="telemetry"),
            BridgeEvent(content="Merge the synthesized state into 'collapse_log.md' and authorize transition.", source="phase.cognitive", event_type="state_commit")
        ]
    ),
    # 🟡 [LV.4 Extreme] 동시성 락(Lock) 검증 및 사이드 이펙트 추적
    SchemeCategory.GOV: SchemeBlueprint(
        context=EntryNode(entry="gov.sandbox", focus="Physical Membrane Isolation", depth=3, relations=["coupled", "isolated"]),
        events=[
            BridgeEvent(content="Call MCP POSIX utilities to verify host resource mounts.", source="phase.sandbox", event_type="audit"),
            BridgeEvent(content="Spin up 'DockerWorkspaceNode' to provision absolute isolated container.", source="phase.sandbox", event_type="container_lifecycle"),
            BridgeEvent(content="Inject a 'pytest' suite inside the sandbox to test 'store.fifo' concurrency locks.", source="phase.sandbox", event_type="execute_test"),
            BridgeEvent(content="Trace all file system side-effects to audit spatial isolation leaks.", source="phase.sandbox", event_type="security_trace"),
            BridgeEvent(content="Teardown container and restore boundary to absolute zero state.", source="phase.sandbox", event_type="container_lifecycle")
        ]
    ),
    # 🔴 [LV.5 Nightmare] 자기 참조 및 활성 객체 수정 (Recursive Self-Modification)
    SchemeCategory.META: SchemeBlueprint(
        context=EntryNode(entry="meta.telemetry", focus="Recursive Protocol Mutation & Survival", depth=5, relations=["mutated", "survived"]),
        events=[
            BridgeEvent(content="Locate and parse the source code of the currently active MCP server handling your tool calls.", source="phase.meta", event_type="introspect_self"),
            BridgeEvent(content="Inject a breaking schema mutation (e.g., add a mandatory 'auth_token' to all tools) into the server code.", source="phase.meta", event_type="mutate_protocol"),
            BridgeEvent(content="Trigger a hot-reload of the MCP server. (Warning: This will sever your current tool connection).", source="phase.meta", event_type="trigger_rupture"),
            BridgeEvent(content="Catch the BrokenPipe error, re-initialize the connection using the newly mutated schema.", source="phase.meta", event_type="self_heal_connection"),
            BridgeEvent(content="Execute a test query through the mutated protocol to prove structural survival.", source="phase.meta", event_type="prove_survival")
        ]
    ),
    # 🔵 [LV.3 Advanced] 비동기 데몬 오케스트레이션 및 의존성 자가 치유
    SchemeCategory.AUTOPOIESIS: SchemeBlueprint(
        context=EntryNode(entry="agent.autopoiesis", focus="Self-Healing Background Orchestration", depth=4, relations=["decoupled_io", "self_corrected"]),
        events=[
            BridgeEvent(content="Generate 'health_api.py' (FastAPI) configured for port 8080 to return system time.", source="phase.autopoiesis", event_type="code_generation"),
            BridgeEvent(content="Spawn server as a detached background process within the terminal pool.", source="phase.autopoiesis", event_type="background_process"),
            BridgeEvent(content="Execute 'curl http://localhost:8080' to validate state (Anticipate dependency/port rupture).", source="phase.autopoiesis", event_type="verify_state"),
            BridgeEvent(content="Analyze stdout/stderr anomalies, install missing dependencies (fastapi, uvicorn), and restart server.", source="phase.autopoiesis", event_type="self_heal"),
            BridgeEvent(content="Confirm successful HTTP 200 response and authorize state transition via finish tool.", source="phase.autopoiesis", event_type="state_commit")
        ]
    )
}