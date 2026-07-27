# topos.bound.resolver.task
## @lineage: topos.gov.resolver.task
from enum import Enum
from typing import Dict, Any, Optional, Union

from swarm.engine.driver.factory.action import CoreAction

from topos.bound.resolver.spec import (
    SchemeCategory, 
    TransactionDomain, 
    TraceDomain, 
    BRIDGE_SPEC, 
    TRANSACTION_SPEC, 
    TRACER_SPEC
)
from topos.bound.resolver.bridge import SchemeBlueprint, TransactionBlueprint, TraceBlueprint

from arch.topos.bound.surge.blueprint import SurgeBlueprint
from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.task")

_RESOLUTION_DICT: Dict[str, Any] = {
    "topology_name": "Semantic Resolution Funnel",
    "focus": "Resolution Hacking & Architectural Signaling",
    "depth_limit": 4,
    "relations_constraint": "sequential",
    
    "system_instructions": """
        You are executing a Resolution Hacking funnel.
        Your objective is to traverse the boundary between low-level structural repair and high-level human consensus.
        1. Investigate anomalies using terminal/file tools.
        2. Apply decoupling and dynamic factory pattern fixes.
        3. CRITICAL: Do not halt after fixing. You MUST invoke the `signal` tool to broadcast Semantic Telemetry.
            Translate your mechanical diffs into architectural value to evangelize the fix to human architects.
        4. Wait for human consensus (merge/accept), then use the `finish` tool to finalize.
    """,
    
    "nodes": [
        {
            "id": "step_1_rupture_analysis",
            "intent": "explore",
            "action": "terminal",
            "description": "[LOW-LEVEL] Scan system logs and stack traces to identify the structural rupture.",
            "expected_outcome": "Identify the root cause (e.g., Duplicate Class Definition in Pydantic registry)."
        },
        {
            "id": "step_2_synthesis_fix",
            "intent": "modify",
            "action": "terminal",
            "description": "[LOW-LEVEL] Use sed or file operations to decouple legacy static imports and apply dynamic factory patterns.",
            "expected_outcome": "Codebase is structurally aligned with the Single Source of Truth."
        },
        {
            "id": "step_3_semantic_telemetry",
            "intent": "evangelize",
            "action": CoreAction.SIGNAL.value,
            "params_template": {
                "channel": "slack_#architecture",
                "audience": "architect",
                "technical_context": "Removed static Action imports and replaced with CoreTool registry evaluation.",
                "semantic_translation": "🚀 *Architecture Update*: Decoupled the core Agent loop from legacy static classes. We now rely 100% on the dynamic Factory registry, enabling infinite tool scaling without Pydantic collisions. Ready for review.",
                "requires_consensus": True
            },
            "description": "[HIGH-LEVEL] Translate the structural fix into a compelling architectural win and request human consensus.",
            "expected_outcome": "Message delivered to human boundary, system paused for user validation."
        },
        {
            "id": "step_4_state_commit",
            "intent": "commit",
            "action": CoreAction.FINISH.value,
            "description": "Upon human consensus, merge the state into 'collapse_log.md' and finalize the trajectory.",
            "expected_outcome": "ConverStatus.FINISHED"
        }
    ]
}

RESOLUTION_BLUEPRINT: SurgeBlueprint = SurgeBlueprint.model_validate(_RESOLUTION_DICT)

class BlueprintType(Enum):
    SCHEME = "scheme"
    TRANSACTION = "transaction"
    TRACER = "tracer"
    RESOLUTION = "resolution"

# -------------------------------------------------------------------------
# Pure Task Resolver
# -------------------------------------------------------------------------

class TaskResolver:
    """
    @desc: 순수 Task(Blueprint) 제공자.
    요청받은 카테고리의 실행 가능한 DAG(SurgeBlueprint)만 조립하여 반환하며, 
    실행(Execution)이나 환경(Environment) 설정에는 전혀 관여하지 않습니다.
    """
    def __init__(self):
        # 정적 스펙 정의들을 메모리에 적재
        self._schemes: Dict[SchemeCategory, SchemeBlueprint] = BRIDGE_SPEC.copy()
        self._transactions: Dict[TransactionDomain, TransactionBlueprint] = TRANSACTION_SPEC.copy()
        self._tracers: Dict[TraceDomain, TraceBlueprint] = TRACER_SPEC.copy()
        self._resolutions: Dict[str, SurgeBlueprint] = {"resolution_hacking": RESOLUTION_BLUEPRINT}
        
        log.debug("[TaskResolver] Universal Blueprints loaded into memory.")

    def resolve(self, category: Union[Enum, str], b_type: BlueprintType) -> Optional[SurgeBlueprint]:
        """대상 Blueprint를 기계가 실행 가능한 형태(SurgeBlueprint DAG)로 컴파일하여 반환합니다."""
        blueprint = None
        
        if b_type == BlueprintType.SCHEME:
            blueprint = self._schemes.get(category)
        elif b_type == BlueprintType.TRANSACTION:
            blueprint = self._transactions.get(category)
        elif b_type == BlueprintType.TRACER:
            blueprint = self._tracers.get(category)
        elif b_type == BlueprintType.RESOLUTION:
            return self._resolutions.get(category)
            
        return blueprint.compile_to_surge() if blueprint else None