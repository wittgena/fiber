# topos.bound.resolver.bridge
## @lineage: topos.gov.resolver.bridge
## @lineage: void.topos.bound.resolver.bridge
import logging
import sys
from typing import List, Dict, Any, Optional, Annotated
from dataclasses import dataclass, asdict
from pydantic import BaseModel, Field

from arch.contract.schema.graph import EntryNode
from arch.contract.schema.resonance import BridgeEvent
from arch.bound.surge.blueprint import SurgeBlueprint, SurgeNode
from swarm.engine.driver.factory.action import CoreAction

log = logging.getLogger(__name__)

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