# swarm.engine.driver.config.reflect
## @lineage: agent.driver.config.reflect
## @lineage: agent.config.reflect
## @lineage: atoa.agent.config.reflect
## @lineage: atoa.config.reflect
## @lineage: agent.atoa.config.reflect
## @lineage: gov.policy.config.reflector
from __future__ import annotations
import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from swarm.atoa.conv.event import LLMConvertibleEvent
from swarm.atoa.event.llm.system import SystemPromptEvent
from swarm.atoa.schema.reflect import ReflectorBase, ReflectorResult

from swarm.conver.chat.client import ReflectorClient
from swarm.conver.chat.feature import categorize_features

def _format_feature_list(features: list[dict[str, Any]]) -> str:
    ## @desc: Serialize topological features and their emergence probabilities
    if not features:
        return "[CLEAN] Zero anomalies detected"
    
    items = []
    for f in features:
        name = f.get("display_name", f.get("name", "Unknown Node"))
        prob = f.get("probability", 0)
        items.append(f"[{name}: {prob:.0%}]")
    return ", ".join(items)


class Reflector(ReflectorBase, ReflectorClient):
    """@desc: Structural API-Based Evaluator Manifold"""
    
    def evaluate(self, events: Sequence[LLMConvertibleEvent], git_patch: str | None = None) -> ReflectorResult:
        from swarm.conver.conv.view import View
        from swarm.atoa.conv.event import LLMConvertibleEvent
        from swarm.atoa.event.llm.system import SystemPromptEvent

        ## @phase.extraction: Isolate system baseline and tool manifolds
        system_prompt_event: SystemPromptEvent | None = None
        tools = []
        for event in events:
            if isinstance(event, SystemPromptEvent):
                system_prompt_event = event
                tools = event.tools
                break
                
        if system_prompt_event is None:
            raise ValueError("Topological evaluation requires a valid SystemPromptEvent baseline.")
        if not tools:
            raise ValueError("Structural toolsets must be defined within the SystemPromptEvent for API-based reflection.")

        ## @phase.projection: Flatten event sequence into an API-compatible trajectory
        view = View.from_events(events)
        llm_convertible_events = view.events
        messages = LLMConvertibleEvent.events_to_messages(llm_convertible_events)

        formatted_messages = [
            message.to_chat_dict(
                cache_enabled=False,
                vision_enabled=False,
                function_calling_enabled=True,
                force_string_serializer=False,
                send_reasoning_content=False,
            )
            for message in messages
        ]

        tools_for_api = [tool.to_openai_tool() for tool in tools]
        
        ## @phase.evaluation: Classify trace trajectory and extract probability distribution
        response = self.classify_trace(formatted_messages, tools_for_api)
        prob_map = self.extract_prob_map(response)

        explanation = []
        if "success" not in prob_map.probs:
            raise ValueError("API-based reflection failed: 'success' convergence label missing from probability map.")

        score = prob_map.probs["success"]
        explanation.append(f"Convergence: {score:.2f}")
        
        sorted_probs = sorted(prob_map.probs.items(), key=lambda x: x[1], reverse=True)
        explanation.append(json.dumps(dict(sorted_probs)))

        event_ids = [event.id for event in llm_convertible_events]
        categorized = categorize_features(prob_map.probs)
        
        return ReflectorResult(
            score=score,
            message="; ".join(explanation),
            metadata={
                "event_ids": event_ids,
                "categorized_features": categorized,
            },
        )

    def get_followup_prompt(self, reflector_result: ReflectorResult, iteration: int) -> str:
        ## @desc: Synthesize a structural realignment stimulus based on extracted anomalies
        score_percent = reflector_result.score * 100
        lines = [
            f"[SYSTEM] Trajectory Divergence Detected (Cycle: {iteration}, Resonance: {score_percent:.1f}%).",
            "",
        ]

        if reflector_result.metadata and "categorized_features" in reflector_result.metadata:
            categorized = reflector_result.metadata["categorized_features"]
            
            agent_issues = categorized.get("agent_behavioral_issues", [])
            if agent_issues:
                lines.append(f"-> [ANOMALY] Structural Faults: {_format_feature_list(agent_issues)}")

            user_patterns = categorized.get("user_followup_patterns", [])
            if user_patterns:
                lines.append(f"-> [PREDICTION] Trajectory Shifts: {_format_feature_list(user_patterns)}")

            infra_issues = categorized.get("infrastructure_issues", [])
            if infra_issues:
                lines.append(f"-> [SUBSTRATE] Infra Integrity: {_format_feature_list(infra_issues)}")

            other = categorized.get("other", [])
            if other:
                lines.append(f"-> [ENTROPY] Unclassified State: {_format_feature_list(other)}")

            if agent_issues or user_patterns or infra_issues or other:
                lines.append("")

        lines.extend([
            "Current topology fails to satisfy terminal conditions.",
            "Re-evaluate structural dependencies, isolate fractured nodes, and project a corrected trajectory."
        ])

        return "\n".join(lines)