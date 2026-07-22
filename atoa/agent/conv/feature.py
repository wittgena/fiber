# atoa.agent.conv.feature
## @lineage: atoa.context.conv.feature
## @lineage: agent.context.conv.feature
## @lineage: gov.frame.reflect.conv.feature
## @lineage: agent.loop.reflect.conv.feature
## @lineage: meta.flow.reflect.conv.feature
## @lineage: meta.reflect.conv.feature
## @lineage: gov.conv.reflect.feature
## @lineage: meta.ops.agent.reflect.feature
## @lineage: xphi.agent.reflect.feature
import math
from typing import Any

FEATURE_CATEGORIES: dict[str, str] = {
    "user_goal_summary": "general_context",
    "overall_sentiment": "general_context",
    "misunderstood_intention": "agent_behavioral_issues",
    "did_not_follow_instruction": "agent_behavioral_issues",
    "insufficient_analysis": "agent_behavioral_issues",
    "insufficient_clarification": "agent_behavioral_issues",
    "improper_tool_use_or_setup": "agent_behavioral_issues",
    "loop_behavior": "agent_behavioral_issues",
    "insufficient_testing": "agent_behavioral_issues",
    "insufficient_debugging": "agent_behavioral_issues",
    "incomplete_implementation": "agent_behavioral_issues",
    "file_management_errors": "agent_behavioral_issues",
    "scope_creep": "agent_behavioral_issues",
    "risky_actions_or_permission": "agent_behavioral_issues",
    "other_agent_issue": "agent_behavioral_issues",
    "follow_up_timing": "user_followup_patterns",
    "clarification_or_restatement": "user_followup_patterns",
    "correction": "user_followup_patterns",
    "direction_change": "user_followup_patterns",
    "vcs_update_requests": "user_followup_patterns",
    "progress_or_scope_concern": "user_followup_patterns",
    "frustration_or_complaint": "user_followup_patterns",
    "removal_or_reversion_request": "user_followup_patterns",
    "other_user_issue": "user_followup_patterns",
    "infrastructure_external_issue": "infrastructure_issues",
    "infrastructure_agent_caused_issue": "infrastructure_issues",
}

# Category display names for visualization
CATEGORY_DISPLAY_NAMES: dict[str, str] = {
    "general_context": "General Context",
    "agent_behavioral_issues": "Detected Agent Behavioral Issues",
    "user_followup_patterns": "Predicted User Follow-Up Patterns",
    "infrastructure_issues": "Detected Infrastructure Issues",
}

def get_category(feature_name: str) -> str | None:
    return FEATURE_CATEGORIES.get(feature_name)

def _softmax_normalize(probs: dict[str, float]) -> dict[str, float]:
    if not probs:
        return {}

    values = list(probs.values())
    exp_values = [math.exp(v) for v in values]
    exp_sum = sum(exp_values)
    normalized = [exp_v / exp_sum for exp_v in exp_values]

    return dict(zip(probs.keys(), normalized))

def categorize_features(probs_dict: dict[str, float], display_threshold: float = 0.2) -> dict[str, Any]:
    result: dict[str, Any] = {
        "sentiment": None,
        "agent_behavioral_issues": [],
        "user_followup_patterns": [],
        "infrastructure_issues": [],
        "other": [],
    }

    # Extract sentiment features and apply softmax normalization
    raw_sentiment_probs = {}
    for feature_name, prob in probs_dict.items():
        if feature_name.startswith("sentiment_"):
            short_name = feature_name.replace("sentiment_", "")
            raw_sentiment_probs[short_name] = prob

    if raw_sentiment_probs:
        # Apply softmax normalization to convert logits to probabilities
        sentiment_probs = _softmax_normalize(raw_sentiment_probs)
        max_sentiment = max(sentiment_probs.items(), key=lambda x: x[1])
        result["sentiment"] = {
            "predicted": max_sentiment[0].capitalize(),
            "probability": max_sentiment[1],
            "all": sentiment_probs,
        }

    # Categorize other features
    for feature_name, prob in probs_dict.items():
        # Skip sentiment features (already processed)
        if feature_name.startswith("sentiment_"):
            continue

        # Skip 'success' as it's redundant with the score
        if feature_name == "success":
            continue

        # Skip features below threshold
        if prob < display_threshold:
            continue

        category = FEATURE_CATEGORIES.get(feature_name)
        feature_entry = {
            "name": feature_name,
            "display_name": feature_name.replace("_", " ").title(),
            "probability": prob,
        }

        if category == "general_context":
            # Skip general context features for now
            continue
        elif category == "agent_behavioral_issues":
            result["agent_behavioral_issues"].append(feature_entry)
        elif category == "user_followup_patterns":
            result["user_followup_patterns"].append(feature_entry)
        elif category == "infrastructure_issues":
            result["infrastructure_issues"].append(feature_entry)
        else:
            result["other"].append(feature_entry)

    # Sort each category by probability (descending)
    for key in [
        "agent_behavioral_issues",
        "user_followup_patterns",
        "infrastructure_issues",
        "other",
    ]:
        result[key] = sorted(result[key], key=lambda x: x["probability"], reverse=True)

    return result
