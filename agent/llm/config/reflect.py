# agent.llm.config.reflect
from __future__ import annotations

import copy
import hashlib
import json
import math
from collections.abc import Sequence
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.error import URLError
from urllib.request import Request, urlopen

import httpx
import jinja2
from jinja2.ext import loopcontrols
from jinja2.sandbox import ImmutableSandboxedEnvironment
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    SecretStr,
    field_validator,
)
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from runtime.client.param import ChatCompletionToolParam
from agent.atoa.conv.event import LLMConvertibleEvent
from agent.atoa.event.llm.system import SystemPromptEvent
from agent.atoa.schema.reflect import ReflectorBase, ReflectorResult

CACHE_DIR = Path.home() / ".cache" / "chat_templates"

def _get_cache_path(tokenizer_name: str) -> Path:
    safe_name = hashlib.md5(tokenizer_name.encode()).hexdigest()
    return CACHE_DIR / f"{safe_name}_tokenizer_config.json"

def _fetch_tokenizer_config(tokenizer_name: str, use_cache: bool = True) -> dict[str, Any]:
    cache_path = _get_cache_path(tokenizer_name)
    if use_cache and cache_path.exists():
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)

    url = f"https://huggingface.co/{tokenizer_name}/raw/main/tokenizer_config.json"
    try:
        request = Request(url, headers={"User-Agent": "chat_template/1.0"})
        with urlopen(request, timeout=30) as response:
            config = json.loads(response.read().decode("utf-8"))
    except URLError as e:
        raise RuntimeError(f"Failed to fetch tokenizer config from {url}: {e}")

    if use_cache:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(config, f)

    return config

@lru_cache(maxsize=16)
def _compile_jinja_template(chat_template: str) -> jinja2.Template:
    def raise_exception(message: str) -> None:
        raise jinja2.exceptions.TemplateError(message)

    def tojson(
        x: Any,
        ensure_ascii: bool = False,
        indent: int | None = None,
        separators: tuple[str, str] | None = None,
        sort_keys: bool = False,
    ) -> str:
        return json.dumps(
            x, ensure_ascii=ensure_ascii, indent=indent, separators=separators, sort_keys=sort_keys
        )

    jinja_env = ImmutableSandboxedEnvironment(
        trim_blocks=True, lstrip_blocks=True, extensions=[loopcontrols],
    )
    jinja_env.filters["tojson"] = tojson
    jinja_env.globals["raise_exception"] = raise_exception
    return jinja_env.from_string(chat_template)

class ChatTemplateRenderer:
    def __init__(
        self,
        tokenizer_name: str | None = None,
        chat_template: str | None = None,
        use_cache: bool = True,
    ):
        if chat_template is not None:
            self._chat_template = chat_template
        elif tokenizer_name is not None:
            config = _fetch_tokenizer_config(tokenizer_name, use_cache=use_cache)
            self._chat_template = config.get("chat_template")
            if self._chat_template is None:
                raise ValueError(f"No chat_template found in tokenizer config for {tokenizer_name}")
        else:
            raise ValueError("Either tokenizer_name or chat_template must be provided")

        self._compiled_template = _compile_jinja_template(self._chat_template)

    @property
    def chat_template(self) -> str:
        assert self._chat_template is not None
        return self._chat_template

    def apply_chat_template(
        self,
        messages: Sequence[dict[str, Any]],
        tools: Sequence[dict[str, Any]] | None = None,
        add_generation_prompt: bool = False,
        **kwargs: Any,
    ) -> str:
        return self._compiled_template.render(
            messages=messages, tools=tools, add_generation_prompt=add_generation_prompt, **kwargs,
        )

def apply_chat_template(
    messages: Sequence[dict[str, Any]],
    tokenizer_name: str | None = None,
    chat_template: str | None = None,
    tools: Sequence[dict[str, Any]] | None = None,
    add_generation_prompt: bool = False,
    use_cache: bool = True,
    **kwargs: Any,
) -> str:
    renderer = ChatTemplateRenderer(
        tokenizer_name=tokenizer_name, chat_template=chat_template, use_cache=use_cache,
    )
    return renderer.apply_chat_template(
        messages=messages, tools=tools, add_generation_prompt=add_generation_prompt, **kwargs,
    )

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

    raw_sentiment_probs = {}
    for feature_name, prob in probs_dict.items():
        if feature_name.startswith("sentiment_"):
            short_name = feature_name.replace("sentiment_", "")
            raw_sentiment_probs[short_name] = prob

    if raw_sentiment_probs:
        sentiment_probs = _softmax_normalize(raw_sentiment_probs)
        max_sentiment = max(sentiment_probs.items(), key=lambda x: x[1])
        result["sentiment"] = {
            "predicted": max_sentiment[0].capitalize(),
            "probability": max_sentiment[1],
            "all": sentiment_probs,
        }

    for feature_name, prob in probs_dict.items():
        if feature_name.startswith("sentiment_") or feature_name == "success" or prob < display_threshold:
            continue

        category = FEATURE_CATEGORIES.get(feature_name)
        feature_entry = {
            "name": feature_name,
            "display_name": feature_name.replace("_", " ").title(),
            "probability": prob,
        }

        if category == "general_context":
            continue
        elif category == "agent_behavioral_issues":
            result["agent_behavioral_issues"].append(feature_entry)
        elif category == "user_followup_patterns":
            result["user_followup_patterns"].append(feature_entry)
        elif category == "infrastructure_issues":
            result["infrastructure_issues"].append(feature_entry)
        else:
            result["other"].append(feature_entry)

    for key in ["agent_behavioral_issues", "user_followup_patterns", "infrastructure_issues", "other"]:
        result[key] = sorted(result[key], key=lambda x: x["probability"], reverse=True)

    return result

class UsageTokens(BaseModel):
    prompt_tokens: int | None = None
    total_tokens: int | None = None
    completion_tokens: int | None = None
    prompt_tokens_details: dict | None = None
    model_config = ConfigDict(extra="allow")

class ClassificationItem(BaseModel):
    index: int | None = None
    label: str | None = None
    probs: list[float]
    num_classes: int | None = None
    model_config = ConfigDict(extra="allow")

class ClassificationResponse(BaseModel):
    id: str | None = None
    object: str | None = None
    created: int | None = None
    model: str | None = None
    data: list[ClassificationItem] = Field(default_factory=list)
    usage: UsageTokens | None = None
    model_config = ConfigDict(extra="allow")

class LabelProbMap(BaseModel):
    probs: dict[str, float]
    order: list[str] | None = None
    model_config = ConfigDict(extra="forbid")

class ReflectorClient(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")
    server_url: str = Field(
        default="https://all-hands-ai--critic-qwen3-4b-serve.modal.run",
        description="Base URL of the vLLM classification service",
    )
    api_key: str | SecretStr = Field(..., description="API key for authenticating with the vLLM service")
    model_name: str = Field(default="critic-qwen3-4b", description="Name of the model to use")
    tokenizer_name: str = Field(
        default="Qwen/Qwen3-4B-Instruct-2507",
        description="HuggingFace tokenizer name for loading chat template",
    )
    pass_tools_definitions: bool = Field(default=True, description="Whether to pass tool definitions to the model")
    timeout_seconds: float = Field(default=300.0, description="Timeout for requests to the model")
    has_success_label: bool = Field(default=True, description="Whether the model predicts success label at index 0")

    _client: httpx.Client = PrivateAttr(default_factory=httpx.Client)
    _template_renderer: ChatTemplateRenderer | None = PrivateAttr(default=None)
    sentiment_labels: tuple[str, ...] = ("sentiment_positive", "sentiment_neutral", "sentiment_negative")
    agent_issue_labels: tuple[str, ...] = (
        "misunderstood_intention", "did_not_follow_instruction", "insufficient_analysis",
        "insufficient_clarification", "improper_tool_use_or_setup", "loop_behavior",
        "insufficient_testing", "insufficient_debugging", "incomplete_implementation",
        "file_management_errors", "scope_creep", "risky_actions_or_permission", "other_agent_issue",
    )
    infra_labels: tuple[str, ...] = ("infrastructure_external_issue", "infrastructure_agent_caused_issue")
    user_followup_labels: tuple[str, ...] = (
        "clarification_or_restatement", "correction", "direction_change", "vcs_update_requests",
        "progress_or_scope_concern", "frustration_or_complaint", "removal_or_reversion_request", "other_user_issue",
    )
    sentiment_map: dict[str, str] = {
        "Positive": "sentiment_positive", "Neutral": "sentiment_neutral", "Negative": "sentiment_negative",
    }

    @field_validator("api_key", mode="before")
    @classmethod
    def _validate_and_convert_api_key(cls, v: str | SecretStr) -> SecretStr:
        secret_value = v.get_secret_value() if isinstance(v, SecretStr) else v
        if not secret_value or not secret_value.strip():
            raise ValueError("api_key must be non-empty")
        return SecretStr(secret_value) if isinstance(v, str) else v

    @property
    def all_labels(self) -> tuple[str, ...]:
        base_labels = self.sentiment_labels + self.agent_issue_labels + self.infra_labels + self.user_followup_labels
        if self.has_success_label:
            return ("success",) + base_labels
        return base_labels

    def _get_template_renderer(self) -> ChatTemplateRenderer:
        if self._template_renderer is None:
            self._template_renderer = ChatTemplateRenderer(tokenizer_name=self.tokenizer_name)
        return self._template_renderer

    @staticmethod
    def normalize_messages(messages: Sequence[dict]) -> Sequence[dict]:
        out: list[dict] = []
        for msg in messages or []:
            content = msg.get("content", "") or ""
            if isinstance(content, list):
                text_parts = [
                    block.get("text", "") for block in content
                    if isinstance(block, dict) and block.get("type") == "text"
                ]
                content = "\n".join(text_parts)
            if not isinstance(content, str):
                content = str(content)
            out.append({"role": msg.get("role", ""), "content": content})
        return out

    def apply_chat_template(
        self, messages: Sequence[dict], tools: Sequence[ChatCompletionToolParam] | None = None,
    ) -> str:
        renderer = self._get_template_renderer()
        msgs = self.normalize_messages(copy.deepcopy(messages))
        tools_dicts: Sequence[dict[str, Any]] | None = (
            cast(Sequence[dict[str, Any]], tools) if tools is not None else None
        )
        if self.pass_tools_definitions and tools_dicts:
            return renderer.apply_chat_template(msgs, tools=tools_dicts, add_generation_prompt=False)
        return renderer.apply_chat_template(msgs, add_generation_prompt=False)

    def classify_trace(
        self, messages: Sequence[dict], tools: Sequence[ChatCompletionToolParam] | None = None,
    ) -> ClassificationResponse:
        formatted = self.apply_chat_template(messages, tools)

        def should_retry(exc: BaseException) -> bool:
            if isinstance(exc, httpx.HTTPStatusError):
                return exc.response.status_code == 500
            return False

        @retry(
            retry=retry_if_exception(should_retry),
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            reraise=True,
        )
        def _post_with_retry():
            api_key_value = self.api_key.get_secret_value() if isinstance(self.api_key, SecretStr) else self.api_key
            resp = self._client.post(
                f"{self.server_url}/classify",
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key_value}"},
                json={"model": self.model_name, "input": formatted},
                timeout=self.timeout_seconds,
            )
            resp.raise_for_status()
            return resp

        resp = _post_with_retry()
        return ClassificationResponse.model_validate(resp.json())

    def extract_prob_map(self, response: ClassificationResponse) -> LabelProbMap:
        if not response.data:
            raise ValueError("empty response.data from server")
        item = response.data[0]
        if not item.probs:
            raise ValueError("server returned empty 'probs'")
        if item.num_classes is not None and item.num_classes != len(item.probs):
            raise ValueError(f"num_classes ({item.num_classes}) does not match len(probs) ({len(item.probs)})")

        probs = [float(x) for x in item.probs]
        if len(probs) != len(self.all_labels):
            raise ValueError(
                f"len(probs) ({len(probs)}) != len(all_labels) ({len(self.all_labels)}). "
                "Ensure server label space matches client label space."
            )

        mapping = {lbl: probs[i] for i, lbl in enumerate(self.all_labels)}
        return LabelProbMap(probs=mapping, order=list(self.all_labels))

    def predict_labels(self, probs: list[float], threshold: float = 0.5) -> list[int]:
        return [1 if p > threshold else 0 for p in probs]

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
        from agent.atoa.context.view import View
        from agent.atoa.conv.event import LLMConvertibleEvent
        from agent.atoa.event.llm.system import SystemPromptEvent

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