# anchor.provider.model.feature
## @lineage: anchor.model.feature
from dataclasses import dataclass
from functools import cache

def model_matches(model: str, patterns: list[str]) -> bool:
    raw = (model or "").strip().lower()
    for pat in patterns:
        token = pat.strip().lower()
        if token in raw:
            return True
    return False


def apply_ordered_model_rules(model: str, rules: list[str]) -> bool:
    raw = (model or "").strip().lower()
    decided: bool | None = None
    for rule in rules:
        token = rule.strip().lower()
        if not token:
            continue
        is_exclude = token.startswith("!")
        core = token[1:] if is_exclude else token
        if core and core in raw:
            decided = not is_exclude
    return bool(decided)


@dataclass(frozen=True)
class ModelFeatures:
    supports_reasoning_effort: bool
    supports_extended_thinking: bool
    supports_prompt_cache: bool
    supports_stop_words: bool
    supports_responses_api: bool
    force_string_serializer: bool
    send_reasoning_content: bool
    supports_prompt_cache_retention: bool

EXTENDED_THINKING_MODELS: list[str] = [
    "claude-sonnet-4-5",
    "claude-sonnet-4-6",
    "claude-haiku-4-5",
]

PROMPT_CACHE_MODELS: list[str] = [
    "claude-3-7-sonnet",
    "claude-sonnet-3-7-latest",
    "claude-3-5-sonnet",
    "claude-3-5-haiku",
    "claude-3-haiku-20240307",
    "claude-3-opus-20240229",
    "claude-sonnet-4",
    "claude-opus-4",
    # Anthropic Haiku 4.5 variants (dash only; official IDs use hyphens)
    "claude-haiku-4-5",
    "claude-sonnet-4-5",
    "claude-sonnet-4-6",
    "claude-opus-4-5",
    "claude-opus-4-6",
    "claude-sonnet-4-6",
]

PROMPT_CACHE_RETENTION_MODELS: list[str] = [
    "gpt-5",
    "gpt-4.1",
    "!mini",
    "gpt-5.1-codex-mini",
    "!azure/",
]

SUPPORTS_STOP_WORDS_FALSE_MODELS: list[str] = [
    "o1",
    "o3",
    "grok-4-0709",
    "grok-code-fast-1",
    "deepseek-r1-0528",
]

RESPONSES_API_MODELS: list[str] = [
    "gpt-5",
    "codex-mini-latest",
]

FORCE_STRING_SERIALIZER_MODELS: list[str] = [
    "deepseek",  # e.g., DeepSeek-V3.2-Exp
    "glm",  # e.g., GLM-4.5 / GLM-4.6
    "groq/kimi-k2-instruct",  # explicit provider-prefixed IDs
    "openrouter/minimax",
]

# Models that we should send full reasoning content
# in the message input
SEND_REASONING_CONTENT_MODELS: list[str] = [
    "kimi-k2-thinking",
    "kimi-k2.5",
    "openrouter/minimax-m2",  # MiniMax-M2 via OpenRouter (interleaved thinking)
    "deepseek/deepseek-reasoner",
]

def get_features(model: str) -> ModelFeatures:
    return ModelFeatures(
        supports_reasoning_effort=True,
        supports_extended_thinking=model_matches(model, EXTENDED_THINKING_MODELS),
        supports_prompt_cache=model_matches(model, PROMPT_CACHE_MODELS),
        supports_stop_words=not model_matches(model, SUPPORTS_STOP_WORDS_FALSE_MODELS),
        supports_responses_api=model_matches(model, RESPONSES_API_MODELS),
        force_string_serializer=model_matches(model, FORCE_STRING_SERIALIZER_MODELS),
        send_reasoning_content=model_matches(model, SEND_REASONING_CONTENT_MODELS),
        supports_prompt_cache_retention=apply_ordered_model_rules(model, PROMPT_CACHE_RETENTION_MODELS),
    )
