# anchor.registry.model.info
## @lineage: bound.surface.model.info
## @lineage: anchor.provider.model.info
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, ClassVar
from pydantic import BaseModel, ConfigDict

from anchor.registry.model.cost import lookup_base_model_info
from anchor.registry.router.config import get_provider_info
from bound.surface.legacy.types import ModelInfo
from bound.surface.client.param.format import BaseLLMModelInfo, type_to_response_format_param
from anchor.registry.model.config.resolver import config

"""@phase.1: Model Info (Cost, Context, Provider Meta)"""
def get_model_info(
    model: str,
    custom_llm_provider: Optional[str] = None,
    api_base: Optional[str] = None,
    api_key: Optional[str] = None,
) -> ModelInfo:
    ## @flow: Retrieve supported OpenAI parameters
    supported_openai_params = config.get_supported_openai_params(model=model, custom_llm_provider=custom_llm_provider)
    
    ## @flow: Fetch base info from registry
    _model_info = lookup_base_model_info(
        model=model,
        custom_llm_provider=custom_llm_provider,
        api_base=api_base,
        api_key=api_key,
    )
    
    ## @flow: Merge provider-specific configurations
    provider_info = get_provider_info(model=model, custom_llm_provider=custom_llm_provider)
    if provider_info:
        for key, value in provider_info.items():
            if value is not None:
                _model_info[key] = value

    return ModelInfo(**_model_info, supported_openai_params=supported_openai_params)

"""@phase.2: Model Prompt Specification (Family, Variant)"""
class ModelPromptSpec(BaseModel):
    """@desc: Defines prompt specification based on LLM family and variant"""
    model_config = ConfigDict(frozen=True)
    family: str | None = None
    variant: str | None = None

    _FAMILY_PATTERNS: ClassVar[dict[str, tuple[str, ...]]] = {
        "openai_gpt": ("gpt-", "o1", "o3", "o4"),
        "anthropic_claude": ("claude",),
        "google_gemini": ("gemini",),
        "meta_llama": ("llama",),
        "mistral": ("mistral",),
        "deepseek": ("deepseek",),
        "alibaba_qwen": ("qwen",),
    }

    _VARIANT_PATTERNS: ClassVar[dict[str, tuple[tuple[str, tuple[str, ...]], ...]]] = {
        "openai_gpt": (
            ("gpt-5-codex", ("gpt-5-codex", "gpt-5.1-codex", "gpt-5.2-codex", "gpt-5.3-codex")),
            ("gpt-5", ("gpt-5", "gpt-5.1", "gpt-5.2", "gpt-5.4")),
        ),
    }

    @classmethod
    def _normalize(cls, name: str | None) -> str:
        return (name or "").strip().lower()

    @classmethod
    def resolve(cls, model_name: str, canonical_name: str | None = None) -> ModelPromptSpec:
        """@desc: Factory method to instantiate spec based on model identifiers"""
        normalized_model = cls._normalize(model_name)
        normalized_canonical = cls._normalize(canonical_name)
        
        family = None
        for fam, patterns in cls._FAMILY_PATTERNS.items():
            if any(p in normalized_model for p in patterns) or \
               (normalized_canonical and any(p in normalized_canonical for p in patterns)):
                family = fam
                break
        
        variant = None
        if family and family in cls._VARIANT_PATTERNS:
            target_name = normalized_canonical or normalized_model
            for var, substrings in cls._VARIANT_PATTERNS[family]:
                if any(sub in target_name for sub in substrings):
                    variant = var
                    break
            
        return cls(family=family, variant=variant)

def get_model_prompt_spec(model_name: str, canonical_name: str | None = None) -> ModelPromptSpec:
    """@desc: Backward compatibility wrapper for resolving model prompt spec."""
    return ModelPromptSpec.resolve(model_name, canonical_name)

"""@phase.3: Model Features (Capabilities, Flags)"""
@dataclass(frozen=True)
class ModelFeatures:
    """@desc: Defines specific capability flags for a given LLM"""
    supports_reasoning_effort: bool
    supports_extended_thinking: bool
    supports_prompt_cache: bool
    supports_stop_words: bool
    supports_responses_api: bool
    force_string_serializer: bool
    send_reasoning_content: bool
    supports_prompt_cache_retention: bool

    _EXTENDED_THINKING: ClassVar[list[str]] = [
        "claude-sonnet-4-5", "claude-sonnet-4-6", "claude-haiku-4-5",
    ]

    _PROMPT_CACHE: ClassVar[list[str]] = [
        "claude-3-7-sonnet", "claude-sonnet-3-7-latest", "claude-3-5-sonnet",
        "claude-3-5-haiku", "claude-3-haiku-20240307", "claude-3-opus-20240229",
        "claude-sonnet-4", "claude-opus-4", "claude-haiku-4-5", "claude-sonnet-4-5",
        "claude-sonnet-4-6", "claude-opus-4-5", "claude-opus-4-6",
    ]

    _PROMPT_CACHE_RETENTION: ClassVar[list[str]] = [
        "gpt-5", "gpt-4.1", "!mini", "gpt-5.1-codex-mini", "!azure/",
    ]

    _NO_STOP_WORDS: ClassVar[list[str]] = [
        "o1", "o3", "grok-4-0709", "grok-code-fast-1", "deepseek-r1-0528",
    ]

    _RESPONSES_API: ClassVar[list[str]] = [
        "gpt-5", "codex-mini-latest",
    ]

    _FORCE_STRING_SERIALIZER: ClassVar[list[str]] = [
        "deepseek", "glm", "groq/kimi-k2-instruct", "openrouter/minimax",
    ]

    _SEND_REASONING_CONTENT: ClassVar[list[str]] = [
        "kimi-k2-thinking", "kimi-k2.5", "openrouter/minimax-m2", "deepseek/deepseek-reasoner",
    ]

    @classmethod
    def _matches(cls, model: str, patterns: list[str]) -> bool:
        raw = (model or "").strip().lower()
        return any(pat.strip().lower() in raw for pat in patterns)

    @classmethod
    def _apply_rules(cls, model: str, rules: list[str]) -> bool:
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

    @classmethod
    def resolve(cls, model: str) -> ModelFeatures:
        """@desc: Resolves and returns all feature flags for a given model"""
        return cls(
            supports_reasoning_effort=True,
            supports_extended_thinking=cls._matches(model, cls._EXTENDED_THINKING),
            supports_prompt_cache=cls._matches(model, cls._PROMPT_CACHE),
            supports_stop_words=not cls._matches(model, cls._NO_STOP_WORDS),
            supports_responses_api=cls._matches(model, cls._RESPONSES_API),
            force_string_serializer=cls._matches(model, cls._FORCE_STRING_SERIALIZER),
            send_reasoning_content=cls._matches(model, cls._SEND_REASONING_CONTENT),
            supports_prompt_cache_retention=cls._apply_rules(model, cls._PROMPT_CACHE_RETENTION),
        )

def get_features(model: str) -> ModelFeatures:
    """@desc: Backward compatibility wrapper for resolving model features."""
    return ModelFeatures.resolve(model)