# agent.anchor.model.info
## @lineage: bound.xor.model.info
## @lineage: eco.model.info
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, ClassVar, Literal, List, Dict
from pydantic import BaseModel, ConfigDict

from agent.anchor.provider.registry import model_cost, lookup_base_model_info, get_llm_provider
from agent.anchor.model.types.general import ModelInfo
from agent.loop.runtime.exception.eco import BadRequestError

from arch.model.config import config
from watcher.plane.emitter import get_emitter

log = get_emitter("model.info")

class ModelSupport:
    """@desc: Centralized resolver for model capabilities, API parameters, and provider support."""
    _lowercase_map: ClassVar[Dict[str, str] | None] = None

    @classmethod
    def _get_cost_key(cls, potential_key: str) -> Optional[str]:
        if potential_key in model_cost:
            return potential_key

        if cls._lowercase_map is None:
            cls._lowercase_map = {k.lower(): k for k in model_cost}

        potential_key_lower = potential_key.lower()
        matched_key = cls._lowercase_map.get(potential_key_lower)
        
        if matched_key and matched_key in model_cost:
            return matched_key

        cls._lowercase_map = {k.lower(): k for k in model_cost}
        matched_key = cls._lowercase_map.get(potential_key_lower)
        if matched_key and matched_key in model_cost:
            return matched_key

        return None

    @classmethod
    def check_capability(
        cls, model: str, custom_llm_provider: Optional[str], key: str, default_if_none: bool = False
    ) -> bool:
        try:
            resolved_model, resolved_provider, _, _ = get_llm_provider(model=model, custom_llm_provider=custom_llm_provider)
            model_info = lookup_base_model_info(model=resolved_model, custom_llm_provider=resolved_provider)
            if model_info.get(key) is not None:
                return bool(model_info.get(key))

            bare_model_key = cls._get_cost_key(resolved_model)
            if bare_model_key:
                bare_entry = model_cost.get(bare_model_key) or {}
                if bare_entry.get(key) is not None:
                    return bool(bare_entry.get(key))

            return default_if_none

        except Exception as e:
            log.debug(f"Capability check failed for {key}. model={model}, provider={custom_llm_provider}. Error: {e}")
            return default_if_none

    @classmethod
    def get_supported_openai_params(
        cls,
        model: str,
        custom_llm_provider: Optional[str] = None,
        request_type: Literal["chat_completion", "embeddings", "transcription"] = "chat_completion",
        base_model: Optional[str] = None,
    ) -> Optional[list]:
        if not custom_llm_provider:
            try:
                custom_llm_provider = get_llm_provider(model=model)[1]
            except BadRequestError:
                return None

        if custom_llm_provider == "openai" and request_type == "transcription":
            if "gpt-4o" in model:
                return config.OpenAIGPTAudioTranscriptionConfig().get_supported_openai_params(model=model)
            return config.OpenAIWhisperAudioTranscriptionConfig().get_supported_openai_params(model=model)

        config_mapping = {
            "anthropic": "AnthropicConfig",
            "huggingface": "HuggingFaceChatConfig",
            "gemini": "GoogleAIStudioGeminiConfig",
            "ollama": "OllamaConfig",
            "openai": "OpenAIConfig",
            "vertex_ai": "VertexAIConfig",
            "bedrock": "AmazonBedrockGlobalConfig",
            "azure": "AzureOpenAIConfig",
        }
        
        provider_key = custom_llm_provider.split("/")[0] if "/" in custom_llm_provider else custom_llm_provider
        config_class_name = config_mapping.get(provider_key, "OpenAILikeChatConfig")
        
        if hasattr(config, config_class_name):
            config_instance = getattr(config, config_class_name)()
            if hasattr(config_instance, "get_supported_openai_params"):
                supported_params = config_instance.get_supported_openai_params(model=model)
                if base_model and base_model != model:
                    base_params = config_instance.get_supported_openai_params(model=base_model)
                    supported_params = list(dict.fromkeys([*(supported_params or []), *(base_params or [])]))
                return supported_params
        return None

    @classmethod
    def get_supported_regions(cls, model: str, custom_llm_provider: Optional[str] = None) -> Optional[List[str]]:
        try:
            resolved_model, resolved_provider, _, _ = get_llm_provider(model=model, custom_llm_provider=custom_llm_provider)
            model_info = lookup_base_model_info(model=resolved_model, custom_llm_provider=resolved_provider)
            model_key = model_info.get("key")
            if model_key:
                model_cost_data = model_cost.get(model_key, {})
                supported_regions = model_cost_data.get("supported_regions")
                if isinstance(supported_regions, list):
                    return supported_regions
            return None
        except Exception as e:
            log.debug(f"Failed to check supported_regions. model={model}. Error: {e}")
            return None


# --- Model Support Helper Wrappers ---

def get_supported_openai_params(
    model: str, custom_llm_provider: Optional[str] = None, request_type: str = "chat_completion", base_model: Optional[str] = None
) -> Optional[list]:
    return ModelSupport.get_supported_openai_params(model, custom_llm_provider, request_type, base_model)

def get_supported_regions(model: str, custom_llm_provider: Optional[str] = None) -> Optional[List[str]]:
    return ModelSupport.get_supported_regions(model, custom_llm_provider)

def supports_httpx_timeout(custom_llm_provider: str) -> bool:
    return custom_llm_provider in ["openai"]

def supports_system_messages(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_system_messages")

def supports_web_search(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_web_search")

def supports_url_context(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_url_context")

def supports_native_streaming(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_native_streaming", default_if_none=True)

def supports_response_schema(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_response_schema")

def supports_parallel_function_calling(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_parallel_function_calling")

def supports_function_calling(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_function_calling")

def supports_tool_choice(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_tool_choice")

def supports_audio_input(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_audio_input")

def supports_pdf_input(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_pdf_input")

def supports_audio_output(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_audio_output")

def supports_prompt_caching(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_prompt_caching")

def supports_computer_use(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_computer_use")

def supports_vision(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_vision")

def supports_reasoning(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_reasoning")

def supports_native_structured_output(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_native_structured_output")

def supports_embedding_image_input(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    return ModelSupport.check_capability(model, custom_llm_provider, "supports_embedding_image_input")


# =========================================================================
# Phase 2: Model Info (Cost, Context, Provider Meta)
# =========================================================================
def get_model_info(
    model: str, custom_llm_provider: Optional[str] = None, api_base: Optional[str] = None, api_key: Optional[str] = None,
) -> ModelInfo:
    supported_openai_params = ModelSupport.get_supported_openai_params(model=model, custom_llm_provider=custom_llm_provider)
    _model_info = lookup_base_model_info(model=model, custom_llm_provider=custom_llm_provider, api_base=api_base, api_key=api_key)
    return ModelInfo(**_model_info, supported_openai_params=supported_openai_params)


# =========================================================================
# Phase 3: Model Prompt Specification & Features
# =========================================================================
class ModelPromptSpec(BaseModel):
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
        normalized_model = cls._normalize(model_name)
        normalized_canonical = cls._normalize(canonical_name)
        
        family = None
        for fam, patterns in cls._FAMILY_PATTERNS.items():
            if any(p in normalized_model for p in patterns) or (normalized_canonical and any(p in normalized_canonical for p in patterns)):
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
    return ModelPromptSpec.resolve(model_name, canonical_name)


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

    _EXTENDED_THINKING: ClassVar[list[str]] = ["claude-sonnet-4-5", "claude-sonnet-4-6", "claude-haiku-4-5"]
    _PROMPT_CACHE: ClassVar[list[str]] = ["claude-3-7-sonnet", "claude-sonnet-3-7-latest", "claude-3-5-sonnet", "claude-3-5-haiku", "claude-3-haiku-20240307", "claude-3-opus-20240229", "claude-sonnet-4", "claude-opus-4", "claude-haiku-4-5", "claude-sonnet-4-5", "claude-sonnet-4-6", "claude-opus-4-5", "claude-opus-4-6"]
    _PROMPT_CACHE_RETENTION: ClassVar[list[str]] = ["gpt-5", "gpt-4.1", "!mini", "gpt-5.1-codex-mini", "!azure/"]
    _NO_STOP_WORDS: ClassVar[list[str]] = ["o1", "o3", "grok-4-0709", "grok-code-fast-1", "deepseek-r1-0528"]
    _RESPONSES_API: ClassVar[list[str]] = ["gpt-5", "codex-mini-latest"]
    _FORCE_STRING_SERIALIZER: ClassVar[list[str]] = ["deepseek", "glm", "groq/kimi-k2-instruct", "openrouter/minimax"]
    _SEND_REASONING_CONTENT: ClassVar[list[str]] = ["kimi-k2-thinking", "kimi-k2.5", "openrouter/minimax-m2", "deepseek/deepseek-reasoner"]

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
            if not token: continue
            is_exclude = token.startswith("!")
            core = token[1:] if is_exclude else token
            if core and core in raw: decided = not is_exclude
        return bool(decided)

    @classmethod
    def resolve(cls, model: str) -> ModelFeatures:
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
    return ModelFeatures.resolve(model)