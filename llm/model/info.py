# fiber.llm.model.info
from __future__ import annotations

from typing import Optional, Dict
from fiber.llm.model.provider.registry import model_cost, lookup_base_model_info
from fiber.llm.model.provider.resolver import get_llm_provider
from fiber.llm.exception.eco import BadRequestError
from fiber.llm.model.config import config
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("model.info")

# 캐싱용 전역 변수
_LOWERCASE_MODEL_MAP: Dict[str, str] | None = None


def _get_cost_key(potential_key: str) -> Optional[str]:
    """registry(model_cost)에서 대소문자 구분 없이 모델 키를 찾습니다."""
    global _LOWERCASE_MODEL_MAP
    if potential_key in model_cost:
        return potential_key

    if _LOWERCASE_MODEL_MAP is None:
        _LOWERCASE_MODEL_MAP = {k.lower(): k for k in model_cost}

    potential_key_lower = potential_key.lower()
    if potential_key_lower in _LOWERCASE_MODEL_MAP:
        matched_key = _LOWERCASE_MODEL_MAP[potential_key_lower]
        if matched_key in model_cost:
            return matched_key

    # 레지스트리가 런타임에 업데이트되었을 경우를 대비한 재캐싱
    _LOWERCASE_MODEL_MAP = {k.lower(): k for k in model_cost}
    matched_key = _LOWERCASE_MODEL_MAP.get(potential_key_lower)
    if matched_key and matched_key in model_cost:
        return matched_key

    return None


def _check_capability(model: str, custom_llm_provider: Optional[str], key: str, default_if_none: bool = False) -> bool:
    """Registry를 조회하여 특정 Capability(예: supports_function_calling) 지원 여부를 확인합니다."""
    try:
        resolved_model, resolved_provider, _, _ = get_llm_provider(model=model, custom_llm_provider=custom_llm_provider)
        model_info = lookup_base_model_info(model=resolved_model, custom_llm_provider=resolved_provider)
        
        if model_info.get(key) is not None:
            return bool(model_info.get(key))

        bare_model_key = _get_cost_key(resolved_model)
        if bare_model_key:
            bare_entry = model_cost.get(bare_model_key) or {}
            if bare_entry.get(key) is not None:
                return bool(bare_entry.get(key))

        return default_if_none
    except Exception as e:
        log.debug(f"Capability check failed for {key}. model={model}, provider={custom_llm_provider}. Error: {e}")
        return default_if_none


def supports_function_calling(model: str, custom_llm_provider: Optional[str] = None) -> bool:
    """모델이 Function Calling(Tools)을 지원하는지 확인합니다."""
    return _check_capability(model, custom_llm_provider, "supports_function_calling", default_if_none=False)


def supports_httpx_timeout(custom_llm_provider: str) -> bool:
    """HTTPX 타임아웃 객체를 네이티브로 지원하는 프로바이더인지 확인합니다."""
    return custom_llm_provider in ["openai"]


def get_supported_openai_params(
    model: str,
    custom_llm_provider: Optional[str] = None,
    request_type: str = "chat_completion",
    base_model: Optional[str] = None,
) -> Optional[list]:
    """해당 프로바이더/모델에서 허용하는 OpenAI 표준 파라미터 리스트를 추출합니다."""
    if not custom_llm_provider:
        try:
            custom_llm_provider = get_llm_provider(model=model)[1]
        except BadRequestError:
            return None

    # Transcription 특수 라우팅 처리
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