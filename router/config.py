# router.config
## @lineage: bound.router.config
## @lineage: anchor.registry.router.config
from __future__ import annotations
from functools import lru_cache
from typing import Callable, Optional, Union
from enum import Enum

from bound.registry.model.config.resolver import config
from bound.registry.model.config.constants import DEFAULT_MAX_LRU_CACHE_SIZE

from adapter.legacy.info import ProviderTypes, ProviderTypesSet, ProviderSpecificModelInfo
from adapter.legacy.action.param.format import BaseLLMModelInfo

class ProviderConfigManager:
    _PROVIDER_CONFIG_MAP: Optional[dict[ProviderTypes, tuple[Callable, bool]]] = None

    @staticmethod
    def _build_provider_config_map() -> dict[ProviderTypes, tuple[Callable, bool]]:
        return {
            # Format: (factory_function, needs_model_parameter: bool)
            ProviderTypes.OPENAI: (lambda: config.OpenAIGPTConfig(), False),
            ProviderTypes.ANTHROPIC: (lambda: config.AnthropicConfig(), False),
            ProviderTypes.A2A: (lambda: config.A2AConfig(), False),
            ProviderTypes.LLAMA: (lambda: config.LlamaAPIConfig(), False),
            ProviderTypes.CHATGPT: (lambda: config.ChatGPTConfig(), False),
            ProviderTypes.CUSTOM: (lambda: config.OpenAILikeChatConfig(), False),
            ProviderTypes.AIOHTTP_OPENAI: (lambda: config.AiohttpOpenAIChatConfig(), False),
            ProviderTypes.HUGGINGFACE: (lambda: config.HuggingFaceChatConfig(), False),
            ProviderTypes.GEMINI: (lambda: config.GoogleAIStudioGeminiConfig(), False),
            ProviderTypes.OLLAMA: (lambda: config.OllamaConfig(), False)
        }

    @staticmethod
    def get_provider_chat_config(
        model: str,
        provider: ProviderTypes,
        base_model: Optional[str] = None,
    ):
        if provider == ProviderTypes.OPENAI:
            if config.openaiOSeriesConfig.is_model_o_series_model(model=model):
                return config.openaiOSeriesConfig
            if config.OpenAIGPT5Config.is_model_gpt_5_model(model=model):
                return config.OpenAIGPT5Config()

        # Initialize provider config map lazily to avoid circular imports
        if ProviderConfigManager._PROVIDER_CONFIG_MAP is None:
            ProviderConfigManager._PROVIDER_CONFIG_MAP = (
                ProviderConfigManager._build_provider_config_map()
            )

        config_entry = ProviderConfigManager._PROVIDER_CONFIG_MAP.get(provider)
        if config_entry is not None:
            config_factory, needs_model = config_entry
            if needs_model:
                return config_factory(model)
            else:
                return config_factory()
        return None

    @staticmethod
    def get_provider_anthropic_messages_config(model: str, provider: ProviderTypes):
        return ProviderConfigManager._get_provider_anthropic_messages_config_cached(model=model, provider=provider)

    @staticmethod
    @lru_cache(maxsize=DEFAULT_MAX_LRU_CACHE_SIZE)
    def _get_provider_anthropic_messages_config_cached(model: str, provider: ProviderTypes):
        if ProviderTypes.ANTHROPIC == provider:
            return config.AnthropicMessagesConfig()
        return None

    @staticmethod
    def get_provider_audio_transcription_config(model: str, provider: ProviderTypes,):
        if ProviderTypes.OPENAI == provider:
            if "gpt-4o" in model:
                return config.OpenAIGPTAudioTranscriptionConfig()
            else:
                return config.OpenAIWhisperAudioTranscriptionConfig()
        return None

    @staticmethod
    def get_provider_responses_api_config(provider: Union[ProviderTypes, str], model: Optional[str] = None,):
        provider_enum: Optional[ProviderTypes] = None
        if isinstance(provider, ProviderTypes):
            provider_enum = provider
        else:
            try:
                provider_enum = ProviderTypes(provider)
            except ValueError:
                pass
        return ProviderConfigManager._get_python_responses_api_config(provider_enum, model)

    @staticmethod
    def _get_python_responses_api_config(provider: Optional[ProviderTypes], model: Optional[str] = None):
        if provider is None:
            return None
        if ProviderTypes.OPENAI == provider:
            return config.OpenAIResponsesAPIConfig()
        elif ProviderTypes.CHATGPT == provider:
            return config.ChatGPTResponsesAPIConfig()
        return None

    @staticmethod
    def get_provider_skills_api_config(provider: ProviderTypes):
        if ProviderTypes.ANTHROPIC == provider:
            return config.AnthropicSkillsConfig()
        return None

    @staticmethod
    def get_provider_evals_api_config(provider: ProviderTypes):
        if ProviderTypes.OPENAI == provider:
            return config.OpenAIEvalsConfig()
        return None

    @staticmethod
    def get_provider_model_info(model: Optional[str], provider: ProviderTypes) -> Optional[BaseLLMModelInfo]:
        if ProviderTypes.OPENAI == provider:
            return config.OpenAIGPTConfig()
        elif ProviderTypes.GEMINI == provider:
            return config.GeminiModelInfo()
        elif ProviderTypes.ANTHROPIC == provider:
            return config.AnthropicModelInfo()
        elif ProviderTypes.OLLAMA == provider or ProviderTypes.OLLAMA_CHAT == provider:
            return config.OllamaModelInfo()
        return None

    @staticmethod
    def get_provider_vector_stores_config(provider: ProviderTypes, api_type: Optional[str] = None):
        if ProviderTypes.OPENAI == provider:
            return config.OpenAIVectorStoreConfig()
        elif ProviderTypes.GEMINI == provider:
            return config.GeminiVectorStoreConfig()
        elif ProviderTypes.S3_VECTORS == provider:
            return config.S3VectorsVectorStoreConfig()
        return None

    @staticmethod
    def get_provider_vector_store_files_config(provider: ProviderTypes):
        if ProviderTypes.OPENAI == provider:
            return config.OpenAIVectorStoreFilesConfig()
        return None

    @staticmethod
    def get_provider_text_to_speech_config(model: str, provider: ProviderTypes):
        return None

def get_provider_info(
    model: str, custom_llm_provider: Optional[str]
) -> Optional[ProviderSpecificModelInfo]:
    provider_config: Optional[BaseLLMModelInfo] = None
    if custom_llm_provider and custom_llm_provider in ProviderTypesSet:
        provider_config = ProviderConfigManager.get_provider_model_info(
            model=model, provider=ProviderTypes(custom_llm_provider)
        )

    model_info: Optional[ProviderSpecificModelInfo] = None
    if provider_config:
        model_info = provider_config.get_provider_info(model=model)
    return model_info