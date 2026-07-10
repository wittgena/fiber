# anchor.registry.router.locator
import re
from typing import Optional, Tuple, Dict, Callable
from urllib.parse import urlparse

from anchor.registry.model.cost import get_provider_for_model
from bound.surface.legacy.param.legacy import LiteLLM_Params
from anchor.registry.model.config.resolver import config
from anchor.registry.model.config.constants import REPLICATE_MODEL_NAME_WITH_ID_LENGTH
from xphi.xor.secret.manager import get_secret_str, get_secret 

from watcher.plane.emitter import get_emitter

log = get_emitter("routing.locator")

PROVIDER_REGISTRY = {
    "openai": ("https://api.openai.com/v1", ["OPENAI_API_KEY"]),
    "text-completion-openai": ("https://api.openai.com/v1", ["OPENAI_API_KEY"]),
    "anthropic": ("https://api.anthropic.com/v1/messages", ["ANTHROPIC_API_KEY"]),
    "anthropic_text": ("https://api.anthropic.com/v1/complete", ["ANTHROPIC_API_KEY"]),
    "cohere": ("https://api.cohere.ai/v1", ["COHERE_API_KEY"]),
    "cohere_chat": ("https://api.cohere.ai/v1", ["COHERE_API_KEY"]),
    "meta_llama": ("https://api.llama.com/compat/v1", ["LLAMA_API_KEY"]),
    "ollama": ("http://localhost:11434", ["OLLAMA_API_KEY"]),
    "vllm": ("http://localhost:8000/v1", ["VLLM_API_KEY"]),
    "hosted_vllm": ("http://localhost:8000/v1", ["VLLM_API_KEY"]),
    "lm_studio": ("http://localhost:1234/v1", ["LM_STUDIO_API_KEY"]),
    "llamafile": ("http://localhost:8080/v1", ["LLAMAFILE_API_KEY"]),
    "langgraph": ("http://localhost:2024", ["LANGGRAPH_API_KEY"]),
    "azure_ai": ("https://models.inference.ai.azure.com", ["AZURE_AI_API_KEY"]),
    "github": ("https://models.inference.ai.azure.com", ["GITHUB_API_KEY"]),
    "github_copilot": ("https://api.githubcopilot.com", ["GITHUB_COPILOT_API_KEY"]),
    "datarobot": (None, ["DATAROBOT_API_KEY"]), 
    "groq": ("https://api.groq.com/openai/v1", ["GROQ_API_KEY"]),
    "mistral": ("https://api.mistral.ai/v1", ["MISTRAL_API_KEY"]),
    "codestral": ("https://codestral.mistral.ai/v1", ["CODESTRAL_API_KEY"]),
    "text-completion-codestral": ("https://codestral.mistral.ai/v1/fim/completions", ["CODESTRAL_API_KEY"]),
    "deepseek": ("https://api.deepseek.com/beta", ["DEEPSEEK_API_KEY"]),
    "perplexity": ("https://api.perplexity.ai", ["PERPLEXITYAI_API_KEY"]),
    "together_ai": ("https://api.together.xyz/v1", ["TOGETHER_API_KEY", "TOGETHER_AI_API_KEY", "TOGETHERAI_API_KEY", "TOGETHER_AI_TOKEN"]),
    "anyscale": ("https://api.endpoints.anyscale.com/v1", ["ANYSCALE_API_KEY"]),
    "deepinfra": ("https://api.deepinfra.com/v1/openai", ["DEEPINFRA_API_KEY"]),
    "baseten": ("https://inference.baseten.co/v1", ["BASETEN_API_KEY"]),
    "fireworks_ai": ("https://api.fireworks.ai/inference/v1", ["FIREWORKS_AI_API_KEY"]),
    "ai21": ("https://api.ai21.com/studio/v1", ["AI21_API_KEY"]),
    "ai21_chat": ("https://api.ai21.com/studio/v1", ["AI21_API_KEY"]),
    "nvidia_nim": ("https://integrate.api.nvidia.com/v1", ["NVIDIA_NIM_API_KEY"]),
    "nvidia_riva": ("grpc.nvcf.nvidia.com:443", ["NVIDIA_RIVA_API_KEY", "NVIDIA_NIM_API_KEY"]),
    "cerebras": ("https://api.cerebras.ai/v1", ["CEREBRAS_API_KEY"]),
    "sambanova": ("https://api.sambanova.ai/v1", ["SAMBANOVA_API_KEY"]),
    "empower": ("https://app.empower.dev/api/v1", ["EMPOWER_API_KEY"]),
    "soniox": ("https://api.soniox.com", ["SONIOX_API_KEY"]),
    "nebius": ("https://api.studio.nebius.ai/v1", ["NEBIUS_API_KEY"]),
    "volcengine": ("https://ark.cn-beijing.volces.com/api/v3", ["VOLCENGINE_API_KEY"]),
    "dashscope": ("https://dashscope-intl.aliyuncs.com/compatible-mode/v1", ["DASHSCOPE_API_KEY"]),
    "moonshot": ("https://api.moonshot.ai/v1", ["MOONSHOT_API_KEY"]),
    "minimax": ("https://api.minimax.io/v1", ["MINIMAX_API_KEY"]),
    "friendliai": ("https://api.friendli.ai/serverless/v1", ["FRIENDLIAI_API_KEY", "FRIENDLI_TOKEN"]),
    "galadriel": ("https://api.galadriel.com/v1", ["GALADRIEL_API_KEY"]),
    "novita": ("https://api.novita.ai/v3/openai", ["NOVITA_API_KEY"]),
    "manus": ("https://api.manus.im", ["MANUS_API_KEY"]),
    "v0": ("https://api.v0.dev/v1", ["V0_API_KEY"]),
    "lambda_ai": ("https://api.lambda.ai/v1", ["LAMBDA_API_KEY"]),
    "inception": ("https://api.inceptionlabs.ai/v1", ["INCEPTION_API_KEY"]),
    "hyperbolic": ("https://api.hyperbolic.xyz/v1", ["HYPERBOLIC_API_KEY"]),
    "vercel_ai_gateway": ("https://ai-gateway.vercel.sh/v1", ["VERCEL_AI_GATEWAY_API_KEY"]),
    "wandb": ("https://api.inference.wandb.ai/v1", ["WANDB_API_KEY"]),
    "publicai": ("https://platform.publicai.co/v1", ["PUBLICAI_API_KEY"]),
    "synthetic": ("https://api.synthetic.new/openai/v1", ["SYNTHETIC_API_KEY"]),
    "apertis": ("https://api.stima.tech/v1", ["STIMA_API_KEY"]),
    "nano-gpt": ("https://nano-gpt.com/api/v1", ["NANOGPT_API_KEY"]),
    "poe": ("https://api.poe.com/v1", ["POE_API_KEY"]),
    "chutes": ("https://llm.chutes.ai/v1/", ["CHUTES_API_KEY"]),
    "featherless_ai": ("https://api.featherless.ai/v1", ["FEATHERLESS_AI_API_KEY"]),
    "nscale": ("https://api.nscale.com/v1", ["NSCALE_API_KEY"]),
    "replicate": ("https://api.replicate.com/v1", ["REPLICATE_API_KEY", "REPLICATE_API_TOKEN"]),
    "vertex_ai": (None, ["VERTEX_AI_API_KEY", "GEMINI_API_KEY"]),
    "gemini": (None, ["GEMINI_API_KEY", "GOOGLE_API_KEY"]),
    "google": (None, ["GEMINI_API_KEY", "GOOGLE_API_KEY"]),
    "bedrock": (None, ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]),
    "watsonx": (None, ["WATSONX_API_KEY"]),
}

_CLAUDE_PATTERN = re.compile(r"^claude-[a-z]+-\d+-\d+(?:-\d{8})?$", re.IGNORECASE)

def _matches_claude_model_pattern(model: str) -> bool:
    """
    Check if a model string matches the Claude model naming pattern.
    Matches patterns like:
    - claude-opus-4-7
    - claude-sonnet-4-6
    - claude-haiku-4-5
    - claude-opus-5-1-20270101 (with optional date suffix)
    """
    return _CLAUDE_PATTERN.match(model) is not None

def _is_non_openai_azure_model(model: str) -> bool:
    """Azure 엔드포인트로 들어오지만 실제로는 OpenAI 모델이 아닌 경우(Cohere, Mistral 등)를 판별"""
    try:
        model_name = model.split("/", 1)[1]
        cohere_models = config.cohere_chat_models or []
        mistral_models = config.mistral_chat_models or []
        if model_name in cohere_models or f"mistral/{model_name}" in mistral_models:
            return True
    except Exception:
        return False
    return False

def handle_cohere_chat_model_custom_llm_provider(model: str, custom_llm_provider: Optional[str] = None) -> Tuple[str, Optional[str]]:
    """유저가 cohere_chat 모델을 단순히 'cohere' 프로바이더로 명시했을 때, 올바른 프로바이더(cohere_chat)로 교정"""
    cohere_models = config.cohere_chat_models or []
    if custom_llm_provider:
        if custom_llm_provider == "cohere" and model in cohere_models:
            return model, "cohere_chat"

    if model and "/" in model:
        _custom_llm_provider, _model = model.split("/", 1)
        if _custom_llm_provider == "cohere" and _model in cohere_models:
            return _model, "cohere_chat"

    return model, custom_llm_provider

def handle_anthropic_text_model_custom_llm_provider(
    model: str, custom_llm_provider: Optional[str] = None
) -> Tuple[str, Optional[str]]:
    """구형 Anthropic Text Completion 모델(claude-2, claude-instant 등)을 anthropic_text 프로바이더로 교정"""
    is_text_model = "claude-2" in model or "claude-instant" in model
    if custom_llm_provider:
        if custom_llm_provider == "anthropic" and is_text_model:
            return model, "anthropic_text"

    if model and "/" in model:
        _custom_llm_provider, _model = model.split("/", 1)
        if (_custom_llm_provider == "anthropic" and ("claude-2" in _model or "claude-instant" in _model)):
            return _model, "anthropic_text"

    return model, custom_llm_provider


class GateBadRequestError(Exception):
    def __init__(self, message: str, model: str):
        super().__init__(message)
        self.model = model

def _endpoint_matches_api_base(endpoint: str, api_base: str) -> bool:
    def _parse(value: str):
        normalized = value if "://" in value else f"https://{value}"
        return urlparse(normalized)
    parsed_endpoint = _parse(endpoint)
    parsed_url = _parse(api_base)
    endpoint_host = (parsed_endpoint.hostname or "").lower()
    url_host = (parsed_url.hostname or "").lower()
    if not endpoint_host or endpoint_host != url_host:
        return False
    endpoint_path = parsed_endpoint.path.rstrip("/")
    if not endpoint_path:
        return True
    url_path = parsed_url.path.rstrip("/")
    return url_path == endpoint_path or url_path.rstrip("/").startswith(endpoint_path + "/")

class LLMProviderResolver:
    """
    모델명, API Base 등을 분석하여 적절한 LLM Provider와 자격 증명을 결정하는 레졸버 클래스.
    LiteLLM 의존성을 완벽히 분리하고 ConfigResolver를 통해 데이터를 조달합니다.
    """
    def __init__(self):
        self._provider_configs = PROVIDER_REGISTRY

    def resolve(
        self,
        model: str,
        custom_llm_provider: Optional[str] = None,
        api_base: Optional[str] = None,
        api_key: Optional[str] = None,
        litellm_params: Optional[LiteLLM_Params] = None,
    ) -> Tuple[str, str, Optional[str], Optional[str]]:
        
        if not model:
            raise ValueError("model parameter is required.")

        ## Params 초기 검증 및 병합
        if litellm_params:
            custom_llm_provider = litellm_params.custom_llm_provider
            api_base = litellm_params.api_base
            api_key = litellm_params.api_key

        dynamic_api_key = None

        ## 특수 케이스 및 예외 모델 정규화
        model, custom_llm_provider, is_resolved = self._resolve_special_cases(model, custom_llm_provider)
        if is_resolved:
            return model, custom_llm_provider, dynamic_api_key, api_base

        ## 접두사(Prefix) 기반 라우팅 분석
        if "/" in model:
            resolved = self._resolve_by_prefix(model, api_base, api_key)
            if resolved:
                return resolved

        ## API Base URL 기반 역추적 스니핑
        if api_base:
            resolved = self._resolve_by_api_base(model, api_base, api_key)
            if resolved:
                return resolved

        ## 최후의 수단: 모델명 브루트포스 매칭
        model, custom_llm_provider = self._resolve_by_model_name(model, custom_llm_provider)
        if not custom_llm_provider:
            raise GateBadRequestError(
                message=f"LLM Provider NOT provided. Pass model as E.g. `completion(model='huggingface/starcoder',..)`. You passed model={model}",
                model=model
            )
        return model, custom_llm_provider, api_key, api_base

    def _resolve_special_cases(self, model: str, custom_llm_provider: Optional[str]) -> Tuple[str, Optional[str], bool]:
        ## Azure의 Cohere/Mistral 호스팅 모델 교정
        if model.startswith("azure/"):
            model_name = model.split("/", 1)[1]
            registry_provider = get_provider_for_model(model_name)
            if registry_provider in ["cohere_chat", "mistral"]:
                return model, "openai", True
                
        model, custom_llm_provider = handle_cohere_chat_model_custom_llm_provider(model, custom_llm_provider)
        model, custom_llm_provider = handle_anthropic_text_model_custom_llm_provider(model, custom_llm_provider)
        if custom_llm_provider == "openrouter" and model.startswith("openrouter/"):
            remainder = model[len("openrouter/"):]
            if "/" in remainder:
                return remainder, custom_llm_provider, True
            return model, custom_llm_provider, True

        return model, custom_llm_provider, False

    def _resolve_by_prefix(self, model: str, api_base: Optional[str], api_key: Optional[str]):
        provider_prefix, actual_model = model.split("/", 1)
        if provider_prefix in self._provider_configs:
            default_base, key_envs = self._provider_configs[provider_prefix]
            api_base = api_base or default_base
            dynamic_api_key = api_key or self._get_secret_from_list(key_envs)
            return actual_model, provider_prefix, dynamic_api_key, api_base
            
        provider_list = config.provider_list or []
        if provider_prefix in provider_list:
            return actual_model, provider_prefix, api_key, api_base
            
        return None

    def _resolve_by_api_base(self, model: str, api_base: str, api_key: Optional[str]):
        endpoints = config.openai_compatible_endpoints or []
        for endpoint in endpoints:
            if _endpoint_matches_api_base(endpoint, api_base):
                provider_name = self._find_provider_by_endpoint(endpoint)
                if provider_name and provider_name in self._provider_configs:
                    _, key_envs = self._provider_configs[provider_name]
                    dynamic_api_key = api_key or self._get_secret_from_list(key_envs)
                    return model, provider_name, dynamic_api_key, api_base
        return None
    
    def _resolve_by_model_name(self, model: str, custom_llm_provider: Optional[str]) -> Tuple[str, Optional[str]]:
        registry_provider = get_provider_for_model(model)
        if registry_provider:
            ## JSON 스펙에 정의된 프로바이더를 즉시 반환 (수백 개의 모델 커버)
            return model, registry_provider

        ## Registry에 없는 특수 패턴 및 Legacy 하위 호환 (Fallback)
        if ":" in model and len(model.split(":")[1]) == REPLICATE_MODEL_NAME_WITH_ID_LENGTH:
            return model, "replicate"

        if "ft:gpt" in model or model.startswith("gpt-"):
            return model, "openai"
        if _matches_claude_model_pattern(model):
            return model, "anthropic"
        return model, custom_llm_provider

    def _get_secret_from_list(self, env_keys: list) -> Optional[str]:
        for key in env_keys:
            val = get_secret_str(key) or get_secret(key)
            if val:
                return val
        return None

    def _find_provider_by_endpoint(self, endpoint: str) -> Optional[str]:
        for provider, config_tuple in self._provider_configs.items():
            if config_tuple is None:
                continue
            base_url, _ = config_tuple
            # base_url이 None인 datarobot, vertex_ai 등 방어 처리
            if base_url and endpoint in base_url:
                return provider
        return None


_resolver_instance = LLMProviderResolver()

def get_llm_provider(
    model: str,
    custom_llm_provider: Optional[str] = None,
    api_base: Optional[str] = None,
    api_key: Optional[str] = None,
    litellm_params: Optional[LiteLLM_Params] = None,
) -> Tuple[str, str, Optional[str], Optional[str]]:
    try:
        return _resolver_instance.resolve(
            model=model,
            custom_llm_provider=custom_llm_provider,
            api_base=api_base,
            api_key=api_key,
            litellm_params=litellm_params
        )
    except Exception as e:
        if isinstance(e, GateBadRequestError):
            raise e
        raise GateBadRequestError(
            message=f"GetLLMProvider Exception - {str(e)}\n\noriginal model: {model}",
            model=model
        )