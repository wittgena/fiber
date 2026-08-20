# agent.anchor.provider.registry
## @lineage: bound.xor.model.registry
## @lineage: eco.model.registry
from __future__ import annotations
import json
import os
import re
import time
import httpx
from pathlib import Path
from urllib.parse import urlparse
from email.utils import formatdate
from typing import Dict, List, Tuple, Optional, Callable, Union

from agent.llm.router.constants import REPLICATE_MODEL_NAME_WITH_ID_LENGTH
from agent.anchor.provider.protype import ProviderTypes
from agent.anchor.model.types.param.legacy import LegacyParams

from arch.model.config import config
from arch.xor.secret.manager import get_secret_str, get_secret
from kernel.bind.resolver import resolve_path 
from watcher.plane.emitter import get_emitter 

log_cost = get_emitter("registry.model")
log_route = get_emitter("routing.locator")

REGISTRY_ROOT = resolve_path("registry") / "llms"
_DEFAULT_MAP_URL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
model_cost_map_url: str = os.getenv("LITELLM_MODEL_COST_MAP_URL", _DEFAULT_MAP_URL)

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

# ==========================================
# 1. Registry Data & Cost Mapping
# ==========================================

class RegistryIO:
    """@desc: Namespace for registry file operations and network I/O."""
    
    @classmethod
    def save_backup(cls, data: dict, filename: str) -> None:
        """@desc: Persists the successfully fetched remote registry payload to local disk."""
        try:
            REGISTRY_ROOT.mkdir(parents=True, exist_ok=True)
            backup_path = REGISTRY_ROOT / filename
            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            log_cost.debug(f"Saved local backup successfully: {backup_path}")
        except Exception as e:
            log_cost.warning(f"Failed to save local backup for {filename}: {e}")

    @classmethod
    def load_backup(cls, filename: str) -> dict:
        """@desc: Retrieves the local backup payload from disk."""
        try:
            backup_path = REGISTRY_ROOT / filename
            if backup_path.exists():
                with open(backup_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            log_cost.warning(f"Local backup not found at {backup_path}")
        except Exception as e:
            log_cost.error(f"Failed to load local backup for {filename}: {e}")
        return {}

    @classmethod
    def _optimize_memory(cls, raw_spec: dict) -> dict:
        """@desc: Drops sparse data (False, 0, null, empty strings) to minimize memory footprint."""
        optimized = {}
        for k, v in raw_spec.items():
            if isinstance(v, dict):
                optimized_sub = cls._optimize_memory(v)
                if optimized_sub:
                    optimized[k] = optimized_sub
            elif v not in (False, 0.0, 0, "", None, [], {}):
                optimized[k] = v
        return optimized

    @classmethod
    def _expand_aliases(cls, cost_map: dict) -> dict:
        """@desc: Expands alias lists into top-level key references within the map."""
        aliases_to_add: Dict[str, dict] = {}
        keys_with_aliases = []

        for model_name, model_info in cost_map.items():
            if not isinstance(model_info, dict):
                continue
            aliases = model_info.get("aliases")
            if isinstance(aliases, list):
                keys_with_aliases.append(model_name)
                for alias in aliases:
                    if alias not in cost_map and alias not in aliases_to_add:
                        aliases_to_add[alias] = model_info 

        for key in keys_with_aliases:
            cost_map[key].pop("aliases", None)

        cost_map.update(aliases_to_add)
        return cost_map

    @classmethod
    def parse_cost_map(cls, raw_data: dict) -> dict:
        """@desc: Unified parser hook specifically for Model Cost Maps."""
        expanded = cls._expand_aliases(raw_data)
        for model_name, spec in expanded.items():
            if isinstance(spec, dict):
                expanded[model_name] = cls._optimize_memory(spec)
        return expanded

    @classmethod
    def fetch_and_validate(
        cls,
        url: str,
        filename: str,
        force_local_env_key: str,
        registry_name: str = "Registry",
        min_count: int = 0,
        parser_hook: Optional[Callable[[dict], dict]] = None,
        cache_ttl_seconds: int = 86400,
    ) -> Tuple[dict, dict]:
        """@desc: Core network fetcher implementing TTL-based Zero-Network Delay and Conditional GET"""
        source_info = {"source": "local", "url": url, "is_env_forced": False, "fallback_reason": None}
        if os.getenv(force_local_env_key, "").lower() == "true":
            source_info["is_env_forced"] = True
            local_data = cls.load_backup(filename)
            return (parser_hook(local_data) if parser_hook else local_data), source_info

        backup_path = REGISTRY_ROOT / filename
        headers = {}
        if backup_path.exists():
            mtime = backup_path.stat().st_mtime
            file_age = time.time() - mtime
            if file_age < cache_ttl_seconds:
                log_cost.debug(f"[{registry_name}] Using valid local cache (TTL < {cache_ttl_seconds}s)")
                local_data = cls.load_backup(filename)
                if local_data and len(local_data) >= min_count:
                    source_info["source"] = "local_cache_ttl"
                    return (parser_hook(local_data) if parser_hook else local_data), source_info

            http_date = formatdate(timeval=mtime, localtime=False, usegmt=True)
            headers["If-Modified-Since"] = http_date

        try:
            response = httpx.get(url, headers=headers, timeout=5)
            if response.status_code == 304:
                log_cost.debug(f"[{registry_name}] Remote map not modified (304). Updating local cache timestamp.")
                os.utime(backup_path, None) 
                local_data = cls.load_backup(filename)
                source_info["source"] = "local_cache_304"
                return (parser_hook(local_data) if parser_hook else local_data), source_info

            response.raise_for_status()
            content = response.json()
            if not isinstance(content, dict) or len(content) < min_count:
                raise ValueError(f"Invalid map format or item count less than min_count({min_count})")

            cls.save_backup(content, filename)
            source_info["source"] = "remote"
            return (parser_hook(content) if parser_hook else content), source_info

        except Exception as e:
            log_cost.warning(f"[{registry_name}] Failed to fetch remote map. Falling back to stale local cache: {e}")
            source_info["fallback_reason"] = str(e)
            source_info["source"] = "stale_local_fallback"
            local_data = cls.load_backup(filename)
            return (parser_hook(local_data) if parser_hook else local_data), source_info

    @classmethod
    def get_model_cost_map(cls, url: str) -> Tuple[dict, dict]:
        """@desc: Concrete factory invoking fetch_and_validate for model cost data."""
        return cls.fetch_and_validate(
            url=url,
            filename="model_prices_and_context_window_backup.json",
            force_local_env_key="LITELLM_LOCAL_MODEL_COST_MAP",
            registry_name="ModelCostMap",
            min_count=50,
            parser_hook=cls.parse_cost_map
        )


# Global Initialization of Cost Map
model_cost, _ = RegistryIO.get_model_cost_map(url=model_cost_map_url)


class ModelCostRegistry:
    @classmethod
    def lookup_base_model_info(
        cls,
        model: str,
        custom_llm_provider: Optional[str] = None,
        api_base: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> dict:
        potential_keys = []
        if custom_llm_provider:
            potential_keys.append(f"{custom_llm_provider}/{model}")
        potential_keys.append(model)
        
        if "/" in model:
            stripped_model = model.split("/", 1)[1]
            if custom_llm_provider:
                potential_keys.append(f"{custom_llm_provider}/{stripped_model}")
            potential_keys.append(stripped_model)

        matched_info = None
        matched_key = None

        for key in potential_keys:
            if key in model_cost:
                matched_info = model_cost[key]
                matched_key = key
                break

        if matched_info is None:
            log_cost.warning(f"Model not mapped in model_cost. model={model}, provider={custom_llm_provider}")
            return {"key": model, "input_cost_per_token": 0.0, "output_cost_per_token": 0.0}

        result = matched_info.copy()
        result["key"] = matched_key
        result.setdefault("input_cost_per_token", 0.0)
        result.setdefault("output_cost_per_token", 0.0)
        return result

    @classmethod
    def get_provider(cls, model_name: str) -> str | None:
        """@desc: Returns the provider string for a given model from the map."""
        return model_cost.get(model_name, {}).get("litellm_provider")

    @classmethod
    def register(cls, new_model_cost: Union[str, dict]) -> dict:
        """@desc: Dynamically injects new model cost and metadata into the global map."""
        loaded_cost = {}
        if isinstance(new_model_cost, dict):
            loaded_cost = new_model_cost
        elif isinstance(new_model_cost, str):
            loaded_cost, _ = RegistryIO.get_model_cost_map(url=new_model_cost)

        skip_providers = {ProviderTypes.GITHUB_COPILOT.value, ProviderTypes.CHATGPT.value}

        for key, value in loaded_cost.items():
            provider = value.get("litellm_provider", "")
            key_str = str(key)
            
            if provider in skip_providers or any(key_str.startswith(f"{p}/") for p in skip_providers):
                existing_model = model_cost.get(key, {})
                model_cost_key = key
            else:
                try:
                    existing_model = cls.lookup_base_model_info(model=key)
                    model_cost_key = existing_model.get("key", key)
                except Exception:
                    existing_model = {}
                    model_cost_key = key
                    
            if existing_model.get("litellm_provider") is None:
                existing_model.pop("litellm_provider", None)
                
            updated_dict = cls._merge_dicts(existing_model, value)
            model_cost.setdefault(model_cost_key, {}).update(updated_dict)
            
            log_cost.debug(f"Added/updated model={model_cost_key} in model_cost")
            cls._update_provider_models(key, value.get("litellm_provider"))
            
        return loaded_cost

    @classmethod
    def _merge_dicts(cls, existing: dict, new_dict: dict) -> dict:
        """@desc: Deep merges dictionary updates, casting stringified numbers."""
        for k, v in new_dict.items():
            if v is not None:
                if isinstance(v, str):
                    existing[k] = cls._convert_numbers(v)
                elif isinstance(v, dict) and isinstance(existing.get(k), dict):
                    existing[k].update(v)
                else:
                    existing[k] = v
        return existing

    @staticmethod
    def _convert_numbers(value: str) -> Union[str, float, int]:
        try:
            if "e" in value.lower() or "." in value:
                return float(value)
            return int(value)
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _update_provider_models(key: str, provider: Optional[str]) -> None:
        """@desc: Appends newly registered models to specific provider config sets."""
        if provider == "openai":
            openai_models = config.get("open_ai_chat_completion_models")
            if openai_models is not None:
                openai_models.add(key)
        elif provider == "anthropic":
            anthropic_models = config.get("anthropic_models")
            if anthropic_models is not None:
                anthropic_models.add(key)


# ==========================================
# 2. Helpers for Routing & Edge Cases
# ==========================================

class GateBadRequestError(Exception):
    def __init__(self, message: str, model: str):
        super().__init__(message)
        self.model = model

def _matches_claude_model_pattern(model: str) -> bool:
    """
    Check if a model string matches the Claude model naming pattern.
    Matches patterns like: claude-opus-4-7, claude-sonnet-4-6, claude-haiku-4-5, etc.
    """
    return _CLAUDE_PATTERN.match(model) is not None

def _is_non_openai_azure_model(model: str) -> bool:
    """Azure 엔드포인트로 들어오지만 실제로는 OpenAI 모델이 아닌 경우(Cohere, Mistral 등)를 판별"""
    try:
        model_name = model.split("/", 1)[1]
        cohere_models = config.get("cohere_chat_models", [])
        mistral_models = config.get("mistral_chat_models", [])
        if model_name in cohere_models or f"mistral/{model_name}" in mistral_models:
            return True
    except Exception:
        return False
    return False

def handle_cohere_chat_model_custom_llm_provider(model: str, custom_llm_provider: Optional[str] = None) -> Tuple[str, Optional[str]]:
    """유저가 cohere_chat 모델을 단순히 'cohere' 프로바이더로 명시했을 때, 올바른 프로바이더(cohere_chat)로 교정"""
    cohere_models = config.get("cohere_chat_models", [])
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


# ==========================================
# 3. LLM Provider Routing Resolver
# ==========================================

class LLMProviderResolver:
    """
    모델명, API Base 등을 분석하여 적절한 LLM Provider와 자격 증명을 결정하는 레졸버 클래스.
    """
    def __init__(self):
        self._provider_configs = PROVIDER_REGISTRY

    def resolve(
        self,
        model: str,
        custom_llm_provider: Optional[str] = None,
        api_base: Optional[str] = None,
        api_key: Optional[str] = None,
        litellm_params: Optional[LegacyParams] = None,
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
            registry_provider = ModelCostRegistry.get_provider(model_name)
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
            
        provider_list = config.get("provider_list", [])
        if provider_prefix in provider_list:
            return actual_model, provider_prefix, api_key, api_base
            
        return None

    def _resolve_by_api_base(self, model: str, api_base: str, api_key: Optional[str]):
        endpoints = config.get("openai_compatible_endpoints", [])
        for endpoint in endpoints:
            if _endpoint_matches_api_base(endpoint, api_base):
                provider_name = self._find_provider_by_endpoint(endpoint)
                if provider_name and provider_name in self._provider_configs:
                    _, key_envs = self._provider_configs[provider_name]
                    dynamic_api_key = api_key or self._get_secret_from_list(key_envs)
                    return model, provider_name, dynamic_api_key, api_base
        return None
    
    def _resolve_by_model_name(self, model: str, custom_llm_provider: Optional[str]) -> Tuple[str, Optional[str]]:
        registry_provider = ModelCostRegistry.get_provider(model)
        if registry_provider:
            ## JSON 스펙에 정의된 프로바이더를 즉시 반환
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
            if base_url and endpoint in base_url:
                return provider
        return None


_resolver_instance = LLMProviderResolver()

def get_llm_provider(
    model: str,
    custom_llm_provider: Optional[str] = None,
    api_base: Optional[str] = None,
    api_key: Optional[str] = None,
    litellm_params: Optional[LegacyParams] = None,
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

# ==========================================
# 4. Exports & Legacy Bindings
# ==========================================

get_provider_for_model = ModelCostRegistry.get_provider
lookup_base_model_info = ModelCostRegistry.lookup_base_model_info
register_model = ModelCostRegistry.register

## Legacy Config Injection
config.model_cost = model_cost
config.register_model = register_model
config._get_model_info_helper = lookup_base_model_info

## Fallback binding for deep legacy contexts
try:
    utils = config.get("utils")
    if utils:
        utils.register_model = register_model
        utils._get_model_info_helper = lookup_base_model_info
except Exception:
    pass