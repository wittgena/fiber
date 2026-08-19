# engine.parser.param.processor
from __future__ import annotations
import copy
import re
import importlib
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Tuple, Union
import httpx
from pydantic import BaseModel
from openai.lib import _parsing, _pydantic

from eco.model.registry import get_llm_provider
from eco.bound.agent.adapter.constants import COMPLETION_HTTP_FALLBACK_SECONDS, DEFAULT_REQUEST_TIMEOUT_SECONDS, REQUEST_TIMEOUT, DEFAULT_CHAT_COMPLETION_PARAM_VALUES, DEFAULT_EMBEDDING_PARAM_VALUES
from arch.model.config import config
from eco.model.types.core import Usage
from eco.model.info import get_features, supports_httpx_timeout, supports_function_calling, get_supported_openai_params
from eco.bound.exception.eco import UnsupportedParamsError
from eco.model.types.openai import ValidUserMessageContentTypes

from eco.client.model.param import ModelResponse
from eco.client.model.execution import ExecutionMetadata, CompletionContext, EmbeddingContext
from watcher.plane.emitter import get_emitter

log = get_emitter("param.processor")

FRAMEWORK_KWARGS = {
    "metadata", "session_id", "trace_id", "call_id", "completion_call_id", "preset_cache_key", "model_info", 
    "model_alias_map", "proxy_server_request", "input_cost_per_token", "output_cost_per_token", 
    "input_cost_per_second", "output_cost_per_second", "cost_per_query", "prompt_id", "prompt_variables",
    "timeout", "request_timeout", "client", "shared_session", "acompletion", "aembedding", "headers", 
    "extra_headers", "custom_llm_provider", "api_key", "api_base", "base_url", "deployment_id", "azure", 
    "aws_region_name", "supports_system_message", "litellm_system_prompt", "base_model"
}
AUTH_PREFIXES = ("aws_", "azure_", "vertex_", "tenant_id", "client_id", "client_secret", "bucket_name")
FLAG_KEYS = {
    "no_log", "no-log", "custom_prompt_dict", "async_call", "ssl_verify", "merge_reasoning_content_in_choices", 
    "use_litellm_proxy", "logger_fn", "verbose", "disable_add_transform_inline_image_block", "log_delegator"
}

PROVIDER_ALIAS = {"vertex_ai_beta": "vertex_ai", "text-completion-openai": "openai", "azure_ai": "azure", "ollama_chat": "ollama"}
CONFIG_MAP = {
    "anthropic": ("anthropic", "AnthropicConfig"),
    "huggingface": ("huggingface", "HuggingFaceChatConfig"), 
    "gemini": ("gemini", "GoogleAIStudioGeminiConfig"), 
    "ollama": ("ollama", "OllamaConfig"),
    "openai": ("openai", "OpenAIConfig"),
    "vertex_ai": ("vertex_ai", "VertexAIConfig"),
}
_OPENAI_REGIONAL_HOSTS = {"eu.api.openai.com": "eu", "us.api.openai.com": "us"}

def _delete_nested_path(data: Dict, path: str):
    try:
        segments = re.findall(r"[^\.\[]+|\[[^\]]*\]", path)
        curr = data
        for i, seg in enumerate(segments):
            is_last = (i == len(segments) - 1)
            key = int(seg[1:-1]) if seg.startswith("[") else seg
            if is_last:
                if isinstance(curr, dict): curr.pop(key, None)
                elif isinstance(curr, list) and isinstance(key, int) and 0 <= key < len(curr): curr.pop(key)
            else:
                curr = curr[key]
    except Exception: pass

def _to_json_schema(model_or_dict: Any) -> Optional[dict]:
    if not model_or_dict: return None
    if isinstance(model_or_dict, dict): return model_or_dict
    if not _parsing._completions.is_basemodel_type(model_or_dict): return model_or_dict
    return {
        "type": "json_schema",
        "json_schema": {"schema": _pydantic.to_strict_json_schema(model_or_dict), "name": model_or_dict.__name__, "strict": True}
    }

def _resolve_timeout(raw: dict, provider: str) -> Union[float, httpx.Timeout]:
    val = raw.get("timeout") or raw.get("request_timeout") or REQUEST_TIMEOUT
    if val in [None, DEFAULT_REQUEST_TIMEOUT_SECONDS]: return COMPLETION_HTTP_FALLBACK_SECONDS
    if isinstance(val, httpx.Timeout) and not supports_httpx_timeout(provider):
        return float(val.read) if val.read is not None else COMPLETION_HTTP_FALLBACK_SECONDS
    return float(val) if not isinstance(val, httpx.Timeout) else val

class BaseProcessor:
    def __init__(self, task_type: str, model: str, raw_kwargs: dict):
        self.task_type = task_type
        self.original_model = model
        
        self.raw = {k: v for k, v in raw_kwargs.items() if k not in FRAMEWORK_KWARGS and k not in FLAG_KEYS and not k.startswith(AUTH_PREFIXES)}
        self.raw["max_retries"] = raw_kwargs.get("max_retries", raw_kwargs.get("num_retries"))
        self.original_kwargs = raw_kwargs.copy()
        
        self.provider, self.api_key, self.api_base, self.model = self._resolve_routing()
        self.provider_key = PROVIDER_ALIAS.get(self.provider, self.provider)
        self.drop_flag = self.raw.pop("drop_params", getattr(config, "drop_params", False))
        
    def _resolve_routing(self) -> Tuple[str, Optional[str], Optional[str], str]:
        custom_prov = self.original_kwargs.get("custom_llm_provider")
        api_base = self.original_kwargs.get("api_base") or self.original_kwargs.get("base_url")
        api_key = None if self.original_kwargs.get("api_key") == "not-needed" else self.original_kwargs.get("api_key")
        deployment_id = self.original_kwargs.get("deployment_id")
        target_model = deployment_id if deployment_id else self.original_model

        if self.original_kwargs.get("azure", False) or deployment_id: custom_prov = "azure"

        res_model, res_prov, dyn_key, res_base = get_llm_provider(
            model=target_model, custom_llm_provider=custom_prov, api_base=api_base, api_key=api_key
        )
        return res_prov, (dyn_key or api_key), res_base, res_model

    def _extract_non_defaults(self) -> dict:
        defaults = DEFAULT_CHAT_COMPLETION_PARAM_VALUES if self.task_type == "chat" else DEFAULT_EMBEDDING_PARAM_VALUES
        drops = ["messages"] if self.task_type == "chat" else ["input"]
        ignore_keys = ["model", "custom_llm_provider", "api_version"] + drops
        add_drops = self.original_kwargs.get("additional_drop_params", [])
        
        return {
            k: v for k, v in self.raw.items()
            if k not in ignore_keys and k in defaults and v != defaults[k] and k not in add_drops
        }

    def _get_vendor_config_class(self) -> Optional[Any]:
        if self.provider_key not in CONFIG_MAP:
            return getattr(config, "OpenAILikeChatConfig", None)

        module_suffix, class_name = CONFIG_MAP[self.provider_key]
        import engine.parser.param.vendor as VENDOR_PARAM_PKG
        
        path = VENDOR_PARAM_PKG.__name__
        search_paths = [f"{path}.{module_suffix}"]
        loaded_class = None
        for path in search_paths:
            try:
                module = importlib.import_module(path)
                if hasattr(module, class_name):
                    loaded_class = getattr(module, class_name)
                    log.debug(f"[Vendor Config] Loaded custom mapper '{class_name}' from {path}")
                    break
            except ImportError:
                continue

        if loaded_class:
            return loaded_class
            
        if self.provider_key == "gemini":
            error_msg = f"[CRITICAL] Gemini 전용 매퍼({class_name})를 찾을 수 없습니다. LiteLLM Fallback을 강제 차단합니다. 검색 경로: {search_paths}"
            log.error(error_msg)
            raise RuntimeError(error_msg)

        fallback_class = getattr(config, class_name, None)
        if fallback_class:
            log.debug(f"[Vendor Config] Using fallback configuration from LiteLLM for {self.provider_key}")
            return fallback_class
            
        return getattr(config, "OpenAILikeChatConfig", None)

    def _build_optional_params(self, non_defaults: dict) -> dict:
        req_type = "embeddings" if self.task_type == "embedding" else None
        conf_class = self._get_vendor_config_class()
        conf_inst = conf_class() if conf_class else None
        
        if conf_inst and hasattr(conf_inst, "get_supported_openai_params"):
            supported = conf_inst.get_supported_openai_params(self.model)
        else:
            supported = get_supported_openai_params(model=self.model, custom_llm_provider=self.provider, base_model=self.original_kwargs.get("base_model"), request_type=req_type) or []
            
        allowed_openai = self.original_kwargs.get("allowed_openai_params", [])
        supported.extend(allowed_openai + ["user", "stream_options", "stream", "max_retries"])
        
        unsupported = {k: v for k, v in non_defaults.items() if k not in supported}
        if "n" in unsupported and unsupported["n"] == 1: unsupported.pop("n")
        
        if unsupported:
            if self.drop_flag:
                for k in unsupported.keys(): non_defaults.pop(k, None)
            else:
                raise UnsupportedParamsError(status_code=500, message=f"{self.provider} does not support parameters: {list(unsupported.keys())} for model={self.model}.")

        optional_params = {}
        
        if conf_inst:
            if hasattr(conf_inst, "map_special_auth_params"):
                optional_params = conf_inst.map_special_auth_params(non_default_params=self.raw, optional_params=optional_params)
            
            if self.provider_key == "ollama":
                if any(k in non_defaults for k in ["functions", "function_call", "tools"]):
                    optional_params["format"] = "json"
                    if "tools" in non_defaults:
                        optional_params["functions_unsupported_model"] = non_defaults.pop("tools")
                        non_defaults.pop("tool_choice", None)

            if hasattr(conf_inst, "map_openai_params"):
                optional_params = conf_inst.map_openai_params(non_default_params=non_defaults, optional_params=optional_params, model=self.model, drop_params=self.drop_flag)

        is_openai_compatible = self.provider in ["openai", "azure"] + getattr(config, "openai_compatible_providers", [])
        defaults = DEFAULT_CHAT_COMPLETION_PARAM_VALUES if self.task_type == "chat" else DEFAULT_EMBEDDING_PARAM_VALUES
        add_drops = self.original_kwargs.get("additional_drop_params", [])

        if is_openai_compatible and "extra_body" not in add_drops:
            extra = self.original_kwargs.get("extra_body") or {}
            for k, v in self.raw.items():
                if k not in defaults and v is not None and k not in add_drops:
                    extra[k] = v
            if extra:
                optional_params["extra_body"] = {**optional_params.get("extra_body", {}), **extra}
                if "metadata" in optional_params["extra_body"] and "prompt" in optional_params["extra_body"]["metadata"]:
                    p = optional_params["extra_body"]["metadata"]["prompt"]
                    if p and hasattr(p, "__dict__"): optional_params["extra_body"]["metadata"]["prompt"] = p.__dict__
        else:
            for k, v in self.raw.items():
                if k not in defaults and v is not None and k not in add_drops:
                    optional_params[k] = v

        for k in allowed_openai:
            if k in non_defaults and k not in optional_params:
                optional_params[k] = non_defaults.pop(k)

        for path in [p for p in add_drops if "." in p or "[" in p]:
            _delete_nested_path(optional_params, path)

        return optional_params


class CompletionProcessor(BaseProcessor):
    def __init__(self, model: str, messages: List, kwargs: dict):
        super().__init__(task_type="chat", model=model, raw_kwargs=kwargs)
        self.original_messages = messages

    def _normalize_tools(self, non_defaults: dict):
        if "tools" in non_defaults:
            is_supported = supports_function_calling(self.model, self.provider)
            if self.provider == "gemini" or self.provider_key == "gemini":
                is_supported = True
                
            if not is_supported:
                if self.drop_flag: non_defaults.pop("tools", None); non_defaults.pop("tool_choice", None)
                else: raise UnsupportedParamsError(status_code=500, message=f"Function calling unsupported by {self.provider} ({self.model}).")
            else:
                tools = non_defaults["tools"]
                non_defaults["tools"] = [t.model_dump(exclude_none=True) if isinstance(t, BaseModel) else (t.copy() if isinstance(t, dict) else t) for t in tools]
                for t in non_defaults["tools"]:
                    t.pop("input_examples", None)
                    if "function" in t: t["function"].pop("input_examples", None)
                    params = t.get("function", {}).get("parameters")
                    if params and "additionalProperties" in params and not params["additionalProperties"]:
                        params.pop("additionalProperties", None)
                if not non_defaults["tools"]: non_defaults.pop("tools")

    def _prepare_messages(self) -> List[dict]:
        msgs = []
        for i, m in enumerate(copy.deepcopy(self.original_messages)):
            if not m.get("role"): m["role"] = "assistant"
            if self.provider_key not in ("openai", "azure") and m.get("role") == "developer": m["role"] = "system"
            
            cleaned = {k: v for k, v in (m.model_dump(exclude_none=True) if isinstance(m, BaseModel) else m).items() if v is not None}
            if cleaned["role"] == "user" and isinstance(cleaned.get("content"), list):
                for item in cleaned["content"]:
                    if isinstance(item, dict) and item.get("type") not in ValidUserMessageContentTypes:
                        raise ValueError(f"Invalid content type in user message at index {i}")
            msgs.append(cleaned)
        return msgs

    def build(self) -> CompletionContext:
        msgs = self._prepare_messages()
        
        non_defaults = self._extract_non_defaults()
        self._normalize_tools(non_defaults)
        
        if "response_format" in non_defaults:
            non_defaults["response_format"] = _to_json_schema(non_defaults["response_format"])

        if "stop" in non_defaults and isinstance(non_defaults["stop"], list) and not self.original_kwargs.get("disable_stop_limit", False):
            non_defaults["stop"] = non_defaults["stop"][:4]

        payload = self._build_optional_params(non_defaults)
        
        md = self.original_kwargs.get("metadata", {})
        sid = self.original_kwargs.get("session_id") or md.get("session_id") or md.get("trace_id")
        tid = self.original_kwargs.get("trace_id") or md.get("trace_id") or md.get("session_id")
        
        meta = ExecutionMetadata(
            session_id=sid, trace_id=tid, metadata=md, preset_cache_key=self.original_kwargs.get("preset_cache_key"),
            data_residency=(_OPENAI_REGIONAL_HOSTS.get(urlparse(self.api_base).hostname.lower()) if self.provider == "openai" and self.api_base else None),
            base_model=self.original_kwargs.get("base_model") or self.original_kwargs.get("model_info", {}).get("base_model"),
            prompt_id=self.original_kwargs.get("prompt_id"), framework_flags={k: v for k, v in self.original_kwargs.items() if k in FLAG_KEYS}
        )

        resp = ModelResponse()
        setattr(resp, "usage", Usage())
        if hasattr(resp, "_hidden_params"):
            resp._hidden_params.update({"custom_llm_provider": self.provider, "region_name": self.original_kwargs.get("aws_region_name")})

        return CompletionContext(
            model=self.model, messages=msgs, custom_llm_provider=self.provider, api_key=self.api_key, api_base=self.api_base, 
            timeout=_resolve_timeout(self.original_kwargs, self.provider), model_response=resp, optional_params=payload, system_meta=meta, 
            headers={**self.original_kwargs.get("headers", {}), **(self.original_kwargs.get("extra_headers") or {})}, 
            stream=self.original_kwargs.get("stream", False), acompletion=self.original_kwargs.get("acompletion", False), 
            shared_session=self.original_kwargs.get("shared_session"), client_instance=self.original_kwargs.get("client"),
            deployment_id=self.original_kwargs.get("deployment_id"), original_kwargs=self.original_kwargs
        )

class EmbeddingProcessor(BaseProcessor):
    def __init__(self, model: str, input_data: Union[str, List[str]], kwargs: dict):
        super().__init__(task_type="embedding", model=model, raw_kwargs=kwargs)
        self.input = input_data

    def build(self) -> EmbeddingContext:
        non_defaults = self._extract_non_defaults()
        
        if self.provider == "openai" and "text-embedding-3" not in self.model and "dimensions" in non_defaults:
            if "dimensions" not in self.original_kwargs.get("allowed_openai_params", []):
                if self.drop_flag: non_defaults.pop("dimensions", None)
                else: raise UnsupportedParamsError(status_code=500, message="dimensions not supported for older OpenAI models.")

        payload = self._build_optional_params(non_defaults)

        return EmbeddingContext(
            model=self.model, input=self.input, custom_llm_provider=self.provider, api_key=self.api_key, api_base=self.api_base, 
            timeout=_resolve_timeout(self.original_kwargs, self.provider), aembedding=self.original_kwargs.get("aembedding", False), 
            optional_params=payload, original_kwargs=self.original_kwargs
        )