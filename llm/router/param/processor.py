# fiber.llm.router.param.processor
from __future__ import annotations
import copy
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Tuple, Union
import httpx
from pydantic import BaseModel
from openai.lib import _parsing, _pydantic

from xphi.arch.bound.client.constants import COMPLETION_HTTP_FALLBACK_SECONDS, DEFAULT_REQUEST_TIMEOUT_SECONDS, REQUEST_TIMEOUT, DEFAULT_CHAT_COMPLETION_PARAM_VALUES, DEFAULT_EMBEDDING_PARAM_VALUES
from fiber.llm.model.provider.resolver import _resolver_instance
from fiber.llm.types.provider.core import Usage
from fiber.llm.exception.eco import UnsupportedParamsError
from fiber.llm.types.provider.openai import ValidUserMessageContentTypes
from fiber.llm.param import ModelResponse
from fiber.gateway.llm.context.metadata import ExecutionMetadata, CompletionContext, EmbeddingContext

from xphi.arch.contract.config.resolver import config
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("param.processor")

FRAMEWORK_KWARGS = {
    "metadata", "session_id", "trace_id", "call_id", "completion_call_id", "preset_cache_key", "model_info", 
    "model_alias_map", "proxy_server_request", "input_cost_per_token", "output_cost_per_token", 
    "input_cost_per_second", "output_cost_per_second", "cost_per_query", "prompt_id", "prompt_variables",
    "timeout", "request_timeout", "client", "shared_session", "acompletion", "aembedding", "headers", 
    "extra_headers", "custom_llm_provider", "api_key", "api_base", "base_url", "deployment_id", "azure", 
    "aws_region_name", "supports_system_message", "litellm_system_prompt", "base_model",
    "drop_params", "allowed_openai_params", "additional_drop_params", "context_management"
}
AUTH_PREFIXES = ("aws_", "azure_", "vertex_", "tenant_id", "client_id", "client_secret", "bucket_name")
FLAG_KEYS = {
    "no_log", "no-log", "custom_prompt_dict", "async_call", "ssl_verify", "merge_reasoning_content_in_choices", 
    "use_litellm_proxy", "logger_fn", "verbose", "disable_add_transform_inline_image_block", "log_delegator"
}

PROVIDER_ALIAS = {"vertex_ai_beta": "vertex_ai", "text-completion-openai": "openai", "azure_ai": "azure", "ollama_chat": "ollama"}
_OPENAI_REGIONAL_HOSTS = {"eu.api.openai.com": "eu", "us.api.openai.com": "us"}

PROVIDER_PARAM_RULES = {
    "defaults": {
        "supported": ["temperature", "top_p", "n", "stream", "stop", "max_tokens", "presence_penalty", "frequency_penalty", "user", "tools", "tool_choice", "logprobs", "top_logprobs", "response_format", "seed"],
        "mapping": {},
        "wrap_in": {},
        "tool_format": "standard"
    },
    "gemini": {
        "supported": ["temperature", "top_p", "top_k", "max_tokens", "max_completion_tokens", "stream", "tools", "tool_choice", "response_format", "n", "stop", "presence_penalty", "frequency_penalty"],
        "mapping": {
            "max_tokens": "max_output_tokens",
            "max_completion_tokens": "max_output_tokens",
            "stop": "stop_sequences"
        },
        "wrap_in": {},
        "tool_format": "gemini_strict"
    },
    "ollama": {
        "supported": ["temperature", "top_p", "top_k", "stream", "tools", "format", "options", "num_ctx", "seed"],
        "mapping": {},
        "wrap_in": {"options": ["temperature", "top_p", "top_k", "num_ctx", "seed"]},
        "tool_format": "standard"
    }
}

def _delete_nested_path(data: Dict, path: str):
    try:
        keys = path.split('.')
        curr = data
        for i, k in enumerate(keys):
            if i == len(keys) - 1:
                if isinstance(curr, dict):
                    curr.pop(k, None)
            else:
                if isinstance(curr, dict) and k in curr:
                    curr = curr[k]
                else:
                    break  # 경로가 끊기면 안전하게 중단
    except Exception:
        pass


def _to_json_schema(model_or_dict: Any) -> Optional[dict]:
    if not model_or_dict: return None
    if isinstance(model_or_dict, dict): return model_or_dict
    if not _parsing._completions.is_basemodel_type(model_or_dict): return model_or_dict
    return {
        "type": "json_schema",
        "json_schema": {"schema": _pydantic.to_strict_json_schema(model_or_dict), "name": model_or_dict.__name__, "strict": True}
    }

## for preserve for comparison
# def _delete_nested_path(data: Dict, path: str):
#     try:
#         segments = re.findall(r"[^\.\[]+|\[[^\]]*\]", path)
#         curr = data
#         for i, seg in enumerate(segments):
#             is_last = (i == len(segments) - 1)
#             key = int(seg[1:-1]) if seg.startswith("[") else seg
#             if is_last:
#                 if isinstance(curr, dict): curr.pop(key, None)
#                 elif isinstance(curr, list) and isinstance(key, int) and 0 <= key < len(curr): curr.pop(key)
#             else:
#                 curr = curr[key]
#     except Exception: pass

class BaseProcessor:
    def __init__(self, task_type: str, model: str, raw_kwargs: dict):
        self.task_type = task_type
        self.original_model = model
        
        self.raw = {k: v for k, v in raw_kwargs.items() if k not in FRAMEWORK_KWARGS and k not in FLAG_KEYS and not k.startswith(AUTH_PREFIXES)}
        self.raw["max_retries"] = raw_kwargs.get("max_retries", raw_kwargs.get("num_retries"))
        self.original_kwargs = raw_kwargs.copy()
        
        self.provider, self.api_key, self.api_base, self.model = self._resolve_routing()
        self.provider_key = PROVIDER_ALIAS.get(self.provider, self.provider)
        self.drop_flag = self.original_kwargs.get("drop_params", getattr(config, "drop_params", False))
        
    def _resolve_routing(self) -> Tuple[str, Optional[str], Optional[str], str]:
        custom_prov = self.original_kwargs.get("custom_llm_provider")
        api_base = self.original_kwargs.get("api_base") or self.original_kwargs.get("base_url")
        api_key = None if self.original_kwargs.get("api_key") == "not-needed" else self.original_kwargs.get("api_key")
        deployment_id = self.original_kwargs.get("deployment_id")
        target_model = deployment_id if deployment_id else self.original_model

        if self.original_kwargs.get("azure", False) or deployment_id: custom_prov = "azure"
        res_model, res_prov, dyn_key, res_base = _resolver_instance.resolve(
            model=target_model, custom_llm_provider=custom_prov, api_base=api_base, api_key=api_key
        )
        return res_prov, (dyn_key or api_key), res_base, res_model

    def _resolve_timeout(self) -> Union[float, httpx.Timeout]:
        val = self.original_kwargs.get("timeout") or self.original_kwargs.get("request_timeout") or REQUEST_TIMEOUT
        if val in [None, DEFAULT_REQUEST_TIMEOUT_SECONDS]: return COMPLETION_HTTP_FALLBACK_SECONDS
        
        if isinstance(val, httpx.Timeout) and not _resolver_instance.supports_httpx_timeout(self.provider):
            return float(val.read) if val.read is not None else COMPLETION_HTTP_FALLBACK_SECONDS
        return float(val) if not isinstance(val, httpx.Timeout) else val

    def _extract_non_defaults(self) -> dict:
        defaults = DEFAULT_CHAT_COMPLETION_PARAM_VALUES if self.task_type == "chat" else DEFAULT_EMBEDDING_PARAM_VALUES
        drops = ["messages"] if self.task_type == "chat" else ["input"]
        ignore_keys = ["model", "custom_llm_provider", "api_version"] + drops
        add_drops = self.original_kwargs.get("additional_drop_params", [])
        
        return {
            k: v for k, v in self.raw.items()
            if k not in ignore_keys and k in defaults and v != defaults[k] and k not in add_drops
        }

    def _build_optional_params(self, non_defaults: dict) -> dict:
        rules = PROVIDER_PARAM_RULES.get(self.provider_key, PROVIDER_PARAM_RULES["defaults"])
        supported_keys = set(rules.get("supported", []))
        allowed_openai = self.original_kwargs.get("allowed_openai_params", [])
        supported_keys.update(allowed_openai)
        supported_keys.update(["user", "stream_options", "stream", "max_retries", "extra_body", "extra_headers"])

        unsupported = {k: v for k, v in non_defaults.items() if k not in supported_keys}
        if "n" in unsupported and unsupported["n"] == 1: unsupported.pop("n")
        
        if unsupported:
            if self.drop_flag:
                for k in list(unsupported.keys()): non_defaults.pop(k, None)
            else:
                raise UnsupportedParamsError(status_code=500, message=f"{self.provider} does not support parameters: {list(unsupported.keys())} for model={self.model}.")

        optional_params = {}
        mapping_rule = rules.get("mapping", {})
        
        # 선언적 1:1 맵핑
        for key, val in non_defaults.items():
            mapped_key = mapping_rule.get(key, key)
            optional_params[mapped_key] = val

        # 선언적 구조화 (Wrap in Dict - 예: Ollama의 options)
        wrap_rule = rules.get("wrap_in", {})
        for wrap_key, target_keys in wrap_rule.items():
            wrap_dict = optional_params.get(wrap_key, {})
            for tk in target_keys:
                if tk in optional_params:
                    wrap_dict[tk] = optional_params.pop(tk)
            if wrap_dict:
                optional_params[wrap_key] = wrap_dict

        # Ollama 특수 룰 (Tools 존재 시 JSON 포맷 강제)
        if self.provider_key == "ollama" and any(k in optional_params for k in ["functions", "tools", "function_call"]):
            optional_params["format"] = "json"

        # Passthrough (extra_body) 처리
        is_openai_compatible = self.provider in ["openai", "azure"] + getattr(config, "openai_compatible_providers", [])
        if is_openai_compatible:
            extra = self.original_kwargs.get("extra_body", {})
            if extra:
                optional_params["extra_body"] = {**optional_params.get("extra_body", {}), **extra}

        # 6. ✨ 명시적 드롭 경로 처리 (단순화된 삭제 로직 적용)
        add_drops = self.original_kwargs.get("additional_drop_params", [])
        for path in add_drops:
            if "." in path:
                _delete_nested_path(optional_params, path)
            else:
                optional_params.pop(path, None)

        return optional_params


class CompletionProcessor(BaseProcessor):
    def __init__(self, model: str, messages: List, kwargs: dict):
        super().__init__(task_type="chat", model=model, raw_kwargs=kwargs)
        self.original_messages = messages

    @staticmethod
    def _gemini_strict_tool_transform(schema: Any):
        if not isinstance(schema, dict): return
        
        if "type" in schema and isinstance(schema["type"], str):
            schema["type"] = schema["type"].upper()
        
        for k in ["summary", "title", "required", "additionalProperties"]:
            schema.pop(k, None)
            
        for key, value in schema.items():
            if isinstance(value, dict):
                CompletionProcessor._gemini_strict_tool_transform(value)
            elif isinstance(value, list):
                for item in value:
                    CompletionProcessor._gemini_strict_tool_transform(item)

    def _normalize_tools(self, non_defaults: dict, rules: dict):
        if "tools" not in non_defaults:
            return

        is_supported = _resolver_instance.supports_function_calling(self.model, self.provider)
        if self.provider_key == "gemini": is_supported = True
            
        if not is_supported:
            if self.drop_flag: 
                non_defaults.pop("tools", None)
                non_defaults.pop("tool_choice", None)
            else: 
                raise UnsupportedParamsError(status_code=500, message=f"Function calling unsupported by {self.provider} ({self.model}).")
            return

        tools = non_defaults["tools"]
        formatted_tools = [t.model_dump(exclude_none=True) if isinstance(t, BaseModel) else (t.copy() if isinstance(t, dict) else t) for t in tools]
        
        # 기본 OpenAI 표준 클리닝
        for t in formatted_tools:
            t.pop("input_examples", None)
            if "function" in t: t["function"].pop("input_examples", None)
            params = t.get("function", {}).get("parameters")
            if params and "additionalProperties" in params and not params["additionalProperties"]:
                params.pop("additionalProperties", None)

        tool_format = rules.get("tool_format", "standard")
        
        # 🚨 [특수 룰] Gemini Protobuf 규격 변환 트리거
        if tool_format == "gemini_strict":
            gemini_declarations = []
            for t in formatted_tools:
                if t.get("type") == "function" and "function" in t:
                    func = dict(t["function"]) 
                    func.pop("summary", None)
                    params = func.get("parameters", {})
                    if params:
                        self._gemini_strict_tool_transform(params)
                        func["parameters"] = params
                    gemini_declarations.append(func)
            
            non_defaults["tools"] = [{"function_declarations": gemini_declarations}] if gemini_declarations else []
            
            # tool_choice 구조 매핑
            tc = non_defaults.pop("tool_choice", None)
            if tc and tc != "auto" and isinstance(tc, dict) and tc.get("function", {}).get("name"):
                non_defaults["tool_config"] = {
                    "function_calling_config": {
                        "mode": "ANY",
                        "allowed_function_names": [tc["function"]["name"]]
                    }
                }
        else:
            non_defaults["tools"] = formatted_tools

        if not non_defaults.get("tools"): 
            non_defaults.pop("tools", None)

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
        
        # 룰 레지스트리 획득 및 툴 정규화 주입
        rules = PROVIDER_PARAM_RULES.get(self.provider_key, PROVIDER_PARAM_RULES["defaults"])
        self._normalize_tools(non_defaults, rules)
        
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
            timeout=self._resolve_timeout(), model_response=resp, optional_params=payload, system_meta=meta, 
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
            timeout=self._resolve_timeout(), aembedding=self.original_kwargs.get("aembedding", False), 
            optional_params=payload, original_kwargs=self.original_kwargs
        )