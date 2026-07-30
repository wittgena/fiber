# phi.tenant.router.llm
import importlib
import inspect
from typing import Dict, Any, Optional, Set

from tenant.model.cost import get_provider_for_model
from phi.tenant.router.mapper.scan import LLMInstalledScanner

import bound.eco.llms as llm_pkg 
from watcher.plane.emitter import get_emitter

log = get_emitter("llm.router")

_LLM_PKG_NAME = llm_pkg.__name__
_LLM_PKG = _LLM_PKG_NAME.replace(".", "/")

class ModuleMissingError(Exception):
    """해당 모듈이 시스템에 존재하지 않을 때 발생하는 치명적 오류"""
    pass

## @state: Core topological boundaries (Batteries-included)
DEFAULT_LLM_REGISTRY = {
    "openai": {
        "module": f"{_LLM_PKG_NAME}.openai.base",
        "class": "OpenAI",
        "tags": ["gpt-4", "gpt-3.5", "o1"],
        "is_native": True,
        "capabilities": {
            "is_function_calling": True,
            "is_openai_like": True,
            "is_multimodal": True,
            "supports_structured_outputs": True
        },
        "accepted_kwargs": [
            "model", "temperature", "max_tokens", "additional_kwargs", 
            "max_retries", "timeout", "api_key", "api_base", "system_prompt"
        ]
    },
    "anthropic": {
        "module": f"{_LLM_PKG_NAME}.anthropic.base",
        "class": "Anthropic",
        "tags": ["claude"],
        "is_native": True,
        "capabilities": {
            "is_function_calling": True,
            "is_openai_like": False,
            "is_multimodal": True,
            "supports_structured_outputs": True
        },
        "accepted_kwargs": [
            "model", "temperature", "max_tokens", "additional_kwargs", 
            "max_retries", "timeout", "api_key", "system_prompt"
        ]
    },
    "gemini": {
        "module": f"{_LLM_PKG_NAME}.google_genai.base",
        "class": "GoogleGenAI",
        "tags": ["gemini", "vertex_ai-language-models", "vertex_ai"],
        "is_native": True,
        "capabilities": {
            "is_function_calling": True,
            "is_openai_like": False,
            "is_multimodal": True,
            "supports_structured_outputs": True
        },
        "accepted_kwargs": [
            "model", "temperature", "max_tokens", "additional_kwargs", 
            "max_retries", "api_key", "system_prompt"
        ]
    }
}

class LLMRouter:
    def __init__(self, base_pkg: str = _LLM_PKG):
        self.scanner = LLMInstalledScanner(base_pkg=base_pkg)
        self.registry = {k: v.copy() for k, v in DEFAULT_LLM_REGISTRY.items()}
        self._blacklisted_providers: Set[str] = set()
        self._merge_dynamic_registry()

    def _merge_dynamic_registry(self):
        log.debug("[Router] Scanning for dynamically trans-ed modules...")
        scanned_data = self.scanner.scan()
        
        dynamic_count = 0
        for provider, info in scanned_data.items():
            if provider in self.registry and self.registry[provider].get("is_native"):
                if _LLM_PKG_NAME in info["module"] and provider in DEFAULT_LLM_REGISTRY:
                    continue
            
            self.registry[provider] = {
                "module": info["module"],
                "class": info["class"],
                "tags": info.get("tags", [provider]),
                "is_native": False,
                "capabilities": info.get("capabilities", {}),
                "accepted_kwargs": info.get("accepted_kwargs", [])
            }
            dynamic_count += 1
            
        log.info(f"[Router] Registry Ready: {len(DEFAULT_LLM_REGISTRY)} Native, {dynamic_count} Dynamic modules.")

    def _fallback_provider_match(self, model_name: str) -> Optional[str]:
        for provider, meta in self.registry.items():
            if provider in model_name:
                return provider
            if any(tag in model_name for tag in meta["tags"]):
                return provider
        return None

    def route_and_load(self, model_name: str, custom_llm_provider: Optional[str] = None, **kwargs) -> Any:
        ## @resolve: 아키텍처 원칙에 따라, 명시적으로 전달된 파라미터를 1순위로 채택
        provider = custom_llm_provider

        ## @resolve: 외부 JSON 기반의 불투명한 맵핑은 2순위 폴백으로 사용
        if not provider:
            provider = get_provider_for_model(model_name)
            
        ## @resolve: 태그 기반 스캔을 3순위로 사용
        if not provider:
            provider = self._fallback_provider_match(model_name)
            
        if not provider:
            raise ModuleMissingError(f"[Error] 모델 '{model_name}'에 대한 Provider를 식별할 수 없습니다.")

        ## @circuit_breaker: 이미 누락이 판명된 모듈이라면 긴 스택 트레이스 없이 즉시 차단
        if provider in self._blacklisted_providers:
            raise ModuleMissingError(
                f"[Fast-Fail] Provider '{provider}' 누락이 확인된 상태입니다. 무의미한 재시도를 차단합니다."
            )

        meta = self.registry.get(provider)
        if not meta:
            ## @rupture: 최초 실패 시 블랙리스트에 등록하고 상세 안내를 출력
            self._blacklisted_providers.add(provider)
            raise ModuleMissingError(
                f"\n[Brane Integration Error] Module '{provider}' is missing from the manifold.\n"
                f"This topology is not natively embedded.\n"
                f"Dynamically transduce via CLI: `python -m trans.llama --category llms --name {provider}`\n"
            )

        module_path = meta["module"]
        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            raise ImportError(f"[Router] Failed to materialize module '{module_path}': {e}")
        
        LLMClass = None
        expected_class_name = meta.get("class")

        if expected_class_name and hasattr(module, expected_class_name):
            LLMClass = getattr(module, expected_class_name)
        else:
            if expected_class_name:
                log.warning(f"[Router] 지정된 클래스 '{expected_class_name}'를 찾을 수 없습니다. 동적 클래스 추론을 시도합니다.")
            
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if obj.__module__ == module.__name__:
                    if name.endswith("LLM") or hasattr(obj, 'chat') or hasattr(obj, 'complete'):
                        LLMClass = obj
                        log.info(f"[Router] 동적 추론 성공: '{name}' 클래스를 LLM 구현체로 바인딩합니다.")
                        break

        if not LLMClass:
            raise RuntimeError(f"[Router] '{module_path}' 내부에서 실행 가능한 LLM 클래스를 찾을 수 없습니다.")

        accepted_kwargs = meta.get("accepted_kwargs", [])
        if accepted_kwargs:
            valid_kwargs = {k: v for k, v in kwargs.items() if k in accepted_kwargs}
            dropped_kwargs = set(kwargs.keys()) - set(valid_kwargs.keys())
            
            if dropped_kwargs:
                log.warning(f"[Router] Filtered unsupported kwargs for {provider} ({model_name}): {dropped_kwargs}")
            
            return LLMClass(model=model_name, **valid_kwargs)
        
        return LLMClass(model=model_name, **kwargs)

    def get_llm_tool_schema(self) -> Dict[str, Any]:
        return {
            "name": "llm_model_router",
            "description": "Dynamically instantiates and returns an LLM execution object based on model topology.",
            "available_providers": list(self.registry.keys())
        }