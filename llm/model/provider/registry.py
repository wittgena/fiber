# fiber.llm.model.provider.registry
from __future__ import annotations

import json
from typing import Dict, Tuple, Optional, Union

from fiber.llm.model.types.support import ProviderTypes

from xphi.arch.model.config import config
from xphi.kernel.space.bind.resolver import resolve_path 
from xphi.watcher.plane.emitter import get_emitter 

log_cost = get_emitter("provider.registry")
REGISTRY_ROOT = resolve_path("registry") / "llms"
DEFAULT_REGISTRY_FILENAME = "model_prices_and_context_window.json"
PROVIDER_KEY = "model_provider"


# =====================================================================
# 1. Provider Key Resolution
# =====================================================================
class ProviderKeyResolver:
    """@desc: Helper to resolve provider logic and skip-patterns before registration."""
    
    SKIP_PROVIDERS = frozenset({ProviderTypes.GITHUB_COPILOT.value, ProviderTypes.CHATGPT.value})

    @classmethod
    def extract_provider(cls, spec: dict) -> str:
        # 💡 상수를 통해 안전하게 프로바이더 값 추출
        return str(spec.get(PROVIDER_KEY, "")).strip()

    @classmethod
    def should_skip_model_lookup(cls, provider: str, model_key: str) -> bool:
        if provider in cls.SKIP_PROVIDERS:
            return True
        if any(model_key.startswith(f"{p}/") for p in cls.SKIP_PROVIDERS):
            return True
        return False


# =====================================================================
# 2. Local Registry I/O
# =====================================================================
class RegistryIO:
    """@desc: Namespace for local registry file operations (Network I/O removed)."""
    
    @classmethod
    def save_registry(cls, data: dict, filename: str = DEFAULT_REGISTRY_FILENAME) -> None:
        try:
            REGISTRY_ROOT.mkdir(parents=True, exist_ok=True)
            target_path = REGISTRY_ROOT / filename
            with open(target_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            log_cost.debug(f"Saved local registry successfully: {target_path}")
        except Exception as e:
            log_cost.warning(f"Failed to save local registry for {filename}: {e}")

    @classmethod
    def load_registry(cls, filename: str = DEFAULT_REGISTRY_FILENAME) -> dict:
        try:
            target_path = REGISTRY_ROOT / filename
            if target_path.exists():
                with open(target_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            log_cost.warning(f"Local registry not found at {target_path}. Returning empty map.")
        except Exception as e:
            log_cost.error(f"Failed to load local registry {filename}: {e}")
        return {}

    @classmethod
    def _optimize_memory(cls, raw_spec: dict) -> dict:
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
    def get_model_cost_map(cls, filename: str = DEFAULT_REGISTRY_FILENAME) -> dict:
        raw_data = cls.load_registry(filename)
        if not raw_data:
            return {}
            
        expanded = cls._expand_aliases(raw_data)
        for model_name, spec in expanded.items():
            if isinstance(spec, dict):
                expanded[model_name] = cls._optimize_memory(spec)
        return expanded

# Global Initialization
model_cost = RegistryIO.get_model_cost_map()


# =====================================================================
# 3. Model Cost Registry Engine
# =====================================================================
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
            log_cost.warning(f"Model not mapped in local model_cost. model={model}, provider={custom_llm_provider}")
            return {"key": model, "input_cost_per_token": 0.0, "output_cost_per_token": 0.0}

        result = matched_info.copy()
        result["key"] = matched_key
        result.setdefault("input_cost_per_token", 0.0)
        result.setdefault("output_cost_per_token", 0.0)
        return result

    @classmethod
    def get_provider(cls, model_name: str) -> str | None:
        # 💡 상수를 통해 접근
        return model_cost.get(model_name, {}).get(PROVIDER_KEY)

    @classmethod
    def register(cls, new_model_cost: Union[str, dict]) -> dict:
        loaded_cost = {}
        if isinstance(new_model_cost, dict):
            loaded_cost = new_model_cost
        elif isinstance(new_model_cost, str):
            loaded_cost = RegistryIO.get_model_cost_map(filename=new_model_cost)

        for key, value in loaded_cost.items():
            key_str = str(key)
            provider = ProviderKeyResolver.extract_provider(value)
            
            if ProviderKeyResolver.should_skip_model_lookup(provider, key_str):
                existing_model = model_cost.get(key, {})
                model_cost_key = key
            else:
                try:
                    existing_model = cls.lookup_base_model_info(model=key)
                    model_cost_key = existing_model.get("key", key)
                except Exception:
                    existing_model = {}
                    model_cost_key = key
                    
            # 💡 기존 모델 정보에 병합 전, 중복 키(PROVIDER_KEY)를 깔끔하게 제거
            if existing_model.get(PROVIDER_KEY) is None:
                existing_model.pop(PROVIDER_KEY, None)
                
            updated_dict = cls._merge_dicts(existing_model, value)
            model_cost.setdefault(model_cost_key, {}).update(updated_dict)
            
            log_cost.debug(f"Added/updated model={model_cost_key} in model_cost")
            cls._update_provider_models(key, provider)
            
        return loaded_cost

    @classmethod
    def _merge_dicts(cls, existing: dict, new_dict: dict) -> dict:
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
        if provider == "openai":
            openai_models = config.get("open_ai_chat_completion_models")
            if openai_models is not None:
                openai_models.add(key)
        elif provider == "anthropic":
            anthropic_models = config.get("anthropic_models")
            if anthropic_models is not None:
                anthropic_models.add(key)


# ==========================================
# Exports & Legacy Bindings
# ==========================================
get_model_cost_registry = ModelCostRegistry.get_provider
lookup_base_model_info = ModelCostRegistry.lookup_base_model_info
register_model = ModelCostRegistry.register

config.model_cost = model_cost
config.register_model = register_model
config._get_model_info_helper = lookup_base_model_info