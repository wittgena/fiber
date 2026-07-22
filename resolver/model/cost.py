# resolver.model.cost
## @lineage: bound.resolver.model.cost
## @lineage: bound.registry.model.cost
## @lineage: anchor.registry.model.cost
## @lineage: anchor.registry.io
from __future__ import annotations
import json
import os
import httpx
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Callable, Union

from resolver.io import RegistryIO
from eco.legacy.info import ProviderTypes
from resolver.model.config.resolver import config

from phase.bind.resolver import resolve_path 
from watcher.plane.emitter import get_emitter 

log = get_emitter("registry.model")

"""@phase.2: Runtime State Initialization"""
_DEFAULT_MAP_URL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
model_cost_map_url: str = os.getenv("LITELLM_MODEL_COST_MAP_URL", _DEFAULT_MAP_URL)

## @desc: Global cost map mutated at runtime. Exported for external modules.
model_cost, _ = RegistryIO.get_model_cost_map(url=model_cost_map_url)

"""@phase.3: Registry Management & Operations"""
class ModelCostRegistry:
    """@desc: Manager for mutating, resolving, and retrieving info from the global `model_cost` map"""

    @classmethod
    def lookup_base_model_info(
        cls,
        model: str,
        custom_llm_provider: Optional[str] = None,
        api_base: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> dict:
        """@desc: Resolves raw model metadata (cost, context) from the global map."""
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
            log.warning(f"Model not mapped in model_cost. model={model}, provider={custom_llm_provider}")
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
                    # [수정됨] 외부 의존성(get_model_info) 호출 및 타입 강제 변환(cast)을 제거하고 
                    # 순수 dict를 반환하는 내부 클래스 메서드를 사용합니다.
                    existing_model = cls.lookup_base_model_info(model=key)
                    model_cost_key = existing_model.get("key", key)
                except Exception:
                    existing_model = {}
                    model_cost_key = key
                    
            if existing_model.get("litellm_provider") is None:
                existing_model.pop("litellm_provider", None)
                
            updated_dict = cls._merge_dicts(existing_model, value)
            model_cost.setdefault(model_cost_key, {}).update(updated_dict)
            
            log.debug(f"Added/updated model={model_cost_key} in model_cost")
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

"""
@phase: External Capability Facade
@desc: Maintains backward compatibility for external module imports.
"""
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