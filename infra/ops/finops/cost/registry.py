# fiber.infra.ops.finops.cost.registry
## @lineage: fiber.llm.provider.cost.registry
from __future__ import annotations

import json
from typing import Dict, Tuple, Optional, Union

from xphi.arch.contract.config.env import config
from xphi.node.space.bind.resolver import resolve_path 
from xphi.watcher.plane.emitter import get_emitter 

log_cost = get_emitter("provider.registry")
REGISTRY_ROOT = resolve_path("abc") / "registry"
DEFAULT_REGISTRY_FILENAME = "model_prices_and_context_window.json"
PROVIDER_KEY = "model_provider"


class RegistryIO:
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

class ModelCostRegistry:
    """단일 진입점(Single Source of Truth)으로 비용 레지스트리를 관리합니다."""
    
    _model_cost: dict = RegistryIO.get_model_cost_map()
    _lowercase_model_map: dict = {k.lower(): k for k in _model_cost}
    
    @classmethod
    def get_cost(cls, model_name: str) -> dict:
        return cls._model_cost.get(model_name, {})

    @classmethod
    def get_model_key_case_insensitive(cls, potential_key: str) -> str | None:
        if potential_key in cls._model_cost:
            return potential_key
        return cls._lowercase_model_map.get(potential_key.lower())

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
            if key in cls._model_cost:
                matched_info = cls._model_cost[key]
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
        return cls._model_cost.get(model_name, {}).get(PROVIDER_KEY)

    @classmethod
    def register(cls, new_model_cost: Union[str, dict]) -> dict:
        loaded_cost = {}
        if isinstance(new_model_cost, dict):
            loaded_cost = new_model_cost
        elif isinstance(new_model_cost, str):
            loaded_cost = RegistryIO.get_model_cost_map(filename=new_model_cost)

        for key, value in loaded_cost.items():
            provider = str(value.get(PROVIDER_KEY, "")).strip()
            
            # [수정됨] 스킵 조건 분기가 사라지며 등록 로직이 대폭 간결해짐
            try:
                existing_model = cls.lookup_base_model_info(model=key)
                model_cost_key = existing_model.get("key", key)
            except Exception:
                existing_model = {}
                model_cost_key = key
                    
            if existing_model.get(PROVIDER_KEY) is None:
                existing_model.pop(PROVIDER_KEY, None)
                
            updated_dict = cls._merge_dicts(existing_model, value)
            cls._model_cost.setdefault(model_cost_key, {}).update(updated_dict)
            
            cls._lowercase_model_map[model_cost_key.lower()] = model_cost_key
            
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