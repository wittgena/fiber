# bound.resolver.model.cost
from __future__ import annotations
import json
import os
import httpx
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Callable, Union
import time
from email.utils import formatdate

from bound.resolver.model.protype import ProviderTypes
from bound.resolver.model.config.resolver import config

from phase.bind.resolver import resolve_path 
from watcher.plane.emitter import get_emitter 

log = get_emitter("registry.model")

REGISTRY_ROOT = resolve_path("registry") / "llms"
_DEFAULT_MAP_URL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
model_cost_map_url: str = os.getenv("LITELLM_MODEL_COST_MAP_URL", _DEFAULT_MAP_URL)

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
            log.debug(f"Saved local backup successfully: {backup_path}")
        except Exception as e:
            log.warning(f"Failed to save local backup for {filename}: {e}")

    @classmethod
    def load_backup(cls, filename: str) -> dict:
        """@desc: Retrieves the local backup payload from disk."""
        try:
            backup_path = REGISTRY_ROOT / filename
            if backup_path.exists():
                with open(backup_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            log.warning(f"Local backup not found at {backup_path}")
        except Exception as e:
            log.error(f"Failed to load local backup for {filename}: {e}")
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
                log.debug(f"[{registry_name}] Using valid local cache (TTL < {cache_ttl_seconds}s)")
                local_data = cls.load_backup(filename)
                if local_data and len(local_data) >= min_count:
                    source_info["source"] = "local_cache_ttl"
                    return (parser_hook(local_data) if parser_hook else local_data), source_info

            http_date = formatdate(timeval=mtime, localtime=False, usegmt=True)
            headers["If-Modified-Since"] = http_date

        try:
            response = httpx.get(url, headers=headers, timeout=5)
            if response.status_code == 304:
                log.debug(f"[{registry_name}] Remote map not modified (304). Updating local cache timestamp.")
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
            log.warning(f"[{registry_name}] Failed to fetch remote map. Falling back to stale local cache: {e}")
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