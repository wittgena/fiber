# anchor.surface.registry.provider
## @lineage: anchor.surface.provider.registry
## @lineage: anchor.provider.registry
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Callable, Union, cast
import httpx

from watcher.plane.emitter import get_emitter 
from phase.bind.resolver import resolve_path 

from bound.channel.config.resolver import config
from anchor.provider.model.info import get_model_info
from anchor.provider.types import ProviderTypes

log = get_emitter("registry.provider")

CONTRACT_ROOT = resolve_path("contract")
REGISTRY_DIR = CONTRACT_ROOT / "registry" / "llms"

# ==========================================
# 1. 원격/로컬 JSON 백업 및 파싱
# ==========================================
def save_local_backup(data: dict, filename: str) -> None:
    """원격에서 성공적으로 가져온 JSON 데이터를 로컬에 백업합니다."""
    try:
        REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
        backup_path = REGISTRY_DIR / filename
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.debug(f"Saved local backup successfully: {backup_path}")
    except Exception as e:
        log.warning(f"Failed to save local backup for {filename}: {e}")

def load_local_backup(filename: str) -> dict:
    """원격 패치 실패 시, 저장된 로컬 백업본을 로드합니다."""
    try:
        backup_path = REGISTRY_DIR / filename
        if backup_path.exists():
            with open(backup_path, "r", encoding="utf-8") as f:
                return json.load(f)
        log.warning(f"Local backup not found at {backup_path}")
    except Exception as e:
        log.error(f"Failed to load local backup for {filename}: {e}")
    return {}

def _optimize_spec_memory(raw_spec: dict) -> dict:
    """희소 데이터(Sparse Data) 제거: False, 0.0, null, 빈 문자열 등을 드랍하여 메모리 점유를 최소화합니다."""
    optimized = {}
    for k, v in raw_spec.items():
        if isinstance(v, dict):
            optimized_sub = _optimize_spec_memory(v)
            if optimized_sub:
                optimized[k] = optimized_sub
        elif v not in (False, 0.0, 0, "", None, [], {}):
            optimized[k] = v
    return optimized

def _expand_model_aliases(model_cost: dict) -> dict:
    """Aliases 리스트를 전개하여 단일 참조로 최상단에 매핑합니다."""
    aliases_to_add: Dict[str, dict] = {}
    keys_with_aliases = []

    for model_name, model_info in model_cost.items():
        if not isinstance(model_info, dict):
            continue
        aliases = model_info.get("aliases")
        if isinstance(aliases, list):
            keys_with_aliases.append(model_name)
            for alias in aliases:
                if alias not in model_cost and alias not in aliases_to_add:
                    aliases_to_add[alias] = model_info 

    for key in keys_with_aliases:
        model_cost[key].pop("aliases", None)

    model_cost.update(aliases_to_add)
    return model_cost

def parse_model_cost_map(raw_data: dict) -> dict:
    """Model Cost Map 전용 통합 파서 훅"""
    expanded = _expand_model_aliases(raw_data)
    for model_name, spec in expanded.items():
        if isinstance(spec, dict):
            expanded[model_name] = _optimize_spec_memory(spec)
    return expanded

def fetch_and_validate_registry(
    url: str,
    filename: str,
    force_local_env_key: str,
    registry_name: str = "Registry",
    min_count: int = 0,
    parser_hook: Optional[Callable[[dict], dict]] = None,
) -> Tuple[dict, dict]:
    source_info = {"source": "local", "url": url, "is_env_forced": False, "fallback_reason": None}
    
    if os.getenv(force_local_env_key, "").lower() == "true":
        source_info["is_env_forced"] = True
        local_data = load_local_backup(filename)
        return (parser_hook(local_data) if parser_hook else local_data), source_info

    try:
        response = httpx.get(url, timeout=5)
        response.raise_for_status()
        content = response.json()
        
        if not isinstance(content, dict) or len(content) < min_count:
            raise ValueError(f"Invalid map format or item count less than min_count({min_count})")

        save_local_backup(content, filename)
        
        source_info["source"] = "remote"
        return (parser_hook(content) if parser_hook else content), source_info

    except Exception as e:
        log.warning(f"[{registry_name}] Failed to fetch remote map. Falling back to local: {e}")
        source_info["fallback_reason"] = str(e)
        local_data = load_local_backup(filename)
        return (parser_hook(local_data) if parser_hook else local_data), source_info

def get_model_cost_map(url: str) -> Tuple[dict, dict]:
    return fetch_and_validate_registry(
        url=url,
        filename="model_prices_and_context_window_backup.json",
        force_local_env_key="LITELLM_LOCAL_MODEL_COST_MAP",
        registry_name="ModelCostMap",
        min_count=50,
        parser_hook=parse_model_cost_map
    )

def get_provider_endpoints_map(url: str) -> Tuple[dict, dict]:
    return fetch_and_validate_registry(
        url=url,
        filename="provider_endpoints_support_backup.json",
        force_local_env_key="LITELLM_LOCAL_PROVIDER_ENDPOINTS",
        registry_name="ProviderEndpoints",
        min_count=5,
        parser_hook=None
    )


# ==========================================
# 2. 런타임 매핑 상태 관리 및 로직
# ==========================================

model_cost_map_url: str = os.getenv(
    "LITELLM_MODEL_COST_MAP_URL",
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json",
)
model_cost, _ = get_model_cost_map(url=model_cost_map_url)

def _get_model_info_helper(
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

    if result.get("input_cost_per_token") is None:
        result["input_cost_per_token"] = 0.0
    if result.get("output_cost_per_token") is None:
        result["output_cost_per_token"] = 0.0

    return result

def register_model(new_model_cost: Union[str, dict]):
    """Dynamically registers new model cost and metadata."""
    loaded_model_cost = {}
    if isinstance(new_model_cost, dict):
        loaded_model_cost = new_model_cost
    elif isinstance(new_model_cost, str):
        loaded_model_cost, _ = get_model_cost_map(url=new_model_cost)

    _skip_get_model_info_providers = {
        ProviderTypes.GITHUB_COPILOT.value,
        ProviderTypes.CHATGPT.value,
    }

    for key, value in loaded_model_cost.items():
        provider = value.get("litellm_provider", "")
        _key_str = str(key)
        
        if provider in _skip_get_model_info_providers or any(
            _key_str.startswith(f"{p}/") for p in _skip_get_model_info_providers
        ):
            existing_model = model_cost.get(key, {})
            model_cost_key = key
        else:
            try:
                existing_model = cast(dict, get_model_info(model=key))
                model_cost_key = existing_model.get("key", key)
            except Exception:
                existing_model = {}
                model_cost_key = key
                
        if existing_model.get("litellm_provider") is None:
            existing_model.pop("litellm_provider", None)
            
        updated_dictionary = _update_dictionary(existing_model, value)
        model_cost.setdefault(model_cost_key, {}).update(updated_dictionary)
        
        _clear_model_info_caches()
        log.debug(f"Added/updated model={model_cost_key} in model_cost")
        
        # 🟢 하드코딩된 litellm 참조를 config.get() 폴백 구조로 우아하게 변경
        if value.get("litellm_provider") == "openai":
            openai_models = config.get("open_ai_chat_completion_models")
            if openai_models is not None:
                openai_models.add(key)
                
        elif value.get("litellm_provider") == "anthropic":
            anthropic_models = config.get("anthropic_models")
            if anthropic_models is not None:
                anthropic_models.add(key)
                
    return loaded_model_cost

def get_provider_for_model(model_name: str) -> str | None:
    model_info = model_cost.get(model_name, {})
    return model_info.get("litellm_provider")

def _update_dictionary(existing_dict: Dict, new_dict: dict) -> dict:
    for k, v in new_dict.items():
        if v is not None:
            if isinstance(v, str):
                existing_dict[k] = _convert_stringified_numbers(v)
            elif isinstance(v, dict):
                existing_nested_dict = existing_dict.get(k)
                if isinstance(existing_nested_dict, dict):
                    existing_nested_dict.update(v)
                    existing_dict[k] = existing_nested_dict
                else:
                    existing_dict[k] = v
            else:
                existing_dict[k] = v
    return existing_dict

def _convert_stringified_numbers(value):
    if isinstance(value, str):
        try:
            if "e" in value.lower() or "." in value:
                return float(value)
            else:
                return int(value)
        except (ValueError, TypeError):
            return value
    return value

def _clear_model_info_caches() -> None:
    try:
        from anchor.provider.model.info import _cached_get_model_info
        _cached_get_model_info.cache_clear()
    except ImportError:
        pass


## Global State Registration (via ConfigResolver)
## config.__setattr__ 내에서 _local_overrides 저장 및 litellm 동기화를 자동 수행
config.model_cost = model_cost
config.register_model = register_model
config._get_model_info_helper = _get_model_info_helper

## 깊은 레거시에서 litellm.utils.* 를 참조하는 경우를 대비한 방어선
try:
    utils = config.get("utils")
    if utils:
        utils.register_model = register_model
        utils._get_model_info_helper = _get_model_info_helper
except Exception:
    pass