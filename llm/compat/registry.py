# fiber.llm.compat.registry
import json
import copy
from pathlib import Path
from typing import Type, Dict, Tuple, List, Any
from pydantic import ValidationError

from fiber.llm.compat.schema import ExtractionRuleSchema, ProviderParamRuleSchema
from fiber.llm.compat.param import PROVIDER_PARAM_RULES as BASE_PROVIDER_PARAM_RULES
from fiber.llm.compat.extraction import EXTRACTION_RULES as BASE_EXTRACTION_RULES

from xphi.arch.contract.config.env import FIBER_COMPAT_RULES_PATH
from xphi.node.space.bind.resolver import resolve_path 
from xphi.watcher.plane.emitter import get_emitter 

log_compat = get_emitter("compat.registry")

COMPAT_REGISTRY_ROOT = resolve_path("abc") / "registry"
DEFAULT_COMPAT_FILENAME = "provider_compat_rules.json"


class CompatRegistryIO:
    @classmethod
    def resolve_target_path(cls, filename: str = DEFAULT_COMPAT_FILENAME) -> Path:
        custom_path = FIBER_COMPAT_RULES_PATH.strip() if FIBER_COMPAT_RULES_PATH else None
        if not custom_path:
            return COMPAT_REGISTRY_ROOT / filename
            
        p = Path(custom_path)
        return p / filename if (p.is_dir() or not p.suffix) else p

    @classmethod
    def load_registry(cls, filename: str = DEFAULT_COMPAT_FILENAME) -> dict:
        target_path = cls.resolve_target_path(filename)
        
        if not target_path.exists():
            log_compat.debug(f"No custom compat registry found at '{target_path}'. Using base rules only.")
            return {}
            
        if not target_path.is_file():
            log_compat.error(f"Target path '{target_path}' is a directory, not a file.")
            return {}

        try:
            with open(target_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log_compat.error(f"Failed to load compat registry at '{target_path}': {e}")
            return {}

    @classmethod
    def validate_and_filter(
        cls, 
        raw_rules: dict, 
        schema_model: Type[Any], 
        rule_name: str
    ) -> Tuple[Dict[str, Any], List[str], List[str]]:
        valid_updates = {}
        success_providers = []
        error_messages = []

        if not isinstance(raw_rules, dict) or not raw_rules:
            return valid_updates, success_providers, error_messages

        for provider, rule_data in raw_rules.items():
            try:
                validated = schema_model(**rule_data).model_dump(exclude_unset=True)
                valid_updates[provider] = validated
                success_providers.append(provider)
            except ValidationError as e:
                error_messages.append(
                    f"[{rule_name} - {provider}] Format validation failed: {e.errors(include_url=False)}"
                )
                
        return valid_updates, success_providers, error_messages

    @classmethod
    def deep_merge(cls, base: dict, override: dict) -> dict:
        """베이스 룰셋을 복사한 뒤 오버라이드 룰을 재귀적으로 병합하여 반환"""
        merged = copy.deepcopy(base)
        
        def _update(target: dict, src: dict):
            for k, v in src.items():
                if isinstance(v, dict) and isinstance(target.get(k), dict):
                    _update(target[k], v)
                else:
                    target[k] = v
                    
        _update(merged, override)
        return merged


def _initialize_compat_rules() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    custom_data = CompatRegistryIO.load_registry()

    valid_extraction, ext_ok, ext_err = CompatRegistryIO.validate_and_filter(
        custom_data.get("extraction_rules", {}), 
        ExtractionRuleSchema, 
        "EXTRACTION"
    )
    valid_params, param_ok, param_err = CompatRegistryIO.validate_and_filter(
        custom_data.get("provider_param_rules", {}), 
        ProviderParamRuleSchema, 
        "PARAM"
    )

    for err_msg in (ext_err + param_err):
        log_compat.warning(err_msg)

    summary = []
    if ext_ok:
        summary.append(f"EXTRACTION({','.join(ext_ok)})")
    if param_ok:
        summary.append(f"PARAM({','.join(param_ok)})")
    if summary:
        log_compat.info(f"Custom compat rules injected -> {' | '.join(summary)}")

    extraction_rules = CompatRegistryIO.deep_merge(BASE_EXTRACTION_RULES, valid_extraction)
    param_rules = CompatRegistryIO.deep_merge(BASE_PROVIDER_PARAM_RULES, valid_params)
    
    return extraction_rules, param_rules

# Global Export
EXTRACTION_RULES, PROVIDER_PARAM_RULES = _initialize_compat_rules()