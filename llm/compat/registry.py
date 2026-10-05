# fiber.llm.compat.registry
import os
import json
import copy
from pathlib import Path
from pydantic import ValidationError
from typing import Type, Dict, Tuple, List, Any

from fiber.llm.compat.schema import (
    StateExtractionRuleSchema,
    StreamExtractionRuleSchema,
    ProviderParamRuleSchema
)
from fiber.llm.compat.state import STATE_EXTRACTION_RULES as BASE_STATE_EXTRACTION_RULES
from fiber.llm.compat.param import PROVIDER_PARAM_RULES as BASE_PROVIDER_PARAM_RULES
from fiber.llm.compat.stream import STREAM_EXTRACTION_RULES as BASE_STREAM_EXTRACTION_RULES

from xphi.arch.contract.config.env import FIBER_COMPAT_RULES_PATH
from xphi.kernel.space.bind.resolver import resolve_path 
from xphi.watcher.plane.emitter import get_emitter 

log_compat = get_emitter("compat.registry")

COMPAT_REGISTRY_ROOT = resolve_path("abc") / "registry"
DEFAULT_COMPAT_FILENAME = "provider_compat_rules.json"

class CompatRegistryIO:
    @classmethod
    def load_registry(cls, filename: str = DEFAULT_COMPAT_FILENAME) -> dict:
        custom_path = FIBER_COMPAT_RULES_PATH.strip() if FIBER_COMPAT_RULES_PATH else None
        target_path = Path(custom_path) if custom_path else COMPAT_REGISTRY_ROOT / filename

        if custom_path:
            p = Path(custom_path)
            if p.is_dir() or not p.suffix:
                target_path = p / filename
            else:
                target_path = p
        else:
            target_path = COMPAT_REGISTRY_ROOT / filename
        
        try:
            if target_path.exists():
                if target_path.is_file():
                    with open(target_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    return data
                else:
                    log_compat.error(f"Target path '{target_path}' is a directory, not a file.")
            else:
                log_compat.debug(f"No custom compat registry found at '{target_path}'. Using base rules only.")
        except Exception as e:
            log_compat.error(f"Failed to load compat registry at '{target_path}': {e}")
        
        return {}

    @classmethod
    def validate_and_filter(cls, raw_rules: dict, schema_model: Type[Any], rule_name: str) -> Tuple[Dict, List[str], List[str]]:
        valid_updates = {}
        success_providers = []
        error_messages = []

        if not raw_rules:
            return valid_updates, success_providers, error_messages

        for provider, rule_data in raw_rules.items():
            try:
                validated = schema_model(**rule_data).model_dump(exclude_unset=True)
                valid_updates[provider] = validated
                success_providers.append(provider)
            except ValidationError as e:
                error_messages.append(f"[{rule_name} - {provider}] Format validation failed: {e.errors(include_url=False)}")
                
        return valid_updates, success_providers, error_messages

    @classmethod
    def _deep_update(cls, base: dict, override: dict) -> dict:
        """재귀적으로 딕셔너리를 병합하여 하위 키가 날아가는 것을 방지"""
        for k, v in override.items():
            if isinstance(v, dict) and k in base and isinstance(base[k], dict):
                cls._deep_update(base[k], v)
            else:
                base[k] = v
        return base


# Global Initialization (부팅 시 1회 로드, 검증 및 병합)
_custom_compat_data = CompatRegistryIO.load_registry()
valid_state_rules, state_ok, state_err = CompatRegistryIO.validate_and_filter(
    _custom_compat_data.get("state_extraction_rules", {}), 
    StateExtractionRuleSchema, 
    "STATE"
)
valid_stream_rules, stream_ok, stream_err = CompatRegistryIO.validate_and_filter(
    _custom_compat_data.get("stream_extraction_rules", {}), 
    StreamExtractionRuleSchema, 
    "STREAM"
)
valid_param_rules, param_ok, param_err = CompatRegistryIO.validate_and_filter(
    _custom_compat_data.get("provider_param_rules", {}), 
    ProviderParamRuleSchema, 
    "PARAM"
)

all_errors = state_err + stream_err + param_err
for err_msg in all_errors:
    log_compat.warning(err_msg)

success_summary = []
if state_ok: success_summary.append(f"STATE({','.join(state_ok)})")
if stream_ok: success_summary.append(f"STREAM({','.join(stream_ok)})")
if param_ok: success_summary.append(f"PARAM({','.join(param_ok)})")

if success_summary:
    log_compat.info(f"Custom compat rules injected safely -> {' | '.join(success_summary)}")

STATE_EXTRACTION_RULES = CompatRegistryIO._deep_update(copy.deepcopy(BASE_STATE_EXTRACTION_RULES), valid_state_rules)
STREAM_EXTRACTION_RULES = CompatRegistryIO._deep_update(copy.deepcopy(BASE_STREAM_EXTRACTION_RULES), valid_stream_rules)
PROVIDER_PARAM_RULES = CompatRegistryIO._deep_update(copy.deepcopy(BASE_PROVIDER_PARAM_RULES), valid_param_rules)