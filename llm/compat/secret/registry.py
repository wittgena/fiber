# fiber.llm.compat.secret.registry
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from pydantic import BaseModel, Field, field_validator

from fiber.llm.compat.secret.redact import (
    BASE_SECRET_RULES,
    SECRET_KEY_PATTERNS,
    configure_redaction,
)
from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log_redact = get_emitter("redact.registry")

REDACT_REGISTRY_ROOT = resolve_path("anchor") / "registry"
DEFAULT_REDACT_FILENAME = "redact_rules.json"
BASE_SECRET_KEYS = frozenset(SECRET_KEY_PATTERNS)


class RedactRulesetSchema(BaseModel):
    patterns: Dict[str, str] = Field(default_factory=dict)
    disabled_rules: List[str] = Field(default_factory=list)
    extra_secret_keys: List[str] = Field(default_factory=list)

    @field_validator("patterns")
    @classmethod
    def validate_regex_dict(cls, rules: Dict[str, str]) -> Dict[str, str]:
        valid: Dict[str, str] = {}
        for rule_id, pattern in rules.items():
            if not isinstance(pattern, str) or not pattern.strip():
                continue
            try:
                re.compile(pattern)
                valid[rule_id] = pattern
            except re.error as e:
                log_redact.warning(f"[REDACT - {rule_id}] Invalid regex skipped ('{pattern}'): {e}")
        return valid

    @field_validator("extra_secret_keys")
    @classmethod
    def validate_secret_keys(cls, keys: List[str]) -> List[str]:
        valid = []
        for k in keys:
            k_clean = k.strip()
            if len(k_clean) < 3:
                log_redact.warning(f"[REDACT - secret_key] Key '{k_clean}' is too short (<3 chars); skipped.")
                continue
            valid.append(k_clean)
        return valid


class RedactRegistryIO:
    @classmethod
    def resolve_target_path(cls, custom_path: Optional[str] = None, filename: str = DEFAULT_REDACT_FILENAME) -> Path:
        raw_path = custom_path or os.getenv("FIBER_REDACT_RULES_PATH")
        if not raw_path or not raw_path.strip():
            return REDACT_REGISTRY_ROOT / filename
        p = Path(raw_path.strip())
        return p / filename if (p.is_dir() or not p.suffix) else p

    @classmethod
    def ensure_default_file(cls, custom_path_env: Optional[str] = None) -> Path:
        """파일이 없거나 빈 파일일 경우 기본 스키마 포맷(patterns, disabled_rules, extra_secret_keys)으로 초기화"""
        target_path = cls.resolve_target_path(custom_path_env)
        if not target_path.exists() or target_path.stat().st_size == 0:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            default_payload = RedactRulesetSchema().model_dump()
            with open(target_path, "w", encoding="utf-8") as f:
                json.dump(default_payload, f, indent=2, ensure_ascii=False)
        return target_path

    @classmethod
    def load_schema(cls, custom_path_env: Optional[str] = None) -> RedactRulesetSchema:
        target_path = cls.resolve_target_path(custom_path_env)
        if not target_path.exists() or not target_path.is_file() or target_path.stat().st_size == 0:
            return RedactRulesetSchema()
        try:
            with open(target_path, "r", encoding="utf-8") as f:
                return RedactRulesetSchema(**json.load(f))
        except Exception as e:
            log_redact.error(f"Failed to load redact registry at '{target_path}': {e}")
            return RedactRulesetSchema()

    @classmethod
    def save_preferences(
        cls,
        disabled_rules: Optional[List[str]] = None,
        extra_secret_keys: Optional[List[str]] = None,
        patterns: Optional[Dict[str, str]] = None,
        custom_path_env: Optional[str] = None,
    ) -> Tuple[Dict[str, str], List[str]]:
        """Dialog 등에서 변경한 설정(disabled_rules, extra_secret_keys, patterns)을 저장하고 런타임 엔진에 즉시 재주입"""
        target_path = cls.ensure_default_file(custom_path_env)
        schema = cls.load_schema(custom_path_env)

        if disabled_rules is not None:
            schema.disabled_rules = sorted(set(disabled_rules))
        if extra_secret_keys is not None:
            schema.extra_secret_keys = sorted({k.strip().upper() for k in extra_secret_keys if len(k.strip()) >= 3})
        if patterns is not None:
            schema.patterns = RedactRulesetSchema.validate_regex_dict(patterns)

        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(schema.model_dump(), f, indent=2, ensure_ascii=False)

        return cls.initialize_and_inject(custom_path_env)

    @classmethod
    def save_disabled_rules(
        cls, disabled_rules: List[str], custom_path_env: Optional[str] = None
    ) -> Tuple[Dict[str, str], List[str]]:
        return cls.save_preferences(disabled_rules=disabled_rules, custom_path_env=custom_path_env)

    @classmethod
    def initialize_and_inject(cls, custom_path_env: Optional[str] = None) -> Tuple[Dict[str, str], List[str]]:
        global REDACT_RULES, SECRET_KEYS

        merged_rules = dict(BASE_SECRET_RULES)
        base_keys = list(BASE_SECRET_KEYS)

        target_path = cls.resolve_target_path(custom_path_env)
        if not target_path.exists() or not target_path.is_file() or target_path.stat().st_size == 0:
            configure_redaction(rules=merged_rules, secret_keys=base_keys)
            REDACT_RULES, SECRET_KEYS = merged_rules, base_keys
            return merged_rules, base_keys

        validated = cls.load_schema(custom_path_env)

        merged_rules.update(validated.patterns)
        disabled_count = 0
        for rule_id in validated.disabled_rules:
            if merged_rules.pop(rule_id, None) is not None:
                disabled_count += 1
            else:
                log_redact.warning(f"[REDACT - disable] Unknown rule_id '{rule_id}' ignored.")

        merged_keys = sorted({k.upper() for k in (base_keys + validated.extra_secret_keys)})
        configure_redaction(rules=merged_rules, secret_keys=merged_keys)
        REDACT_RULES, SECRET_KEYS = merged_rules, merged_keys

        log_redact.debug(
            f"Custom redact rules injected -> +{len(validated.patterns)} rules, "
            f"-{disabled_count} disabled, +{len(validated.extra_secret_keys)} keys"
        )
        return merged_rules, merged_keys


REDACT_RULES: Dict[str, str] = {}
SECRET_KEYS: List[str] = []
RedactRegistryIO.initialize_and_inject()