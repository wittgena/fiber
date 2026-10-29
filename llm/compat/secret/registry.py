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


class RedactRulesetSchema(BaseModel):
    """
    redact_rules.json 선언적 스키마:
    - patterns          : {rule_id: regex_str} 형태의 추가 또는 오버라이드 정규식 사전
    - disabled_rules    : 비활성화할 내장 룰의 정확한 rule_id 리스트 (예: ["context7_key", "generic_password_kv"])
    - extra_secret_keys : Dict Key 및 URL 쿼리 파라미터 마스킹에 공통 적용할 민감 키워드 리스트 (3글자 이상)
    """
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
                # 개별 정규식 문법 오류 시 해당 rule_id만 건너뛰고 나머지는 정상 반영 (Partial Update)
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
    def initialize_and_inject(cls, custom_path_env: Optional[str] = None) -> Tuple[Dict[str, str], List[str]]:
        merged_rules = dict(BASE_SECRET_RULES)
        base_keys = list(SECRET_KEY_PATTERNS)

        target_path = cls.resolve_target_path(custom_path_env)
        if not target_path.exists() or not target_path.is_file():
            log_redact.debug(f"No custom redact registry found at '{target_path}'. Using base rules only.")
            return merged_rules, base_keys

        try:
            with open(target_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
            validated = RedactRulesetSchema(**raw_data)
        except Exception as e:
            log_redact.error(f"Failed to load redact registry at '{target_path}': {e}")
            return merged_rules, base_keys

        # 정확한 rule_id 기반 비활성화 (부분 문자열 매칭 오류 원천 차단)
        disabled_count = 0
        for rule_id in validated.disabled_rules:
            if merged_rules.pop(rule_id, None) is not None:
                disabled_count += 1
            else:
                log_redact.warning(f"[REDACT - disable] Unknown rule_id '{rule_id}' ignored.")

        # Key-Value 패턴 병합 (신규 rule_id 추가 또는 기존 rule_id 정규식 오버라이드)
        merged_rules.update(validated.patterns)

        # 민감 키 병합 (Dict Key 및 URL 쿼리 파라미터 공통 적용)
        merged_keys = list({k.upper() for k in (base_keys + validated.extra_secret_keys)})
        configure_redaction(
            rules=merged_rules,
            secret_keys=merged_keys,
        )

        log_redact.info(
            f"Custom redact rules injected -> +{len(validated.patterns)} rules, "
            f"-{disabled_count} disabled, +{len(validated.extra_secret_keys)} keys"
        )
        return merged_rules, merged_keys


REDACT_RULES, SECRET_KEYS = RedactRegistryIO.initialize_and_inject()