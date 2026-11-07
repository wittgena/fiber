# fiber.infra.mcp.guard.preset
from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple
import orjson

from xphi.node.space.bind.resolver import resolve_path

RAW_BUILTIN_PRESETS: Dict[str, Dict[str, Any]] = {
    "sql": {
        "label": "SQL / DB",
        "mode": "totp",
        "priority": 30,
        "pattern": r"\b(DROP|TRUNCATE|DELETE|ALTER|GRANT|REVOKE)\b",
        "target_hints": ["postgres", "mysql", "sqlite", "supabase", "neon", "db", "sql", "redis"],
        "description": "Destructive DDL/DML queries (Postgres, MySQL, SQLite, Supabase)",
    },
    "secrets": {
        "label": "Secrets / Env",
        "mode": "totp",
        "priority": 35,
        "pattern": r"\b(\.env|id_rsa|id_ed25519|credentials|private_key|secret_key|vault|passwd|shadow)\b",
        "target_hints": ["vault", "1password", "secret", "keychain", "kms", "auth"],
        "description": "Sensitive credentials, SSH keys & vault access",
    },
    "cloud": {
        "label": "Cloud / K8s",
        "mode": "totp",
        "priority": 30,
        "pattern": r"\b(destroy|terminate|teardown|uninstall|scale_down|drain)\b|delete_(pod|deployment|namespace|service|bucket|instance)",
        "target_hints": ["kubernetes", "k8s", "docker", "aws", "gcp", "terraform", "pulumi", "deployer", "reaper"],
        "description": "Infrastructure & cluster resource destruction",
    },
    "payment": {
        "label": "FinOps / Pay",
        "mode": "totp",
        "priority": 40,
        "pattern": r"\b(transfer|withdraw|refund|payout|charge|cancel_subscription|burn)\b",
        "target_hints": ["stripe", "paypal", "payment", "billing", "finops", "margin", "ledger"],
        "description": "Financial transactions, payouts & budget mutations",
    },
    "fs": {
        "label": "FS / Shell",
        "mode": "button",
        "priority": 20,
        "pattern": r"\b(rm|rmdir|unlink|shred|mkfs|dd|chmod|chown|sudo|kill|pkill)\b|remove_file|delete_file|write_file|move_file",
        "target_hints": ["filesystem", "local-fs", "fs", "shell", "bash", "terminal", "cmd", "desktop-commander"],
        "description": "File deletion, permission changes & shell execution",
    },
    "git": {
        "label": "Git / Repo",
        "mode": "button",
        "priority": 20,
        "pattern": r"push\s+--force|reset\s+--hard|clean\s+-f|branch\s+-D|delete_repo|delete_branch|merge_pull_request",
        "target_hints": ["github", "gitlab", "git", "bitbucket", "repomix"],
        "description": "Force push, branch/repo deletion & PR merge",
    },
    "comms": {
        "label": "Msg / Webhook",
        "mode": "button",
        "priority": 15,
        "pattern": r"\b(send_email|send_message|post_message|broadcast|delete_channel|kick_user|ban_user)\b",
        "target_hints": ["slack", "discord", "gmail", "email", "smtp", "notion", "linear", "jira"],
        "description": "Outbound messaging & channel moderation",
    },
    "browser": {
        "label": "Web / Submit",
        "mode": "button",
        "priority": 15,
        "pattern": r"\b(submit|checkout|purchase|confirm_order|delete_account|upload_file)\b",
        "target_hints": ["playwright", "puppeteer", "browser", "selenium", "firecrawl", "fetch"],
        "description": "Browser form submission, checkout & account actions",
    },
}

@dataclass(frozen=True)
class PreflightPreset:
    key: str
    label: str
    mode: str  # 'totp' | 'button'
    pattern: str
    description: str = ""
    target_hints: Tuple[str, ...] = field(default_factory=tuple)
    priority: int = 10

    @property
    def token_ref(self) -> str:
        """mcp.json 저장용 별칭 토큰 (예: '@sql')"""
        return f"@{self.key}"

    @property
    def raw_env_value(self) -> str:
        """단일 프리셋 직렬화 값 (예: 'button:...' 또는 '...')"""
        if not self.pattern:
            return ""
        return f"button:{self.pattern}" if self.mode == "button" else self.pattern

    @classmethod
    def from_dict(
        cls,
        key: str,
        data: Dict[str, Any],
        base: Optional[PreflightPreset] = None,
    ) -> PreflightPreset:
        """RAW_BUILTIN_PRESETS 또는 외부 JSON 딕셔너리를 PreflightPreset으로 매핑"""
        return cls(
            key=key,
            label=str(data.get("label") or (base.label if base else key.upper())),
            mode=str(data.get("mode") or (base.mode if base else "totp")).lower(),
            pattern=str(data.get("pattern") or (base.pattern if base else "")),
            description=str(data.get("description") or (base.description if base else "")),
            target_hints=tuple(data.get("target_hints") or (base.target_hints if base else ())),
            priority=int(data.get("priority", base.priority if base else 25)),
        )


BUILTIN_PRESETS: Dict[str, PreflightPreset] = {
    k: PreflightPreset.from_dict(k, v) for k, v in RAW_BUILTIN_PRESETS.items()
}

class PreflightPresetRegistry:
    DEFAULT_FILENAME = "preflight_rules.json"
    _PREFIX_RE = re.compile(r"^(totp|button|simple|nototp|1click)\s*:\s*(.+)$", re.IGNORECASE)

    @classmethod
    def get_registry_path(cls) -> Path:
        env_path = os.environ.get("FIBER_PREFLIGHT_RULES_PATH", "").strip()
        return Path(env_path) if env_path else (resolve_path("anchor") / "registry" / cls.DEFAULT_FILENAME)

    @classmethod
    def load_all(cls) -> Dict[str, PreflightPreset]:
        """BUILTIN_PRESETS 위에 .anchor/registry/preflight_rules.json을 오버라이드 병합"""
        merged = dict(BUILTIN_PRESETS)
        reg_path = cls.get_registry_path()
        if not reg_path.is_file():
            return merged

        try:
            raw = orjson.loads(reg_path.read_bytes())
            if not isinstance(raw, dict):
                return merged

            disabled = {str(x).strip().lower() for x in raw.get("disabled_presets", [])}
            for d_key in disabled:
                merged.pop(d_key, None)

            custom_map = raw.get("presets", raw)
            if isinstance(custom_map, dict):
                for k, v in custom_map.items():
                    key = k.strip().lower()
                    if key in ("presets", "disabled_presets") or key in disabled or not isinstance(v, dict):
                        continue
                    merged[key] = PreflightPreset.from_dict(key, v, base=merged.get(key))
        except Exception:
            pass
        return merged

    @classmethod
    def expand_expression(cls, expr: Optional[str]) -> Tuple[Optional[str], bool, Dict[str, str]]:
        if not expr or not expr.strip():
            return None, True, {}

        raw = expr.strip()
        explicit_mode: Optional[str] = None
        m = cls._PREFIX_RE.match(raw)
        if m:
            explicit_mode = "button" if m.group(1).lower() != "totp" else "totp"
            raw = m.group(2).strip()

        presets = cls.load_all()
        tokens = [t.strip() for t in re.split(r"\s*\+\s*", raw) if t.strip()]

        pattern_parts: List[str] = []
        rule_modes: Dict[str, str] = {}
        derived_modes: List[str] = []

        for tok in tokens:
            if tok.startswith("@"):
                preset = presets.get(tok[1:].lower())
                if preset and preset.pattern:
                    pattern_parts.append(f"(?:{preset.pattern})")
                    eff_mode = explicit_mode or preset.mode
                    rule_modes[preset.pattern] = eff_mode
                    derived_modes.append(eff_mode)
            else:
                pattern_parts.append(f"(?:{tok})")
                eff_mode = explicit_mode or "totp"
                rule_modes[tok] = eff_mode
                derived_modes.append(eff_mode)

        if not pattern_parts:
            return None, True, {}

        combined_regex = "|".join(pattern_parts)
        require_totp = (explicit_mode == "totp") if explicit_mode else ("totp" in derived_modes or not derived_modes)
        return combined_regex, require_totp, rule_modes

    @classmethod
    def recommend_for_target(cls, target_id: str, exec_cmd: str = "") -> List[PreflightPreset]:
        """타겟 이름과 실행 명령어를 기반으로 매칭되는 프리셋들을 우선순위(priority) 순으로 반환"""
        haystack = f"{target_id} {exec_cmd}".lower()
        matched = [
            p for p in cls.load_all().values()
            if any(hint in haystack for hint in p.target_hints)
        ]
        matched.sort(key=lambda p: p.priority, reverse=True)
        return matched