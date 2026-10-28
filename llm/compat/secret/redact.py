# fiber.llm.compat.secret.redact
import copy
import re
from collections.abc import Iterable, Mapping
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
import httpx

_REDACTED = "<redacted>"

SECRET_KEY_PATTERNS = frozenset(
    {
        "AUTHORIZATION",
        "CERTIFICATE",
        "COOKIE",
        "CREDENTIAL",
        "JWT",
        "KEY",
        "OAUTH",
        "PASSPHRASE",
        "PASSWORD",
        "PRIVATE",
        "SECRET",
        "SESSION",
        "SIGNATURE",
        "TOKEN",
    }
)

REDACT_ALL_VALUES_KEYS = frozenset({"environment", "env", "headers", "acp_env"})

# 고유 Rule ID(Key)와 정규식(Value)으로 구조화된 베이스 룰셋 (SSOT)
BASE_SECRET_RULES: Dict[str, str] = {
    # Standard & Cloud Providers
    "aws_access_key_id": r"(?:AKIA|ASIA)[0-9A-Z]{16}",
    "aws_kv_credential": r"(?:aws_secret_access_key|aws_session_token|aws_access_key_id)\s*[:=]\s*[A-Za-z0-9/+=]{20,}",
    "gcp_api_key": r"AIza[0-9A-Za-z\-_]{35}",
    "gcp_oauth_token": r"\bya29\.[A-Za-z0-9_.~+/-]+",

    # Common Tokens & Keys
    "bearer_token": r"Bearer\s+[A-Za-z0-9\-._~+/]{10,}=*",
    "basic_auth": r"Basic\s+[A-Za-z0-9+/]{10,}={0,2}",
    "jwt_token": r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*",
    "pem_private_key": r"-----BEGIN[A-Z \-]*PRIVATE KEY-----[\s\S]*?-----END[A-Z \-]*PRIVATE KEY-----",

    # Service Specific (GitHub, Stripe, Slack, etc.)
    "github_token": r"gh[pousr]_[A-Za-z0-9_]{20,}",
    "stripe_key": r"(?:sk|rk)_(?:test|live)_[a-zA-Z0-9]{24,}",
    "slack_webhook": r"hooks\.slack\.com/services/T[a-zA-Z0-9_]+/B[a-zA-Z0-9_]+/[a-zA-Z0-9_]+",
    "slack_token": r"xox[bp]-[A-Za-z0-9_-]{20,}",

    # LLM & Third-Party APIs
    "llm_vendor_key": r"sk-(?:or-v1|proj|ant-(?:api|oat)\d{2})-[A-Za-z0-9_-]{20,}",
    "openai_legacy_key": r"sk-[A-Za-z0-9\-_]{20,}",
    "openhands_key": r"sk-oh-[A-Za-z0-9]{20,}",
    "groq_key": r"gsk_[A-Za-z0-9]{20,}",
    "huggingface_token": r"hf_[A-Za-z0-9]{20,}",
    "together_ai_key": r"tgp_v1_[A-Za-z0-9_-]{20,}",
    "context7_key": r"ctx7sk-[A-Za-z0-9_-]{10,}",
    "claude_web_key": r"cla_[A-Za-z0-9_-]{20,}",
    "sentry_token": r"sntryu_[A-Za-z0-9]{10,}",
    "linear_api_key": r"lin_api_[A-Za-z0-9]{10,}",
    "tavily_api_key": r"tvly-[A-Za-z0-9_-]{10,}",
    "jira_token": r"ATATT3x[A-Za-z0-9_-]{10,}",
    "databricks_token": r"dapi[0-9a-f]{32}",

    # Key-Value / Generic formats (Tolerant spacing)
    "azure_kv_secret": r"(?:client_secret|azure_password|azure_username)['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+",
    "generic_api_key_kv": r"(?:api[_-]?key|x-api-key|x-ak-[A-Za-z0-9\-_]{20,})['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]{8,}",
    "generic_password_kv": r"(?:^|(?<=\W))\w*(?:password|passwd|client_secret|secret_key|_secret)['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+",
    "generic_config_secret_kv": r"(?:master_key|xai_key|database_url|db_url|connection_string|signing_key|encryption_key|auth_token|access_token|refresh_token|slack_webhook_url|webhook_url|database_connection_string|huggingface_token|jwt_secret|tavilyApiKey)['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+",
    "url_query_secret": r"(?<=[?&])(?:key|token|secret|api_key|access_token|tavilyApiKey)=[^\s&'\"]{8,}",

    # Auth Headers & Service Accounts
    "uri_basic_auth": r"(?<=://)[^\s'\"]*:[^\s'\"@]+(?=@)",
    "gcp_service_account_json": r'\{[^{}]*"type"\s*:\s*"service_account"[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',
    "inline_private_key_kv": r"""private_key['\"]?\s*[:=]\s*['\"]?(?:-----BEGIN[A-Z \-]*PRIVATE KEY-----[\s\S]*?-----END[A-Z \-]*PRIVATE KEY-----|[^\s,'\"})\]{}>]+)""",
}

# 런타임 활성 상태 (외부 레지스트리에서 주입 시 갱신됨)
ACTIVE_SECRET_RULES: Dict[str, str] = dict(BASE_SECRET_RULES)
ACTIVE_SECRET_PATTERNS: List[str] = list(ACTIVE_SECRET_RULES.values())


def _build_secret_patterns(patterns: Iterable[str] = ACTIVE_SECRET_PATTERNS) -> "re.Pattern[str]":
    pattern_list = list(patterns) or [r"(?!)"]
    return re.compile("|".join(pattern_list), re.IGNORECASE)


_SECRET_RE: "re.Pattern[str]" = _build_secret_patterns(ACTIVE_SECRET_PATTERNS)
_SECRET_RE_BYTES: "re.Pattern[bytes]" = re.compile(
    "|".join(ACTIVE_SECRET_PATTERNS).encode("utf-8"), re.IGNORECASE
)


def configure_redaction(
    rules: Mapping[str, str] | None = None,
    secret_keys: Iterable[str] | None = None,
) -> None:
    """상위 Registry(fiber)에서 병합된 Key-Value 정규식 룰셋과 민감 키를 역주입(DIP)하는 훅"""
    global ACTIVE_SECRET_RULES, ACTIVE_SECRET_PATTERNS, _SECRET_RE, _SECRET_RE_BYTES, SECRET_KEY_PATTERNS

    if rules is not None:
        ACTIVE_SECRET_RULES = dict(rules)
        ACTIVE_SECRET_PATTERNS = list(ACTIVE_SECRET_RULES.values())
        _SECRET_RE = _build_secret_patterns(ACTIVE_SECRET_PATTERNS)
        joined_bytes = "|".join(ACTIVE_SECRET_PATTERNS).encode("utf-8") if ACTIVE_SECRET_PATTERNS else b"(?!)"
        _SECRET_RE_BYTES = re.compile(joined_bytes, re.IGNORECASE)

    if secret_keys is not None:
        SECRET_KEY_PATTERNS = frozenset(k.upper() for k in secret_keys)


def is_secret_key(key: str) -> bool:
    key_upper = key.upper()
    return any(pattern in key_upper for pattern in SECRET_KEY_PATTERNS)


def redact_string(value: str) -> str:
    """Scrub known secret/credential patterns from a string and return the result."""
    return _SECRET_RE.sub(_REDACTED, value)


def redact_bytes(value: bytes, mask: bytes = b"***REDACTED***") -> bytes:
    """UTF-8 디코딩 없이 바이트 스트림에 직접 단일 C-Regex 마스킹 수행"""
    return _SECRET_RE_BYTES.sub(mask, value)


def redact_url_params(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return url

    if not parsed.query:
        return url

    params = parse_qs(parsed.query, keep_blank_values=True)
    redacted_params: dict[str, list[str]] = {}
    for param_name, values in params.items():
        # SENSITIVE_URL_PARAMS 중복을 제거하고 is_secret_key() 단일 기준으로 통합
        if is_secret_key(param_name):
            redacted_params[param_name] = [_REDACTED] * len(values)
        else:
            redacted_params[param_name] = values

    redacted_query = urlencode(redacted_params, doseq=True)
    return urlunparse(parsed._replace(query=redacted_query))


def _redact_all_values(value: Any) -> Any:
    """Recursively redact all values while preserving structure (key names)."""
    if isinstance(value, Mapping):
        return {k: _redact_all_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_all_values(item) for item in value]
    return _REDACTED


def sanitize_payload(obj: Any) -> Any:
    """Single-pass traversal to redact sensitive dictionary keys, URL queries, and raw string secrets."""
    if isinstance(obj, Mapping):
        sanitized = {}
        for key, value in obj.items():
            key_str = str(key)
            if key_str.lower() in REDACT_ALL_VALUES_KEYS:
                sanitized[key] = _redact_all_values(value)
            elif is_secret_key(key_str):
                sanitized[key] = _REDACTED
            else:
                sanitized[key] = sanitize_payload(value)
        return sanitized

    if isinstance(obj, list):
        return [sanitize_payload(item) for item in obj]

    if isinstance(obj, str):
        if "?" in obj:
            obj = redact_url_params(obj)
        return redact_string(obj)

    return obj


def sanitize_config(config: dict[str, Any]) -> dict[str, Any]:
    """Deepcopy and apply a single-pass redaction logic."""
    return sanitize_payload(copy.deepcopy(config))


def http_error_log_content(response: httpx.Response) -> str | dict:
    try:
        return sanitize_payload(response.json())
    except Exception:
        body_len = len(response.text or "")
        return f"<non-JSON response body omitted ({body_len} chars)>"

class FastRegexRedactionEngine:
    """@desc: 내장(Registry 동적 주입 포함) 또는 커스텀 패턴을 단일 C-Regex 바이트 엔진으로 실행"""
    __slots__ = ("mask_token", "_custom_regex")  # 인스턴스 dict 생성 방지 및 속성 접근 가속

    def __init__(self, patterns: Optional[List[str]] = None, mask_token: bytes = b"***REDACTED***"):
        self.mask_token = mask_token
        if patterns is not None:
            joined = "|".join(patterns).encode("utf-8") if patterns else b"(?!)"
            self._custom_regex: Optional[re.Pattern[bytes]] = re.compile(joined, re.IGNORECASE)
        else:
            self._custom_regex = None

    def execute(self, payload: bytes) -> bytes:
        regex = self._custom_regex or _SECRET_RE_BYTES
        return regex.sub(self.mask_token, payload)

    def contains_match(self, payload: bytes) -> bool:
        regex = self._custom_regex or _SECRET_RE_BYTES
        return regex.search(payload) is not None

import fiber.llm.compat.secret.registry as _registry_bootstrap  # noqa: E402, F401