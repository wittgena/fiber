# xor.secure.secret.redact
## @lineage: xphi.xor.secure.secret.redact
## @lineage: xphi.xor.secure.secret.access.redact
## @lineage: xphi.xor.secret.redact
import copy
import re
from collections.abc import Mapping
from typing import Any, List
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
import httpx

SECRET_KEY_PATTERNS = frozenset(
    {
        "AUTHORIZATION",
        "COOKIE",
        "CREDENTIAL",
        "KEY",
        "PASSWORD",
        "SECRET",
        "SESSION",
        "TOKEN",
    }
)

REDACT_ALL_VALUES_KEYS = frozenset({"environment", "env", "headers", "acp_env"})
SENSITIVE_URL_PARAMS = frozenset(
    {
        "tavilyapikey",
        "apikey",
        "api_key",
        "token",
        "access_token",
        "secret",
        "key",
    }
)


def is_secret_key(key: str) -> bool:
    key_upper = key.upper()
    return any(pattern in key_upper for pattern in SECRET_KEY_PATTERNS)


def _redact_all_values(value: Any) -> Any:
    """Recursively redact all values while preserving structure (key names)."""
    if isinstance(value, Mapping):
        return {k: _redact_all_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_all_values(item) for item in value]
    return "<redacted>"


def sanitize_dict(content: Any) -> Any:
    if isinstance(content, Mapping):
        sanitized = {}
        for key, value in content.items():
            key_str = str(key)
            key_lower = key_str.lower()
            if key_lower in REDACT_ALL_VALUES_KEYS:
                sanitized[key] = _redact_all_values(value)
            elif is_secret_key(key_str):
                sanitized[key] = "<redacted>"
            else:
                sanitized[key] = sanitize_dict(value)
        return sanitized
    if isinstance(content, list):
        return [sanitize_dict(item) for item in content]
    return content


def http_error_log_content(response: httpx.Response) -> str | dict:
    try:
        return sanitize_dict(response.json())
    except Exception:
        body_len = len(response.text or "")
        return f"<non-JSON response body omitted ({body_len} chars)>"


def redact_url_params(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return url

    if not parsed.query:
        return url

    # parse_qs returns values as lists; keep_blank_values preserves params
    # with empty values so the reconstructed URL matches the original shape.
    params = parse_qs(parsed.query, keep_blank_values=True)

    redacted_params: dict[str, list[str]] = {}
    for param_name, values in params.items():
        if param_name.lower() in SENSITIVE_URL_PARAMS or is_secret_key(param_name):
            redacted_params[param_name] = ["<redacted>"] * len(values)
        else:
            redacted_params[param_name] = values

    # doseq=True tells urlencode to unpack the value lists correctly.
    redacted_query = urlencode(redacted_params, doseq=True)
    return urlunparse(parsed._replace(query=redacted_query))


def _walk_redact_urls(obj: Any) -> Any:
    """Recursively walk a nested dict/list, applying URL param redaction to strings."""
    if isinstance(obj, dict):
        return {k: _walk_redact_urls(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_redact_urls(item) for item in obj]
    if isinstance(obj, str) and "?" in obj:
        return redact_url_params(obj)
    return obj


def sanitize_config(config: dict[str, Any]) -> dict[str, Any]:
    config = copy.deepcopy(config)
    config = sanitize_dict(config)
    config = _walk_redact_urls(config)
    return config


def redact_text_secrets(text: str) -> str:
    # api_key='...' patterns (single or double quotes)
    text = re.sub(r"api_key='[^']*'", "api_key='<redacted>'", text)
    text = re.sub(r'api_key="[^"]*"', 'api_key="<redacted>"', text)

    # Dict entries with sensitive key names
    text = re.sub(
        r"('[A-Z_]*(?:KEY|SECRET|TOKEN|PASSWORD)[A-Z_]*':\s*')[^']*(')",
        r"\g<1><redacted>\2",
        text,
    )
    text = re.sub(
        r'("[A-Z_]*(?:KEY|SECRET|TOKEN|PASSWORD)[A-Z_]*":\s*")[^"]*(")',
        r"\g<1><redacted>\2",
        text,
    )

    # URL query params
    text = re.sub(
        r"((?:tavilyApiKey|apiKey|api_key|token|access_token|secret|key)=)"
        r"[^&\s'\")\]]+",
        r"\g<1><redacted>",
        text,
        flags=re.IGNORECASE,
    )

    # Authorization header values
    text = re.sub(
        r"('Authorization':\s*')[^']*(')",
        r"\g<1><redacted>\2",
        text,
    )

    # X-Session-API-Key header values
    text = re.sub(
        r"('X-Session-API-Key':\s*')[^']*(')",
        r"\g<1><redacted>\2",
        text,
    )

    # Bare API key literals (common provider formats)
    text = redact_api_key_literals(text)

    return text


# Compiled pattern for bare API key literals from common providers.
# Each branch matches a known prefix followed by the key body.
# Word boundaries (\b) prevent matching partial tokens.
_API_KEY_LITERAL_RE = re.compile(
    r"\b("
    # OpenRouter / OpenAI / Anthropic
    r"sk-(?:or-v1|proj|ant-(?:api|oat)\d{2})-[A-Za-z0-9_-]{20,}"
    r"|gsk_[A-Za-z0-9]{20,}"  # GROQ
    r"|hf_[A-Za-z0-9]{20,}"  # HuggingFace
    r"|tgp_v1_[A-Za-z0-9_-]{20,}"  # Together AI
    r"|ghp_[A-Za-z0-9]{20,}"  # GitHub PAT (classic)
    r"|github_pat_[A-Za-z0-9_]{20,}"  # GitHub PAT (fine-grained)
    r"|sk-oh-[A-Za-z0-9]{20,}"  # session tokens
    r"|ctx7sk-[A-Za-z0-9_-]{10,}"  # Context7 MCP keys
    r"|cla_[A-Za-z0-9_-]{20,}"  # Claude.ai MCP tokens
    r"|sntryu_[A-Za-z0-9]{10,}"  # Sentry tokens
    r"|lin_api_[A-Za-z0-9]{10,}"  # Linear API tokens
    r"|tvly-[A-Za-z0-9_-]{10,}"  # Tavily keys
    r"|ATATT3x[A-Za-z0-9_-]{10,}"  # Jira/Atlassian tokens
    r"|xoxb-[A-Za-z0-9_-]{20,}"  # Slack bot tokens
    r"|xoxp-[A-Za-z0-9_-]{20,}"  # Slack user tokens
    r"|Bearer\s+[A-Za-z0-9_.-]{20,}"  # Bearer tokens
    r")"
)


def redact_api_key_literals(text: str) -> str:
    """Replace bare API key literals from common providers with ``<redacted>``.
    - Matches known key prefixes (OpenAI, Anthropic, OpenRouter, GROQ, HuggingFace, etc.) anywhere in the text.
    """
    return _API_KEY_LITERAL_RE.sub("<redacted>", text)

_REDACTED = "REDACTED"

def _build_secret_patterns() -> "re.Pattern[str]":
    patterns: List[str] = [
        # PEM private key / certificate blocks
        r"-----BEGIN[A-Z \-]*PRIVATE KEY-----[\s\S]*?-----END[A-Z \-]*PRIVATE KEY-----",
        # GCP OAuth2 access tokens (ya29.*)
        r"\bya29\.[A-Za-z0-9_.~+/-]+",
        # Credential %s formatting (space separator, no key= prefix)
        r"(?:client_secret|azure_password|azure_username)\s+[^\s,'\"})\]{}>]+",
        # AWS access key IDs
        r"(?:AKIA|ASIA)[0-9A-Z]{16}",
        # AWS secrets / session tokens / access key IDs (key=value)
        r"(?:aws_secret_access_key|aws_session_token|aws_access_key_id)"
        r"\s*[:=]\s*[A-Za-z0-9/+=]{20,}",
        # Bearer tokens (OAuth, JWT, etc.)
        r"Bearer\s+[A-Za-z0-9\-._~+/]{10,}=*",
        # Basic auth headers
        r"Basic\s+[A-Za-z0-9+/]{10,}={0,2}",
        # OpenAI / Anthropic sk- prefixed keys
        r"sk-[A-Za-z0-9\-_]{20,}",
        # Generic api_key / api-key / apikey (handles 'key': 'value' dict repr)
        r"(?:api[_-]?key)['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]{8,}",
        # x-api-key / api-key header values (handles 'key': 'value' dict repr)
        r"(?:x-api-key|api-key)['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+",
        # Anthropic internal header keys
        r"x-ak-[A-Za-z0-9\-_]{20,}",
        # Google API keys (bare key value)
        r"AIza[0-9A-Za-z\-_]{35}",
        # URL query-param key=VALUE (e.g. ?key=AIza... or &key=...) — catches the
        # full "key=<secret>" fragment so the value is redacted regardless of format.
        r"(?<=[?&])key=[^\s&'\"]{8,}",
        # Password / secret params (handles key=value and 'key': 'value')
        # Word boundary prevents O(n^2) backtracking on long word-char runs.
        r"(?:^|(?<=\W))\w*(?:password|passwd|client_secret|secret_key|_secret)"
        r"['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+",
        # Database connection string credentials (scheme://user:pass@host)
        r"(?<=://)[^\s'\"]*:[^\s'\"@]+(?=@)",
        # Databricks personal access tokens
        r"dapi[0-9a-f]{32}",
        # Module-level provider keys logged as litellm.<provider>_key=<value>
        r"litellm\.[A-Za-z0-9_]*_key['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+",
        # ── Key-name-based redaction ──
        # Catches secrets inside dicts/config dumps by matching on the KEY name
        # regardless of what the value looks like.
        # e.g. 'master_key': 'any-value-here', "database_url": "postgres://..."
        # private_key with PEM-aware value capture
        r"""private_key['\"]?\s*[:=]\s*['\"]?(?:-----BEGIN[A-Z \-]*PRIVATE KEY-----[\s\S]*?-----END[A-Z \-]*PRIVATE KEY-----|[^\s,'\"})\]{}>]+)""",
        r"(?:master_key|xai_key|database_url|db_url|connection_string|"
        r"signing_key|encryption_key|"
        r"auth_token|access_token|refresh_token|"
        r"slack_webhook_url|webhook_url|"
        r"database_connection_string|"
        r"huggingface_token|jwt_secret)"
        r"""['\"]?\s*[:=]\s*['\"]?[^\s,'\"})\]{}>]+""",
        # Raw JWTs (without Bearer prefix)
        r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*",
        # Azure SAS tokens in URLs
        r"[?&]sig=[A-Za-z0-9%+/=]+",
        # Full JSON service-account blobs (single-line and multi-line)
        r'\{[^{}]*"type"\s*:\s*"service_account"[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',
    ]
    return re.compile("|".join(patterns), re.IGNORECASE)


_SECRET_RE = _build_secret_patterns()

def redact_string(value: str) -> str:
    """Scrub known secret/credential patterns from *value* and return the result."""
    return _SECRET_RE.sub(_REDACTED, value)
