# bound.surface.legacy.transport.base
## @lineage: bound.transport.http.base
import ssl
from enum import Enum
import asyncio
import concurrent.futures
import inspect
import os
import socket
import sys
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union
import httpx
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.config.constants import (
    COMPLETION_HTTP_FALLBACK_SECONDS,
    HTTP_HANDLER_CONNECT_TIMEOUT_SECONDS,
)
from watcher.plane.emitter import get_emitter

log = get_emitter("http.base")

VerifyTypes = Union[str, bool, ssl.SSLContext]
class httpxSpecialProvider(str, Enum):
    LoggingCallback = "logging_callback"
    GuardrailCallback = "guardrail_callback"
    Caching = "caching"
    Oauth2Check = "oauth2_check"
    Oauth2Register = "oauth2_register"
    SecretManager = "secret_manager"
    PassThroughEndpoint = "pass_through_endpoint"
    PromptFactory = "prompt_factory"
    SSO_HANDLER = "sso_handler"
    Search = "search"
    MCP = "mcp"
    RAG = "rag"
    A2AProvider = "a2a_provider"
    AgentHealthCheck = "agent_health_check"
    A2A = "a2a"
    PromptManagement = "prompt_management"
    UI = "ui"

def get_default_headers() -> dict:
    user_agent = os.environ.get("LITELLM_USER_AGENT")
    if user_agent is not None:
        return {"User-Agent": user_agent}
    return {"User-Agent": f"gate/{version}"}

_DEFAULT_TIMEOUT = httpx.Timeout(
    timeout=COMPLETION_HTTP_FALLBACK_SECONDS,
    connect=HTTP_HANDLER_CONNECT_TIMEOUT_SECONDS,
)
_STREAMING_ERROR_BODY_READ_TIMEOUT_SECONDS = 5.0
_HTTPX_CLIENT_CACHE: Dict[str, Any] = {}

def _prepare_request_data_and_content(data: Optional[Union[dict, str, bytes]] = None, content: Any = None) -> Tuple[Optional[Union[dict, Mapping]], Any]:
    request_data = None
    request_content = content
    if data is not None:
        if isinstance(data, (bytes, str)):
            if content is None:
                request_content = data
        else:
            request_data = data
    return request_data, request_content

def _safe_get_response_text(response: httpx.Response) -> str:
    try:
        return response.text
    except Exception:
        return ""