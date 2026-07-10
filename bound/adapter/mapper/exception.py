# bound.adapter.mapper.exception
"""
@desc: 
- Standardized LLM Provider exception mapping module
- Replaces legacy hardcoded provider branching with a robust, unified pipeline
@flow: Extract Request/Response Context -> Match Semantic Regex -> Match HTTP Status Code -> Fallback
"""
import json
import re
import traceback
from typing import Any, Optional, Dict
import httpx

from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.provider import ProviderTypes
from bound.surface.exception import (
    APIConnectionError,
    APIError,
    AuthenticationError,
    BadGatewayError,
    BadRequestError,
    ContentPolicyViolationError,
    ContextWindowExceededError,
    InternalServerError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout,
    UnprocessableEntityError,
)
from xphi.xor.secret.redact import redact_string

from watcher.plane.emitter import get_emitter

log = get_emitter("mapping.exception")

## @map: standard_http_status
STATUS_CODE_MAPPING = {
    400: BadRequestError,
    401: AuthenticationError,
    403: PermissionDeniedError,
    404: NotFoundError,
    408: Timeout,
    413: BadRequestError,
    422: UnprocessableEntityError,
    424: BadRequestError,
    429: RateLimitError,
    500: InternalServerError,
    502: BadGatewayError,
    503: ServiceUnavailableError,
    504: Timeout,
}

## @map: semantic_error_regex
SEMANTIC_ERROR_REGEX = [
    (re.compile(r"context limit|maximum context|string too long|too many tokens|length limit exceeded|inputs.*max_new_tokens", re.IGNORECASE), ContextWindowExceededError),
    (re.compile(r"content_policy|responsibleaipolicy|filtered|safety system|blocked", re.IGNORECASE), ContentPolicyViolationError),
    (re.compile(r"rate[\s_\-]*limit|quota exceeded|resource exhausted|capacity exceeded|\b429\b", re.IGNORECASE), RateLimitError),
    (re.compile(r"invalid api key|api key not valid|unable to locate credentials|authentication error|unauthorized", re.IGNORECASE), AuthenticationError),
    (re.compile(r"model_not_found|deploymentnotfound", re.IGNORECASE), NotFoundError),
    (re.compile(r"invalid_encrypted_content|invalid_request_error|malformed input", re.IGNORECASE), BadRequestError),
    (re.compile(r"timeout|timed out", re.IGNORECASE), Timeout),
]


def exception_type(
    model: Optional[str],
    original_exception: Exception,
    custom_llm_provider: Optional[str],
    completion_kwargs: Optional[Dict[str, Any]] = None, # [FIX 2] 가변 기본 인자 방지
    extra_kwargs: Optional[Dict[str, Any]] = None,      # [FIX 2] 가변 기본 인자 방지
):
    completion_kwargs = completion_kwargs or {}
    extra_kwargs = extra_kwargs or {}

    ## @phase: validation, @desc: Return immediately if already mapped
    if any(isinstance(original_exception, exc_type) for exc_type in config.LITELLM_EXCEPTION_TYPES):
        return original_exception

    ## @phase: extraction, @desc: Extract headers
    litellm_response_headers = getattr(original_exception, "headers", None) or \
                               getattr(getattr(original_exception, "response", None), "headers", None)

    try:
        ## @phase: extraction, @desc: Safely extract error string and status code
        error_str = redact_string(str(getattr(original_exception, "message", original_exception)))
        status_code = getattr(original_exception, "status_code", None)
        response_obj = getattr(original_exception, "response", None)

        provider_name = (custom_llm_provider.capitalize() if custom_llm_provider else "Unknown")
        exception_provider_prefix = f"{provider_name}Exception"

        ## @phase: context_building, @desc: Assemble extra debug information
        extra_information = f"\nModel: {model}"
        try:
            # [FIX 1] 지연 임포트(Lazy Import) 적용하여 순환 참조(Circular Dependency) 고리 완벽 차단
            from bound.surface.bridge.api import get_api_base
            _api_base = get_api_base(model=model or "", optional_params=extra_kwargs)
            if _api_base: 
                extra_information += f"\nAPI Base: `{_api_base}`"
        except Exception:
            pass

        common_kwargs = {
            "message": f"{exception_provider_prefix} - {error_str}",
            "llm_provider": custom_llm_provider,
            "model": model,
            "response": response_obj,
            "debug_info": extra_information,
        }

        ## @phase: semantic_mapping - Regex-based mapping for priority business logic errors
        for pattern, exception_class in SEMANTIC_ERROR_REGEX:
            if pattern.search(error_str):
                raised_exc = exception_class(**common_kwargs)
                setattr(raised_exc, "litellm_response_headers", litellm_response_headers)
                raise raised_exc

        ## @phase: status_code_mapping - Standard mapping based on HTTP status codes
        if status_code in STATUS_CODE_MAPPING:
            exception_class = STATUS_CODE_MAPPING[status_code]
            raised_exc = exception_class(**common_kwargs)
            setattr(raised_exc, "litellm_response_headers", litellm_response_headers)
            raise raised_exc

        ## @phase: fallback - Handle unknown errors or generic 500+ server errors
        if status_code and status_code >= 500:
            raised_exc = APIError(status_code=status_code, **common_kwargs)
        else:
            raised_exc = APIConnectionError(
                message=f"{exception_provider_prefix} APIConnectionError - {error_str}\n{redact_string(traceback.format_exc())}",
                llm_provider=custom_llm_provider,
                model=model,
                request=getattr(original_exception, "request", None),
                debug_info=extra_information
            )
        setattr(raised_exc, "litellm_response_headers", litellm_response_headers)
        raise raised_exc

    except Exception as e:
        ## @phase: catch_all - Final safety net for pipeline failures
        if hasattr(e, "litellm_response_headers"):
            raise e
            
        fallback_exc = APIConnectionError(
            message=f"{original_exception}\n{redact_string(traceback.format_exc())}",
            llm_provider=custom_llm_provider or "",
            model=model or "",
        )
        setattr(fallback_exc, "litellm_response_headers", litellm_response_headers)
        raise fallback_exc