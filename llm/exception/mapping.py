# fiber.llm.exception.mapping
from __future__ import annotations
import json
import re
import traceback
from typing import Any, Optional, Dict
import httpx

from fiber.llm.exception.eco import (
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
    EXCEPTION_TYPES,
    is_context_window_exceeded,
    looks_like_auth_error,
    looks_like_malformed_conversation_history_error
)
from xphi.xor.secret.redact import redact_string
from fiber.llm.exception.types import (
    LLMAuthenticationError,
    LLMBadRequestError,
    LLMContextWindowExceedError,
    LLMMalformedConversationHistoryError,
    LLMRateLimitError,
    LLMServiceUnavailableError,
    LLMTimeoutError,
)
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("exception.mapping")

def map_provider_exception(exception: Exception) -> Exception:
    if is_context_window_exceeded(exception):
        return LLMContextWindowExceedError(str(exception))

    if looks_like_malformed_conversation_history_error(exception):
        return LLMMalformedConversationHistoryError(str(exception))

    if looks_like_auth_error(exception):
        return LLMAuthenticationError(str(exception))

    if isinstance(exception, RateLimitError):
        return LLMRateLimitError(str(exception))

    if isinstance(exception, Timeout):
        return LLMTimeoutError(str(exception))

    if isinstance(
        exception, (APIConnectionError, ServiceUnavailableError, InternalServerError)
    ):
        return LLMServiceUnavailableError(str(exception))

    if isinstance(exception, BadRequestError):
        return LLMBadRequestError(str(exception))

    return exception

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
    completion_kwargs: Optional[Dict[str, Any]] = None, 
    extra_kwargs: Optional[Dict[str, Any]] = None,      
):
    completion_kwargs = completion_kwargs or {}
    extra_kwargs = extra_kwargs or {}

    ## @phase: validation, @desc: Return immediately if already mapped
    if any(isinstance(original_exception, exc_type) for exc_type in EXCEPTION_TYPES):
        return original_exception

    ## @phase: extraction, @desc: Extract headers
    response_headers = getattr(original_exception, "headers", None) or \
                       getattr(getattr(original_exception, "response", None), "headers", None)

    try:
        ## @phase: extraction, @desc: Safely extract error string and status code
        error_str = redact_string(str(getattr(original_exception, "message", original_exception)))
        status_code = getattr(original_exception, "status_code", None)
        response_obj = getattr(original_exception, "response", None)

        provider_name = (custom_llm_provider.capitalize() if custom_llm_provider else "Unknown")
        
        ## @phase: context_building, @desc: Assemble extra debug information
        extra_information = f"\nModel: {model}"
        try:
            from fiber.llm.model.provider.resolver import get_api_base
            _api_base = get_api_base(model=model or "", optional_params=extra_kwargs)
            if _api_base: 
                extra_information += f"\nAPI Base: `{_api_base}`"
        except Exception:
            pass

        common_kwargs = {
            "message": f"[{provider_name}] {error_str}",
            "llm_provider": custom_llm_provider,
            "model": model,
            "response": response_obj,
            "debug_info": extra_information,
        }

        ## @phase: semantic_mapping - Regex-based mapping for priority business logic errors
        for pattern, exception_class in SEMANTIC_ERROR_REGEX:
            if pattern.search(error_str):
                raised_exc = exception_class(**common_kwargs)
                setattr(raised_exc, "response_headers", response_headers)
                raise raised_exc

        ## @phase: status_code_mapping - Standard mapping based on HTTP status codes
        if status_code in STATUS_CODE_MAPPING:
            exception_class = STATUS_CODE_MAPPING[status_code]
            raised_exc = exception_class(**common_kwargs)
            setattr(raised_exc, "response_headers", response_headers)
            raise raised_exc

        ## @phase: fallback - Handle unknown errors or generic 500+ server errors
        if status_code and status_code >= 500:
            raised_exc = APIError(status_code=status_code, **common_kwargs)
        else:
            raised_exc = APIConnectionError(
                message=f"[{provider_name}] {error_str}\n{redact_string(traceback.format_exc())}",
                llm_provider=custom_llm_provider,
                model=model,
                request=getattr(original_exception, "request", None),
                debug_info=extra_information
            )
        setattr(raised_exc, "response_headers", response_headers)
        raise raised_exc

    except Exception as e:
        ## @phase: catch_all - Final safety net for pipeline failures
        if hasattr(e, "response_headers"):
            raise e
            
        _safe_provider = custom_llm_provider.capitalize() if custom_llm_provider else "Unknown"
        fallback_exc = APIConnectionError(
            message=f"[{_safe_provider}] {original_exception}\n{redact_string(traceback.format_exc())}",
            llm_provider=custom_llm_provider or "",
            model=model or "",
        )
        setattr(fallback_exc, "response_headers", response_headers)
        raise fallback_exc