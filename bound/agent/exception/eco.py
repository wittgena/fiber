# bound.agent.exception.eco
## @lineage: ext.router.exception.eco
## @lineage: engine.exception.eco
from __future__ import annotations
from typing import Any, Dict, Optional, Union
from enum import Enum
import httpx
import openai
from bound.agent.exception.types import LLMContextWindowExceedError, LLMMalformedConversationHistoryError

LONG_PROMPT_PATTERNS: list[str] = [
    "contextwindowexceedederror",
    "prompt is too long",
    "input length and `max_tokens` exceed context limit",
    "please reduce the length of",
    "the request exceeds the available context size",
    "context length exceeded",
    "input exceeds the context window",
    "context window exceeds limit",  # Minimax provider
]

MALFORMED_HISTORY_PATTERNS: list[str] = [
    "tool_use ids were found without `tool_result` blocks immediately after",
    (
        "each `tool_use` block must have a corresponding `tool_result` block "
        "in the next message"
    ),
    "each tool_use must have a single result",
    "found multiple `tool_result` blocks with id:",
    "unexpected `tool_use_id` found in `tool_result` blocks",
    (
        "each `tool_result` block must have a corresponding `tool_use` block "
        "in the previous message"
    ),
]

def is_context_window_exceeded(exception: Exception) -> bool:
    if isinstance(exception, (ContextWindowExceededError, LLMContextWindowExceedError)):
        return True

    if not isinstance(exception, (BadRequestError, OpenAIError, APIConnectionError)):
        return False

    s = str(exception).lower()
    return any(p in s for p in LONG_PROMPT_PATTERNS)

def looks_like_malformed_conversation_history_error(exception: Exception) -> bool:
    if isinstance(exception, LLMMalformedConversationHistoryError):
        return True

    if not isinstance(exception, (BadRequestError, OpenAIError, APIConnectionError)):
        return False

    s = str(exception).lower()
    return any(p in s for p in MALFORMED_HISTORY_PATTERNS)

AUTH_PATTERNS: list[str] = [
    "invalid api key",
    "unauthorized",
    "missing api key",
    "invalid authentication",
    "access denied",
]

def looks_like_auth_error(exception: Exception) -> bool:
    if not isinstance(exception, (BadRequestError, OpenAIError)):
        return False
    s = str(exception).lower()
    if any(p in s for p in AUTH_PATTERNS):
        return True
    for code in ("status 401", "status 403"):
        if code in s:
            return True
    return False

class BraneCommonStrings(Enum):
    redacted_by_brane = "redacted by brane. 'brane.turn_off_message_logging=True'"
    llm_provider_not_provided = "Unmapped LLM provider for this endpoint. You passed model={model}, custom_llm_provider={custom_llm_provider}."

def _ensure_mock_response(response: Optional[httpx.Response], status_code: int) -> httpx.Response:
    if response is not None and isinstance(response, httpx.Response) and hasattr(response, "_request"):
        return response
    return httpx.Response(
        status_code=status_code,
        request=httpx.Request(method="POST", url="https://api.brane.local/v1"),
    )

def _ensure_mock_request(request: Optional[httpx.Request]) -> httpx.Request:
    if request is not None and isinstance(request, httpx.Request):
        return request
    return httpx.Request(method="POST", url="https://api.brane.local/v1")

class BraneExceptionMixin:
    def init_brane_attrs(
        self, message: str, llm_provider: Optional[str], model: Optional[str], 
        debug_info: Optional[str] = None, max_retries: Optional[int] = None, num_retries: Optional[int] = None
    ):
        self.message = f"{self.__class__.__name__}: {message}"
        self.llm_provider = llm_provider
        self.model = model
        self.debug_info = debug_info
        self.max_retries = max_retries
        self.num_retries = num_retries

    def __str__(self) -> str:
        _msg = self.message
        if getattr(self, "num_retries", None):
            _msg += f" | Brane Retried: {self.num_retries} times"
        if getattr(self, "max_retries", None):
            _msg += f", Max Retries: {self.max_retries}"
        return _msg

    def __repr__(self) -> str:
        return self.__str__()

class BaseLLMException(Exception, BraneExceptionMixin):
    def __init__(
        self, status_code: int, message: str, headers: Optional[Union[dict, httpx.Headers]] = None,
        request: Optional[httpx.Request] = None, response: Optional[httpx.Response] = None, body: Optional[dict] = None,
    ):
        self.status_code = status_code
        self.headers = headers
        self.request = _ensure_mock_request(request)
        self.response = response or httpx.Response(status_code=status_code, request=self.request)
        self.body = body
        self.init_brane_attrs(message, None, None)
        Exception.__init__(self, self.message)

class AuthenticationError(openai.AuthenticationError, BraneExceptionMixin):
    def __init__(self, message, llm_provider, model, response=None, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = 401
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        self.response = _ensure_mock_response(response, self.status_code)
        openai.AuthenticationError.__init__(self, self.message, response=self.response, body=None)

class NotFoundError(openai.NotFoundError, BraneExceptionMixin):
    def __init__(self, message, model, llm_provider, response=None, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = 404
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        self.response = _ensure_mock_response(response, self.status_code)
        openai.NotFoundError.__init__(self, self.message, response=self.response, body=None)

class BadRequestError(openai.BadRequestError, BraneExceptionMixin):
    def __init__(self, message, model, llm_provider, response=None, debug_info=None, max_retries=None, num_retries=None, body=None):
        self.status_code = 400
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        self.response = _ensure_mock_response(response, self.status_code)
        openai.BadRequestError.__init__(self, self.message, response=self.response, body=body)

class ImageFetchError(BadRequestError):
    pass

class UnprocessableEntityError(openai.UnprocessableEntityError, BraneExceptionMixin): # type: ignore
    def __init__(self, message, model, llm_provider, response, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = 422
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        openai.UnprocessableEntityError.__init__(self, self.message, response=response, body=None)

class Timeout(openai.APITimeoutError, BraneExceptionMixin): # type: ignore
    def __init__(self, message, model, llm_provider, debug_info=None, max_retries=None, num_retries=None, headers=None, exception_status_code=408):
        self.status_code = exception_status_code
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        self.headers = headers
        request = _ensure_mock_request(None)
        openai.APITimeoutError.__init__(self, request=request)

class PermissionDeniedError(openai.PermissionDeniedError, BraneExceptionMixin): # type: ignore
    def __init__(self, message, llm_provider, model, response, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = 403
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        openai.PermissionDeniedError.__init__(self, self.message, response=response, body=None)

class RateLimitError(openai.RateLimitError, BraneExceptionMixin): # type: ignore
    def __init__(self, message, llm_provider, model, response=None, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = 429
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        self.response = _ensure_mock_response(response, self.status_code)
        self.code, self.type = "429", "throttling_error"
        openai.RateLimitError.__init__(self, self.message, response=self.response, body=None)

class ContextWindowExceededError(BadRequestError): 
    pass

class RejectedRequestError(BadRequestError):
    def __init__(self, message, model, llm_provider, request_data, debug_info=None):
        super().__init__(message=message, model=model, llm_provider=llm_provider, debug_info=debug_info)
        self.request_data = request_data

class ContentPolicyViolationError(BadRequestError):
    def __init__(self, message, model, llm_provider, response=None, debug_info=None, provider_specific_fields=None, body=None):
        super().__init__(message=message, model=model, llm_provider=llm_provider, response=response, debug_info=debug_info, body=body)
        self.provider_specific_fields = provider_specific_fields

class ServiceUnavailableError(openai.APIStatusError, BraneExceptionMixin): # type: ignore
    def __init__(self, message, llm_provider, model, response=None, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = 503
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        self.response = _ensure_mock_response(response, self.status_code)
        openai.APIStatusError.__init__(self, self.message, response=self.response, body=None)

class BadGatewayError(openai.APIStatusError, BraneExceptionMixin): # type: ignore
    def __init__(self, message, llm_provider, model, response=None, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = 502
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        self.response = _ensure_mock_response(response, self.status_code)
        openai.APIStatusError.__init__(self, self.message, response=self.response, body=None)

class InternalServerError(openai.InternalServerError, BraneExceptionMixin): # type: ignore
    def __init__(self, message, llm_provider, model, response=None, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = 500
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        self.response = _ensure_mock_response(response, self.status_code)
        openai.InternalServerError.__init__(self, self.message, response=self.response, body=None)

class APIError(openai.APIError, BraneExceptionMixin): # type: ignore
    def __init__(self, status_code: int, message, llm_provider, model, request=None, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = status_code
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        request = _ensure_mock_request(request)
        openai.APIError.__init__(self, self.message, request=request, body=None)

class APIConnectionError(openai.APIConnectionError, BraneExceptionMixin): # type: ignore
    def __init__(self, message, llm_provider, model, request=None, debug_info=None, max_retries=None, num_retries=None):
        self.status_code = 500
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        self.request = _ensure_mock_request(request)
        openai.APIConnectionError.__init__(self, message=self.message, request=self.request)

class APIResponseValidationError(openai.APIResponseValidationError, BraneExceptionMixin): # type: ignore
    def __init__(self, message, llm_provider, model, debug_info=None, max_retries=None, num_retries=None):
        self.init_brane_attrs(message, llm_provider, model, debug_info, max_retries, num_retries)
        response = _ensure_mock_response(None, 500)
        openai.APIResponseValidationError.__init__(self, response=response, body=None, message=self.message)

class JSONSchemaValidationError(APIResponseValidationError):
    def __init__(self, model: str, llm_provider: str, raw_response: str, schema: str) -> None:
        self.raw_response = raw_response
        self.schema = schema
        message = f"model={model}, returned an invalid response={raw_response}, for schema={schema}."
        super().__init__(message=message, llm_provider=llm_provider, model=model)

class OpenAIError(openai.OpenAIError):
    def __init__(self, original_exception=None):
        super().__init__()
        self.llm_provider = "openai"

class UnsupportedParamsError(BadRequestError):
    def __init__(self, message, llm_provider=None, model=None, status_code=400, response=None, debug_info=None, max_retries=None, num_retries=None):
        super().__init__(message=message, model=model, llm_provider=llm_provider, response=response, debug_info=debug_info, max_retries=max_retries, num_retries=num_retries)
        self.status_code = status_code

EXCEPTION_TYPES = [
    AuthenticationError, NotFoundError, BadRequestError, UnprocessableEntityError,
    UnsupportedParamsError, Timeout, PermissionDeniedError, RateLimitError,
    ContextWindowExceededError, RejectedRequestError, ContentPolicyViolationError,
    InternalServerError, ServiceUnavailableError, BadGatewayError, APIError,
    APIConnectionError, APIResponseValidationError, OpenAIError, JSONSchemaValidationError,
]

class BudgetExceededError(Exception):
    def __init__(
        self, current_cost: float, max_budget: float, message: Optional[str] = None
    ):
        self.current_cost = current_cost
        self.max_budget = max_budget
        self.status_code = 429
        message = (
            message
            or f"Budget has been exceeded! Current cost: {current_cost}, Max budget: {max_budget}"
        )
        self.message = message
        super().__init__(message)

class MockException(openai.APIError):
    def __init__(
        self,
        status_code: int,
        message,
        llm_provider,
        model,
        request: Optional[httpx.Request] = None,
        debug_info: Optional[str] = None,
        max_retries: Optional[int] = None,
        num_retries: Optional[int] = None,
    ):
        self.status_code = status_code
        self.message = "brane.MockException: {}".format(message)
        self.llm_provider = llm_provider
        self.model = model
        self.debug_info = debug_info
        self.max_retries = max_retries
        self.num_retries = num_retries
        if request is None:
            request = httpx.Request(method="POST", url="https://api.openai.com/v1")
        super().__init__(self.message, request=request, body=None)  # type: ignore


class BraneUnknownProvider(BadRequestError):
    def __init__(self, model: str, custom_llm_provider: Optional[str] = None):
        self.message = BraneCommonStrings.llm_provider_not_provided.value.format(
            model=model, custom_llm_provider=custom_llm_provider
        )
        super().__init__(
            self.message, model=model, llm_provider=custom_llm_provider, response=None
        )

    def __str__(self):
        return self.message


class GuardrailRaisedException(Exception):
    def __init__(
        self,
        guardrail_name: Optional[str] = None,
        message: str = "",
        should_wrap_with_default_message: bool = True,
        status_code: int = 400,
    ):
        default_message = f"Guardrail raised an exception, Guardrail: {guardrail_name}, Message: {message}"
        self.guardrail_name = guardrail_name
        self.status_code = status_code
        self.message = default_message if should_wrap_with_default_message else message
        super().__init__(self.message)


class BlockedPiiEntityError(Exception):
    def __init__(
        self,
        entity_type: str,
        guardrail_name: Optional[str] = None,
        status_code: int = 400,
    ):
        """
        Raised when a blocked entity is detected by a guardrail.
        """
        self.entity_type = entity_type
        self.guardrail_name = guardrail_name
        self.status_code = status_code
        self.message = f"Blocked entity detected: {entity_type} by Guardrail: {guardrail_name}. This entity is not allowed to be used in this request."
        super().__init__(self.message)


class MidStreamFallbackError(ServiceUnavailableError):  # type: ignore
    def __init__(
        self,
        message: str,
        model: str,
        llm_provider: str,
        original_exception: Optional[Exception] = None,
        response: Optional[httpx.Response] = None,
        debug_info: Optional[str] = None,
        max_retries: Optional[int] = None,
        num_retries: Optional[int] = None,
        generated_content: str = "",
        is_pre_first_chunk: bool = False,
    ):
        original_status = getattr(original_exception, "status_code", None)
        self.status_code = int(original_status) if original_status is not None else 503
        self.message = f"brane.MidStreamFallbackError: {message}"
        self.model = model
        self.llm_provider = llm_provider
        self.original_exception = original_exception
        self.debug_info = debug_info
        self.max_retries = max_retries
        self.num_retries = num_retries
        self.generated_content = generated_content
        self.is_pre_first_chunk = is_pre_first_chunk

        # Create a response if one wasn't provided
        if response is None:
            self.response = httpx.Response(
                status_code=self.status_code,
                request=httpx.Request(
                    method="POST",
                    url=f"https://{llm_provider}.com/v1/",
                ),
            )
        else:
            self.response = response

        # Save the original attributes before they are overridden by ServiceUnavailableError
        _saved_response = self.response
        _saved_request = getattr(self.response, "request", None) or httpx.Request(
            method="POST", url=f"https://{llm_provider}.com/v1/"
        )
        _saved_message = self.message

        # Call the parent constructor (which hardcodes status_code=503 and modifies the response object)
        super().__init__(
            message=self.message,
            llm_provider=llm_provider,
            model=model,
            response=self.response,
            debug_info=self.debug_info,
            max_retries=self.max_retries,
            num_retries=self.num_retries,
        )

        # Restore the propagated status and original response/request objects
        self.status_code = int(original_status) if original_status is not None else 503
        self.response = _saved_response
        self.request = _saved_request
        self.message = _saved_message
        self.args = (_saved_message,)

    def __str__(self):
        _message = self.message
        if self.num_retries:
            _message += f" Brane Retried: {self.num_retries} times"
        if self.max_retries:
            _message += f", Brane Max Retries: {self.max_retries}"
        if self.original_exception:
            _message += f" Original exception: {type(self.original_exception).__name__}: {str(self.original_exception)}"
        return _message

    def __repr__(self):
        return self.__str__()


class ModifyResponseException(Exception):
    def __init__(
        self,
        message: str,
        model: str,
        request_data: Dict[str, Any],
        guardrail_name: Optional[str] = None,
        detection_info: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.model = model
        self.request_data = request_data
        self.guardrail_name = guardrail_name
        self.detection_info = detection_info or {}
        super().__init__(message)

class GuardrailInterventionNormalStringError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.__str__()


class SensitiveDataRouteException(Exception):
    def __init__(
        self,
        route_to_model: str,
        session_id: str,
        guardrail_name: Optional[str] = None,
        detection_info: Optional[Dict[str, Any]] = None,
        message: Optional[str] = None,
        sticky_session_routing: bool = True,
    ):
        self.route_to_model = route_to_model
        self.session_id = session_id
        self.guardrail_name = guardrail_name
        self.detection_info = detection_info or {}
        self.sticky_session_routing = sticky_session_routing
        self.message = (
            message
            or f"Sensitive data detected by {guardrail_name}. Routing to model: {route_to_model}"
        )
        super().__init__(self.message)
