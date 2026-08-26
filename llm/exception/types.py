# llm.exception.types
## @lineage: agent.loop.runtime.exception.types
## @lineage: agent.runtime.exception.types
## @lineage: ator.runtime.exception.types
## @lineage: bound.eco.exception.types
## @lineage: eco.bound.exception.types
## @lineage: bound.agent.exception.types
## @lineage: ext.router.exception.types
## @lineage: engine.exception.types
from xphi.arch.contract.event.next import ToposId
ConversationID = ToposId

class DriverError(Exception):
    message: str
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return self.message

class LLMNoResponseError(DriverError):
    def __init__(
        self,
        message: str = (
            "LLM did not return a response. This is only seen in Gemini models so far."
        ),
    ) -> None:
        super().__init__(message)

class LLMMalformedActionError(DriverError):
    def __init__(self, message: str = "Malformed response") -> None:
        super().__init__(message)


class LLMNoActionError(DriverError):
    def __init__(self, message: str = "Agent must return an action") -> None:
        super().__init__(message)


class LLMResponseError(DriverError):
    def __init__(
        self, message: str = "Failed to retrieve action from LLM response"
    ) -> None:
        super().__init__(message)

class FunctionCallConversionError(DriverError):
    def __init__(self, message: str) -> None:
        super().__init__(message)

class FunctionCallValidationError(DriverError):
    def __init__(self, message: str) -> None:
        super().__init__(message)

class FunctionCallNotExistsError(DriverError):
    def __init__(self, message: str) -> None:
        super().__init__(message)

class LLMNoResponseError(DriverError):
    def __init__(
        self,
        message: str = (
            "LLM did not return a response. This is only seen in Gemini models so far."
        ),
    ) -> None:
        super().__init__(message)

class LLMContextWindowExceedError(DriverError):
    def __init__(
        self,
        message: str = "Conversation history longer than LLM context window limit. "
    ) -> None:
        super().__init__(message)

class LLMMalformedConversationHistoryError(DriverError):
    def __init__(
        self,
        message: str = (
            "Conversation history produced an invalid LLM request. "
            "Consider retrying with condensed history and investigating the "
            "event stream."
        ),
    ) -> None:
        super().__init__(message)


class LLMContextWindowTooSmallError(DriverError):
    def __init__(
        self,
        context_window: int,
        min_required: int = 16384,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = (
                f"The configured model has a context window of {context_window:,} "
                f"tokens, which is below the minimum of {min_required:,} tokens "
                "required for meta agent to function properly.\n\n"
                "For local LLMs (Ollama, LM Studio, etc.), increase the context "
                "window.\n"
                "For cloud providers, verify you're using the correct model "
                "variant.\n\n"
                "To override this check (not recommended), set the environment "
                "variable:\n"
                "  ALLOW_SHORT_CONTEXT_WINDOWS=true"
            )
        super().__init__(message)
        self.context_window = context_window
        self.min_required = min_required


class LLMAuthenticationError(DriverError):
    def __init__(self, message: str = "Invalid or missing API credentials") -> None:
        super().__init__(message)


class LLMRateLimitError(DriverError):
    def __init__(self, message: str = "Rate limit exceeded") -> None:
        super().__init__(message)


class LLMTimeoutError(DriverError):
    def __init__(self, message: str = "LLM request timed out") -> None:
        super().__init__(message)


class LLMServiceUnavailableError(DriverError):
    def __init__(self, message: str = "LLM service unavailable") -> None:
        super().__init__(message)


class LLMBadRequestError(DriverError):
    def __init__(self, message: str = "Bad request to LLM provider") -> None:
        super().__init__(message)


# Other
class UserCancelledError(Exception):
    def __init__(self, message: str = "User cancelled the request") -> None:
        super().__init__(message)


class OperationCancelled(Exception):
    def __init__(self, message: str = "Operation was cancelled") -> None:
        super().__init__(message)

class WebSocketConnectionError(RuntimeError):
    def __init__(
        self,
        conversation_id: ConversationID,
        timeout: float,
        message: str | None = None,
    ) -> None:
        self.conversation_id = conversation_id
        self.timeout = timeout
        default_msg = (
            f"WebSocket subscription did not complete within {timeout} seconds "
            f"for conversation {conversation_id}. Events may be missed."
        )
        super().__init__(message or default_msg)


class ConversationRunError(RuntimeError):
    conversation_id: ConversationID
    persistence_dir: str | None
    original_exception: BaseException

    def __init__(
        self,
        conversation_id: ConversationID,
        original_exception: BaseException,
        persistence_dir: str | None = None,
        message: str | None = None,
    ) -> None:
        self.conversation_id = conversation_id
        self.persistence_dir = persistence_dir
        self.original_exception = original_exception
        default_msg = self._build_error_message(
            conversation_id, original_exception, persistence_dir
        )
        super().__init__(message or default_msg)

    @staticmethod
    def _build_error_message(
        conversation_id: ConversationID,
        original_exception: BaseException,
        persistence_dir: str | None,
    ) -> str:
        """Build a detailed error message with debugging information."""
        lines = [
            f"Conversation run failed for id={conversation_id}: {original_exception}",
        ]

        if persistence_dir:
            lines.append(f"\nConversation logs are stored at: {persistence_dir}")
            lines.append("\nTo help debug this issue, please file a bug report at:")
            lines.append("and attach the conversation logs from the directory above.")

        return "\n".join(lines)
