# fiber.llm.router.handle.utils
## @lineage: fiber.llm.router.handler.utils
import functools
import inspect
import logging
from typing import (
    Any,
    Dict,
    Optional,
    Protocol,
    Sequence,
    Type,
    runtime_checkable,
)
from typing_extensions import Annotated

from fiber.llm.types.llm.block import (
    ChatMessage,
    ChatResponseAsyncGen,
    ChatResponseGen,
    CompletionResponseAsyncGen,
    CompletionResponseGen,
)
from fiber.llm.types.inter.base import TokenAsyncGen, TokenGen
from fiber.gateway.llm.mapper.pydantic import (
    BaseModel,
    WithJsonSchema,
    Field,
    field_validator,
    ValidationError,
)

logger = logging.getLogger(__name__)

# ==========================================
# 1. Data Models
# ==========================================
class ToolSelection(BaseModel):
    """
    LLM이 선택한 도구(Tool)와 매개변수를 담는 표준 데이터 모델.
    """
    tool_id: str = Field(description="Tool ID to select.")
    tool_name: str = Field(description="Tool name to select.")
    tool_kwargs: Dict[str, Any] = Field(description="Keyword arguments for the tool.")

    @field_validator("tool_kwargs", mode="wrap")
    @classmethod
    def ignore_non_dict_arguments(cls, v: Any, handler: Any) -> Dict[str, Any]:
        """
        LLM이 잘못된 타입(예: 문자열 등)으로 인자를 반환할 경우, 
        에러를 발생시키지 않고 빈 딕셔너리로 안전하게 치환(Fail-safe).
        """
        try:
            return handler(v)
        except ValidationError:
            logger.warning(f"Invalid tool_kwargs format received: {v}. Defaulting to empty dict.")
            return handler({})


# ==========================================
# 2. Protocols & Type Aliases
# ==========================================
@runtime_checkable
class MessagesToPromptType(Protocol):
    """채팅 메시지 리스트를 단일 문자열 프롬프트로 변환하는 함수의 프로토콜"""
    def __call__(self, messages: Sequence[ChatMessage]) -> str:
        pass


@runtime_checkable
class CompletionToPromptType(Protocol):
    """기본 프롬프트 문자열을 모델 특화 프롬프트로 변환하는 함수의 프로토콜"""
    def __call__(self, prompt: str) -> str:
        pass


# Pydantic JSON Schema와 통합된 타입 힌팅
MessagesToPromptCallable = Annotated[
    Optional[MessagesToPromptType],
    WithJsonSchema({"type": "string"}),
]

CompletionToPromptCallable = Annotated[
    Optional[CompletionToPromptType],
    WithJsonSchema({"type": "string"}),
]


# ==========================================
# 3. Stream Generator Utilities
# ==========================================
def stream_completion_response_to_tokens(
    completion_response_gen: CompletionResponseGen,
) -> TokenGen:
    """Completion 응답 제너레이터를 단순 텍스트 토큰 제너레이터로 변환"""
    def gen() -> TokenGen:
        for response in completion_response_gen:
            yield response.delta or ""
    return gen()


def stream_chat_response_to_tokens(
    chat_response_gen: ChatResponseGen,
) -> TokenGen:
    """Chat 응답 제너레이터를 단순 텍스트 토큰 제너레이터로 변환"""
    def gen() -> TokenGen:
        for response in chat_response_gen:
            yield response.delta or ""
    return gen()


async def astream_completion_response_to_tokens(
    completion_response_gen: CompletionResponseAsyncGen,
) -> TokenAsyncGen:
    """(비동기) Completion 응답 제너레이터를 단순 텍스트 토큰 제너레이터로 변환"""
    async def gen() -> TokenAsyncGen:
        async for response in completion_response_gen:
            yield response.delta or ""
    return gen()


async def astream_chat_response_to_tokens(
    chat_response_gen: ChatResponseAsyncGen,
) -> TokenAsyncGen:
    """(비동기) Chat 응답 제너레이터를 단순 텍스트 토큰 제너레이터로 변환"""
    async def gen() -> TokenAsyncGen:
        async for response in chat_response_gen:
            yield response.delta or ""
    return gen()


def default_completion_to_prompt(prompt: str) -> str:
    """별도의 변환 없이 프롬프트를 그대로 반환하는 기본 콜백"""
    return prompt


# ==========================================
# 4. Reflection & Compatibility Utilities
# ==========================================
@functools.lru_cache(maxsize=1000)
def _supports_tool_required(cls: Type[Any], tool_required: bool) -> bool:
    """
    주어진 클래스(LLM)가 `tool_required` 인자를 네이티브로 지원하는지 검사합니다.
    (하위 호환성 유지를 위한 리플렉션 유틸리티)
    
    Args:
        cls: 검사할 클래스 타입 (보통 LLM의 하위 클래스)
        tool_required: 검사 트리거 플래그
    """
    # Type[Any]를 사용하여 LLM 모듈과의 순환 참조(Circular Import) 방지
    supported = (
        "tool_required" in inspect.signature(cls._prepare_chat_with_tools).parameters
    )
    
    if not supported and tool_required:
        logger.warning(
            f"The 'tool_required' parameter is not supported by this version of {cls.__name__}. "
            "Please upgrade the integration to the latest version to enforce tool usage."
        )
        
    return supported