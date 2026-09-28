# fiber.llm.types.llm.response
from __future__ import annotations

import base64
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    Generator,
    List,
    Optional,
    Union,
    cast,
    TYPE_CHECKING,
)

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from fiber.gateway.llm.mapper.pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    model_validator,
)

from fiber.llm.types.llm.block import (
    MessageRole,
    BaseRecursiveContentBlock,
    ContentBlock,
    TextBlock,
    ImageBlock,
)
from xphi.arch.bound.client.constants import DEFAULT_CONTEXT_WINDOW, DEFAULT_NUM_OUTPUTS
if TYPE_CHECKING:
    from fiber.llm.router.tools.types import ToolOutput
    from fiber.llm.types.inter.component import ImageDocument


# ==========================================
# 1. Base Metadata & Logging Types
# ==========================================
class LogProb(BaseModel):
    """토큰별 로그 확률(Log probability) 정보"""
    token: str = Field(default_factory=str)
    logprob: float = Field(default_factory=float)
    bytes: List[int] = Field(default_factory=list)


class LLMMetadata(BaseModel):
    """LLM 모델의 스펙 및 기능 지원 여부를 정의하는 메타데이터"""
    model_config = ConfigDict(
        protected_namespaces=("pydantic_model_",), arbitrary_types_allowed=True
    )
    context_window: int = Field(
        default=DEFAULT_CONTEXT_WINDOW,
        description="모델이 처리할 수 있는 최대 컨텍스트 토큰 수",
    )
    num_output: int = Field(
        default=DEFAULT_NUM_OUTPUTS,
        description="모델이 생성할 수 있는 최대 출력 토큰 수",
    )
    is_chat_model: bool = Field(
        default=False,
        description="Chat 인터페이스(메시지 시퀀스 입력) 지원 여부",
    )
    is_function_calling_model: bool = Field(
        default=False,
        description="Function Calling / Tool Use 지원 여부",
    )
    model_name: str = Field(
        default="unknown",
        description="로깅 및 디버깅을 위한 모델 식별자",
    )
    system_role: MessageRole = Field(
        default=MessageRole.SYSTEM,
        description="해당 모델이 시스템 프롬프트에 사용하는 Role (예: SYSTEM, CHATBOT)",
    )


# ==========================================
# 2. Core Chat Message Type
# ==========================================
class ChatMessage(BaseRecursiveContentBlock):
    """
    단일 턴(Turn)의 채팅 메시지를 표현하는 객체.
    내부적으로 텍스트, 이미지 등의 다중 블록(ContentBlock)을 포함할 수 있습니다.
    """
    role: MessageRole = Field(default=MessageRole.USER)
    additional_kwargs: dict[str, Any] = Field(default_factory=dict)
    blocks: list[ContentBlock] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _parse_legacy_content(cls, data: Any) -> Any:
        """
        [개선됨] 기존 Pydantic __init__ 오버라이딩을 대체.
        `content` 필드로 들어오는 단일 문자열/리스트를 `blocks` 구조로 안전하게 변환합니다.
        """
        if isinstance(data, dict) and "content" in data:
            content_val = data.pop("content")
            if content_val is not None:
                blocks = data.get("blocks", [])
                if isinstance(content_val, str):
                    blocks.append(TextBlock(text=content_val))
                elif isinstance(content_val, list):
                    blocks.extend(content_val)
                data["blocks"] = blocks
        return data

    @model_validator(mode="after")
    def legacy_additional_kwargs_image(self) -> Self:
        """하위 호환성: additional_kwargs 내의 이미지를 ImageBlock으로 변환"""
        if documents := self.additional_kwargs.get("images"):
            # ImageDocument 타입 추론
            from fiber.llm.types.inter.component import ImageDocument
            documents = cast(list[ImageDocument], documents)
            for doc in documents:
                img_base64_bytes = doc.resolve_image(as_base64=True).read()
                self.blocks.append(ImageBlock(image=img_base64_bytes))
        return self

    @classmethod
    def nested_blocks_field_name(cls) -> str:
        return "blocks"

    @property
    def content(self) -> str | None:
        """하위 호환성을 위해 TextBlock들의 텍스트를 결합하여 반환"""
        content_strs = [
            block.text for block in self.blocks if isinstance(block, TextBlock)
        ]
        ct = "\n".join(content_strs) or None
        if ct is None and len(content_strs) == 1:
            return ""
        return ct

    @content.setter
    def content(self, content: str) -> None:
        if not self.blocks:
            self.blocks = [TextBlock(text=content)]
        elif len(self.blocks) == 1 and isinstance(self.blocks[0], TextBlock):
            self.blocks = [TextBlock(text=content)]
        else:
            raise ValueError(
                "ChatMessage contains multiple blocks, use 'ChatMessage.blocks' instead."
            )

    def __str__(self) -> str:
        return f"{self.role.value}: {self.content}"

    @classmethod
    def from_str(
        cls,
        content: str,
        role: Union[MessageRole, str] = MessageRole.USER,
        **kwargs: Any,
    ) -> Self:
        if isinstance(role, str):
            role = MessageRole(role)
        return cls(role=role, blocks=[TextBlock(text=content)], **kwargs)

    def _recursive_serialization(self, value: Any) -> Any:
        if isinstance(value, BaseModel):
            value.model_rebuild()
            return value.model_dump()
        if isinstance(value, dict):
            return {
                key: self._recursive_serialization(val)
                for key, val in value.items()
            }
        if isinstance(value, list):
            return [self._recursive_serialization(item) for item in value]
        if isinstance(value, bytes):
            return base64.b64encode(value).decode("utf-8")
        return value

    @field_serializer("additional_kwargs", check_fields=False)
    def serialize_additional_kwargs(self, value: Any, _info: Any) -> Any:
        return self._recursive_serialization(value)


# ==========================================
# 3. Response DTOs
# ==========================================
class ChatResponse(BaseModel):
    """표준 LLM 채팅 응답"""
    message: ChatMessage
    raw: Optional[Any] = Field(default=None, description="LLM 공급자(API)의 원본 응답 객체")
    delta: Optional[str] = Field(default=None, description="스트리밍 시 사용되는 청크 델타")
    logprobs: Optional[List[List[LogProb]]] = None
    additional_kwargs: dict = Field(default_factory=dict)

    def __str__(self) -> str:
        return str(self.message)


class CompletionResponse(BaseModel):
    """단일 텍스트 완성(Completion) 응답"""
    text: str
    additional_kwargs: dict = Field(default_factory=dict)
    raw: Optional[Any] = Field(default=None, description="LLM 공급자(API)의 원본 응답 객체")
    logprobs: Optional[List[List[LogProb]]] = None
    delta: Optional[str] = Field(default=None, description="스트리밍 시 사용되는 청크 델타")

    def __str__(self) -> str:
        return self.text


class AgentChatResponse(BaseModel):
    """
    LLM 에이전트 워크플로우의 최종 결과물 컨테이너 (LlamaIndex 종속성 제거 완결)
    """
    response: str = Field(
        description="LLM이 도구를 활용하여 최종적으로 도출한 사용자 대상 응답"
    )
    sources: List['ToolOutput'] = Field(
        default_factory=list,
        description="에이전트가 답변을 생성하기 위해 호출한 도구(Tool)들의 실행 이력"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="토큰 사용량, 추론 시간(Latency), 에이전트 스텝 수 등의 메타데이터"
    )

    def __str__(self) -> str:
        return self.response

ChatResponseGen = Generator[ChatResponse, None, None]
ChatResponseAsyncGen = AsyncGenerator[ChatResponse, None]

CompletionResponseGen = Generator[CompletionResponse, None, None]
CompletionResponseAsyncGen = AsyncGenerator[CompletionResponse, None]