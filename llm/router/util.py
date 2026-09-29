# fiber.llm.router.util
from __future__ import annotations
import asyncio
import base64
import concurrent.futures
import contextvars
import functools
import inspect
import json
import logging
import os
import re
from binascii import Error as BinasciiError
from io import BytesIO
from pathlib import Path
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Sequence,
    Type,
    TypeVar,
    Union,
    runtime_checkable,
    TYPE_CHECKING,
)
from urllib.parse import urlparse

import platformdirs
import requests
from typing_extensions import Annotated

from fiber.gateway.llm.state.pydantic import (
    BaseModel,
    WithJsonSchema,
    Field,
    field_validator,
    ValidationError,
)
from xphi.arch.contract.config import env

if TYPE_CHECKING:
    from fiber.llm.types.inter.response import (
        ChatMessage,
        ChatResponseAsyncGen,
        ChatResponseGen,
        CompletionResponseAsyncGen,
        CompletionResponseGen,
    )
    from fiber.llm.types.inter.block import ContentBlock, TextBlock

    from fiber.llm.types.inter.base import TokenAsyncGen, TokenGen


logger = logging.getLogger(__name__)

T = TypeVar("T")
DEFAULT_NUM_WORKERS = 4

# ==========================================
# 1. Async & Concurrency Utilities
# ==========================================
def asyncio_run(coro: Coroutine) -> Any:
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            ctx = contextvars.copy_context()

            def run_coro_in_thread() -> Any:
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return ctx.run(new_loop.run_until_complete, coro)
                finally:
                    new_loop.close()

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_coro_in_thread)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        try:
            return asyncio.run(coro)
        except RuntimeError:
            raise RuntimeError(
                "Detected nested async. Please use nest_asyncio.apply() to allow nested event loops. "
                "Or, use async entry methods like `aquery()`, `aretriever`, `achat`, etc."
            )


# ==========================================
# 2. File, Network & Progress Utilities
# ==========================================
def get_tqdm_iterable(
    items: Iterable, show_progress: bool, desc: str, total: Optional[int] = None
) -> Iterable:
    """Optionally get a tqdm iterable."""
    if show_progress:
        try:
            from tqdm.auto import tqdm
            return tqdm(items, desc=desc, total=total)
        except ImportError:
            pass
    return items


def get_cache_dir() -> str:
    """Locate a platform-appropriate cache directory."""
    if "ROUTER_CACHE_DIR" in os.environ:
        path = Path(os.environ["ROUTER_CACHE_DIR"])
    else:
        path = Path(platformdirs.user_cache_dir("router"))
        
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def resolve_binary(
    raw_bytes: Optional[bytes] = None,
    path: Optional[Union[str, Path]] = None,
    url: Optional[str] = None,
    as_base64: bool = False,
) -> BytesIO:
    """Resolve binary data from bytes, file path, or URL."""
    if raw_bytes is not None:
        try:
            decoded_bytes = base64.b64decode(raw_bytes, validate=True)
        except BinasciiError:
            decoded_bytes = raw_bytes

        if as_base64:
            return BytesIO(base64.b64encode(decoded_bytes))
        return BytesIO(decoded_bytes)

    elif path is not None:
        path = Path(path) if isinstance(path, str) else path
        data = path.read_bytes()
        if as_base64:
            return BytesIO(base64.b64encode(data))
        return BytesIO(data)

    elif url is not None:
        parsed_url = urlparse(url)
        if parsed_url.scheme == "data":
            data_part = parsed_url.path

            if "," not in data_part:
                raise ValueError("Invalid data URL format: missing comma separator")

            metadata, url_data = data_part.split(",", 1)
            is_base64_encoded = metadata.endswith(";base64")

            if is_base64_encoded:
                decoded_data = base64.b64decode(url_data)
                if as_base64:
                    return BytesIO(base64.b64encode(decoded_data))
                return BytesIO(decoded_data)
            else:
                if as_base64:
                    return BytesIO(base64.b64encode(url_data.encode("utf-8")))
                return BytesIO(url_data.encode("utf-8"))

        headers = {"User-Agent": env.USER_AGENT}
        response = requests.get(url, headers=headers, timeout=(60, 60))
        response.raise_for_status()
        if as_base64:
            return BytesIO(base64.b64encode(response.content))
        return BytesIO(response.content)

    raise ValueError("No valid source provided to resolve binary data!")


# ==========================================
# 3. Text & JSON Parsing Utilities
# ==========================================
def truncate_text(text: str, max_length: int) -> str:
    """Truncate text to a maximum length."""
    if len(text) <= max_length:
        return text
    if max_length - 3 < 0:
        return text[:max_length]
    return text[: max_length - 3] + "..."


def parse_partial_json(s: str) -> Dict:
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass

    new_s = ""
    stack = []
    is_inside_string = False
    escaped = False
    for char in s:
        if is_inside_string:
            if char == '"' and not escaped:
                is_inside_string = False
            elif char == "\n" and not escaped:
                char = "\\n"  
            elif char == "\\":
                escaped = not escaped
            else:
                escaped = False
        else:
            if char == '"':
                is_inside_string = True
                escaped = False
            elif char == "{":
                stack.append("}")
            elif char == "[":
                stack.append("]")
            elif char == "}" or char == "]":
                if stack and stack[-1] == char:
                    stack.pop()
                else:
                    raise ValueError("Malformed partial JSON encountered.")

        new_s += char

    if is_inside_string and '"' in new_s and ":" not in new_s[new_s.rindex('"') :]:
        new_s = new_s[: new_s.rindex('"')]
    elif is_inside_string:
        new_s += '"'

    new_s = new_s.rstrip()
    if new_s.endswith(":"):
        new_s += " null"
    elif new_s.endswith(","):
        new_s = new_s[:-1]

    for closing_char in reversed(stack):
        new_s += closing_char

    try:
        return json.loads(new_s)
    except json.JSONDecodeError:
        raise ValueError("Malformed partial JSON encountered.")


class SafeFormatter:
    def __init__(self, format_dict: Optional[Dict[str, str]] = None):
        self.format_dict = format_dict or {}

    def format(self, format_string: str) -> str:
        return re.sub(r"\{([^{}]+)\}", self._replace_match, format_string)

    def parse(self, format_string: str) -> List[str]:
        return re.findall(
            r"\{([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\}", format_string
        )

    def _replace_match(self, match: re.Match) -> str:
        key = match.group(1)
        value = self.format_dict.get(key, match.group(0))
        if isinstance(value, bytes):
            return resolve_binary(value, as_base64=True).read().decode("utf-8")
        return str(value)


def format_string(string_to_format: str, **kwargs: str) -> str:
    """Format a string with kwargs."""
    formatter = SafeFormatter(format_dict=kwargs)
    return formatter.format(string_to_format)


def format_content_blocks(
    content_blocks: List["ContentBlock"], **kwargs: str
) -> List["ContentBlock"]:
    """Format content blocks with kwargs."""
    # 런타임 isinstance 체크를 위해 함수 내부 지연 임포트 사용
    from fiber.llm.types.inter.block import TextBlock
    
    formatter = SafeFormatter(format_dict=kwargs)
    formatted_blocks: List["ContentBlock"] = []
    
    for block in content_blocks:
        if isinstance(block, TextBlock):
            formatted_blocks.append(TextBlock(text=formatter.format(block.text)))
        else:
            formatted_blocks.append(block)

    return formatted_blocks


def get_template_vars(template_str: str) -> List[str]:
    variables = []
    formatter = SafeFormatter()
    for variable_name in formatter.parse(template_str):
        if variable_name:
            variables.append(variable_name)
    return variables


# ==========================================
# 4. LLM Domain Data Models
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
# 5. LLM Protocols & Type Aliases
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


MessagesToPromptCallable = Annotated[
    Optional[MessagesToPromptType],
    WithJsonSchema({"type": "string"}),
]

CompletionToPromptCallable = Annotated[
    Optional[CompletionToPromptType],
    WithJsonSchema({"type": "string"}),
]


# ==========================================
# 6. LLM Stream Generator Utilities
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
# 7. Reflection & Compatibility Utilities
# ==========================================
@functools.lru_cache(maxsize=1000)
def _supports_tool_required(cls: Type[Any], tool_required: bool) -> bool:
    """
    주어진 클래스(LLM)가 `tool_required` 인자를 네이티브로 지원하는지 검사합니다.
    """
    supported = (
        "tool_required" in inspect.signature(cls._prepare_chat_with_tools).parameters
    )
    
    if not supported and tool_required:
        logger.warning(
            f"The 'tool_required' parameter is not supported by this version of {cls.__name__}. "
            "Please upgrade the integration to the latest version to enforce tool usage."
        )
        
    return supported