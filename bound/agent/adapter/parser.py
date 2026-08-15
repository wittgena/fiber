# bound.agent.adapter.parser
## @lineage: ext.router.adapter.parser
## @lineage: router.adapter.parser
import asyncio
import base64
import concurrent.futures
import contextvars
import json
import os
import re
from binascii import Error as BinasciiError
from functools import partial
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
    TypeVar,
    Union,
    runtime_checkable,
    TYPE_CHECKING,
)
from urllib.parse import urlparse

import platformdirs
import requests

from bound.agent.adapter.callback.dispatcher import dispatcher

# ---------------------------------------------------------
# [순환 참조 해결 1] 타입 힌트용으로만 임포트 (런타임에는 실행되지 않음)
# ---------------------------------------------------------
if TYPE_CHECKING:
    from bound.agent.llm.model.types.block import ContentBlock, TextBlock


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


@dispatcher.span
async def run_jobs(
    jobs: List[Coroutine[Any, Any, T]],
    show_progress: bool = False,
    workers: int = DEFAULT_NUM_WORKERS,
    desc: Optional[str] = None,
) -> List[T]:
    semaphore = asyncio.Semaphore(workers)

    @dispatcher.span
    async def worker(job: Coroutine) -> Any:
        async with semaphore:
            return await job

    pool_jobs = [worker(job) for job in jobs]

    if show_progress:
        from tqdm.asyncio import tqdm_asyncio
        results = await tqdm_asyncio.gather(*pool_jobs, desc=desc)
    else:
        results = await asyncio.gather(*pool_jobs)

    return results

# ==========================================
# 2. Tokenizer & Text Utilities
# ==========================================

@runtime_checkable
class Tokenizer(Protocol):
    def encode(self, text: str, *args: Any, **kwargs: Any) -> List[Any]: ...

_GLOBAL_TOKENIZER: Optional[Callable[[str], List[Any]]] = None

def set_global_tokenizer(tokenizer: Union[Tokenizer, Callable[[str], list]]) -> None:
    """Set the global tokenizer internally without relying on external libraries."""
    global _GLOBAL_TOKENIZER
    if isinstance(tokenizer, Tokenizer):
        _GLOBAL_TOKENIZER = tokenizer.encode
    else:
        _GLOBAL_TOKENIZER = tokenizer


def get_tokenizer(model_name: str = "gpt-3.5-turbo") -> Callable[[str], List]:
    """Get the global tokenizer, initializing with tiktoken if necessary."""
    global _GLOBAL_TOKENIZER

    if _GLOBAL_TOKENIZER is None:
        try:
            import tiktoken
        except ImportError:
            raise ImportError(
                "`tiktoken` package not found, please run `pip install tiktoken`"
            )

        should_revert = False
        if "TIKTOKEN_CACHE_DIR" not in os.environ:
            should_revert = True
            os.environ["TIKTOKEN_CACHE_DIR"] = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "_static/tiktoken_cache",
            )

        enc = tiktoken.encoding_for_model(model_name)
        tokenizer = partial(enc.encode, allowed_special="all")
        set_global_tokenizer(tokenizer)

        if should_revert:
            del os.environ["TIKTOKEN_CACHE_DIR"]

        assert _GLOBAL_TOKENIZER is not None

    return _GLOBAL_TOKENIZER


def truncate_text(text: str, max_length: int) -> str:
    """Truncate text to a maximum length."""
    if len(text) <= max_length:
        return text
    if max_length - 3 < 0:
        return text[:max_length]
    return text[: max_length - 3] + "..."


SAMPLE_TEXT = """
LLMs are a phenomenal piece of technology for knowledge generation and reasoning.
LlamaIndex is a "data framework" to help you build LLM apps by augmenting them with your own private data.
It offers data connectors, ways to structure your data, and an advanced retrieval/query interface.
"""


# ==========================================
# 3. File & Progress Utilities
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

        headers = {
            "User-Agent": "surgent/0.0 (https://surgent.ai; info@surgent.ai) surgent-core/0.0"
        }
        response = requests.get(url, headers=headers, timeout=(60, 60))
        response.raise_for_status()
        if as_base64:
            return BytesIO(base64.b64encode(response.content))
        return BytesIO(response.content)

    raise ValueError("No valid source provided to resolve binary data!")


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
                char = "\\n"  # Replace the newline character with the escape sequence.
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


# ==========================================
# 4. Prompt & Formatting Utilities
# ==========================================

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
    # ---------------------------------------------------------
    # [순환 참조 해결 2] 함수 내부에서 지연 임포트(Lazy Import) 실행
    # ---------------------------------------------------------
    from bound.agent.llm.model.types.block import TextBlock
    
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