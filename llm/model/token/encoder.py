# fiber.llm.model.token.encoder
from typing import Dict, List, Union, cast, Optional, Protocol, runtime_checkable, Any, Callable
from pydantic import BaseModel
import os
import tiktoken
import time
import random
from functools import lru_cache, partial

from fiber.llm.model.types.provider.openai import AllMessageValues
from fiber.llm.model.types.provider.general import SelectTokenizerResponse
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("token.encoder")


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


def get_tokenizer(model_name: str = "gpt-3.5-turbo") -> Callable[[str], List[Any]]:
    """Get the tokenizer callable."""
    global _GLOBAL_TOKENIZER
    if _GLOBAL_TOKENIZER is not None:
        return _GLOBAL_TOKENIZER

    return partial(encode, model=model_name)


# Core Functions
@lru_cache(maxsize=1)
def get_default_encoding(model_name: str = "cl100k_base") -> tiktoken.Encoding:
    max_retries = 5
    retry_delay = 0.1

    for attempt in range(max_retries):
        try:
            return tiktoken.get_encoding(model_name)
        except (FileExistsError, OSError) as e:
            if attempt == max_retries - 1:
                log.error(f"Failed to load tiktoken encoding '{model_name}' after {max_retries} attempts. Error: {e}")
                raise

            delay = retry_delay * (2 ** attempt) + random.uniform(0, 0.1)
            log.debug(f"[Encoding] Cache collision. Retrying in {delay:.2f}s... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)


def convert_list_message_to_dict(messages: List):
    new_messages = []
    for message in messages:
        convert_msg_to_dict = cast(AllMessageValues, convert_to_dict(message))
        cleaned_message = {k: v for k, v in convert_msg_to_dict.items() if v is not None}
        new_messages.append(cleaned_message)
    return new_messages


def convert_to_dict(message: Union[BaseModel, dict]) -> dict:
    if isinstance(message, BaseModel):
        return message.model_dump(exclude_none=True)
    elif isinstance(message, dict):
        return message
    else:
        raise TypeError(f"Invalid message type: {type(message)}. Expected dict or Pydantic model.")


@lru_cache(maxsize=16)
def _load_custom_hf_tokenizer(identifier: str, revision: str = "main") -> Any:
    """명시적으로 요청된 커스텀 토크나이저만 로드하며, @lru_cache로 반복 네트워크/디스크 I/O를 원천 차단"""
    from tokenizers import Tokenizer as HFTokenizer
    if os.path.isfile(identifier):
        return HFTokenizer.from_file(identifier)

    if os.getenv("FIBER_ALLOW_HF_HUB_TOKENIZER", "false").lower() == "true":
        return HFTokenizer.from_pretrained(identifier, revision=revision)
    raise FileNotFoundError(f"Local tokenizer '{identifier}' not found and FIBER_ALLOW_HF_HUB_TOKENIZER is disabled.")

def _get_tokenizer(model: str, custom_tokenizer: Optional[Dict] = None) -> SelectTokenizerResponse:
    """네트워크 I/O가 없는 인메모리 tiktoken(cl100k_base)을 기본 사용"""
    if custom_tokenizer:
        identifier = custom_tokenizer.get("identifier", "")
        revision = custom_tokenizer.get("revision", "main")
        if identifier:
            try:
                tokenizer = _load_custom_hf_tokenizer(identifier, revision)
                return {"type": "huggingface_tokenizer", "tokenizer": tokenizer}
            except Exception as e:
                log.debug(f"Custom tokenizer '{identifier}' unavailable ({e}). Falling back to default tiktoken.")
    return {"type": "openai_tokenizer", "tokenizer": get_default_encoding()}

def encode(
    text: str,
    model: str = "gpt-3.5-turbo",
    custom_tokenizer: Optional[Union[dict, SelectTokenizerResponse]] = None,
) -> List[int]:
    if not text:
        return []

    if custom_tokenizer and "tokenizer" in custom_tokenizer:
        tokenizer_config = custom_tokenizer
    else:
        tokenizer_config = _get_tokenizer(model=model, custom_tokenizer=custom_tokenizer)

    tokenizer_obj = tokenizer_config["tokenizer"]
    try:
        if isinstance(tokenizer_obj, tiktoken.Encoding):
            return tokenizer_obj.encode(text, allowed_special="all")

        enc = tokenizer_obj.encode(text)
        if hasattr(enc, "ids"):
            return enc.ids
        return enc
    except Exception as e:
        log.error(f"[Encoder] Error encoding text '{text[:20]}...': {e}")
        return []