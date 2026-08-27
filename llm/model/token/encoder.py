# fiber.llm.model.token.encoder
## @lineage: llm.model.token.encoder
from typing import Dict, List, Type, Union, cast, Optional
from pydantic import BaseModel
import tiktoken
import time
import random
from functools import lru_cache

from fiber.llm.model.types.openai import AllMessageValues
from fiber.llm.model.types.general import SelectTokenizerResponse
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("token.encoder")

@lru_cache(maxsize=1)
def get_default_encoding(model_name: str = "cl100k_base") -> tiktoken.Encoding:
    max_retries = 5
    retry_delay = 0.1  

    for attempt in range(max_retries):
        try:
            # 외부 경로 주입 없이 순수하게 표준 API만 호출합니다.
            return tiktoken.get_encoding(model_name)
            
        except (FileExistsError, OSError) as e:
            # Gunicorn 등 다중 워커가 동시에 최초 부팅될 때 
            # OS 임시 폴더에 캐시 파일을 동시에 쓰려다 발생하는 충돌(Race Condition) 방어
            if attempt == max_retries - 1:
                log.error(f"Failed to load tiktoken encoding '{model_name}' after {max_retries} attempts. Error: {e}")
                raise
            
            delay = retry_delay * (2 ** attempt) + random.uniform(0, 0.1)
            log.debug(f"[Encoding] Cache collision. Retrying in {delay:.2f}s... (Attempt {attempt+1}/{max_retries})")
            time.sleep(delay)

def convert_list_message_to_dict(messages: List):
    new_messages = []
    for message in messages:
        convert_msg_to_dict = cast(AllMessageValues, convert_to_dict(message))
        # 만약 cleanup_none_field_in_message 함수가 별도로 존재한다면 아래 코드를 유지하지만,
        # 현재 컨텍스트에 없다면 딕셔너리 컴프리헨션으로 안전하게 None을 제거합니다.
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


# =========================================================================
# [NEW] Integration with HuggingFace Tokenizers and Counter.py Facade
# =========================================================================

def _get_tokenizer(model: str, custom_tokenizer: Optional[Dict] = None) -> SelectTokenizerResponse:
    """
    splitter.py 로직 차용: 모델명에 맞는 적절한 토크나이저(HuggingFace 또는 tiktoken)를 반환합니다.
    """
    if custom_tokenizer:
        # custom_tokenizer 사용 시의 단순화된 지원 (전체 로직은 splitter에 위임)
        from tokenizers import Tokenizer
        identifier = custom_tokenizer.get("identifier", "")
        revision = custom_tokenizer.get("revision", "main")
        try:
            tokenizer = Tokenizer.from_pretrained(identifier, revision=revision)
            return {"type": "huggingface_tokenizer", "tokenizer": tokenizer}
        except Exception as e:
            log.warning(f"Failed to load custom tokenizer: {e}. Falling back to default.")

    # Llama 계열 체크
    model_lower = model.lower()
    if "llama" in model_lower:
        try:
            from tokenizers import Tokenizer
            repo = "Xenova/llama-3-tokenizer" if "llama-3" in model_lower else "hf-internal-testing/llama-tokenizer"
            tokenizer = Tokenizer.from_pretrained(repo)
            return {"type": "huggingface_tokenizer", "tokenizer": tokenizer}
        except ImportError:
            log.debug("tokenizers library not found. Falling back to tiktoken.")
        except Exception as e:
            log.debug(f"Failed to load specific llama tokenizer: {e}. Falling back to tiktoken.")

    # 기본값 (OpenAI cl100k_base 등)
    return {"type": "openai_tokenizer", "tokenizer": get_default_encoding()}

def encode(text: str, model: str = "gpt-3.5-turbo", custom_tokenizer: Optional[Union[dict, SelectTokenizerResponse]] = None) -> List[int]:
    """
    @desc: counter.py에서 기대하는 범용 문자열 인코딩 함수입니다.
    선택된 토크나이저(tiktoken 또는 HuggingFace)를 사용하여 텍스트를 토큰 ID 리스트로 변환합니다.
    """
    if not text:
        return []

    # custom_tokenizer가 이미 SelectTokenizerResponse 규격인 경우
    if custom_tokenizer and "tokenizer" in custom_tokenizer:
        tokenizer_config = custom_tokenizer
    else:
        tokenizer_config = _get_tokenizer(model=model, custom_tokenizer=custom_tokenizer)

    tokenizer_obj = tokenizer_config["tokenizer"]

    try:
        # Tiktoken 처리
        if isinstance(tokenizer_obj, tiktoken.Encoding):
            return tokenizer_obj.encode(text, disallowed_special=())
        
        # HuggingFace Tokenizer 처리
        enc = tokenizer_obj.encode(text)
        if hasattr(enc, "ids"):
            return enc.ids
        return enc
    except Exception as e:
        log.error(f"[Encoder] Error encoding text '{text[:20]}...': {e}")
        return []