# bound.gateway.token.splitter
## @lineage: gateway.token.splitter
## @lineage: bound.eco.token.splitter
## @lineage: bound.token.splitter
## @lineage: bound.surface.token.splitter
## @lineage: bound.adapter.surface.token.splitter
from functools import lru_cache
from typing import Callable, List, Optional, Dict
from tokenizers import Tokenizer
import tiktoken

from bound.gateway.token.convert import get_default_encoding
from eco.legacy.types import CustomHuggingfaceTokenizer, SelectTokenizerResponse
from bound.resolver.model.config.constants import DEFAULT_MAX_LRU_CACHE_SIZE
from watcher.plane.emitter import get_emitter

log = get_emitter("token.splitter")

@lru_cache(maxsize=DEFAULT_MAX_LRU_CACHE_SIZE)
def _select_tokenizer_helper(model: str) -> SelectTokenizerResponse:
    """Selects the appropriate tokenizer based on the model name"""
    try:
        # Fallback logic based on model naming conventions rather than global state
        if "llama-2" in model.lower():
            tokenizer = Tokenizer.from_pretrained("hf-internal-testing/llama-tokenizer")
            return {"type": "huggingface_tokenizer", "tokenizer": tokenizer}
        
        elif "llama-3" in model.lower():
            tokenizer = Tokenizer.from_pretrained("Xenova/llama-3-tokenizer")
            return {"type": "huggingface_tokenizer", "tokenizer": tokenizer}
    except Exception as e:
        log.debug(f"Error selecting huggingface tokenizer: {e}. Falling back to default.")

    return {"type": "openai_tokenizer", "tokenizer": get_default_encoding()}

def create_pretrained_tokenizer(identifier: str, revision="main", auth_token: Optional[str] = None):
    try:
        tokenizer = Tokenizer.from_pretrained(
            identifier, revision=revision, auth_token=auth_token  # type: ignore
        )
    except Exception as e:
        log.error(f"Error creating pretrained tokenizer: {e}. Defaulting to version without 'auth_token'.")
        tokenizer = Tokenizer.from_pretrained(identifier, revision=revision)
    return {"type": "huggingface_tokenizer", "tokenizer": tokenizer}

def _get_tokenizer(model: str, custom_tokenizer: Optional[Dict] = None) -> SelectTokenizerResponse:
    if custom_tokenizer is not None:
        return create_pretrained_tokenizer(
            identifier=custom_tokenizer.get("identifier", ""),
            revision=custom_tokenizer.get("revision", "main"),
            auth_token=custom_tokenizer.get("auth_token")
        )
    return _select_tokenizer_helper(model=model)

def _strip_huggingface_special_token_ids(tokenizer: Tokenizer, tokens: List[int]) -> List[int]:
    try:
        added_tokens_decoder = tokenizer.get_added_tokens_decoder()
    except Exception:
        return tokens

    special_token_ids = {
        token_id
        for token_id, added_token in added_tokens_decoder.items()
        if getattr(added_token, "special", False)
    }
    if not special_token_ids:
        return tokens
    return [token for token in tokens if token not in special_token_ids]

class TokenSplitter:
    """
    @manifold: Pure Token-Safe Text Splitter
    @desc: 
    - Replaces LlamaIndex's SentenceSplitter.
    - Slices based on Token IDs to prevent multibyte (e.g., Korean) data corruption.
    - Absorbs tokenization logic internally for complete decoupling.
    """
    def __init__(
        self,
        chunk_size: int,
        chunk_overlap: int,
        model: str = "gpt-3.5-turbo",
        custom_tokenizer: Optional[Dict] = None
    ):
        if chunk_overlap >= chunk_size:
            raise ValueError("[TokenSplitter] chunk_overlap must be strictly less than chunk_size.")

        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.model = model
        
        # Load the tokenizer configuration once upon instantiation
        self._tokenizer_config = _get_tokenizer(model=self.model, custom_tokenizer=custom_tokenizer)

    def encode(self, text: str) -> List[int]:
        """Internal encoding function using the loaded tokenizer configuration."""
        tokenizer_obj = self._tokenizer_config["tokenizer"]
        
        if isinstance(tokenizer_obj, tiktoken.Encoding):
            enc = tokenizer_obj.encode(text, disallowed_special=())
            return enc
        else:
            enc = tokenizer_obj.encode(text)
            if hasattr(enc, "ids"):
                return enc.ids
            return enc

    def decode(self, tokens: List[int], skip_special_tokens: bool = True) -> str:
        """Internal decoding function using the loaded tokenizer configuration."""
        if not tokens:
            return ""
            
        tokenizer_obj = self._tokenizer_config["tokenizer"]
        
        if self._tokenizer_config["type"] == "huggingface_tokenizer":
            if skip_special_tokens:
                tokens = _strip_huggingface_special_token_ids(tokenizer_obj, tokens)
            return tokenizer_obj.decode(tokens, skip_special_tokens=skip_special_tokens)
            
        return tokenizer_obj.decode(tokens)

    def split_text(self, text: str) -> List[str]:
        if not text:
            return []

        token_ids = self.encode(text)
        total_tokens = len(token_ids)

        if total_tokens <= self.chunk_size:
            return [text]

        chunks: List[str] = []
        start_idx = 0

        while start_idx < total_tokens:
            end_idx = min(start_idx + self.chunk_size, total_tokens)
            chunk_tokens = token_ids[start_idx:end_idx]

            try:
                ## @safe_boundary: Decode token IDs back to a string safely
                chunk_text = self.decode(chunk_tokens)
                chunks.append(chunk_text)
            except Exception as e:
                log.error(f"[TokenSplitter] Chunk decoding failed (start={start_idx}, end={end_idx}): {e}")

            if end_idx == total_tokens:
                break
            
            ## @overlap: Calculate next starting point
            start_idx += (self.chunk_size - self.chunk_overlap)
            
        return chunks