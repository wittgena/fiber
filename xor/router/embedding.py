# xor.router.embedding
## @lineage: router.embedding
## @lineage: bound.router.embedding
## @lineage: anchor.registry.router.embedding
import importlib
from typing import Any

import eco.llama.anchor.embeddings as embedding_pkg
from watcher.plane.emitter import get_emitter

log = get_emitter("embedding.router")

_EMBED_PKG_NAME = embedding_pkg.__name__  ## @ex: "anchor.inter.embeddings"

## @state: Core topological boundaries (Batteries-included)
DEFAULT_EMBED_REGISTRY = {
    "openai": {
        "module": f"{_EMBED_PKG_NAME}.openai.base", 
        "class": "OpenAIEmbedding",
        "is_local": False,
        "capabilities": {
            "supports_query_instruction": False,
            "supports_multimodal": False
        },
        "accepted_kwargs": ["model_name", "api_key", "api_base", "timeout"]
    },
    "fastembed": {
        "module": f"{_EMBED_PKG_NAME}.fastembed.base", 
        "class": "FastEmbedEmbedding",
        "is_local": True,
        "capabilities": {
            "supports_query_instruction": False,
            "supports_multimodal": False
        },
        "accepted_kwargs": ["model_name", "max_length", "threads"]
    },
    "huggingface": {
        "module": f"{_EMBED_PKG_NAME}.huggingface.base", 
        "class": "HuggingFaceEmbedding",
        "is_local": True,
        "capabilities": {
            "supports_query_instruction": True,
            "supports_multimodal": True
        },
        "accepted_kwargs": ["model_name", "max_length", "normalize", "query_instruction", "device"]
    }
}

class EmbeddingRouter:
    """@manifold: LlamaIndex Embedding Instantiation Router"""
    def __init__(self):
        self.registry = DEFAULT_EMBED_REGISTRY.copy()
        
        ## @promise: Integrate dynamic EmbeddingScanner for hot-patching local modules.
        # self._merge_dynamic_registry()

    def route_and_load(self, model_name: str, **kwargs) -> Any:
        provider = self._infer_provider(model_name)
        meta = self.registry.get(provider)
        
        if not meta:
            raise ValueError(f"[EmbeddingRouter] '{provider}'에 대한 임베딩 모듈이 없습니다.")
            
        ## @promise: Implement Validator pattern to filter unsupported kwargs safely.
        # accepted_kwargs = meta.get("accepted_kwargs", [])
        # valid_kwargs = {k: v for k, v in kwargs.items() if k in accepted_kwargs}
        
        ## @promise: Support capability-based routing (e.g., inject query/doc instructions if supported).
        # if meta.get("capabilities", {}).get("supports_query_instruction"):
        #     kwargs.setdefault("query_instruction", "Represent this sentence for searching relevant passages: ")

        module = importlib.import_module(meta["module"])
        EmbedClass = getattr(module, meta["class"])
        
        ## @promise: Inject auto-detected hardware profiles (device, threads) for local models.
        # if meta.get("is_local"):
        #     kwargs.setdefault("device", infer_torch_device())
        return EmbedClass(model_name=model_name, **kwargs)

    def _infer_provider(self, model_name: str) -> str:
        for provider in self.registry.keys():
            if provider in model_name:
                return provider
        
        log.debug(f"[EmbeddingRouter] Provider not explicitly found for '{model_name}'. Falling back to openai.")
        return "openai" # fallback