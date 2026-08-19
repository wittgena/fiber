# eco.model.router.embedding
## @lineage: engine.router.embedding
## @lineage: engine.router.registry.embedding
import importlib
from typing import Any

import eco.bound.agent.embedding as embedding_pkg
from watcher.plane.emitter import get_emitter

log = get_emitter("embedding.router")

_EMBED_PKG_NAME = embedding_pkg.__name__  ## @ex: "anchor.inter.embeddings"

## @state: Core topological boundaries (Batteries-included)
DEFAULT_EMBED_REGISTRY = {
    "openai": {
        "module": f"{_EMBED_PKG_NAME}.openai", 
        "class": "OpenAIEmbedding",
        "is_local": False,
        "capabilities": {
            "supports_query_instruction": False,
            "supports_multimodal": False
        },
        "accepted_kwargs": ["model_name", "api_key", "api_base", "timeout"]
    },
    "ollama": {
        "module": f"{_EMBED_PKG_NAME}.ollama", 
        "class": "OllamaEmbedding",
        "is_local": True,
        "capabilities": {
            "supports_query_instruction": False,
            "supports_multimodal": False
        },
        "accepted_kwargs": ["model_name", "max_length", "threads"]
    }
}

class EmbeddingRouter:
    """@manifold: LlamaIndex Embedding Instantiation Router"""
    def __init__(self):
        self.registry = DEFAULT_EMBED_REGISTRY.copy()

    def route_and_load(self, model_name: str, **kwargs) -> Any:
        provider = self._infer_provider(model_name)
        meta = self.registry.get(provider)
        
        if not meta:
            raise ValueError(f"[EmbeddingRouter] '{provider}'에 대한 임베딩 모듈이 없습니다.")

        module = importlib.import_module(meta["module"])
        EmbedClass = getattr(module, meta["class"])
        return EmbedClass(model_name=model_name, **kwargs)

    def _infer_provider(self, model_name: str) -> str:
        for provider in self.registry.keys():
            if provider in model_name:
                return provider
        
        log.debug(f"[EmbeddingRouter] Provider not explicitly found for '{model_name}'. Falling back to openai.")
        return "openai" # fallback