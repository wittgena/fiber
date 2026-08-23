# dphi.model.inter.registry
## @lineage: agent.llm.router.inter.registry
## @lineage: bound.xor.model.router.inter.registry
## @lineage: eco.model.router.inter.registry
## @lineage: engine.router.inter.registry
## @lineage: engine.client.inter.registry
from typing import Dict

from fiber.agent.llm.router.base import BaseProviderAdapter, GenericHTTPAdapter
from fiber.dphi.model.inter.llm import InterLLMAdapter
from fiber.dphi.model.inter.embedding import InterEmbeddingAdapter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("registry.adapter")

class AdapterRegistry:
    """@state: Multi-dimensional topological boundaries"""
    _adapters: Dict[str, Dict[str, BaseProviderAdapter]] = {
        "llm": {},
        "embedding": {}
    }
    _fallback_adapters: Dict[str, BaseProviderAdapter] = {}
    _is_initialized: bool = False

    @classmethod
    def setup_defaults(cls):
        ## @phase: Initialize primary kernels (Lazy Load Boundary)
        if cls._is_initialized:
            return

        log.debug("[Registry] 시스템 코어 다중 위상(Multi-topology) 레지스트리 초기화 시작")
        llm_generic = GenericHTTPAdapter()
        llm_inter = InterLLMAdapter()
        cls._fallback_adapters["llm"] = llm_generic
        for provider in ["ollama", "huggingface"]:
            cls._adapters["llm"][provider] = llm_generic

        for provider in ["inter", "anthropic", "gemini"]:
            cls._adapters["llm"][provider] = llm_inter

        embed_inter = InterEmbeddingAdapter()
        cls._fallback_adapters["embedding"] = embed_inter 
        for provider in ["openai", "azure", "cohere", "inter"]:
            cls._adapters["embedding"][provider] = embed_inter

        ## @seal: Lock initialization state
        cls._is_initialized = True
        log.debug("[Registry] 시스템 코어 레지스트리 초기화 완료")

    @classmethod
    def register(cls, task_type: str, provider_name: str, adapter: BaseProviderAdapter):
        ## @mutate: Dynamically inject or overwrite a topological mapping
        # [수정]: 등록 전 초기화를 강제하여 커스텀 어댑터가 덮어씌워지는 현상 방어
        if not cls._is_initialized:
            cls.setup_defaults()

        if task_type not in cls._adapters:
            cls._adapters[task_type] = {}
        
        cls._adapters[task_type][provider_name] = adapter
        log.debug(f"[Registry] '{task_type}' 위상에 '{provider_name}' 어댑터 동적 등록됨.")

    @classmethod
    def get_adapter(cls, task_type: str, provider_name: str) -> BaseProviderAdapter:
        ## @resolve: Extract adapter by traversing Task -> Provider manifold
        if not cls._is_initialized:
            cls.setup_defaults()
            
        task_manifold = cls._adapters.get(task_type, {})
        fallback = cls._fallback_adapters.get(task_type)
        
        adapter = task_manifold.get(provider_name, fallback)
        if not adapter:
            raise ValueError(f"[Registry Error] '{task_type}' 작업을 처리할 폴백 어댑터조차 구성되지 않았습니다.")
            
        return adapter