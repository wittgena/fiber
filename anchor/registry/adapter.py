# anchor.registry.adapter
## @lineage: anchor.provider.adapter
## @lineage: anchor.surface.registry.adapter
## @lineage: bound.adapter.provider.registry
"""
@manifold: Multi-dimensional Adapter Registry (Microkernel Core)
@flow: (Task Type, Provider Vector) -> Lazy Instantiation -> Adapter Binding
@desc: 
- Anchors provider identities to their corresponding structural execution adapters across multiple task topologies (llm, embedding).
- Ensures singleton lazy-loading to prevent premature memory collapse.
"""
from typing import Dict, Optional

from anchor.inter.bound.adapter.base import BaseProviderAdapter, OpenAICompatibleAdapter, GenericHTTPAdapter
from anchor.inter.bound.adapter.llm import InterLLMAdapter
from anchor.inter.bound.adapter.embedding import InterEmbeddingAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("registry.adapter")

class AdapterRegistry:
    """@state: Multi-dimensional topological boundaries"""
    
    # 1차원: Task (llm, embedding), 2차원: Provider
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

        # ---------------------------------------------------------
        # [Topology 1] LLM Generation (텍스트 생성)
        # ---------------------------------------------------------
        llm_openai = OpenAICompatibleAdapter()
        llm_generic = GenericHTTPAdapter()
        llm_inter = InterLLMAdapter()

        cls._fallback_adapters["llm"] = llm_generic

        for provider in ["openai", "custom_openai", "azure", "groq", "mistral", "anyscale", "deepinfra"]:
            cls._adapters["llm"][provider] = llm_openai

        for provider in ["ollama", "huggingface"]:
            cls._adapters["llm"][provider] = llm_generic

        for provider in ["inter", "anthropic", "gemini"]:
            cls._adapters["llm"][provider] = llm_inter


        # ---------------------------------------------------------
        # [Topology 2] Embedding (벡터 변환)
        # ---------------------------------------------------------
        embed_inter = InterEmbeddingAdapter()
        
        # 임베딩은 기본적으로 모두 LlamaIndex(InterAdapter)를 태우도록 폴백 설정
        cls._fallback_adapters["embedding"] = embed_inter 

        for provider in ["openai", "azure", "cohere", "inter"]:
            cls._adapters["embedding"][provider] = embed_inter


        ## @seal: Lock initialization state
        cls._is_initialized = True
        log.debug("[Registry] 시스템 코어 레지스트리 초기화 완료")

    @classmethod
    def register(cls, task_type: str, provider_name: str, adapter: BaseProviderAdapter):
        ## @mutate: Dynamically inject or overwrite a topological mapping
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