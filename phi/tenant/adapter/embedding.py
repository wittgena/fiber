# phi.tenant.adapter.embedding
from typing import List, Union
from phi.runtime.executor.pre import EmbeddingContext
from phi.tenant.adapter.base import BaseProviderAdapter
from phi.tenant.router.embedding import EmbeddingRouter

class InterEmbeddingAdapter(BaseProviderAdapter):
    def __init__(self):
        self.router = EmbeddingRouter()

    async def execute(self, ctx: EmbeddingContext):
        ## @phase: Topological Projection (Dynamic Instantiation)
        llama_kwargs = {
            "api_key": ctx.api_key,
            "api_base": ctx.api_base,
            "timeout": ctx.timeout if isinstance(ctx.timeout, (int, float)) else 60.0,
        }
        
        ## @bind: Merge residual vectors
        for k, v in ctx.optional_params.items():
            if k not in llama_kwargs:
                llama_kwargs[k] = v

        ## @purge: Drop null vectors to preserve native boundaries
        llama_kwargs = {k: v for k, v in llama_kwargs.items() if v is not None}

        try:
            # LlamaIndex BaseEmbedding을 상속받은 객체를 동적 로드
            embed_model = self.router.route_and_load(model_name=ctx.model, **llama_kwargs)
        except Exception as e:
            raise RuntimeError(f"[LlamaBridge] Embedding 모델 인스턴스 생성 실패: {e}")

        ## @phase: State Mapping (Context Input -> Text List)
        # Brane(litellm)의 입력은 단일 문자열이거나 문자열의 리스트일 수 있음
        raw_inputs: Union[str, List[str]] = ctx.input
        texts = raw_inputs if isinstance(raw_inputs, list) else [raw_inputs]

        ## @phase: Execution & Boundary Resolution
        # 임베딩은 스트리밍이 없으므로 일괄(Batch) 처리 수행
        if getattr(ctx, "aembedding", False): # 비동기 처리 여부
            embeddings = await embed_model.aget_text_embedding_batch(texts)
        else:
            embeddings = embed_model.get_text_embedding_batch(texts)

        ## @resolve: Extract spatial vectors and inject into Brane's ModelResponse boundary
        # LlamaIndex는 List[List[float]] 형태를 반환하므로, 이를 Brane 표준(OpenAI 호환) 포맷으로 변환
        data_objects = []
        for idx, emb in enumerate(embeddings):
            data_objects.append({
                "object": "embedding",
                "index": idx,
                "embedding": emb
            })

        # ctx.model_response에 데이터 및 메타데이터 세팅
        if hasattr(ctx, "model_response"):
            setattr(ctx.model_response, "data", data_objects)
            setattr(ctx.model_response, "model", ctx.model)
            setattr(ctx.model_response, "object", "list")
            
            # 주의: LlamaIndex 기본 임베딩은 토큰 사용량을 직접 반환하지 않으므로, 
            # 필요한 경우 CallbackManager를 통해 추출하거나 임시(Mock) 데이터를 넣어야 합니다.
            setattr(ctx.model_response, "usage", {"prompt_tokens": -1, "total_tokens": -1})
            
            return ctx.model_response
        else:
            # Context에 response 뼈대가 없다면 직접 딕셔너리 반환
            return {
                "object": "list",
                "data": data_objects,
                "model": ctx.model,
                "usage": {"prompt_tokens": -1, "total_tokens": -1}
            }